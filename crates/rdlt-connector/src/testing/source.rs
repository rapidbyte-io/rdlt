//! Source clauses.

use std::collections::BTreeSet;
use std::num::NonZeroUsize;

use bytes::Bytes;

use super::{Clause, ClauseResult, Outcome, Report, Violation, bounded, bounded_call, outcome};
use crate::catalog::{Catalog, Checkpointing, StreamSpec};
use crate::cursor::Cursor;
use crate::id::StreamName;
use crate::sink::{Push, SourceEvent, partition_channel};
use crate::source::{
    Partition, ReadRequest, Source, SourceConnector, SourceFactory, source_factory,
};
use crate::spec::ConnectContext;
use crate::state::StreamState;

/// Resumes are checked from at most this many checkpoints per partition.
const RESUME_SAMPLES: usize = 5;

/// The clauses [`certify_source`] checks, in order.
pub const SOURCE_CLAUSES: &[Clause] = &[
    Clause {
        id: "S-CHECK",
        statement: "check succeeds for a valid configuration",
    },
    Clause {
        id: "S-DISCOVER",
        statement: "discover lists at least one stream and is stable across calls",
    },
    Clause {
        id: "S-PLAN",
        statement: "every stream plans at least one partition, with distinct ids",
    },
    Clause {
        id: "S-RESUME",
        statement: "a read resumed from a checkpoint yields exactly the data after it",
    },
    Clause {
        id: "S-STOP",
        statement: "a read asked to stop ends promptly and cleanly",
    },
    Clause {
        id: "S-BARRIER",
        statement: "an on-demand stream answers a pending barrier with a checkpoint",
    },
];

/// Certifies source connector `C` with `config`.
pub async fn certify_source<C: SourceConnector>(config: serde_json::Value) -> Report {
    certify_source_factory(source_factory::<C>().as_ref(), config).await
}

/// Certifies the source `factory` creates from `config`.
pub async fn certify_source_factory(
    factory: &dyn SourceFactory,
    config: serde_json::Value,
) -> Report {
    let connector = factory.spec().id.to_string();
    let results =
        match bounded_call("connect", factory.connect(config, ConnectContext::new())).await {
            Ok(source) => check_all(source.as_ref()).await,
            Err(Violation(reason)) => SOURCE_CLAUSES
                .iter()
                .map(|clause| ClauseResult {
                    clause: *clause,
                    outcome: Outcome::Failed(format!("connect failed: {reason}")),
                })
                .collect(),
        };
    Report { connector, results }
}

async fn check_all(source: &dyn Source) -> Vec<ClauseResult> {
    let catalog = bounded_call("discover", source.discover()).await;
    let mut results = Vec::new();
    for clause in SOURCE_CLAUSES {
        let outcome = match (&catalog, clause.id) {
            (_, "S-CHECK") => outcome(bounded_call("check", source.check()).await),
            (_, "S-DISCOVER") => outcome(discover_is_stable(source, catalog.as_ref().ok()).await),
            (Err(Violation(reason)), _) => Outcome::Failed(format!("discover failed: {reason}")),
            (Ok(catalog), "S-PLAN") => outcome(plans_are_valid(source, catalog).await),
            (Ok(catalog), "S-RESUME") => outcome(resumes_are_exact(source, catalog).await),
            (Ok(catalog), "S-STOP") => outcome(stops_are_prompt(source, catalog).await),
            (Ok(catalog), _) => barriers_are_answered(source, catalog).await,
        };
        results.push(ClauseResult {
            clause: *clause,
            outcome,
        });
    }
    results
}

async fn discover_is_stable(source: &dyn Source, first: Option<&Catalog>) -> Result<(), Violation> {
    let first = first.ok_or("discover failed")?;
    if first.is_empty() {
        return Err("the catalog is empty".into());
    }
    let second = bounded_call("discover", source.discover()).await?;
    if &second != first {
        return Err("two discovers returned different catalogs".into());
    }
    Ok(())
}

async fn plan(source: &dyn Source, stream: &StreamName) -> Result<Vec<Partition>, Violation> {
    bounded_call("plan", source.plan(stream, &StreamState::default()))
        .await
        .map_err(|Violation(reason)| Violation::from(format!("plan {stream}: {reason}")))
}

async fn plans_are_valid(source: &dyn Source, catalog: &Catalog) -> Result<(), Violation> {
    for stream in catalog.iter() {
        let partitions = plan(source, stream.name()).await?;
        if partitions.is_empty() {
            return Err(format!("stream {} planned no partitions", stream.name()).into());
        }
        let distinct: BTreeSet<_> = partitions.iter().map(Partition::id).collect();
        if distinct.len() != partitions.len() {
            return Err(format!("stream {} planned repeated partition ids", stream.name()).into());
        }
    }
    Ok(())
}

/// Everything one partition read produced, split at checkpoints.
#[derive(Default)]
struct Recording {
    /// Data sealed by each checkpoint, in order.
    segments: Vec<Vec<Push>>,
    /// The checkpoint that sealed each segment.
    checkpoints: Vec<Cursor>,
    /// Data after the last checkpoint.
    tail: Vec<Push>,
    /// Barriers the checkpoints answered.
    answered: Vec<u64>,
}

async fn record(
    source: &dyn Source,
    stream: &StreamSpec,
    partition: &Partition,
    cursor: Option<Cursor>,
    barrier: Option<u64>,
) -> Result<Recording, Violation> {
    let (sink, mut feed) = partition_channel(NonZeroUsize::new(64).expect("64 is non-zero"));
    if let Some(barrier) = barrier {
        feed.request_checkpoint(barrier);
    }
    let request = ReadRequest {
        stream: stream.name().clone(),
        partition: partition.clone(),
        cursor,
    };
    let collect = async {
        let mut recording = Recording::default();
        while let Some(event) = feed.recv().await {
            match event {
                SourceEvent::Push(push) => recording.tail.push(normalize(push)),
                SourceEvent::Checkpoint { cursor, answers } => {
                    recording.segments.push(std::mem::take(&mut recording.tail));
                    recording.checkpoints.push(cursor);
                    recording.answered.extend(answers);
                }
                SourceEvent::Log { .. } | SourceEvent::Metric { .. } => {}
            }
        }
        recording
    };
    let what = format!("reading {} partition {}", stream.name(), partition.id());
    let (read, recording) = bounded(&what, async {
        tokio::join!(source.read(request, sink), collect)
    })
    .await?;
    read.map_err(|error| Violation::from(format!("{what}: {error}")))?;
    Ok(recording)
}

/// Rewrites JSON pushes canonically, so equal rows compare equal whatever their formatting.
pub(super) fn normalize(push: Push) -> Push {
    match push {
        Push::Json(bytes) => {
            let rows: Vec<serde_json::Value> =
                match serde_json::from_slice::<serde_json::Value>(&bytes) {
                    Ok(serde_json::Value::Array(rows)) => rows,
                    _ => serde_json::Deserializer::from_slice(&bytes)
                        .into_iter()
                        .filter_map(Result::ok)
                        .collect(),
                };
            Push::Json(Bytes::from(
                serde_json::to_vec(&rows).expect("JSON values serialize"),
            ))
        }
        other => other,
    }
}

async fn resumes_are_exact(source: &dyn Source, catalog: &Catalog) -> Result<(), Violation> {
    for stream in catalog.iter() {
        for partition in plan(source, stream.name()).await? {
            let full = record(source, stream, &partition, None, None).await?;
            for (index, cursor) in full.checkpoints.iter().enumerate().take(RESUME_SAMPLES) {
                let resumed =
                    record(source, stream, &partition, Some(cursor.clone()), None).await?;
                let expected: Vec<&Push> = full.segments[index + 1..]
                    .iter()
                    .flatten()
                    .chain(&full.tail)
                    .collect();
                let actual: Vec<&Push> = resumed
                    .segments
                    .iter()
                    .flatten()
                    .chain(&resumed.tail)
                    .collect();
                if expected != actual {
                    return Err(format!(
                        "stream {} partition {}: resuming from checkpoint {} yielded {} pushes, expected {}",
                        stream.name(),
                        partition.id(),
                        index + 1,
                        actual.len(),
                        expected.len()
                    ).into());
                }
            }
        }
    }
    Ok(())
}

async fn stops_are_prompt(source: &dyn Source, catalog: &Catalog) -> Result<(), Violation> {
    for stream in catalog.iter() {
        for partition in plan(source, stream.name()).await? {
            let (sink, feed) = partition_channel(NonZeroUsize::MIN);
            feed.stop();
            let request = ReadRequest {
                stream: stream.name().clone(),
                partition: partition.clone(),
                cursor: None,
            };
            let what = format!(
                "a stopped read of {} partition {}",
                stream.name(),
                partition.id()
            );
            bounded(&what, source.read(request, sink))
                .await?
                .map_err(|error| Violation::from(format!("{what}: {error}")))?;
        }
    }
    Ok(())
}

async fn barriers_are_answered(source: &dyn Source, catalog: &Catalog) -> Outcome {
    let on_demand: Vec<&StreamSpec> = catalog
        .iter()
        .filter(|stream| stream.checkpointing() == Checkpointing::OnDemand)
        .collect();
    if on_demand.is_empty() {
        return Outcome::Skipped("no stream checkpoints on demand".to_owned());
    }
    let check = async {
        for stream in on_demand {
            for partition in plan(source, stream.name()).await? {
                let recording = record(source, stream, &partition, None, Some(1)).await?;
                let pushed = recording
                    .segments
                    .iter()
                    .flatten()
                    .chain(&recording.tail)
                    .next()
                    .is_some();
                if pushed && !recording.answered.contains(&1) {
                    return Err(format!(
                        "stream {} partition {} never answered barrier 1",
                        stream.name(),
                        partition.id()
                    )
                    .into());
                }
            }
        }
        Ok(())
    };
    outcome(check.await)
}
