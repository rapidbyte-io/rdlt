//! Source clauses.

mod acks;
mod json;
mod partition;
pub(super) mod recording;
pub(super) mod resume;
mod stop;

use std::collections::BTreeSet;
use std::future::Future;
use std::num::NonZeroUsize;
use std::sync::Arc;

use super::limits::CLAUSE_TIMEOUT;
use super::{
    Clause, ClauseResult, Observed, Outcome, Reason, Report, Violation, bounded_call, outcome,
    timed,
};
use crate::catalog::{Catalog, Checkpointing, StreamSpec};
use crate::cursor::Cursor;
use crate::id::StreamName;
use crate::sink::partition_channel;
use crate::source::{
    ACKNOWLEDGED_CODE, AcknowledgedReader, Partition, PartitionPlan, ReadRequest, Source,
    SourceConnector, SourceFactory, source_factory,
};
use crate::spec::ConnectContext;
use crate::state::{PartitionState, StreamState};
use recording::Budget;

/// The clauses [`certify_source`] checks, in order.
pub const SOURCE_CLAUSES: &[Clause] = &[
    Clause {
        id: "S-CHECK",
        statement: "check succeeds exactly when a read does, and both do for a valid \
                    configuration",
        unless: "",
    },
    Clause {
        id: "S-DISCOVER",
        statement: "discover lists at least one stream and is stable across calls",
        unless: "",
    },
    Clause {
        id: "S-PLAN",
        statement: "every stream plans at least one partition, with distinct ids",
        unless: "",
    },
    Clause {
        id: "S-RESUME",
        statement: "a read resumed from a checkpoint yields exactly the data after it",
        unless: "",
    },
    Clause {
        id: "S-PARTITION",
        statement: "a stream's planned partitions cover it exactly once, and those planned again \
                    from where they stood cover what is left exactly once",
        unless: "every stream plans a partition that never ends",
    },
    Clause {
        id: "S-STOP",
        statement: "a read asked to stop ends promptly and cleanly, a following read of a \
                    partition that never ends too, while it waits for data",
        unless: "",
    },
    Clause {
        id: "S-BARRIER",
        statement: "an on-demand stream answers a pending barrier with a checkpoint",
        unless: "no stream checkpoints on demand",
    },
    Clause {
        id: "S-ACK",
        statement: "a stream's position outside the engine moves only when the engine \
                    tells it a cursor is committed, and then to that cursor",
        unless: "the source does not tell where it stands, or reads no stream as changes or incrementally",
    },
];

/// Certifies source connector `C` with `config`; where it tells where it stands
/// ([`SourceConnector::ACKNOWLEDGES`]), `S-ACK` checks that too.
pub async fn certify_source<C: SourceConnector>(config: serde_json::Value) -> Report {
    certify_source_factory(source_factory::<C>().as_ref(), config).await
}

/// Certifies the source `factory` creates from `config`.
pub async fn certify_source_factory(
    factory: &dyn SourceFactory,
    config: serde_json::Value,
) -> Report {
    certify_source_factory_observed(factory, config, &Observed::new()).await
}

/// Certifies the source `factory` creates from `config`, telling `observed` each clause's
/// result as its check ends: what was found is then known of a certification that is cut.
pub async fn certify_source_factory_observed(
    factory: &dyn SourceFactory,
    config: serde_json::Value,
    observed: &Observed,
) -> Report {
    let connector = factory.spec().id.to_string();
    observed.named(&connector);
    let results = match connected(factory, config).await {
        Ok((source, told)) => check_all(source.as_ref(), told, observed).await,
        Err(violation) => {
            let outcome = violation.of("connect failed").outcome();
            let failed = |clause: &Clause| ClauseResult {
                clause: *clause,
                outcome: outcome.clone(),
            };
            let failed: Vec<ClauseResult> = SOURCE_CLAUSES.iter().map(failed).collect();
            failed
                .iter()
                .cloned()
                .for_each(|failed| observed.tell(failed));
            failed
        }
    };
    Report { connector, results }
}

/// What tells where a source stands, or why asking failed; none where the source does not tell.
type Told = Option<Result<Arc<dyn AcknowledgedReader>, Violation>>;

/// The source `factory` connects with `config`, and what tells where it stands, where it can.
///
/// A source that turns out not to tell is certified as one that says nothing; one whose reader
/// fails to connect is certified in every clause but `S-ACK`, which fails.
async fn connected(
    factory: &dyn SourceFactory,
    config: serde_json::Value,
) -> Result<(Arc<dyn Source>, Told), Violation> {
    let context = ConnectContext::new();
    let mut told = None;
    if factory.acknowledges() {
        let asked = async {
            match factory
                .connect_acknowledging(config.clone(), context.clone())
                .await
            {
                Err(error) if error.code() == Some(ACKNOWLEDGED_CODE) => Ok(None),
                connected => connected.map(Some),
            }
        };
        match bounded_call("connect", asked).await {
            Ok(Some((source, reader))) => return Ok((source, Some(Ok(reader)))),
            Ok(None) => {}
            Err(violation) => {
                told = Some(Err(
                    violation.of("connecting what tells where the source stands")
                ));
            }
        }
    }
    let source = bounded_call("connect", factory.connect(config, context)).await?;
    Ok((Arc::from(source), told))
}

/// Where the source stands before any clause reads it, which `S-ACK`, last, checks: asked within
/// a clause's bound, however many partitions the source plans.
async fn stood(
    source: &dyn Source,
    told: Told,
    catalog: Option<&Catalog>,
) -> Option<Result<acks::Told, Violation>> {
    match (told, catalog) {
        (Some(Ok(reader)), Some(catalog)) => {
            let standing = acks::standing(source, reader.as_ref(), catalog);
            let standing = tokio::time::timeout(CLAUSE_TIMEOUT, standing).await;
            let standing = standing.unwrap_or_else(|_| {
                Err(Violation::from(format_args!(
                    "asking where the stream's partitions stand took longer than \
                     {CLAUSE_TIMEOUT:?}"
                )))
            });
            Some(standing.map(|standing| (reader, standing)))
        }
        (Some(Err(violation)), _) => Some(Err(violation)),
        _ => None,
    }
}

async fn check_all(source: &dyn Source, told: Told, observed: &Observed) -> Vec<ClauseResult> {
    let catalog = bounded_call("discover", source.discover()).await;
    let mut told = stood(source, told, catalog.as_ref().ok()).await;
    let mut results = Vec::new();
    for clause in SOURCE_CLAUSES {
        // What a clause holds of the source's reads, all of them together.
        let budget = Budget::new();
        let outcome = match (&catalog, clause.id) {
            (_, "S-CHECK") => {
                outcome(timed(check_agrees_with_read(source, catalog.as_ref())).await)
            }
            (_, "S-DISCOVER") => {
                outcome(timed(discover_is_stable(source, catalog.as_ref().ok())).await)
            }
            (Err(Violation { reason, .. }), _) => {
                Outcome::Failed(Reason::new(format_args!("discover failed: {reason}")))
            }
            (Ok(catalog), "S-PLAN") => outcome(timed(plans_are_valid(source, catalog)).await),
            (Ok(catalog), "S-RESUME") => {
                // Boxed, as each clause that reads is: its reads' state would otherwise weigh on
                // every certification's future.
                let resumed = Box::pin(resume::resumes_are_exact(source, catalog, &budget));
                outcome(timed(resumed).await)
            }
            (Ok(catalog), "S-PARTITION") => {
                within(Box::pin(partition::partitions_cover_exactly_once(
                    source, catalog, &budget,
                )))
                .await
            }
            (Ok(catalog), "S-STOP") => {
                outcome(timed(Box::pin(stop::stops_are_prompt(source, catalog, &budget))).await)
            }
            (Ok(catalog), "S-ACK") => {
                // Boxed: its reads' state would otherwise weigh on every certification's future.
                within(Box::pin(acks::acknowledged_only_when_committed(
                    source,
                    told.take(),
                    catalog,
                    &budget,
                )))
                .await
            }
            (Ok(catalog), _) => {
                within(Box::pin(barriers_are_answered(source, catalog, &budget))).await
            }
        };
        let result = ClauseResult {
            clause: *clause,
            outcome,
        };
        observed.tell(result.clone());
        results.push(result);
    }
    results
}

/// `clause`'s outcome, or a failure once it takes longer than [`CLAUSE_TIMEOUT`].
async fn within(clause: impl Future<Output = Outcome>) -> Outcome {
    tokio::time::timeout(CLAUSE_TIMEOUT, clause)
        .await
        .unwrap_or_else(|_| {
            let reason = format_args!("the clause took longer than {CLAUSE_TIMEOUT:?}");
            Outcome::Failed(Reason::new(reason))
        })
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

/// The partitions `stream`'s first plan names, each from where the engine starts it.
async fn plan(
    source: &dyn Source,
    stream: &StreamName,
) -> Result<Vec<(Partition, Option<Cursor>)>, Violation> {
    let fresh = StreamState::default();
    bounded_call("plan", source.plan(stream, &fresh))
        .await
        .map(|planned| placed(&planned, &fresh))
        .map_err(|violation| violation.of(format_args!("plan {stream}")))
}

/// The partitions of `plan`, each from where the engine would read it from `state`, which records
/// no partition done: a plan beginning a new phase places them at its starts, and any other
/// resumes each from its recorded cursor, or else its beginning.
fn placed(plan: &PartitionPlan, state: &StreamState) -> Vec<(Partition, Option<Cursor>)> {
    let begins = plan.phase.is_some_and(|phase| phase != state.phase);
    plan.partitions
        .iter()
        .map(|partition| {
            let cursor = match state.partitions.get(partition.id()) {
                _ if begins => plan.starts.get(partition.id()).cloned(),
                Some(PartitionState::Cursor(cursor)) => Some(cursor.clone()),
                Some(PartitionState::Done) | None => None,
            };
            (partition.clone(), cursor)
        })
        .collect()
}

async fn plans_are_valid(source: &dyn Source, catalog: &Catalog) -> Result<(), Violation> {
    for stream in catalog.iter() {
        let partitions: Vec<Partition> = plan(source, stream.name())
            .await?
            .into_iter()
            .map(|(partition, _)| partition)
            .collect();
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

/// Checks the source, and starts a read of its first stream's first partition; they must agree.
async fn check_agrees_with_read(
    source: &dyn Source,
    catalog: Result<&Catalog, &Violation>,
) -> Result<(), Violation> {
    let checked = bounded_call("check", source.check()).await;
    let read = match catalog {
        Ok(catalog) => read_starts(source, catalog).await,
        Err(Violation { reason, .. }) => Err(format!("discover failed: {reason}").into()),
    };
    match (checked, read) {
        (Ok(()), Ok(())) => Ok(()),
        (Err(Violation { reason, .. }), Ok(())) => {
            Err(format!("check failed ({reason}), yet a read succeeded").into())
        }
        (Ok(()), Err(Violation { reason, .. })) => {
            Err(format!("check succeeded, yet a read failed: {reason}").into())
        }
        (Err(violation), Err(_)) => Err(violation.of("check failed")),
    }
}

/// Starts a read of the first partition of `catalog`'s first stream, and stops it once it has
/// sent its first event, or had a moment to: a read of a quiet stream may have nothing to send.
///
/// A read that fails does so as it starts, or as it stops; one still quiet a moment after it was
/// asked to stop started without error, and is dropped.
async fn read_starts(source: &dyn Source, catalog: &Catalog) -> Result<(), Violation> {
    let Some(stream) = catalog.iter().next() else {
        return Ok(());
    };
    let Some((partition, start)) = plan(source, stream.name()).await?.into_iter().next() else {
        return Ok(());
    };
    let (sink, mut feed) = partition_channel(NonZeroUsize::new(64).expect("64 is non-zero"));
    let request = ReadRequest::new(stream.name().clone(), partition.clone(), start);
    let started = async {
        drop(tokio::time::timeout(START_WINDOW, feed.recv()).await);
        feed.stop();
        while feed.recv().await.is_some() {}
    };
    let what = format!("reading {} partition {}", stream.name(), partition.id());
    let reading = async { tokio::join!(source.read(request, sink), started) };
    match tokio::time::timeout(START_WINDOW + STOP_WINDOW, reading).await {
        Ok((read, ())) => read.map_err(|error| Violation::from(format!("{what}: {error}"))),
        Err(_) => Ok(()),
    }
}

/// How long a read has to send its first event before it is asked to stop.
const START_WINDOW: std::time::Duration = std::time::Duration::from_secs(1);

/// How long a read asked to stop has to end, or fail, before it counts as started.
const STOP_WINDOW: std::time::Duration = std::time::Duration::from_secs(5);

async fn barriers_are_answered(source: &dyn Source, catalog: &Catalog, budget: &Budget) -> Outcome {
    let on_demand: Vec<&StreamSpec> = catalog
        .iter()
        .filter(|stream| stream.checkpointing() == Checkpointing::OnDemand)
        .collect();
    if on_demand.is_empty() {
        return Outcome::Inapplicable("no stream checkpoints on demand".into());
    }
    let check = async {
        for stream in on_demand {
            for (partition, start) in plan(source, stream.name()).await? {
                let read = (stream, &partition);
                let recording = recording::record(source, read, start, Some(1), budget).await?;
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
