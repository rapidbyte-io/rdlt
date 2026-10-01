//! One partition's read, recorded: what it pushed, split at its checkpoints.

use std::num::NonZeroUsize;

use bytes::Bytes;

use crate::catalog::StreamSpec;
use crate::cursor::Cursor;
use crate::sink::{Push, SourceEvent, partition_channel};
use crate::source::{Partition, ReadRequest, Source};
use crate::testing::{Violation, bounded};

/// Everything one partition read produced, split at checkpoints.
#[derive(Default)]
pub(super) struct Recording {
    /// Data sealed by each checkpoint, in order.
    pub(super) segments: Vec<Vec<Push>>,
    /// The checkpoint that sealed each segment.
    pub(super) checkpoints: Vec<Cursor>,
    /// Data after the last checkpoint.
    pub(super) tail: Vec<Push>,
    /// Barriers the checkpoints answered.
    pub(super) answered: Vec<u64>,
}

pub(super) async fn record(
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
    let request = ReadRequest::new(stream.name().clone(), partition.clone(), cursor);
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
                SourceEvent::Log { .. }
                | SourceEvent::Metric { .. }
                | SourceEvent::Replan
                | SourceEvent::Behind { .. } => {}
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
pub(in crate::testing) fn normalize(push: Push) -> Push {
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
