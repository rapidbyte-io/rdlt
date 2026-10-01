//! `S-STOP`: a read asked to stop ends promptly and cleanly, a following read of a partition that
//! never ends too, while it waits for data.

use std::collections::BTreeMap;
use std::num::NonZeroUsize;

use tokio_util::sync::CancellationToken;

use super::recording::record;
use super::{START_WINDOW, STOP_WINDOW, plan};
use crate::catalog::{Catalog, StreamSpec};
use crate::cursor::Cursor;
use crate::sink::partition_channel;
use crate::source::{Partition, ReadRequest, Source};
use crate::state::{PartitionState, StreamState};
use crate::testing::{Violation, bounded, bounded_call};

pub(super) async fn stops_are_prompt(
    source: &dyn Source,
    catalog: &Catalog,
) -> Result<(), Violation> {
    for stream in catalog.iter() {
        for (partition, start) in plan(source, stream.name()).await? {
            let (sink, feed) = partition_channel(NonZeroUsize::MIN);
            feed.stop();
            let request = ReadRequest::new(stream.name().clone(), partition.clone(), start);
            let what = format!(
                "a stopped read of {} partition {}",
                stream.name(),
                partition.id()
            );
            bounded(&what, source.read(request, sink))
                .await?
                .map_err(|error| Violation::from(format!("{what}: {error}")))?;
        }
        for (partition, start) in unbounded(source, stream).await? {
            stops_following(source, stream, &partition, start).await?;
        }
    }
    Ok(())
}

/// The unbounded partitions of `stream`, with where their phase starts them: those of its first
/// phase that has any, reached as the engine reaches a phase, each partition of the ones before
/// read to its end and the stream planned again from where they ended.
async fn unbounded(
    source: &dyn Source,
    stream: &StreamSpec,
) -> Result<Vec<(Partition, Option<Cursor>)>, Violation> {
    let name = stream.name();
    let mut state = StreamState::default();
    for walked in 0..PHASES {
        let planned = bounded_call("plan", source.plan(name, &state)).await?;
        let phase = planned.phase.unwrap_or(state.phase);
        if walked > 0 && phase == state.phase {
            break;
        }
        let unbounded: Vec<_> = planned
            .partitions
            .iter()
            .filter(|partition| partition.is_unbounded())
            .map(|partition| {
                (
                    partition.clone(),
                    planned.starts.get(partition.id()).cloned(),
                )
            })
            .collect();
        if !unbounded.is_empty() {
            return Ok(unbounded);
        }
        let mut ended = BTreeMap::new();
        for partition in &planned.partitions {
            let start = planned.starts.get(partition.id()).cloned();
            let read = record(source, stream, partition, start, None).await?;
            let end = match (read.checkpoints.last(), read.tail.is_empty()) {
                (_, false) => Some(PartitionState::Done),
                (Some(last), true) => Some(PartitionState::Cursor(last.clone())),
                (None, true) => None,
            };
            if let Some(end) = end {
                ended.insert(partition.id().clone(), end);
            }
        }
        state = StreamState {
            phase,
            partitions: ended,
            ..StreamState::default()
        };
    }
    Ok(Vec::new())
}

/// How many phases of a stream the clause reads to reach its unbounded partitions.
const PHASES: usize = 4;

/// How many of a following read's events are drained, at most, before it is asked to stop.
const DRAINED: usize = 10_000;

/// How long a following read's events are drained before it is asked to stop, if it never goes
/// quiet.
const FOLLOWED: std::time::Duration = std::time::Duration::from_secs(10);

/// A read following unbounded `partition` of `stream`, asked to stop once caught up, as its
/// quiet shows, ends within [`STOP_WINDOW`], cleanly: waiting for data, it hears the stop.
async fn stops_following(
    source: &dyn Source,
    stream: &StreamSpec,
    partition: &Partition,
    start: Option<Cursor>,
) -> Result<(), Violation> {
    let (sink, mut feed) = partition_channel(NonZeroUsize::new(64).expect("64 is non-zero"));
    let request = ReadRequest::new(stream.name().clone(), partition.clone(), start).following(true);
    let what = format!(
        "a following read of {} partition {} asked to stop",
        stream.name(),
        partition.id()
    );
    // Where the read outlives the stop window, it is given up on: dropped, and the clause failed.
    let gave_up = CancellationToken::new();
    let watch = async {
        let until = tokio::time::Instant::now() + FOLLOWED;
        for _ in 0..DRAINED {
            let deadline = until.min(tokio::time::Instant::now() + START_WINDOW);
            match tokio::time::timeout_at(deadline, feed.recv()).await {
                Ok(Some(_)) => {}
                Ok(None) => return true,
                Err(_) => break,
            }
        }
        feed.stop();
        let drained = async { while feed.recv().await.is_some() {} };
        let ended = tokio::time::timeout(STOP_WINDOW, drained).await.is_ok();
        if !ended {
            gave_up.cancel();
        }
        ended
    };
    let read = async {
        tokio::select! {
            biased;
            () = gave_up.cancelled() => None,
            read = source.read(request, sink) => Some(read),
        }
    };
    match tokio::join!(read, watch) {
        (Some(read), true) => read.map_err(|error| Violation::from(format!("{what}: {error}"))),
        _ => Err(Violation::from(format!(
            "{what} did not end within {STOP_WINDOW:?} of the stop"
        ))),
    }
}
