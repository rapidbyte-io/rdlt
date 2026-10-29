//! A remote writer's write and flush cut at every point they wait, then the writer used again.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use rdlt_connector::{DestinationWriter as _, SegmentId, WriteStats};
use rdlt_testkit::cancel::{self, Ended};
use rdlt_wire::{Decoder, Limits};

use super::{Connector, deadline_exceeded, decoded, first_value, fixture, keyed, stats};
use crate::remote::write::RemoteWriter;

/// Bytes: what the stream holds each way, less than a frame of a test, so its writes wait part
/// way.
const BUFFERED: usize = 64;

/// How long the connector takes before it takes each frame.
const PACE: Duration = Duration::from_millis(10);

/// Long enough for the connector to take every frame written.
const SETTLE: Duration = Duration::from_secs(1);

/// Bounds every wait of a check.
const WAIT: Duration = Duration::from_secs(60);

/// What the connector took of the writer's frames.
#[derive(Debug, Default)]
struct Took {
    /// The first value of each batch decoded, or why a frame failed to decode.
    batches: Vec<Result<String, String>>,
    /// The rows each flush answered.
    flushes: Vec<u64>,
}

/// A writer, and what the connector it writes to took.
struct Writing {
    writer: RemoteWriter,
    took: Arc<Mutex<Took>>,
    /// The batches written before the write a test cuts.
    before: usize,
}

impl Writing {
    /// What the connector took, at once; never held across an await, which the connector would
    /// wait on.
    fn took<R>(&self, read: impl FnOnce(&Took) -> R) -> R {
        read(&self.took.lock().expect("the record is whole"))
    }
}

/// Takes the writer's frames, `PACE` before each, decoding them as the connector does, and
/// answers each flush with the rows decoded since the last.
async fn connector(mut connector: Connector, took: Arc<Mutex<Took>>) {
    let mut decoder = Decoder::new(Limits::default());
    let mut staged = 0;
    loop {
        tokio::time::sleep(PACE).await;
        let Ok(Some(frame)) = connector.frames.next().await else {
            return;
        };
        match decoded(&mut decoder, frame) {
            None => {
                took.lock()
                    .expect("the record is whole")
                    .flushes
                    .push(staged);
                connector.answer(stats(staged)).await;
                staged = 0;
            }
            Some(Ok(None)) => {}
            Some(Ok(Some(batch))) => {
                staged += u64::try_from(batch.num_rows()).expect("a batch's rows fit");
                let mut took = took.lock().expect("the record is whole");
                took.batches.push(Ok(first_value(&batch)));
            }
            Some(Err(error)) => {
                took.lock()
                    .expect("the record is whole")
                    .batches
                    .push(Err(error));
            }
        }
    }
}

/// A writer whose connector runs beside it.
///
/// Where `older`, a write of an older dictionary of the same id went first.
async fn writing(older: bool) -> Writing {
    let (writer, end) = fixture(BUFFERED);
    let took = Arc::new(Mutex::new(Took::default()));
    tokio::spawn(connector(end, Arc::clone(&took)));
    let mut writing = Writing {
        writer,
        took,
        before: 0,
    };
    if older {
        tokio::time::timeout(WAIT, writing.writer.write(SegmentId(1), keyed("old-alpha")))
            .await
            .expect("the older write ends")
            .expect("the older write goes");
        writing.before = 1;
    }
    writing
}

/// Writes a batch whose dictionary's first value is `alpha`, and flushes it.
async fn write_and_flush(writer: &mut RemoteWriter) -> rdlt_connector::Result<WriteStats> {
    writer.write(SegmentId(1), keyed("alpha")).await?;
    writer.flush().await
}

/// Fails unless the writer refuses a write and a flush at once, as a writer whose flush went
/// unanswered, or whose frame was cut part way, does.
async fn refuses(writer: &mut RemoteWriter) {
    let written = tokio::time::timeout(WAIT, writer.write(SegmentId(2), keyed("alpha")))
        .await
        .expect("the writer refuses at once");
    deadline_exceeded(written);
    let flushed = tokio::time::timeout(WAIT, writer.flush())
        .await
        .expect("the writer refuses at once");
    deadline_exceeded(flushed);
}

/// Holds the writer to its model once the connector has taken every frame written, after a
/// write and flush cut at `ended` or run to their end.
async fn checked(writing: &mut Writing, ended: Ended<rdlt_connector::Result<WriteStats>>) {
    // A frame cut part way: what follows it would be read as its rest.
    let torn = writing
        .writer
        .pending
        .as_ref()
        .is_some_and(|(frame, _)| frame.is_begun());
    tokio::time::sleep(SETTLE).await;
    let stats = match ended {
        Ended::Returned(stats) => Some(stats.expect("the write and the flush go")),
        // A flush taken but unanswered: its stats would be taken for a later flush's.
        Ended::Cancelled { .. } if torn || writing.took(|took| !took.flushes.is_empty()) => {
            refuses(&mut writing.writer).await;
            None
        }
        Ended::Cancelled { at } => {
            let stats = tokio::time::timeout(WAIT, write_and_flush(&mut writing.writer))
                .await
                .unwrap_or_else(|_| panic!("the write retried after wait {at} ends"))
                .unwrap_or_else(|error| panic!("the write retried after wait {at} goes: {error}"));
            tokio::time::sleep(SETTLE).await;
            Some(stats)
        }
    };
    if let Some(stats) = stats {
        let flushed = writing.took(|took| took.flushes.last().copied());
        assert_eq!(
            Some(stats.rows),
            flushed,
            "the flush answers the rows the connector staged"
        );
    }
    let before = writing.before;
    writing.took(|took| {
        assert!(
            took.batches
                .iter()
                .skip(before)
                .all(|batch| batch.as_deref() == Ok("alpha")),
            "each batch decodes as written: {took:?}"
        );
    });
}

#[test]
fn a_write_and_flush_cut_wherever_they_wait_leave_the_writer_whole_or_refusing() {
    for older in [false, true] {
        let points = cancel::at_every_point(
            async || writing(older).await,
            async |writing: &mut Writing| write_and_flush(&mut writing.writer).await,
            async |writing: &mut Writing, ended: Ended<rdlt_connector::Result<WriteStats>>| {
                checked(writing, ended).await;
            },
        );
        // Room in the stream within each frame, the yield between pieces, and the flush's
        // stats.
        assert!(
            points >= 5,
            "{points} waits, after an older dictionary: {older}"
        );
    }
}
