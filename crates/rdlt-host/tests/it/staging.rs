//! What a write stages between two flushes is bounded on both ends: a served destination refuses
//! more than its limit, and the host's writer flushes before it would send more.

use std::sync::Arc;
use std::time::Duration;

use arrow_array::RecordBatch;
use rdlt_connector::serve::Served;
use rdlt_connector::wire::v1;
use rdlt_connector::{
    ConnectorError, Destination as _, LoadId, OpenContext, PipelineId, Role, SegmentId,
    destination_factory,
};
use rdlt_connector_reference::MemoryDestination;
use rdlt_host::remote::RawCall;
use rdlt_host::{Connection, Options, RemoteDestination};
use rdlt_wire::limits::MIN_FRAME_BYTES;
use rdlt_wire::stream::{Call, Kind, Outgoing, Rows};
use rdlt_wire::{Cut, Encoder, Limits};

use crate::sessions::{raw_session_within, table};
use crate::support::connectors::{Gate, Writes, Writing};
use crate::support::served_within;

/// A destination served from memory.
fn memory() -> Served {
    Served::new().with_destination(destination_factory::<MemoryDestination>())
}

/// Limits under which a write may stage four frames of the least bytes a peer may ask for
/// between flushes: sixteen mebibytes.
fn limits() -> Limits {
    Limits {
        frame_bytes: MIN_FRAME_BYTES,
        ..Limits::default()
    }
}

/// Rows: the ids of a frame of a mebibyte.
const MEBIBYTE: i64 = 131_072;

fn ids(count: i64) -> RecordBatch {
    let values: Vec<i64> = (0..count).collect();
    RecordBatch::try_from_iter([("id", Arc::new(arrow_array::Int64Array::from(values)) as _)])
        .expect("a valid batch")
}

/// A write's frames: its schema, then `batches` frames of a mebibyte of ids, a flush after every
/// `flush_every` of them.
fn frames(batches: usize, flush_every: usize) -> Vec<Outgoing> {
    use v1::write_frame::Frame;
    let batch = ids(MEBIBYTE);
    let mut encoder = Encoder::default();
    let message = |frame| {
        Outgoing::message(None, &v1::WriteFrame { frame: Some(frame) }).expect("a message fits")
    };
    let mut sent = vec![message(Frame::Schema(v1::WriteSchema {
        version: 1,
        ipc_schema: encoder.schema(&batch.schema()).expect("the schema encodes"),
    }))];
    for index in 1..=batches {
        let mut cut = Cut::new(batch.clone(), limits());
        while let Some(pieces) = encoder.piece(&mut cut).expect("the batch encodes") {
            for piece in pieces {
                let kind = Kind::Batch {
                    rows: Rows::Arrow,
                    dictionary: piece.is_dictionary(),
                    tag: 1,
                };
                sent.push(Outgoing::new(kind, piece.into_pieces()).expect("a frame fits"));
            }
        }
        if index % flush_every == 0 {
            sent.push(message(Frame::Flush(v1::Unit {})));
        }
    }
    sent
}

/// What a raw write to `served` of `batches` frames of a mebibyte of ids answers with, a flush
/// sent after every `flush_every` of them: the error it ended with, if any, and how many flushes
/// it answered.
async fn raw_write(
    served: Served,
    store: &str,
    batches: usize,
    flush_every: usize,
) -> (Option<ConnectorError>, usize) {
    let (client, session) = raw_session_within(served, store, limits()).await;
    let start = v1::WriteStart {
        session,
        table: Some(v1::TableRef::from(&table())),
    };
    let mut write = RawCall::open(&client, Call::Write, &start, &Limits::default())
        .await
        .expect("the stream opens")
        .expect("the write opens");
    for frame in frames(batches, flush_every) {
        // A refused write takes nothing more.
        if !write.send(frame).await {
            break;
        }
    }
    // The write's answers, up to its refusal or the stats of its last flush.
    let (mut error, mut flushes) = (None, 0);
    let expected = batches / flush_every;
    while let Some(answer) = tokio::time::timeout(Duration::from_secs(30), write.next())
        .await
        .expect("the write answers")
        .expect("the write's answers arrive")
    {
        let ack = write.write_ack(&answer).expect("an answer decodes");
        match ack.ack {
            Some(v1::write_ack::Ack::Error(refused)) => {
                error = Some(refused);
                break;
            }
            Some(v1::write_ack::Ack::Flushed(_)) => flushes += 1,
            None => {}
        }
        if flushes == expected {
            break;
        }
    }
    let error = error.map(|error| ConnectorError::try_from(error).expect("a connector error"));
    (error, flushes)
}

#[tokio::test]
async fn a_served_write_refuses_more_than_it_may_stage_between_flushes() {
    // Forty frames of a mebibyte pass the sixteen a write may stage.
    let (error, flushes) = raw_write(memory(), "staged_refused", 40, usize::MAX).await;
    let error = error.expect("the write is refused");
    assert_eq!(error.code(), Some("limit_exceeded"), "{error}");
    let limit = error.limit().expect("a refusal names its limit");
    assert_eq!(
        (limit.name, limit.limit),
        ("staged bytes", 4 * MIN_FRAME_BYTES)
    );
    assert!(limit.actual > limit.limit);
    assert_eq!(flushes, 0);
}

#[tokio::test]
async fn a_served_write_that_flushes_in_time_stages_as_much_as_it_likes() {
    let (error, flushes) = raw_write(memory(), "staged_flushed", 40, 8).await;
    assert!(error.is_none(), "{error:?}");
    assert_eq!(flushes, 5);
}

#[tokio::test]
async fn a_served_write_whose_writer_falls_behind_refuses_the_same_frame() {
    // The writer takes two frames and then waits, while frames decode ahead of it.
    static GATE: Gate = Gate::after(2);
    let (prompt, _) = raw_write(memory(), "staged_prompt", 40, usize::MAX).await;
    let opening = tokio::spawn(async {
        tokio::time::sleep(Duration::from_millis(200)).await;
        GATE.open();
    });
    let behind = Served::new().with_destination(Writes::factory(Writing::Gated(&GATE)));
    let (behind, flushes) = raw_write(behind, "staged_behind", 40, usize::MAX).await;
    opening.await.expect("the gate opens");
    let (prompt, behind) = (prompt.expect("refused"), behind.expect("refused"));
    assert_eq!(behind.code(), Some("limit_exceeded"), "{behind}");
    assert_eq!(behind.limit(), prompt.limit());
    assert_eq!(flushes, 0);
    // Every frame before the refused frame is written, and none after it: fifteen frames of a
    // mebibyte and their headers fit the sixteen mebibytes, and a sixteenth does not.
    let kept = GATE.kept.lock().expect("the lock is not poisoned");
    assert_eq!(kept.len(), 15);
}

#[tokio::test]
async fn the_hosts_writer_flushes_before_it_stages_more_than_the_destination_holds() {
    // Frames of a mebibyte; and frames of nearly the frame limit, few of which fill what the
    // destination may stage.
    let near = i64::try_from(MIN_FRAME_BYTES * 15 / 16 / 8).expect("fits");
    for (store, rows, writes) in [("staged_host", MEBIBYTE, 40), ("staged_spare", near, 10)] {
        let served = Served::new().with_destination(destination_factory::<MemoryDestination>());
        let io = served_within(served, limits());
        let config = serde_json::json!({ "store": store });
        let connection = Connection::connect(io, Role::Destination, &config, Options::default())
            .await
            .expect("the destination handshakes");
        let destination =
            RemoteDestination::new(connection).expect("its capabilities are declared");
        let context = OpenContext {
            pipeline: PipelineId::parse("staging").expect("a valid pipeline id"),
            load_id: LoadId::from_parts(std::time::UNIX_EPOCH, 1),
        };
        let mut opened = destination.open(&context).await.expect("the session opens");
        let mut writer = opened
            .session
            .writer(&table())
            .await
            .expect("the writer opens");
        // Writes with no flush asked for: the writer flushes as the destination's limit nears.
        for _ in 0..writes {
            writer
                .write(SegmentId(1), ids(rows))
                .await
                .expect("the write is staged");
        }
        let stats = writer.flush().await.expect("the flush answers");
        // The stats of the flushes the writer made itself are in those of the flush asked for.
        assert_eq!(stats.rows, writes * rows.unsigned_abs(), "{store}");
    }
}
