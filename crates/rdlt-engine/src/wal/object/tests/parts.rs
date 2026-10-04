//! Chunks longer than a part: uploaded in parts as they are staged, published by a head naming
//! their body, and read, removed and given up as a whole.

use std::io;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use object_store::memory::InMemory;
use object_store::{ObjectStoreExt as _, PutPayload};
use rdlt_testkit::objects::{Fault, Op, faultless};

use super::{always, chunk, judged, keys, objects, opened, options, pipeline, tries};
use crate::env::SystemClock;
use crate::wal::object::calls::Calls;
use crate::wal::object::head::REFERENCE;
use crate::wal::object::keys::{Name, parse_name};
use crate::wal::{StagedChunk, WalStore};

/// Stages `bytes` as `chunk` of `pipeline`'s log in `wal`, three bytes an append, opening the log
/// where it is not open.
async fn staged(
    wal: &dyn WalStore,
    pipeline: &rdlt_connector::PipelineId,
    chunk: crate::wal::Chunk,
    bytes: &'static [u8],
) -> Box<dyn StagedChunk> {
    match wal.open_log(pipeline, chunk.load).await {
        Err(error) if error.kind() != io::ErrorKind::AlreadyExists => panic!("opens: {error}"),
        _ => {}
    }
    let mut staged = wal.stage(pipeline, chunk).await.expect("stages");
    for piece in bytes.chunks(3) {
        staged
            .append(Bytes::from_static(piece))
            .await
            .expect("appends");
    }
    staged
}

const LONG: &[u8] = b"0123456789abcdef!";

fn ops(objects: &rdlt_testkit::objects::Faulty<InMemory>) -> Vec<Op> {
    objects.calls().into_iter().map(|call| call.op).collect()
}

#[tokio::test]
async fn a_chunk_longer_than_a_part_is_uploaded_in_parts_and_read_back_whole_and_by_range() {
    let objects = objects(faultless());
    let wal = opened(&objects, options(4)).await;
    let orders = pipeline("parts");
    let at = chunk(1, 0);
    let before = objects.calls().len();
    staged(&wal, &orders, at, LONG)
        .await
        .publish()
        .await
        .expect("publishes");
    let made = &ops(&objects)[before..];
    assert_eq!(
        made.iter().filter(|op| **op == Op::Begin).count(),
        1,
        "{made:?}"
    );
    assert_eq!(
        made.iter().filter(|op| **op == Op::Part).count(),
        5,
        "{made:?}"
    );
    assert_eq!(
        made.iter().filter(|op| **op == Op::Complete).count(),
        1,
        "{made:?}"
    );
    assert_eq!(
        wal.chunks(&orders, at.load).await.expect("lists"),
        [(0, 17)]
    );
    let head = objects
        .inner()
        .head(&wal.shared.keys.head(&orders, at))
        .await
        .expect("a head");
    assert_eq!(head.size, REFERENCE as u64, "the head names the body");
    assert_eq!(
        &wal.read(&orders, at, 0, 17).await.expect("reads")[..],
        LONG
    );
    assert_eq!(
        &wal.read(&orders, at, 3, 6).await.expect("reads")[..],
        b"345678"
    );
    assert_eq!(
        &wal.read(&orders, at, 15, 100).await.expect("reads")[..],
        b"f!"
    );
    assert!(
        wal.read(&orders, at, 17, 4)
            .await
            .expect("reads")
            .is_empty()
    );
    assert!(
        wal.read(&orders, at, 40, 4)
            .await
            .expect("reads")
            .is_empty()
    );
}

#[tokio::test]
async fn a_chunk_of_a_part_or_less_is_published_by_one_put() {
    let objects = objects(faultless());
    let wal = opened(&objects, options(4)).await;
    let orders = pipeline("whole");
    let before = objects.calls().len();
    staged(&wal, &orders, chunk(1, 0), b"abcd")
        .await
        .publish()
        .await
        .expect("publishes");
    assert!(!ops(&objects)[before..].contains(&Op::Begin));
    assert_eq!(
        wal.chunks(&orders, chunk(1, 0).load).await.expect("lists"),
        [(0, 4)]
    );
}

#[tokio::test]
async fn a_chunk_in_parts_that_finds_its_name_taken_deletes_its_body() {
    let objects = objects(faultless());
    let wal = opened(&objects, options(4)).await;
    let orders = pipeline("fenced");
    let at = chunk(1, 2);
    let writer = staged(&wal, &orders, at, LONG).await;
    staged(&wal, &orders, at, b"fen")
        .await
        .publish()
        .await
        .expect("the fence lands first");
    let refused = writer.publish().await.expect_err("fenced");
    assert_eq!(refused.kind(), io::ErrorKind::AlreadyExists);
    let left = keys(&objects).await;
    let body = |key: &&String| {
        let name = key.rsplit('/').next().unwrap_or_default();
        matches!(parse_name(name), Some(Name::Body(..)))
    };
    assert_eq!(left.iter().filter(body).count(), 0, "{left:?}");
    assert_eq!(
        &wal.read(&orders, at, 0, 10).await.expect("reads")[..],
        b"fen"
    );
}

#[tokio::test]
async fn removing_a_chunk_in_parts_deletes_its_head_and_its_body() {
    let objects = objects(faultless());
    let wal = opened(&objects, options(4)).await;
    let orders = pipeline("removed");
    for number in [0, 1] {
        staged(&wal, &orders, chunk(1, number), LONG)
            .await
            .publish()
            .await
            .expect("publishes");
    }
    wal.remove(&orders, chunk(1, 0)).await.expect("removes");
    let left = keys(&objects).await;
    assert!(
        left.iter().all(|key| !key.contains("/00000000.")),
        "{left:?}"
    );
    assert_eq!(
        wal.chunks(&orders, chunk(1, 0).load).await.expect("lists"),
        [(1, 17)]
    );
    assert!(wal.read(&orders, chunk(1, 0), 0, 4).await.is_err());
}

#[tokio::test(start_paused = true)]
async fn a_part_failing_every_attempt_fails_its_append_and_gives_up_the_upload() {
    let objects = objects(faultless());
    let wal = opened(
        &objects,
        tries(2, Duration::from_secs(1))
            .with_part_bytes(std::num::NonZeroUsize::new(4).expect("four")),
    )
    .await;
    let orders = pipeline("failed");
    wal.open_log(&orders, chunk(1, 0).load)
        .await
        .expect("opens");
    let mut staged = wal.stage(&orders, chunk(1, 0)).await.expect("stages");
    objects.plan(always(Fault::Fail, |call| call.op == Op::Part));
    let refused = staged
        .append(Bytes::from_static(LONG))
        .await
        .expect_err("fails");
    assert_eq!(
        judged(refused),
        (Some("wal_storage_unavailable".to_owned()), true)
    );
    assert_eq!(ops(&objects).last(), Some(&Op::Abort));
}

#[tokio::test]
async fn a_staging_given_up_or_deleted_gives_up_its_upload() {
    let objects = objects(faultless());
    let wal = opened(&objects, options(4)).await;
    let orders = pipeline("given-up");
    let discarded = staged(&wal, &orders, chunk(1, 0), LONG).await;
    discarded.discard().await.expect("discards");
    assert_eq!(ops(&objects).last(), Some(&Op::Abort));
    let deleted = staged(&wal, &orders, chunk(1, 0), LONG).await;
    wal.remove_staged(&orders, chunk(1, 0).load)
        .await
        .expect("deletes");
    let refused = deleted.publish().await.expect_err("deleted");
    assert_eq!(refused.kind(), io::ErrorKind::NotFound);
    assert_eq!(ops(&objects).last(), Some(&Op::Abort));
    assert_eq!(
        wal.chunks(&orders, chunk(1, 0).load).await.expect("lists"),
        []
    );
}

#[tokio::test(start_paused = true)]
async fn a_completion_whose_answer_was_lost_is_known_by_the_body_it_left() {
    let objects = objects(faultless());
    let wal = opened(&objects, options(4)).await;
    let orders = pipeline("completed");
    let staged = staged(&wal, &orders, chunk(1, 0), LONG).await;
    let mut lost = true;
    objects.plan(Box::new(move |call| {
        if call.op == Op::Complete && std::mem::take(&mut lost) {
            Fault::Answerless
        } else {
            Fault::None
        }
    }));
    staged.publish().await.expect("its own body");
    assert_eq!(
        &wal.read(&orders, chunk(1, 0), 0, 17).await.expect("reads")[..],
        LONG
    );
}

#[tokio::test(start_paused = true)]
async fn a_completion_that_fails_where_no_body_of_its_length_is_fails() {
    let inner = Arc::new(InMemory::new());
    let calls = Calls {
        objects: Arc::clone(&inner) as _,
        clock: Arc::new(SystemClock),
        options: tries(1, Duration::from_secs(1)),
    };
    let key = "logs/body".into();
    let id = "unknown".to_owned();
    let missing = calls.complete(&key, &id, Vec::new(), 4).await;
    assert!(missing.is_err(), "no upload and no body");
    inner
        .put(&key, PutPayload::from_static(b"abc"))
        .await
        .expect("puts");
    let other = calls.complete(&key, &id, Vec::new(), 4).await;
    assert!(
        other.is_err(),
        "a body of another length is not this upload's"
    );
    assert!(calls.complete(&key, &id, Vec::new(), 3).await.is_ok());
}

#[tokio::test]
async fn a_chunk_holds_as_many_parts_as_an_upload_may_and_the_engine_is_told_half() {
    let objects = objects(faultless());
    let wal = opened(&objects, options(1)).await;
    // Half for the batches the engine's bound counts, half for the frames it does not.
    assert_eq!(wal.chunk_bytes().map(std::num::NonZero::get), Some(5_000));
    let orders = pipeline("bounded");
    wal.open_log(&orders, chunk(1, 0).load)
        .await
        .expect("opens");
    let mut staged = wal.stage(&orders, chunk(1, 0)).await.expect("stages");
    staged
        .append(Bytes::from(vec![7; 9_999]))
        .await
        .expect("within the parts");
    staged
        .append(Bytes::from_static(b"!"))
        .await
        .expect("at the last part");
    let refused = staged
        .append(Bytes::from_static(b"!"))
        .await
        .expect_err("past it");
    assert_eq!(
        judged(refused),
        (Some("wal_storage_unsupported".to_owned()), false)
    );
}

#[tokio::test(start_paused = true)]
async fn a_staging_given_up_where_the_store_never_answers_ends_at_its_deadlines() {
    let objects = objects(faultless());
    let deadline = Duration::from_secs(3);
    let wal = opened(
        &objects,
        tries(2, deadline).with_part_bytes(std::num::NonZeroUsize::new(4).expect("four")),
    )
    .await;
    let orders = pipeline("abandoned");
    let staged = staged(&wal, &orders, chunk(1, 0), LONG).await;
    objects.plan(always(Fault::Hang, |call| call.op == Op::Abort));
    let limit = crate::env::Clock::sleep(&SystemClock, deadline * 3);
    tokio::select! {
        biased;
        discarded = staged.discard() => discarded.expect("discards"),
        () = limit => panic!("the upload is given up within its attempts' deadlines"),
    }
    assert_eq!(
        ops(&objects).iter().filter(|op| **op == Op::Abort).count(),
        2
    );
}
