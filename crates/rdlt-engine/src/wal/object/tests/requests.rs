//! How each request is tried: how many times, within what deadline, with what waits between, and
//! what its last failure says.

use std::io;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use object_store::memory::InMemory;
use object_store::{ObjectStoreExt as _, PutPayload};
use parking_lot::Mutex;
use rdlt_testkit::objects::{Call, Fault, Op, faultless};

use rdlt_connector::PipelineId;

use super::{always, any, chunk, every, judged, objects, opened, options, pipeline, tries};
use crate::env::{Clock, Sleep};
use crate::wal::object::ObjectStoreWal;
use crate::wal::object::calls::Calls;
use crate::wal::{StagedChunk, WalStore};

/// A clock that sleeps on tokio's clock and draws `random`, in turn.
#[derive(Debug)]
struct Drawn {
    random: Mutex<Vec<u64>>,
}

impl Clock for Drawn {
    #[expect(
        clippy::disallowed_methods,
        reason = "the test's clock is tokio's paused one"
    )]
    fn sleep(&self, duration: Duration) -> Sleep {
        Box::pin(tokio::time::sleep(duration))
    }

    fn random(&self) -> u64 {
        self.random.lock().remove(0)
    }
}

fn calls(random: Vec<u64>) -> Calls {
    Calls {
        objects: Arc::new(InMemory::new()),
        clock: Arc::new(Drawn {
            random: Mutex::new(random),
        }),
        options: options(1 << 20).with_backoff(Duration::from_millis(100), Duration::from_secs(1)),
    }
}

#[test]
fn each_wait_is_drawn_up_to_twice_the_last_and_never_past_the_longest() {
    let ms = |ms: u64| ms * 1_000_000;
    // Each draw is taken modulo one more than the longest wait it may be.
    let draws = vec![
        ms(100),
        ms(200),
        ms(400),
        ms(800),
        ms(1_000),
        ms(1_000),
        ms(1_000) + 1,
        7,
    ];
    let calls = calls(draws);
    let waits: Vec<Duration> = [1, 2, 3, 4, 5, 6, 40, 40]
        .into_iter()
        .map(|failed| calls.backoff(failed))
        .collect();
    let millis = Duration::from_millis;
    assert_eq!(
        waits,
        [
            millis(100),
            millis(200),
            millis(400),
            millis(800),
            millis(1_000),
            millis(1_000),
            Duration::ZERO,
            Duration::from_nanos(7),
        ]
    );
}

#[test]
fn an_attempt_s_deadline_grows_by_the_mib_it_moves() {
    let calls = Calls {
        options: options(1 << 20).with_deadline(Duration::from_secs(30), Duration::from_secs(2)),
        ..calls(Vec::new())
    };
    assert_eq!(calls.deadline(0), Duration::from_secs(30));
    assert_eq!(calls.deadline(1), Duration::from_secs(32));
    assert_eq!(calls.deadline(1 << 20), Duration::from_secs(32));
    assert_eq!(calls.deadline((1 << 20) + 1), Duration::from_secs(34));
    // No object holds more than a chunk of ten thousand parts, here of a MiB each.
    let most = Duration::from_secs(30) + Duration::from_secs(2) * 10_000;
    assert_eq!(calls.deadline(u64::MAX), most);
    assert_eq!(calls.deadline(10_000 << 20), most);
    assert_eq!(calls.deadline((10_000 << 20) + 1), most);
    assert_eq!(calls.deadline((9_999 << 20) + 1), most);
    assert_eq!(
        calls.deadline(9_999 << 20),
        Duration::from_secs(30 + 2 * 9_999)
    );
}

fn puts(call: &Call) -> bool {
    matches!(call.op, Op::Put { .. })
}

fn creates(call: &Call) -> bool {
    call.op == (Op::Put { create: true })
}

/// Chunk 0 of a log of `orders` opened in `wal`, staged and holding five bytes, to be published.
async fn publishing(wal: &ObjectStoreWal, orders: &PipelineId) -> Box<dyn StagedChunk> {
    wal.open_log(orders, chunk(1, 0).load).await.expect("opens");
    let mut staged = wal.stage(orders, chunk(1, 0)).await.expect("stages");
    staged
        .append(Bytes::from_static(b"chunk"))
        .await
        .expect("appends");
    staged
}

#[tokio::test(start_paused = true)]
async fn a_request_failing_every_attempt_is_unavailable_retryably_after_its_attempts() {
    let objects = objects(faultless());
    let wal = opened(&objects, tries(3, Duration::from_secs(1))).await;
    let staged = publishing(&wal, &pipeline("unavailable")).await;
    objects.plan(always(Fault::Fail, puts));
    let before = objects.calls().len();
    let refused = staged.publish().await.expect_err("fails");
    let made: Vec<_> = objects.calls()[before..].to_vec();
    assert_eq!(made.iter().filter(|call| puts(call)).count(), 3, "{made:?}");
    let (code, retryable) = judged(refused);
    assert_eq!(code.as_deref(), Some("wal_storage_unavailable"));
    assert!(retryable);
}

#[tokio::test(start_paused = true)]
async fn a_request_that_never_answers_is_given_up_at_each_deadline() {
    let objects = objects(faultless());
    let deadline = Duration::from_secs(10);
    let wal = opened(
        &objects,
        tries(2, deadline).with_backoff(Duration::ZERO, Duration::ZERO),
    )
    .await;
    objects.plan(always(Fault::Hang, any));
    let env = crate::env::SystemEnv::one_core();
    let start = crate::env::Env::instant(&env);
    let refused = wal
        .loads(&pipeline("hung"))
        .await
        .expect_err("never answered");
    let elapsed = crate::env::Env::instant(&env) - start;
    assert_eq!(elapsed, deadline * 2, "two attempts, each its deadline");
    let (code, retryable) = judged(refused);
    assert_eq!(code.as_deref(), Some("wal_storage_unavailable"));
    assert!(retryable);
}

#[tokio::test(start_paused = true)]
async fn a_failure_before_the_last_attempt_is_tried_again_and_succeeds() {
    let objects = objects(faultless());
    let wal = opened(&objects, tries(2, Duration::from_secs(1))).await;
    let orders = pipeline("retried");
    let staged = publishing(&wal, &orders).await;
    objects.plan(every(2, Fault::Fail, any));
    // Every second request fails: each fails at most once, and its retry answers.
    staged.publish().await.expect("publishes");
    let load = chunk(1, 0).load;
    assert_eq!(wal.chunks(&orders, load).await.expect("lists"), [(0, 5)]);
}

#[tokio::test(start_paused = true)]
async fn a_put_whose_answer_was_lost_is_known_as_its_own() {
    let objects = objects(faultless());
    let wal = opened(&objects, options(1 << 20)).await;
    let orders = pipeline("lost");
    let load = chunk(1, 0).load;
    // The mark's put lands and its answer is lost; the retry finds the name taken by itself.
    let mut answerless = true;
    objects.plan(Box::new(move |call| {
        if call.op == (Op::Put { create: true }) && std::mem::take(&mut answerless) {
            Fault::Answerless
        } else {
            Fault::None
        }
    }));
    wal.open_log(&orders, load).await.expect("its own mark");
    let mut staged = wal.stage(&orders, chunk(1, 0)).await.expect("stages");
    staged
        .append(Bytes::from_static(b"chunk"))
        .await
        .expect("appends");
    let mut answerless = true;
    objects.plan(Box::new(move |call| {
        if call.op == (Op::Put { create: true }) && std::mem::take(&mut answerless) {
            Fault::Answerless
        } else {
            Fault::None
        }
    }));
    staged.publish().await.expect("its own chunk");
    assert_eq!(wal.chunks(&orders, load).await.expect("lists"), [(0, 5)]);
}

#[tokio::test(start_paused = true)]
async fn a_create_answered_taken_at_once_is_another_s_and_after_an_unknown_attempt_is_read_back() {
    let objects = objects(faultless());
    let wal = opened(&objects, options(1 << 20)).await;
    let orders = pipeline("taken");
    let mut raced = true;
    objects.plan(Box::new(move |call| {
        if call.op == (Op::Put { create: true }) && std::mem::take(&mut raced) {
            Fault::Raced
        } else {
            Fault::None
        }
    }));
    let refused = wal
        .open_log(&orders, chunk(1, 0).load)
        .await
        .expect_err("taken");
    assert_eq!(refused.kind(), io::ErrorKind::AlreadyExists);
    // An attempt that fails first leaves its outcome unknown: a name then answered taken is read
    // back, and found empty, is created again.
    let staged = publishing(&wal, &orders).await;
    let mut answers = vec![Fault::Fail, Fault::Raced].into_iter();
    objects.plan(Box::new(move |call| match call.op {
        Op::Put { create: true } => answers.next().unwrap_or(Fault::None),
        _ => Fault::None,
    }));
    let reads = objects.ranges().len();
    staged.publish().await.expect("made again");
    assert_eq!(
        objects.ranges().len(),
        reads,
        "a read back looks, reading no bytes"
    );
    let load = chunk(1, 0).load;
    assert_eq!(wal.chunks(&orders, load).await.expect("lists"), [(0, 5)]);
}

#[tokio::test(start_paused = true)]
async fn a_create_raced_on_every_attempt_after_an_unknown_one_is_unavailable() {
    let objects = objects(faultless());
    let wal = opened(&objects, tries(3, Duration::from_secs(1))).await;
    let staged = publishing(&wal, &pipeline("raced")).await;
    let mut first = true;
    objects.plan(Box::new(move |call| match call.op {
        Op::Put { create: true } if std::mem::take(&mut first) => Fault::Fail,
        Op::Put { create: true } => Fault::Raced,
        _ => Fault::None,
    }));
    let refused = staged.publish().await.expect_err("never lands");
    assert_eq!(
        judged(refused),
        (Some("wal_storage_unavailable".to_owned()), true)
    );
}

#[tokio::test(start_paused = true)]
async fn a_create_whose_read_back_is_refused_reports_the_refusal_and_creates_nothing_more() {
    let objects = objects(faultless());
    let wal = opened(&objects, options(1 << 20)).await;
    let staged = publishing(&wal, &pipeline("unread")).await;
    let mut answers = vec![Fault::Fail, Fault::Raced].into_iter();
    objects.plan(Box::new(move |call| match call.op {
        Op::Put { create: true } => answers.next().unwrap_or(Fault::None),
        Op::Get => Fault::Denied,
        _ => Fault::None,
    }));
    let before = objects.calls().len();
    let refused = staged.publish().await.expect_err("refused");
    assert_eq!(
        judged(refused),
        (Some("wal_storage_denied".to_owned()), false)
    );
    let made = objects.calls()[before..]
        .iter()
        .filter(|call| creates(call))
        .count();
    assert_eq!(made, 2, "the attempt failed and the one answered taken");
}

#[tokio::test(start_paused = true)]
async fn a_staging_whose_look_at_its_log_is_refused_reports_the_refusal_not_a_closed_log() {
    let objects = objects(faultless());
    let wal = opened(&objects, options(1 << 20)).await;
    let orders = pipeline("looked");
    wal.open_log(&orders, chunk(1, 0).load)
        .await
        .expect("opens");
    objects.plan(always(Fault::Denied, |call| call.op == Op::Get));
    let Err(refused) = wal.stage(&orders, chunk(1, 0)).await else {
        panic!("staged unseen")
    };
    assert_ne!(refused.kind(), io::ErrorKind::NotFound);
    assert_eq!(
        judged(refused),
        (Some("wal_storage_denied".to_owned()), false)
    );
}

#[tokio::test(start_paused = true)]
async fn a_refusal_of_the_credentials_is_denied_and_final() {
    let objects = objects(faultless());
    let wal = opened(&objects, options(1 << 20)).await;
    objects.plan(always(Fault::Denied, any));
    let before = objects.calls().len();
    let refused = wal.loads(&pipeline("denied")).await.expect_err("denied");
    assert_eq!(
        objects.calls().len() - before,
        1,
        "a refusal is not tried again"
    );
    assert_eq!(
        judged(refused),
        (Some("wal_storage_denied".to_owned()), false)
    );
}

#[tokio::test(start_paused = true)]
async fn a_request_the_store_does_not_implement_is_unsupported_and_final() {
    let objects = objects(faultless());
    let wal = opened(&objects, options(1 << 20)).await;
    objects.plan(always(Fault::Unsupported, any));
    let refused = wal
        .loads(&pipeline("unsupported"))
        .await
        .expect_err("unsupported");
    assert_eq!(
        judged(refused),
        (Some("wal_storage_unsupported".to_owned()), false)
    );
}

#[tokio::test]
async fn a_read_answered_with_more_than_was_asked_is_refused() {
    use futures_util::stream;
    use object_store::{GetResult, GetResultPayload, ObjectMeta};
    let meta = ObjectMeta {
        location: "logs/x".into(),
        last_modified: chrono::DateTime::UNIX_EPOCH,
        size: 10,
        e_tag: None,
        version: None,
    };
    let bytes = Bytes::from_static(b"0123456789");
    let got = GetResult {
        payload: GetResultPayload::Stream(Box::pin(stream::iter([Ok(bytes)]))),
        meta,
        range: 0..10,
        attributes: object_store::Attributes::default(),
        extensions: object_store::Extensions::default(),
    };
    let gathered = super::super::calls::gathered(got, 5)
        .await
        .expect("streams");
    assert_eq!(
        gathered.expect_err("too long").kind(),
        io::ErrorKind::InvalidData
    );
}

#[tokio::test]
async fn a_read_to_the_end_of_a_chunk_asks_for_the_rest_of_it_however_long_it_is() {
    use object_store::GetRange;
    let objects = objects(faultless());
    let wal = opened(&objects, options(1 << 20)).await;
    let orders = pipeline("rest");
    let at = chunk(1, 0);
    wal.open_log(&orders, at.load).await.expect("opens");
    let mut staged = wal.stage(&orders, at).await.expect("stages");
    staged
        .append(Bytes::from_static(b"0123456789"))
        .await
        .expect("appends");
    staged.publish().await.expect("publishes");
    let before = objects.ranges().len();
    // A range ending past what a signed 64-bit length holds is one some S3 servers refuse.
    let read = wal.read(&orders, at, 2, u64::MAX - 2).await.expect("reads");
    assert_eq!(&read[..], b"23456789");
    let read = wal.read(&orders, at, 2, 3).await.expect("reads");
    assert_eq!(&read[..], b"234");
    let edge = wal
        .read(&orders, at, 1, i64::MAX as u64 - 1)
        .await
        .expect("reads");
    assert_eq!(&edge[..], b"123456789");
    let past = wal
        .read(&orders, at, 1, i64::MAX as u64)
        .await
        .expect("reads");
    assert_eq!(&past[..], b"123456789");
    let ranges: Vec<_> = objects.ranges()[before..]
        .iter()
        .flatten()
        .cloned()
        .collect();
    assert_eq!(
        ranges[ranges.len() - 4..],
        [
            GetRange::Offset(2),
            GetRange::Bounded(2..5),
            GetRange::Bounded(1..i64::MAX as u64),
            GetRange::Offset(1),
        ]
    );
}

#[tokio::test(start_paused = true)]
async fn a_listing_steady_but_slow_is_given_up_past_the_deadline_of_its_pages_in_all() {
    // Each object comes within a page's deadline; a listing may take 66 pages' deadlines, as
    // many as its most objects fill and one more.
    let page = Duration::from_secs(1);
    for (marks, listed) in [(131_u128, true), (140, false)] {
        let objects = objects(faultless());
        let wal = opened(&objects, tries(1, page)).await;
        let orders = pipeline("steady");
        for load in 0..marks {
            let mark = wal.shared.keys.mark(&orders, chunk(load + 1, 0).load);
            objects
                .inner()
                .put(&mark, PutPayload::from_static(b""))
                .await
                .expect("puts");
        }
        objects.plan(always(Fault::Drip(page / 2), |call| call.op == Op::List));
        let loads = wal.loads(&orders).await;
        if listed {
            assert_eq!(loads.expect("listed in time").len(), 131);
        } else {
            let refused = loads.expect_err("past its pages' deadline");
            assert_eq!(
                judged(refused),
                (Some("wal_storage_unavailable".to_owned()), true)
            );
        }
    }
}

#[tokio::test(start_paused = true)]
async fn a_listing_whose_page_never_comes_is_given_up_at_a_page_s_deadline() {
    let objects = objects(faultless());
    let deadline = Duration::from_secs(10);
    let wal = opened(
        &objects,
        tries(2, deadline).with_backoff(Duration::ZERO, Duration::ZERO),
    )
    .await;
    objects.plan(always(Fault::Hang, |call| call.op == Op::List));
    let env = crate::env::SystemEnv::one_core();
    let start = crate::env::Env::instant(&env);
    let refused = wal
        .chunks(&pipeline("paged"), chunk(1, 0).load)
        .await
        .expect_err("never listed");
    let elapsed = crate::env::Env::instant(&env) - start;
    assert_eq!(
        elapsed,
        deadline * 2,
        "two attempts, each a page's deadline"
    );
    assert_eq!(
        judged(refused),
        (Some("wal_storage_unavailable".to_owned()), true)
    );
}

#[tokio::test]
async fn a_log_s_directory_holding_more_than_a_listing_holds_is_unreadable() {
    use object_store::{ObjectStoreExt as _, PutPayload};
    let objects = objects(faultless());
    let wal = opened(&objects, options(1 << 20)).await;
    let orders = pipeline("crowded");
    let load = chunk(1, 0).load;
    for number in 0..=crate::limits::OBJECT_LISTED {
        let key = format!("logs/p.crowded/logs/{load}/{number:08}.wal").into();
        objects
            .inner()
            .put(&key, PutPayload::from_static(b"x"))
            .await
            .expect("puts");
    }
    let refused = wal.chunks(&orders, load).await.expect_err("crowded");
    assert_eq!(judged(refused), (Some("wal_unreadable".to_owned()), false));
    let key = format!("logs/p.crowded/logs/{load}/00000000.wal").into();
    objects.inner().delete(&key).await.expect("deletes");
    assert_eq!(
        wal.chunks(&orders, load).await.expect("lists").len(),
        crate::limits::OBJECT_LISTED
    );
}

#[tokio::test(start_paused = true)]
async fn a_pipeline_holding_more_logs_than_a_listing_holds_is_unreadable() {
    let objects = objects(faultless());
    let wal = opened(&objects, options(1 << 20)).await;
    let orders = pipeline("crowded");
    let most = u128::try_from(crate::limits::OBJECT_LISTED).expect("a count");
    let key = |log: u128| format!("logs/p.crowded/logs/{}/00000000.wal", chunk(log, 0).load);
    for log in 0..=most {
        objects
            .inner()
            .put(&key(log).into(), PutPayload::from_static(b"x"))
            .await
            .expect("puts");
    }
    let refused = wal.leftovers(&orders).await.expect_err("crowded");
    assert_eq!(judged(refused), (Some("wal_unreadable".to_owned()), false));
    objects
        .inner()
        .delete(&key(0).into())
        .await
        .expect("deletes");
    assert_eq!(
        wal.leftovers(&orders).await.expect("lists").len(),
        crate::limits::OBJECT_LISTED
    );
}

#[tokio::test(start_paused = true)]
async fn a_deletion_answered_as_finding_nothing_is_done() {
    let objects = objects(faultless());
    let wal = opened(&objects, options(1 << 20)).await;
    let orders = pipeline("gone");
    let at = chunk(1, 0);
    wal.open_log(&orders, at.load).await.expect("opens");
    let mut staged = wal.stage(&orders, at).await.expect("stages");
    staged
        .append(Bytes::from_static(b"gone"))
        .await
        .expect("appends");
    staged.publish().await.expect("publishes");
    objects.plan(always(Fault::Missing, |call| call.op == Op::Delete));
    wal.remove(&orders, at).await.expect("gone already");
    wal.remove_log(&orders, at.load)
        .await
        .expect("gone already");
}

#[tokio::test(start_paused = true)]
async fn a_deletion_refused_is_reported_and_leaves_the_chunk() {
    let objects = objects(faultless());
    let wal = opened(&objects, options(1 << 20)).await;
    let orders = pipeline("kept");
    let at = chunk(1, 0);
    wal.open_log(&orders, at.load).await.expect("opens");
    let mut staged = wal.stage(&orders, at).await.expect("stages");
    staged
        .append(Bytes::from_static(b"kept"))
        .await
        .expect("appends");
    staged.publish().await.expect("publishes");
    objects.plan(always(Fault::Denied, |call| call.op == Op::Delete));
    let refused = wal.remove(&orders, at).await.expect_err("refused");
    assert_eq!(
        judged(refused),
        (Some("wal_storage_denied".to_owned()), false)
    );
    objects.plan(faultless());
    assert_eq!(
        wal.chunks(&orders, at.load).await.expect("lists"),
        [(0, 4)],
        "the chunk is still listed"
    );
}

#[tokio::test(start_paused = true)]
async fn a_removal_refused_a_look_at_the_chunk_s_head_reports_the_refusal() {
    let objects = objects(faultless());
    let wal = opened(&objects, options(1 << 20)).await;
    let orders = pipeline("unseen");
    let at = chunk(1, 0);
    wal.open_log(&orders, at.load).await.expect("opens");
    let mut staged = wal.stage(&orders, at).await.expect("stages");
    staged
        .append(Bytes::from_static(b"unseen"))
        .await
        .expect("appends");
    staged.publish().await.expect("publishes");
    // Another process knows nothing of the chunk, and is refused its head.
    let other = opened(&objects, options(1 << 20)).await;
    objects.plan(always(Fault::Denied, |call| call.op == Op::Get));
    let refused = other.remove(&orders, at).await.expect_err("refused");
    assert_eq!(
        judged(refused),
        (Some("wal_storage_denied".to_owned()), false)
    );
    objects.plan(faultless());
    assert_eq!(
        wal.chunks(&orders, at.load).await.expect("lists"),
        [(0, 6)],
        "nothing was deleted"
    );
}

#[tokio::test(start_paused = true)]
async fn a_create_that_lands_after_its_attempt_gave_up_is_known_as_its_own() {
    let objects = objects(faultless());
    let wal = opened(&objects, tries(3, Duration::from_secs(1))).await;
    let orders = pipeline("late");
    let mut late = true;
    // The first create lands half a second after it was made, while its attempt waits out its
    // deadline of a second: the next attempt finds the name taken, by its own.
    objects.plan(Box::new(move |call| {
        if call.op == (Op::Put { create: true }) && std::mem::take(&mut late) {
            Fault::Late(Duration::from_millis(500))
        } else {
            Fault::None
        }
    }));
    wal.open_log(&orders, chunk(1, 0).load)
        .await
        .expect("its own mark");
    assert_eq!(wal.loads(&orders).await.expect("lists"), [chunk(1, 0).load]);
}

#[tokio::test(start_paused = true)]
async fn a_mark_whose_create_may_land_later_opens_no_log_and_lands_as_one_holding_nothing() {
    let objects = objects(faultless());
    let wal = opened(&objects, tries(3, Duration::from_secs(1))).await;
    let orders = pipeline("given-up");
    let load = chunk(1, 0).load;
    // The mark's create is given up, and lands five seconds later.
    objects.plan(always(Fault::Late(Duration::from_secs(5)), creates));
    let before = objects.calls().len();
    let refused = wal.open_log(&orders, load).await.expect_err("not opened");
    assert_eq!(
        judged(refused),
        (Some("wal_storage_unavailable".to_owned()), true)
    );
    let made = objects.calls()[before..]
        .iter()
        .filter(|call| creates(call))
        .count();
    assert_eq!(made, 1, "a create that may still land is never made again");
    // Landed, it opens a log no chunk was published in, which a replay fences and removes.
    objects.plan(faultless());
    tokio::time::advance(Duration::from_secs(6)).await;
    assert_eq!(wal.loads(&orders).await.expect("lists"), [load]);
    assert_eq!(wal.chunks(&orders, load).await.expect("lists"), []);
}

#[test]
fn a_refusal_among_an_answer_s_sources_is_reported_for_good_however_deep() {
    use super::super::fault::{Answer, Ask, answer};
    use crate::wal::object::StoreRefusal;
    let refusal = || StoreRefusal::new("a certificate no trusted root signs", None);
    let held = io::Error::new(io::ErrorKind::InvalidData, refusal());
    let nested = io::Error::other(io::Error::new(io::ErrorKind::InvalidData, refusal()));
    let causes: [Box<dyn std::error::Error + Send + Sync>; 4] = [
        Box::new(nested),
        Box::new(refusal()),
        Box::new(object_store::Error::Generic {
            store: "inner",
            source: Box::new(refusal()),
        }),
        Box::new(held),
    ];
    for cause in causes {
        let error = object_store::Error::Generic {
            store: "S3",
            source: cause,
        };
        let Answer::Final(refused) = answer("logs/x", error, Ask::Other) else {
            panic!("tried again");
        };
        assert_eq!(
            judged(refused),
            (Some("wal_storage_refused".to_owned()), false)
        );
    }
    let failed = object_store::Error::Generic {
        store: "S3",
        source: Box::new(io::Error::other("reset")),
    };
    assert!(matches!(
        answer("logs/x", failed, Ask::Other),
        Answer::Transient(_)
    ));
    // A conflict answers a create, and passes anything else.
    let conflict = || object_store::Error::AlreadyExists {
        path: "logs/x".to_owned(),
        source: "409".into(),
    };
    assert!(matches!(
        answer("logs/x", conflict(), Ask::Create),
        Answer::Final(_)
    ));
    assert!(matches!(
        answer("logs/x", conflict(), Ask::Other),
        Answer::Transient(_)
    ));
}
