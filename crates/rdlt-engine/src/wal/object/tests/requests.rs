//! How each request is tried: how many times, within what deadline, with what waits between, and
//! what its last failure says.

use std::io;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use object_store::memory::InMemory;
use parking_lot::Mutex;
use rdlt_testkit::objects::{Call, Fault, Op, faultless};

use super::{always, any, chunk, every, judged, objects, opened, options, pipeline, tries};
use crate::env::{Clock, Sleep};
use crate::wal::WalStore;
use crate::wal::object::calls::Calls;

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
    let most = Duration::from_secs(30) + Duration::from_secs(2) * u32::MAX;
    assert_eq!(
        calls.deadline(u64::MAX),
        most,
        "a length past u32::MAX MiB counts as that"
    );
}

fn puts(call: &Call) -> bool {
    matches!(call.op, Op::Put { .. })
}

#[tokio::test(start_paused = true)]
async fn a_request_failing_every_attempt_is_unavailable_retryably_after_its_attempts() {
    let objects = objects(faultless());
    let wal = opened(&objects, tries(3, Duration::from_secs(1))).await;
    let orders = pipeline("unavailable");
    objects.plan(always(Fault::Fail, puts));
    let before = objects.calls().len();
    let refused = wal
        .open_log(&orders, chunk(1, 0).load)
        .await
        .expect_err("fails");
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
    let env = crate::env::SystemEnv::new(
        crate::compute::RayonPool::new(std::num::NonZeroUsize::MIN).expect("a pool"),
    );
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
    objects.plan(every(2, Fault::Fail, any));
    let load = chunk(1, 0).load;
    // Every second request fails: each fails at most once, and its retry answers.
    wal.open_log(&orders, load).await.expect("opens");
    assert_eq!(wal.loads(&orders).await.expect("lists"), [load]);
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
async fn a_create_answered_as_raced_by_one_that_did_not_land_is_made_again() {
    let objects = objects(faultless());
    let wal = opened(&objects, options(1 << 20)).await;
    let mut raced = true;
    objects.plan(Box::new(move |call| {
        if call.op == (Op::Put { create: true }) && std::mem::take(&mut raced) {
            Fault::Raced
        } else {
            Fault::None
        }
    }));
    let orders = pipeline("raced");
    wal.open_log(&orders, chunk(1, 0).load)
        .await
        .expect("opens");
    assert_eq!(wal.loads(&orders).await.expect("lists"), [chunk(1, 0).load]);
}

#[tokio::test(start_paused = true)]
async fn a_create_raced_on_every_attempt_is_unavailable() {
    let objects = objects(faultless());
    let wal = opened(&objects, tries(3, Duration::from_secs(1))).await;
    objects.plan(always(Fault::Raced, |call| {
        call.op == Op::Put { create: true }
    }));
    let refused = wal
        .open_log(&pipeline("raced"), chunk(1, 0).load)
        .await
        .expect_err("never lands");
    assert_eq!(
        judged(refused),
        (Some("wal_storage_unavailable".to_owned()), true)
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
