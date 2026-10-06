use std::num::NonZeroUsize;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use rdlt_connector::LoadId;

use super::{Clock, Env, Sleep, SystemClock, SystemEnv};
use crate::compute::{ComputePool, Cores};
use crate::wal::{LocalWal, WalStore};

fn system_env() -> SystemEnv {
    SystemEnv::one_core()
}

#[tokio::test(start_paused = true)]
async fn system_env_sleeps_and_measures_on_the_runtime_clock() {
    let env = system_env();
    let start = env.instant();
    env.sleep(Duration::from_hours(1)).await;
    assert!(env.instant() - start >= Duration::from_hours(1));
}

#[test]
fn system_env_reads_the_real_wall_clock() {
    let first_of_2026 = UNIX_EPOCH + Duration::from_hours(490_896);
    assert!(system_env().now() > first_of_2026);
}

#[test]
fn system_env_keeps_write_ahead_logs_only_where_given_a_store() {
    assert!(system_env().wal().is_none());
    let store: Arc<dyn WalStore> = Arc::new(LocalWal::new("unused"));
    let env = system_env().with_wal(Arc::clone(&store));
    let kept = env.wal().expect("the store it was given");
    assert!(Arc::ptr_eq(&kept, &store));
}

#[test]
fn system_env_load_ids_are_distinct() {
    let env = system_env();
    assert_ne!(env.load_id(), env.load_id());
}

/// A clock fixed at `now` and a random source that replays `random` in order.
struct Fixed {
    now: SystemTime,
    random: Mutex<Vec<u64>>,
    inner: SystemEnv,
}

impl Env for Fixed {
    fn now(&self) -> SystemTime {
        self.now
    }

    fn instant(&self) -> Instant {
        self.inner.instant()
    }

    fn sleep(&self, duration: Duration) -> Sleep {
        self.inner.sleep(duration)
    }

    fn random(&self) -> u64 {
        self.random.lock().unwrap().remove(0)
    }

    fn compute(&self) -> &dyn ComputePool {
        self.inner.compute()
    }

    fn cores(&self) -> NonZeroUsize {
        self.inner.cores()
    }
}

#[test]
fn load_ids_take_the_first_random_value_as_the_high_word() {
    let now = UNIX_EPOCH + Duration::from_hours(490_896);
    let env = Fixed {
        now,
        random: Mutex::new(vec![3, 5]),
        inner: system_env(),
    };
    assert_eq!(env.load_id(), LoadId::from_parts(now, (3 << 64) + 5));
}

#[test]
fn system_env_random_values_differ() {
    let env = system_env();
    assert_ne!(env.random(), env.random());
}

#[tokio::test]
async fn system_env_runs_compute_jobs_on_its_pool() {
    let env = system_env();
    let threads = crate::compute::run_all(
        env.compute(),
        [|| std::thread::current().name().map(str::to_owned)],
    )
    .await;
    assert!(threads[0].as_deref().unwrap().starts_with("rdlt-compute-"));
}

#[tokio::test(start_paused = true)]
async fn the_system_clock_sleeps_on_the_runtime_clock() {
    let env = system_env();
    let start = env.instant();
    SystemClock.sleep(Duration::from_hours(1)).await;
    assert!(env.instant() - start >= Duration::from_hours(1));
}

#[test]
fn the_system_clock_s_random_values_differ() {
    assert_ne!(SystemClock.random(), SystemClock.random());
}

#[test]
fn system_env_declares_the_cores_it_was_given_not_the_host_s() {
    for count in [1, 3, 64] {
        let count = NonZeroUsize::new(count).unwrap();
        let env = SystemEnv::try_new(Cores::new(count, count)).unwrap();
        assert_eq!(env.cores(), count);
    }
}

#[test]
fn a_system_env_from_a_runtime_declares_the_host_s_cores() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap();
    let env = SystemEnv::try_from_runtime(runtime.handle()).unwrap();
    assert!(env.cores().get() >= 1);
}
