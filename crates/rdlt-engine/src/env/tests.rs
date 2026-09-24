use std::num::NonZeroUsize;
use std::sync::Mutex;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use rdlt_connector::LoadId;

use super::{Env, Sleep, SystemEnv};
use crate::compute::{ComputePool, RayonPool};

fn system_env() -> SystemEnv {
    SystemEnv::new(RayonPool::new(NonZeroUsize::MIN).unwrap())
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
