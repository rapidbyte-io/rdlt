use std::num::NonZeroUsize;
use std::time::{Duration, UNIX_EPOCH};

use super::{Env, SystemEnv};
use crate::compute::RayonPool;

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
fn system_env_random_values_differ() {
    let env = system_env();
    assert_ne!(env.random(), env.random());
}

#[tokio::test]
async fn system_env_runs_compute_jobs_on_its_pool() {
    let env = system_env();
    let thread = crate::compute::run(env.compute(), || {
        std::thread::current().name().map(str::to_owned)
    })
    .await;
    assert!(thread.unwrap().starts_with("rdlt-compute-"));
}
