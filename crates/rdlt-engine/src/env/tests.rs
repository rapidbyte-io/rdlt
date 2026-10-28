use std::num::NonZeroUsize;
use std::sync::Arc;
use std::time::{Duration, UNIX_EPOCH};

use super::{Env, InlineEnv, SystemEnv};
use crate::compute::Cores;

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
fn system_env_random_values_differ() {
    let env = system_env();
    assert_ne!(env.random(), env.random());
}

#[test]
fn an_inline_env_s_random_values_differ() {
    assert_ne!(InlineEnv.random(), InlineEnv.random());
}

#[tokio::test]
async fn system_env_runs_compute_jobs_on_its_pool() {
    let env = system_env();
    let pool = crate::compute::Pool::new(Arc::new(env), Arc::default());
    let threads = pool
        .run_all([|| std::thread::current().name().map(str::to_owned)])
        .await;
    assert!(threads[0].as_deref().unwrap().starts_with("rdlt-compute-"));
}

#[test]
fn system_env_declares_the_cores_it_was_given_not_the_host_s() {
    for count in [1, 3, 64] {
        let count = NonZeroUsize::new(count).unwrap();
        let env = SystemEnv::try_new(Cores::new(count)).unwrap();
        assert_eq!(env.cores(), count);
    }
}

#[test]
fn a_system_env_from_the_host_declares_the_host_s_cores() {
    let env = SystemEnv::try_from_host().unwrap();
    assert_eq!(env.cores(), Cores::try_from_host().unwrap().count());
}
