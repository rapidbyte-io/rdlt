use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use rdlt_engine::Env;

use crate::seed::{Seed, run};

/// How long ten sleeps of a tenth of a second take in virtual time, `perturbed` or not.
fn slept(perturbed: bool) -> Duration {
    run(Seed::new(7), |env| async move {
        env.perturb(perturbed);
        let start = tokio::time::Instant::now();
        for _ in 0..10 {
            env.sleep(Duration::from_millis(100)).await;
        }
        start.elapsed()
    })
}

#[test]
fn a_perturbed_sleep_lasts_a_little_longer_and_alike_for_a_seed() {
    assert_eq!(slept(false), Duration::from_secs(1));
    let perturbed = slept(true);
    assert!(
        perturbed > Duration::from_secs(1) && perturbed <= Duration::from_millis(1_110),
        "{perturbed:?}"
    );
    assert_eq!(slept(true), perturbed, "the seed perturbs alike");
}

#[test]
fn a_perturbed_pool_runs_some_jobs_later_but_runs_them_all() {
    run(Seed::new(7), |env| async move {
        let run_jobs = |count: usize| {
            let done = Arc::new(AtomicUsize::new(0));
            for _ in 0..count {
                let done = Arc::clone(&done);
                env.compute().execute(Box::new(move || {
                    done.fetch_add(1, Ordering::SeqCst);
                }));
            }
            done
        };
        assert_eq!(run_jobs(100).load(Ordering::SeqCst), 100, "inline");
        env.perturb(true);
        let done = run_jobs(100);
        assert!(done.load(Ordering::SeqCst) < 100, "some jobs wait");
        for _ in 0..20 {
            tokio::task::yield_now().await;
        }
        assert_eq!(done.load(Ordering::SeqCst), 100, "and every job runs");
    });
}
