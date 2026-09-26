use std::panic;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant, UNIX_EPOCH};

use parking_lot::Mutex;
use rdlt_engine::Env;
use rdlt_sim::{Seed, run, seeds};
use tokio::task::JoinSet;

type Trace = Vec<(u32, u64, Duration)>;

/// Eight tasks that each sleep for random durations and record what they observe.
fn trace(seed: Seed) -> Trace {
    run(seed, |env| async move {
        let log = Arc::new(Mutex::new(Vec::new()));
        let mut tasks = JoinSet::new();
        for task in 0..8u32 {
            let (env, log) = (Arc::clone(&env), Arc::clone(&log));
            tasks.spawn(async move {
                for _ in 0..4 {
                    env.sleep(Duration::from_millis(env.random() % 1000)).await;
                    let at = env
                        .now()
                        .duration_since(UNIX_EPOCH)
                        .expect("simulated time is after 1970");
                    let value = env.random();
                    log.lock().push((task, value, at));
                }
            });
        }
        while tasks.join_next().await.is_some() {}
        log.lock().clone()
    })
}

#[test]
fn the_same_seed_replays_identically() {
    for seed in seeds(64) {
        assert_eq!(trace(seed), trace(seed), "seed {seed}");
    }
}

#[test]
fn load_ids_replay_with_the_seed() {
    let ids = |seed| run(seed, |env| async move { (env.load_id(), env.load_id()) });
    assert_eq!(ids(Seed::new(9)), ids(Seed::new(9)));
    assert_ne!(ids(Seed::new(9)).0, ids(Seed::new(9)).1);
    assert_ne!(ids(Seed::new(9)), ids(Seed::new(10)));
}

#[test]
fn different_seeds_diverge() {
    assert_ne!(trace(Seed::new(1)), trace(Seed::new(2)));
}

#[test]
fn virtual_time_passes_without_waiting() {
    let started = Instant::now();
    let slept = run(Seed::new(3), |env| async move {
        let start = env.instant();
        env.sleep(Duration::from_hours(24)).await;
        env.instant() - start
    });
    assert!(slept >= Duration::from_hours(24));
    assert!(started.elapsed() < Duration::from_secs(5));
}

#[test]
fn the_wall_clock_starts_at_the_simulated_epoch() {
    let now = run(Seed::new(4), |env| async move { env.now() });
    assert_eq!(now, UNIX_EPOCH + Duration::from_hours(490_896));
}

#[test]
fn compute_jobs_run_inline() {
    let ran = run(Seed::new(5), |env| async move {
        let flag = Arc::new(AtomicBool::new(false));
        let job_flag = Arc::clone(&flag);
        env.compute()
            .execute(Box::new(move || job_flag.store(true, Ordering::SeqCst)));
        flag.load(Ordering::SeqCst)
    });
    assert!(ran);
}

#[test]
fn a_failing_scenario_panics_with_its_payload() {
    let outcome =
        panic::catch_unwind(|| run(Seed::new(7), |_| async { panic!("scenario failed") }));
    let payload = outcome.unwrap_err();
    assert_eq!(payload.downcast_ref::<&str>(), Some(&"scenario failed"));
}

#[test]
fn the_same_seed_leaves_the_destination_alike() {
    // A fixed few, however many seeds the run covers: each replays twice.
    let digests: Vec<_> = (0..20)
        .map(Seed::new)
        .map(|seed| (seed, rdlt_sim::check_exactly_once(seed)))
        .collect();
    for (seed, digest) in &digests {
        assert_eq!(rdlt_sim::check_exactly_once(*seed), *digest, "seed {seed}");
    }
    let distinct: std::collections::BTreeSet<String> = digests
        .iter()
        .map(|(_, digest)| format!("{digest:?}"))
        .collect();
    assert!(
        distinct.len() > 1,
        "different seeds leave different contents"
    );
}
