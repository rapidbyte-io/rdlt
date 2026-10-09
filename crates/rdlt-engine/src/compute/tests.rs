use std::num::NonZeroUsize;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Mutex, mpsc};
use std::time::{Duration, Instant, SystemTime};

use proptest::prelude::*;
use tokio::task::JoinSet;

use super::{ComputePool, Cores, Inline, Pool, RayonPool};
use crate::env::{Env, InlineEnv, Sleep, SystemEnv};
use crate::report::{PoolCounters, Tally};

fn cores(count: usize, workers: usize) -> Cores {
    Cores::new(
        NonZeroUsize::new(count).unwrap(),
        NonZeroUsize::new(workers).unwrap(),
    )
}

proptest! {
    /// The pool gets every core the runtime's workers leave, and one thread however many
    /// workers there are.
    #[test]
    fn the_pool_gets_the_cores_the_workers_leave_and_one_thread_at_least(
        count in 1usize..=1024,
        workers in 1usize..=1024,
    ) {
        let sized = cores(count, workers);
        prop_assert_eq!(sized.count().get(), count);
        prop_assert_eq!(sized.workers().get(), workers);
        let threads = sized.compute_threads().get();
        if workers < count {
            prop_assert_eq!(threads + workers, count);
        } else {
            prop_assert_eq!(threads, 1);
        }
    }

    /// The suggested split gives the pool a thread and the runtime two workers at least, where
    /// there are two cores, and every core to one of them from three on; one or two cores take
    /// one thread more than there are.
    #[test]
    fn a_count_of_cores_splits_into_workers_and_compute_threads(count in 1usize..=1024) {
        let split = Cores::from_count(NonZeroUsize::new(count).unwrap());
        prop_assert_eq!(split.count().get(), count);
        prop_assert_eq!(split.workers().get(), count.min(2).max(count / 2));
        prop_assert!(split.compute_threads().get() >= 1);
        let threads = split.workers().get() + split.compute_threads().get();
        if count >= 3 {
            prop_assert_eq!(threads, count);
        } else {
            prop_assert_eq!(threads, count + 1);
        }
    }
}

fn pool() -> Pool {
    let env = SystemEnv::try_new(cores(3, 1)).unwrap();
    Pool::new(Arc::new(env), Arc::default())
}

#[tokio::test]
async fn jobs_run_on_pool_threads() {
    let pool = pool();
    let threads = pool
        .run_all([|| std::thread::current().name().map(str::to_owned)])
        .await;
    assert!(threads[0].as_deref().unwrap().starts_with("rdlt-compute-"));
}

#[tokio::test]
async fn results_come_back_in_the_order_of_the_work_whatever_order_jobs_finish_in() {
    let pool = pool();
    let (done, wait) = mpsc::channel();
    let wait = Mutex::new(wait);
    // The first job finishes only once the last has run.
    let jobs: Vec<Box<dyn FnOnce() -> u8 + Send>> = vec![
        Box::new(move || {
            wait.lock().unwrap().recv().unwrap();
            0
        }),
        Box::new(|| 1),
        Box::new(move || {
            done.send(()).unwrap();
            2
        }),
    ];
    assert_eq!(pool.run_all(jobs).await, [0, 1, 2]);
}

#[tokio::test]
#[expect(
    clippy::disallowed_methods,
    reason = "the panic must reach a task outside any scope"
)]
async fn a_panicking_job_panics_its_caller_and_the_pool_keeps_working() {
    let pool = pool();
    let mut tasks = JoinSet::new();
    let job_pool = pool.clone();
    tasks.spawn(async move { job_pool.run_all([|| -> u32 { panic!("job failed") }]).await });

    let joined = tasks.join_next().await.unwrap();

    assert!(joined.unwrap_err().is_panic());
    assert_eq!(pool.run_all([|| 7]).await, [7]);
}

#[tokio::test]
async fn pool_threads_have_the_stack_a_shredding_job_asks_for() {
    let pool = pool();
    let remaining = pool.run_all([stacker::remaining_stack]).await;
    assert!(
        remaining[0].is_some_and(|bytes| bytes >= 6 * 1024 * 1024),
        "{remaining:?}"
    );
}

#[test]
fn a_pool_starts_one_thread_for_each_core_the_workers_leave() {
    for (count, workers, threads) in [(4, 1, 3), (4, 2, 2), (4, 4, 1), (2, 8, 1), (1, 1, 1)] {
        let pool = RayonPool::try_new(cores(count, workers)).unwrap();
        assert_eq!(
            pool.pool.current_num_threads(),
            threads,
            "{count} cores, {workers} workers"
        );
    }
}

#[test]
fn cores_beside_a_runtime_are_the_host_s_and_the_runtime_s_workers() {
    for workers in [1, 3] {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(workers)
            .build()
            .unwrap();
        let cores = Cores::try_beside(runtime.handle()).unwrap();
        assert!(cores.count().get() >= 1);
        assert_eq!(cores.workers().get(), workers);
    }
}

#[test]
fn the_host_s_cores_split_as_their_count_does() {
    let host = Cores::try_from_host().unwrap();
    assert_eq!(host, Cores::from_count(host.count()));
}

#[test]
fn the_suggested_split_gives_the_runtime_half_the_cores_and_two_workers_at_least() {
    for (count, workers, threads) in [
        (1, 1, 1),
        (2, 2, 1),
        (3, 2, 1),
        (4, 2, 2),
        (5, 2, 3),
        (8, 4, 4),
        (9, 4, 5),
        (32, 16, 16),
    ] {
        let split = Cores::from_count(NonZeroUsize::new(count).unwrap());
        assert_eq!(split.workers().get(), workers, "{count} cores");
        assert_eq!(split.compute_threads().get(), threads, "{count} cores");
    }
}

#[cfg(target_os = "linux")]
#[test]
#[expect(
    clippy::disallowed_methods,
    reason = "the count the engine's cores must agree with is the standard library's own"
)]
fn the_host_s_cores_are_those_its_affinity_mask_leaves() {
    use rustix::thread::{CpuSet, sched_getaffinity, sched_setaffinity};
    let allowed = sched_getaffinity(None).unwrap();
    // A cgroup CPU quota also caps the count, so the mask can only lower what the host reports.
    let before = std::thread::available_parallelism().unwrap().get();
    assert_eq!(Cores::try_from_host().unwrap().count().get(), before);
    let mut two = CpuSet::new();
    let chosen: Vec<usize> = (0..CpuSet::MAX_CPU)
        .filter(|cpu| allowed.is_set(*cpu))
        .take(2)
        .collect();
    for cpu in &chosen {
        two.set(*cpu);
    }
    sched_setaffinity(None, &two).unwrap();
    assert_eq!(
        Cores::try_from_host().unwrap().count().get(),
        chosen.len().min(before)
    );
}

/// A clock whose `n`th reading is `n` squared seconds after the first, and an inline pool.
struct Squares {
    first: Instant,
    readings: AtomicU32,
}

impl Env for Squares {
    fn now(&self) -> SystemTime {
        InlineEnv.now()
    }

    fn instant(&self) -> Instant {
        let reading = u64::from(self.readings.fetch_add(1, Ordering::SeqCst));
        self.first + Duration::from_secs(reading * reading)
    }

    fn sleep(&self, duration: Duration) -> Sleep {
        InlineEnv.sleep(duration)
    }

    fn random(&self) -> u64 {
        InlineEnv.random()
    }

    fn compute(&self) -> &dyn ComputePool {
        &Inline
    }

    fn cores(&self) -> NonZeroUsize {
        NonZeroUsize::MIN
    }
}

#[tokio::test]
async fn each_job_is_timed_from_its_queueing_to_its_start_and_through_its_run() {
    let env = Squares {
        first: InlineEnv.instant(),
        readings: AtomicU32::new(0),
    };
    let tally = Arc::new(Tally::default());
    let pool = Pool::new(Arc::new(env), Arc::clone(&tally));
    // Each job reads the clock as it is queued, starts and ends: 0, 1 and 4 seconds, then 9,
    // 16 and 25.
    assert_eq!(pool.run_all([|| 1, || 2]).await, [1, 2]);
    let counted = PoolCounters {
        jobs: 2,
        queued: Duration::from_secs(1 + 7),
        running: Duration::from_secs(3 + 9),
    };
    assert_eq!(tally.counters().pool, counted);
}
