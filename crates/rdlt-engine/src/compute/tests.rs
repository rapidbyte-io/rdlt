use std::num::NonZeroUsize;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::sync::{Arc, mpsc};
use std::time::{Duration, Instant, SystemTime};

use parking_lot::Mutex;
use proptest::prelude::*;
use tokio::task::JoinSet;

use super::{ComputePool, Cores, Inline, Pool, RayonPool};
use crate::env::{Env, InlineEnv, Sleep, SystemEnv};
use crate::report::{PoolCounters, Tally};

fn cores(count: usize) -> Cores {
    Cores::new(NonZeroUsize::new(count).unwrap())
}

proptest! {
    /// The runtime takes half the cores, rounded down, and two workers at least where there are
    /// two.
    #[test]
    fn the_runtime_takes_half_the_cores_and_two_workers_at_least(count in 1usize..=1024) {
        let workers = cores(count).runtime_workers().get();
        prop_assert_eq!(workers, count.min(2).max(count / 2));
    }
}

fn pool() -> Pool {
    let env = SystemEnv::try_new(cores(2)).unwrap();
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
            wait.lock().recv().unwrap();
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
fn a_pool_starts_a_thread_for_each_core() {
    for count in (1..=64).chain([256]) {
        let pool = RayonPool::try_new(cores(count)).unwrap();
        assert_eq!(pool.pool.current_num_threads(), count, "{count} cores");
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

    fn wait_aside(&self, wait: &mut dyn FnMut()) {
        wait();
    }
}

#[tokio::test]
async fn each_job_is_timed_from_its_queueing_to_its_start_through_its_run_and_back() {
    let env = Squares {
        first: InlineEnv.instant(),
        readings: AtomicU32::new(0),
    };
    let tally = Arc::new(Tally::default());
    let pool = Pool::new(Arc::new(env), Arc::clone(&tally));
    // Each job reads the clock as it is queued, starts and ends: 0, 1 and 4 seconds, then 9,
    // 16 and 25. Its caller reads it as it takes each result: 36, then 49.
    assert_eq!(pool.run_all([|| 1, || 2]).await, [1, 2]);
    let counted = PoolCounters {
        jobs: 2,
        queued: Duration::from_secs(1 + 7),
        running: Duration::from_secs(3 + 9),
        returned: Duration::from_secs(32 + 24),
    };
    assert_eq!(tally.counters().pool, counted);
}

/// An environment whose compute jobs run on its pool.
struct On<P>(P);

impl<P: ComputePool + 'static> Env for On<P> {
    fn now(&self) -> SystemTime {
        InlineEnv.now()
    }

    fn instant(&self) -> Instant {
        InlineEnv.instant()
    }

    fn sleep(&self, duration: Duration) -> Sleep {
        InlineEnv.sleep(duration)
    }

    fn random(&self) -> u64 {
        InlineEnv.random()
    }

    fn compute(&self) -> &dyn ComputePool {
        &self.0
    }

    fn cores(&self) -> NonZeroUsize {
        NonZeroUsize::MIN
    }

    fn wait_aside(&self, wait: &mut dyn FnMut()) {
        wait();
    }
}

/// An environment that counts the waits it is asked to run and tells of the first, whose compute
/// jobs run on its pool.
struct Counting<P> {
    pool: P,
    waits: AtomicU32,
    told: Mutex<Option<mpsc::Sender<()>>>,
}

impl<P: ComputePool + 'static> Env for Counting<P> {
    fn now(&self) -> SystemTime {
        InlineEnv.now()
    }

    fn instant(&self) -> Instant {
        InlineEnv.instant()
    }

    fn sleep(&self, duration: Duration) -> Sleep {
        InlineEnv.sleep(duration)
    }

    fn random(&self) -> u64 {
        InlineEnv.random()
    }

    fn compute(&self) -> &dyn ComputePool {
        &self.pool
    }

    fn cores(&self) -> NonZeroUsize {
        NonZeroUsize::MIN
    }

    fn wait_aside(&self, wait: &mut dyn FnMut()) {
        self.waits.fetch_add(1, Ordering::SeqCst);
        if let Some(told) = self.told.lock().take() {
            told.send(()).unwrap();
        }
        wait();
    }
}

/// The host's environment, telling when a wait is asked of it.
struct Signalling {
    env: SystemEnv,
    entered: Mutex<Option<mpsc::Sender<()>>>,
}

impl Env for Signalling {
    fn now(&self) -> SystemTime {
        self.env.now()
    }

    fn instant(&self) -> Instant {
        self.env.instant()
    }

    fn sleep(&self, duration: Duration) -> Sleep {
        self.env.sleep(duration)
    }

    fn random(&self) -> u64 {
        self.env.random()
    }

    fn compute(&self) -> &dyn ComputePool {
        self.env.compute()
    }

    fn cores(&self) -> NonZeroUsize {
        self.env.cores()
    }

    fn wait_aside(&self, wait: &mut dyn FnMut()) {
        if let Some(entered) = self.entered.lock().take() {
            entered.send(()).unwrap();
        }
        self.env.wait_aside(wait);
    }
}

/// Jobs kept until they are run, as a pool whose threads are all busy keeps them.
#[derive(Default)]
struct Kept(Mutex<Vec<super::Job>>);

impl ComputePool for Kept {
    fn execute(&self, job: super::Job) {
        self.0.lock().push(job);
    }
}

/// Runs each job on a thread of its own.
struct Threads;

impl ComputePool for Threads {
    #[expect(
        clippy::disallowed_methods,
        reason = "the job must run while the test's thread lets its caller go"
    )]
    fn execute(&self, job: super::Job) {
        std::thread::spawn(job);
    }
}

/// Notes in its order when it is dropped.
struct Noted(&'static str, Arc<Mutex<Vec<&'static str>>>);

impl Drop for Noted {
    fn drop(&mut self) {
        self.1.lock().push(self.0);
    }
}

/// Bounds every wait of a check.
const WAIT: Duration = Duration::from_secs(60);

/// Polls `call` once, which leaves it waiting for its jobs.
fn waiting<F: Future>(call: std::pin::Pin<&mut F>) {
    let waker = std::task::Waker::noop();
    let polled = call.poll(&mut std::task::Context::from_waker(waker));
    assert!(polled.is_pending(), "the jobs ended");
}

#[test]
fn a_job_its_caller_stops_waiting_for_is_dropped_unrun_as_the_caller_lets_go() {
    let kept = Arc::new(On(Kept::default()));
    let pool = Pool::new(Arc::clone(&kept) as Arc<dyn Env>, Arc::default());
    let order = Arc::new(Mutex::new(Vec::new()));
    let held = Noted("the job's", Arc::clone(&order));
    let ran = Arc::new(AtomicBool::new(false));
    let running = Arc::clone(&ran);
    let job = move || {
        running.store(true, Ordering::SeqCst);
        drop(held);
    };
    let mut call = Box::pin(pool.run_all([job]));
    waiting(call.as_mut());
    drop(call);
    order.lock().push("let go");
    for job in std::mem::take(&mut *kept.0.0.lock()) {
        job();
    }
    assert_eq!(*order.lock(), ["the job's", "let go"]);
    assert!(!ran.load(Ordering::SeqCst), "the job ran once let go");
}

#[test]
fn a_caller_that_stops_waiting_waits_for_its_running_job_to_end() {
    let pool = Pool::new(Arc::new(On(Threads)), Arc::default());
    let order = Arc::new(Mutex::new(Vec::new()));
    let (started, has_started) = mpsc::channel();
    let (go, goes) = mpsc::channel::<()>();
    let ended = Noted("ended", Arc::clone(&order));
    let job = move || {
        started.send(()).unwrap();
        goes.recv_timeout(WAIT).unwrap();
        drop(ended);
    };
    let mut call = Box::pin(pool.run_all([job]));
    waiting(call.as_mut());
    has_started.recv_timeout(WAIT).unwrap();
    let (gone, has_gone) = mpsc::channel();
    let let_go = Noted("let go", Arc::clone(&order));
    std::thread::scope(|scope| {
        // The caller is let go on a thread of its own, which waits for the job.
        scope.spawn(move || {
            drop(call);
            drop(let_go);
            gone.send(()).unwrap();
        });
        let early = has_gone.recv_timeout(Duration::from_millis(100));
        assert_eq!(
            early,
            Err(mpsc::RecvTimeoutError::Timeout),
            "the caller did not wait"
        );
        go.send(()).unwrap();
        has_gone.recv_timeout(WAIT).unwrap();
    });
    assert_eq!(*order.lock(), ["ended", "let go"]);
}

#[test]
fn only_a_running_job_is_waited_for_through_the_environment() {
    let kept = Arc::new(Counting {
        pool: Kept::default(),
        waits: AtomicU32::new(0),
        told: Mutex::new(None),
    });
    let pool = Pool::new(Arc::clone(&kept) as Arc<dyn Env>, Arc::default());
    let mut unstarted = Box::pin(pool.run_all([|| ()]));
    waiting(unstarted.as_mut());
    drop(unstarted);
    assert_eq!(
        kept.waits.load(Ordering::SeqCst),
        0,
        "a job not started needs no wait"
    );

    let (told, has_told) = mpsc::channel();
    let threads = Arc::new(Counting {
        pool: Threads,
        waits: AtomicU32::new(0),
        told: Mutex::new(Some(told)),
    });
    let pool = Pool::new(Arc::clone(&threads) as Arc<dyn Env>, Arc::default());
    let (started, has_started) = mpsc::channel();
    let (go, goes) = mpsc::channel::<()>();
    let job = move || {
        started.send(()).unwrap();
        goes.recv_timeout(WAIT).unwrap();
    };
    let mut running = Box::pin(pool.run_all([job]));
    waiting(running.as_mut());
    has_started.recv_timeout(WAIT).unwrap();
    std::thread::scope(|scope| {
        // Dropped on a thread of its own, the caller waits, and only then does the job end.
        scope.spawn(move || drop(running));
        has_told.recv_timeout(WAIT).unwrap();
        go.send(()).unwrap();
    });
    assert_eq!(threads.waits.load(Ordering::SeqCst), 1);
}

/// `future` on a runtime of one worker, bounded by [`WAIT`].
#[expect(
    clippy::disallowed_methods,
    reason = "the test bounds a wait on a real runtime, whose clock it does not control"
)]
fn within_wait<F: Future>(runtime: &tokio::runtime::Runtime, future: F) -> F::Output {
    runtime
        .block_on(async { tokio::time::timeout(WAIT, future).await })
        .expect("within the bound")
}

/// A runtime of one worker, as a one-core machine has.
fn one_worker() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(1)
        .enable_all()
        .build()
        .unwrap()
}

#[test]
fn a_cancelled_call_waiting_for_its_job_leaves_the_runtime_its_worker() {
    let runtime = one_worker();
    let pool = pool();
    let (started, has_started) = mpsc::channel();
    let (release, released) = mpsc::channel::<()>();
    let job = move || {
        started.send(()).unwrap();
        released.recv_timeout(WAIT * 2).ok();
    };
    let call = runtime.spawn(async move {
        let pool = pool;
        pool.run_all([job]).await;
    });
    has_started.recv_timeout(WAIT).unwrap();
    call.abort();
    // The task is dropped on the runtime's one worker, whose wait must leave it to this task.
    // Read here on a channel, since a blocked worker would stop the runtime's own timers too.
    let (ran, has_run) = mpsc::channel();
    drop(runtime.spawn(async move { ran.send(7) }));
    assert_eq!(has_run.recv_timeout(WAIT / 2).unwrap(), 7);
    release.send(()).unwrap();
    assert!(within_wait(&runtime, call).unwrap_err().is_cancelled());
}

#[test]
fn a_call_dropped_as_its_runtime_shuts_down_still_waits_for_its_job() {
    let runtime = one_worker();
    let (entered, has_entered) = mpsc::channel();
    let env = Signalling {
        env: SystemEnv::try_new(cores(2)).unwrap(),
        entered: Mutex::new(Some(entered)),
    };
    let pool = Pool::new(Arc::new(env), Arc::default());
    let (started, has_started) = mpsc::channel();
    let (go, goes) = mpsc::channel::<()>();
    let ended = Arc::new(AtomicBool::new(false));
    let ending = Arc::clone(&ended);
    let job = move || {
        started.send(()).unwrap();
        goes.recv_timeout(WAIT).unwrap();
        ending.store(true, Ordering::SeqCst);
    };
    drop(runtime.spawn(async move {
        let pool = pool;
        pool.run_all([job]).await;
    }));
    has_started.recv_timeout(WAIT).unwrap();
    std::thread::scope(|scope| {
        // Shut down from another thread, whose runtime drops the task on its worker, which
        // asks for the wait: only then is the job let go.
        scope.spawn(move || drop(runtime));
        has_entered.recv_timeout(WAIT).unwrap();
        go.send(()).unwrap();
    });
    assert!(
        ended.load(Ordering::SeqCst),
        "the job ended before the drop returned"
    );
}

#[test]
fn a_wait_outside_a_runtime_runs_on_this_thread() {
    let env = SystemEnv::try_new(cores(1)).unwrap();
    let mut ran = false;
    env.wait_aside(&mut || ran = true);
    assert!(ran);
}

#[test]
fn a_wait_on_a_current_thread_runtime_runs_on_this_thread() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap();
    let env = SystemEnv::try_new(cores(1)).unwrap();
    let mut ran = false;
    runtime.block_on(async { env.wait_aside(&mut || ran = true) });
    assert!(ran);
}
