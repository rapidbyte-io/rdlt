use std::num::NonZeroUsize;
use std::sync::{Arc, Mutex, mpsc};

use tokio::task::JoinSet;

use super::{RayonPool, run_all};

fn pool() -> Arc<RayonPool> {
    Arc::new(RayonPool::new(NonZeroUsize::new(2).unwrap()).unwrap())
}

#[tokio::test]
async fn jobs_run_on_pool_threads() {
    let pool = pool();
    let threads = run_all(
        pool.as_ref(),
        [|| std::thread::current().name().map(str::to_owned)],
    )
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
    assert_eq!(run_all(pool.as_ref(), jobs).await, [0, 1, 2]);
}

#[tokio::test]
#[expect(
    clippy::disallowed_methods,
    reason = "the panic must reach a task outside any scope"
)]
async fn a_panicking_job_panics_its_caller_and_the_pool_keeps_working() {
    let pool = pool();
    let mut tasks = JoinSet::new();
    let job_pool = Arc::clone(&pool);
    tasks.spawn(
        async move { run_all(job_pool.as_ref(), [|| -> u32 { panic!("job failed") }]).await },
    );

    let joined = tasks.join_next().await.unwrap();

    assert!(joined.unwrap_err().is_panic());
    assert_eq!(run_all(pool.as_ref(), [|| 7]).await, [7]);
}
