use std::num::NonZeroUsize;
use std::sync::Arc;

use tokio::task::JoinSet;

use super::{RayonPool, run};

fn pool() -> Arc<RayonPool> {
    Arc::new(RayonPool::new(NonZeroUsize::new(2).unwrap()).unwrap())
}

#[tokio::test]
async fn run_returns_the_value_computed_on_a_pool_thread() {
    let pool = pool();
    let thread = run(pool.as_ref(), || {
        std::thread::current().name().map(str::to_owned)
    })
    .await;
    assert!(thread.unwrap().starts_with("rdlt-compute-"));
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
    tasks.spawn(async move { run(job_pool.as_ref(), || -> u32 { panic!("job failed") }).await });

    let joined = tasks.join_next().await.unwrap();

    assert!(joined.unwrap_err().is_panic());
    assert_eq!(run(pool.as_ref(), || 7).await, 7);
}
