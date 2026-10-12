use std::num::NonZeroU64;
use std::sync::Arc;

use super::Logged;
use crate::bench::memory_wal;
use crate::{Cores, LocalWal, WalStore};

fn cores() -> Cores {
    Cores::new(
        std::num::NonZeroUsize::new(2).unwrap(),
        std::num::NonZeroUsize::MIN,
    )
}

#[test]
fn a_logged_run_commits_once_every_so_many_batches_and_logs_every_frame() {
    let every = NonZeroU64::new(4).unwrap();
    let logged = Logged::try_new(cores(), memory_wal(), 16, 1_000, Some(every)).unwrap();
    let report = logged.run();
    assert_eq!(report.rows, 16_000);
    // Four commits by size, then the closing one at the end of the load.
    assert_eq!(report.counters.commits.size, 4);
    assert_eq!(report.counters.commits.end, 1);
    assert_eq!(report.commits, 5);
    let log = report.counters.log;
    assert!(log.appended >= logged.frame_bytes(), "{log:?}");
    // The log's first chunk, then a chunk for each commit after.
    assert_eq!(log.requests.publish, 6);
}

#[test]
fn a_logged_run_on_a_local_disk_leaves_no_log_once_it_commits() {
    let dir = tempfile::tempdir().unwrap();
    let store: Arc<dyn WalStore> = Arc::new(LocalWal::new(dir.path()));
    let logged = Logged::try_new(cores(), Arc::clone(&store), 8, 1_000, None).unwrap();
    let report = logged.run();
    assert_eq!(report.commits, 1);
    let pipeline = rdlt_connector::PipelineId::parse("logged").unwrap();
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let loads = runtime.block_on(store.loads(&pipeline)).unwrap();
    assert_eq!(loads, []);
}

#[test]
fn a_refused_commit_leaves_a_log_whose_scan_reads_every_batch() {
    let dir = tempfile::tempdir().unwrap();
    let store: Arc<dyn WalStore> = Arc::new(LocalWal::new(dir.path()));
    let logged = Logged::try_new(cores(), store, 8, 1_000, None).unwrap();
    let report = logged.leave();
    assert_eq!(report.commits, 0);
    let read = crate::bench::replay_scan(dir.path(), "logged").unwrap();
    assert_eq!(read.batches, 8);
    assert!(read.logged >= logged.frame_bytes(), "{read:?}");
    assert!(read.read >= read.logged, "{read:?}");
}

#[test]
#[expect(clippy::cast_precision_loss, reason = "a ratio of two byte counts")]
fn a_load_committing_each_batch_writes_each_frame_once_and_publishes_once_more_than_it_commits() {
    let every = NonZeroU64::new(1).unwrap();
    let logged = Logged::try_new(cores(), memory_wal(), 8, 8_192, Some(every)).unwrap();
    let report = logged.run();
    assert_eq!(report.counters.commits.size, 8);
    assert_eq!(report.counters.commits.end, 1);
    assert_eq!(report.commits, 9);
    let log = report.counters.log;
    // The log's first chunk, then a chunk for each commit.
    assert_eq!(log.requests.publish, 10);
    assert_eq!(log.carried, 0);
    assert_eq!(log.reliefs, 0);
    // Beside the batches, the writer adds headers, seals, commits and ends.
    let ratio = log.appended as f64 / logged.frame_bytes() as f64;
    assert!((1.0..1.04).contains(&ratio), "{ratio}");
}

#[test]
fn every_batch_of_a_run_logs_a_frame_of_one_size() {
    let logged = Logged::try_new(cores(), memory_wal(), 3, 1_000, None).unwrap();
    assert_eq!(logged.frame_bytes(), 3 * Logged::frame_len(1_000));
    assert!(Logged::frame_len(1_000) < Logged::frame_len(2_000));
}
