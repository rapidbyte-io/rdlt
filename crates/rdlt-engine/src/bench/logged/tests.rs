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
    // Every batch of a run is of one size, so its frame is too.
    assert_eq!(logged.frame_bytes(), 16 * Logged::frame_len(1_000));
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
    assert_eq!(read.chunks, 1);
    assert!(read.logged >= logged.frame_bytes(), "{read:?}");
    // The chunk is read whole once, as the fence takes the log, and its batch frames once more.
    assert!(
        read.read > read.logged && read.read < 2 * read.logged,
        "{read:?}"
    );
    // The replay deletes its fence, so the log reads the same again.
    assert_eq!(
        crate::bench::replay_scan(dir.path(), "logged").unwrap(),
        read
    );
}

#[test]
fn a_log_under_a_bound_of_twice_its_frames_spans_chunks_each_read_once() {
    let dir = tempfile::tempdir().unwrap();
    let store: Arc<dyn WalStore> = Arc::new(LocalWal::new(dir.path()));
    let bound = NonZeroU64::new(Logged::frame_len(1_000) * 16 * 2).unwrap();
    let logged = Logged::limited(cores(), (store, bound), 16, 1_000, None).unwrap();
    assert_eq!(logged.leave().commits, 0);
    let read = crate::bench::replay_scan(dir.path(), "logged").unwrap();
    assert_eq!(read.batches, 16, "{read:?}");
    assert!(read.chunks > 4, "{read:?}");
    assert!(read.read < 2 * read.logged, "{read:?}");
}
