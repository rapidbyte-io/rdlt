//! A load that stalls while another attempt takes its log over, replays it and removes it: once
//! it wakes it never publishes or answers a commit again, and nothing of it is left to replay.

use std::sync::Arc;

use super::super::super::local::LocalWal;
use super::super::super::memory::MemoryWal;
use super::super::super::scan;
use super::super::super::store::WalStore;
use super::super::super::taken::{self, Taken};
use super::super::WalWriter;
use super::{Driving, load, owner, pipeline, table};
use crate::error::ErrorKind;

/// What a scan reads frames of.
const FRAME_BYTES: u64 = 1 << 28;

/// Takes the log over as a replay does, then releases and removes it: the commits it found
/// pending.
async fn replayed(store: &dyn WalStore) -> usize {
    let Taken::Fenced { number } = taken::take(store, &pipeline(), load(), FRAME_BYTES)
        .await
        .expect("takes")
    else {
        panic!("the log holds a commit to replay");
    };
    let scanned = scan::scan(store, &pipeline(), load(), FRAME_BYTES)
        .await
        .expect("scans");
    let pending = scanned.pending().count();
    assert!(
        taken::release(store, &pipeline(), load(), number)
            .await
            .expect("releases")
    );
    store
        .remove_log(&pipeline(), load())
        .await
        .expect("removes");
    pending
}

/// Runs the stall over `store`, segment 3 left open across the first commit where `open`.
async fn stalled(store: Arc<dyn WalStore>, open: bool) {
    store
        .open_log(&pipeline(), load())
        .await
        .expect("the log opens");
    let tally = Arc::default();
    let (writer, task) =
        WalWriter::start(Arc::clone(&store), owner(), u64::MAX, Arc::clone(&tally));
    let mut log = Driving::new(writer, tally);
    let replayer = Arc::clone(&store);
    let drive = async move {
        log.send(table(0)).await;
        log.batch(1, 0).await;
        if open {
            log.batch(3, 0).await;
        }
        log.commit(1, &[1]).await.expect("durable");
        log.committed(1).await;
        // The load stalls; its receipt is in no chunk yet, so the replay finds commit 1 pending.
        assert_eq!(replayed(replayer.as_ref()).await, 1);
        log.batch(2, 0).await;
        let answered = log.commit(2, &[2]).await;
        let error = answered.expect_err("the load is fenced");
        assert_eq!(error.kind(), ErrorKind::Fenced, "{error}");
        assert_eq!(error.code(), Some("wal_fenced"), "{error}");
        assert_eq!(replayer.loads(&pipeline()).await.expect("lists"), []);
        assert_eq!(replayer.leftovers(&pipeline()).await.expect("lists"), []);
        assert_eq!(
            replayer.chunks(&pipeline(), load()).await.expect("lists"),
            []
        );
    };
    let (_, ()) = tokio::join!(task, drive);
}

#[tokio::test]
async fn a_load_whose_log_a_replay_took_and_removed_never_answers_a_commit_again() {
    for open in [false, true] {
        stalled(Arc::new(MemoryWal::default()), open).await;
        let base = tempfile::tempdir().expect("a temporary directory");
        stalled(Arc::new(LocalWal::new(base.path())), open).await;
    }
}
