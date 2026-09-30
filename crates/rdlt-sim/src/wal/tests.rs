use std::time::UNIX_EPOCH;

use bytes::Bytes;
use rdlt_connector::{LoadId, PipelineId};
use rdlt_engine::{Chunk, WalStore};

use super::SimWal;
use crate::rng::SplitMix64;

fn pipeline() -> PipelineId {
    PipelineId::parse("orders").expect("a valid pipeline")
}

fn chunk(number: u64) -> Chunk {
    Chunk {
        load: LoadId::from_parts(UNIX_EPOCH, 1),
        number,
    }
}

#[tokio::test]
async fn a_crash_keeps_what_was_durable_and_at_most_the_rest() {
    for seed in 0..200 {
        let wal = SimWal::default();
        wal.append(&pipeline(), chunk(0), Bytes::from_static(b"durable"))
            .await
            .expect("appends");
        wal.sync(&pipeline(), chunk(0)).await.expect("syncs");
        wal.append(&pipeline(), chunk(0), Bytes::from_static(b" pending"))
            .await
            .expect("appends");
        wal.crash(&pipeline(), &mut SplitMix64::new(seed));
        let kept = wal
            .read(&pipeline(), chunk(0), 0, 100)
            .await
            .expect("reads");
        assert!(kept.starts_with(b"durable"), "seed {seed}: {kept:?}");
        assert!(kept.len() <= b"durable pending".len());
    }
}

#[tokio::test]
async fn a_log_has_one_claimant_until_it_lets_go() {
    let wal = SimWal::default();
    let load = chunk(0).load;
    let held = wal.claim(&pipeline(), load).await.expect("claims");
    assert!(held.is_some());
    assert!(
        wal.claim(&pipeline(), load)
            .await
            .expect("claims")
            .is_none()
    );
    drop(held);
    assert!(
        wal.claim(&pipeline(), load)
            .await
            .expect("claims")
            .is_some()
    );
}

#[tokio::test]
async fn a_faulty_disk_fails_now_and_then_and_a_mended_one_never() {
    let wal = SimWal::default();
    wal.set_faults(Some(SplitMix64::new(7)));
    let mut failed = 0;
    for _ in 0..1000 {
        failed += usize::from(wal.sync(&pipeline(), chunk(0)).await.is_err());
    }
    assert!((1..100).contains(&failed), "{failed} of 1000 failed");
    wal.set_faults(None);
    for _ in 0..100 {
        wal.sync(&pipeline(), chunk(0)).await.expect("never fails");
    }
}

#[tokio::test]
async fn it_holds_logs_until_every_one_is_removed() {
    let wal = SimWal::default();
    assert!(!wal.holds_logs());
    wal.append(&pipeline(), chunk(0), Bytes::from_static(b"frame"))
        .await
        .expect("appends");
    assert!(wal.holds_logs());
    wal.remove_log(&pipeline(), chunk(0).load)
        .await
        .expect("removes");
    assert!(!wal.holds_logs());
}

#[tokio::test]
async fn a_crash_leaves_every_other_pipeline_s_logs_as_they_were() {
    let wal = SimWal::default();
    let other = PipelineId::parse("users").expect("a valid pipeline");
    for owner in [pipeline(), other.clone()] {
        wal.append(&owner, chunk(0), Bytes::from_static(b"never synced"))
            .await
            .expect("appends");
    }
    for seed in 0..50 {
        wal.crash(&pipeline(), &mut SplitMix64::new(seed));
    }
    let kept = wal.read(&other, chunk(0), 0, 100).await.expect("reads");
    assert_eq!(&kept[..], b"never synced");
}
