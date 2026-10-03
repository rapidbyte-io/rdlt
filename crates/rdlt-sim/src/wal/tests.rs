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

/// Stages `bytes` as `chunk` of `owner`'s log in `wal` and publishes it.
async fn published(wal: &SimWal, owner: &PipelineId, chunk: Chunk, bytes: &'static [u8]) {
    let mut staged = wal.stage(owner, chunk).await.expect("stages");
    staged
        .append(Bytes::from_static(bytes))
        .await
        .expect("appends");
    staged.publish().await.expect("publishes");
}

#[tokio::test]
async fn a_crash_keeps_every_chunk_published_and_loses_every_one_staged() {
    let wal = SimWal::default();
    let other = PipelineId::parse("users").expect("a valid pipeline");
    published(&wal, &pipeline(), chunk(0), b"durable").await;
    let mut lost = wal.stage(&pipeline(), chunk(1)).await.expect("stages");
    lost.append(Bytes::from_static(b"staged"))
        .await
        .expect("appends");
    let mut kept = wal.stage(&other, chunk(0)).await.expect("stages");
    kept.append(Bytes::from_static(b"another's"))
        .await
        .expect("appends");
    wal.crash(&pipeline());
    assert!(lost.publish().await.is_err(), "staged before the crash");
    kept.publish()
        .await
        .expect("another pipeline's worker runs on");
    assert_eq!(
        wal.chunks(&pipeline(), chunk(0).load).await.expect("lists"),
        [(0, 7)]
    );
    // What the restarted worker stages publishes.
    published(&wal, &pipeline(), chunk(1), b"again").await;
}

#[tokio::test]
async fn a_log_s_removal_goes_by_number_and_a_crash_part_way_leaves_the_highest() {
    let wal = SimWal::default();
    for number in [0, 1, 2] {
        published(&wal, &pipeline(), chunk(number), b"chunk").await;
    }
    let orders = pipeline();
    let mut removal = wal.remove_log(&orders, chunk(0).load);
    let mut context = std::task::Context::from_waker(std::task::Waker::noop());
    // The removal stops at its first yield, as a crash there leaves it.
    assert!(removal.as_mut().poll(&mut context).is_pending());
    drop(removal);
    assert_eq!(
        wal.chunks(&pipeline(), chunk(0).load).await.expect("lists"),
        [(1, 5), (2, 5)]
    );
}

#[tokio::test]
async fn a_faulty_disk_fails_now_and_then_and_a_mended_one_never() {
    let wal = SimWal::default();
    wal.set_faults(Some(SplitMix64::new(7)));
    let mut failed = 0;
    for _ in 0..1000 {
        failed += usize::from(wal.remove(&pipeline(), chunk(0)).await.is_err());
    }
    assert!((1..100).contains(&failed), "{failed} of 1000 failed");
    wal.set_faults(None);
    for _ in 0..100 {
        wal.remove(&pipeline(), chunk(0))
            .await
            .expect("never fails");
    }
}

#[tokio::test]
async fn it_holds_logs_until_every_one_is_removed() {
    let wal = SimWal::default();
    assert!(!wal.holds_logs());
    published(&wal, &pipeline(), chunk(0), b"frame").await;
    assert!(wal.holds_logs());
    wal.remove_log(&pipeline(), chunk(0).load)
        .await
        .expect("removes");
    assert!(!wal.holds_logs());
}

#[tokio::test]
async fn a_faulty_disk_fails_a_publish_or_a_removal_now_and_then_and_keeps_the_chunk() {
    let wal = SimWal::default();
    wal.set_faults(Some(SplitMix64::new(7)));
    let (mut unpublished, mut unremoved) = (0, 0);
    for number in 0..1000 {
        let chunk = chunk(number);
        let Ok(mut staged) = wal.stage(&pipeline(), chunk).await else {
            continue;
        };
        staged
            .append(Bytes::from_static(b"frame"))
            .await
            .expect("appends");
        if staged.publish().await.is_err() {
            unpublished += 1;
            continue;
        }
        if wal.remove(&pipeline(), chunk).await.is_err() {
            unremoved += 1;
            let kept = wal.chunks(&pipeline(), chunk.load).await.expect("lists");
            assert!(kept.iter().any(|(kept, _)| *kept == number));
        }
    }
    assert!(
        (1..100).contains(&unpublished),
        "{unpublished} of 1000 failed"
    );
    assert!((1..100).contains(&unremoved), "{unremoved} of 1000 failed");
}
