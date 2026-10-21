use std::time::UNIX_EPOCH;

use bytes::Bytes;
use rdlt_connector::{LoadId, PipelineId};
use rdlt_engine::{Chunk, WalStore};

use super::{READS, SimWal};
use crate::oracle::charged;
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

/// Opens `load`'s log of `owner` in `wal` where it is not open yet.
async fn opened(wal: &SimWal, owner: &PipelineId, load: LoadId) {
    if !wal.loads(owner).await.expect("lists").contains(&load) {
        wal.open_log(owner, load).await.expect("opens");
    }
}

/// Stages `bytes` as `chunk` of `owner`'s log in `wal`, opening it where it is not, and publishes
/// it.
async fn published(wal: &SimWal, owner: &PipelineId, chunk: Chunk, bytes: &'static [u8]) {
    opened(wal, owner, chunk.load).await;
    let mut staged = wal.stage(owner, chunk).await.expect("stages");
    staged
        .append(Bytes::from_static(bytes))
        .await
        .expect("appends");
    staged.publish().await.expect("publishes");
}

/// The chunks of `load`'s log of `owner` in `wal`, listed again where a faulty disk fails it.
async fn listed(wal: &SimWal, owner: &PipelineId, load: LoadId) -> Vec<(u64, u64)> {
    for _ in 0..64 {
        if let Ok(chunks) = wal.chunks(owner, load).await {
            return chunks;
        }
    }
    panic!("a disk failing one listing in a hundred failed 64 in a row");
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
    opened(&wal, &other, chunk(0).load).await;
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
async fn a_log_s_removal_closes_it_first_and_a_crash_part_way_leaves_what_leftovers_list() {
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
    assert_eq!(wal.loads(&orders).await.expect("lists"), []);
    assert_eq!(
        wal.leftovers(&orders).await.expect("lists"),
        [chunk(0).load]
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
    opened(&wal, &pipeline(), chunk(0).load).await;
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
            let kept = listed(&wal, &pipeline(), chunk.load).await;
            assert!(kept.iter().any(|(kept, _)| *kept == number));
        }
    }
    assert!(
        (1..100).contains(&unpublished),
        "{unpublished} of 1000 failed"
    );
    assert!((1..100).contains(&unremoved), "{unremoved} of 1000 failed");
}

#[tokio::test]
async fn a_published_chunk_counts_as_the_worlds_until_it_is_removed() {
    let wal = SimWal::default();
    let before = charged();
    published(&wal, &pipeline(), chunk(0), b"durable").await;
    assert!(charged() - before >= 7, "{}", charged() - before);
    wal.remove(&pipeline(), chunk(0)).await.expect("removes");
    assert_eq!(charged(), before);
}

#[tokio::test]
async fn a_staged_chunk_counts_as_the_worlds_until_it_is_dropped() {
    let wal = SimWal::default();
    let before = charged();
    opened(&wal, &pipeline(), chunk(0).load).await;
    let mut staged = wal.stage(&pipeline(), chunk(0)).await.expect("stages");
    staged
        .append(Bytes::from(vec![1_u8; 1_000]))
        .await
        .expect("appends");
    staged
        .append(Bytes::from(vec![2_u8; 10]))
        .await
        .expect("appends");
    assert_eq!(charged() - before, 2_000);
    drop(staged);
    assert_eq!(charged(), before);
}

#[tokio::test]
async fn a_faulty_disk_fails_a_listing_a_read_or_a_staging_s_removal_now_and_then() {
    let wal = SimWal::default();
    let orders = pipeline();
    let load = chunk(0).load;
    published(&wal, &orders, chunk(0), b"frame").await;
    wal.set_faults(Some(SplitMix64::new(7)));
    let mut failed = [0_usize; 5];
    for _ in 0..5000 {
        failed[0] += usize::from(wal.loads(&orders).await.is_err());
        failed[1] += usize::from(wal.leftovers(&orders).await.is_err());
        failed[2] += usize::from(wal.chunks(&orders, load).await.is_err());
        failed[3] += usize::from(wal.read(&orders, chunk(0), 0, 5).await.is_err());
        failed[4] += usize::from(wal.remove_staged(&orders, load).await.is_err());
    }
    assert!(
        failed.iter().all(|failed| (1..250).contains(failed)),
        "{failed:?} of 5000 failed"
    );
    assert!(
        failed[3] < failed[2],
        "reads fail less often than listings: {failed:?}"
    );
    wal.set_faults(None);
    assert_eq!(wal.chunks(&orders, load).await.expect("lists"), [(0, 5)]);
    let read = wal.read(&orders, chunk(0), 0, 5).await.expect("reads");
    assert_eq!(read, Bytes::from_static(b"frame"));
}

fn hiccuped(error: Option<std::io::Error>) -> bool {
    error.is_some_and(|error| error.kind() == std::io::ErrorKind::Interrupted)
}

#[tokio::test]
async fn a_disk_s_writes_draw_as_their_generator_does_and_every_read_from_another_stream() {
    let orders = pipeline();
    let load = chunk(0).load;
    let wals: [SimWal; 8] = std::array::from_fn(|_| SimWal::default());
    for (number, wal) in wals.iter().enumerate() {
        if number != 6 {
            published(wal, &orders, chunk(0), b"frame").await;
        }
        wal.set_faults(Some(SplitMix64::new(7)));
    }
    let mut writes = SplitMix64::new(7);
    let apart = SplitMix64::new(SplitMix64::new(7).next_u64() ^ READS);
    let mut reads: [SplitMix64; 5] = std::array::from_fn(|_| apart.clone());
    let mut expected = [const { Vec::new() }; 8];
    let mut seen = [const { Vec::new() }; 8];
    for _ in 0..2000 {
        expected[0].push(writes.chance(10));
        for (draws, per_mille) in [10, 10, 10, 2, 10].into_iter().enumerate() {
            expected[draws + 1].push(reads[draws].chance(per_mille));
        }
        seen[0].push(wals[0].remove(&orders, chunk(0)).await.is_err());
        seen[1].push(wals[1].loads(&orders).await.is_err());
        seen[2].push(wals[2].leftovers(&orders).await.is_err());
        seen[3].push(wals[3].chunks(&orders, load).await.is_err());
        seen[4].push(wals[4].read(&orders, chunk(0), 0, 5).await.is_err());
        seen[5].push(wals[5].remove_staged(&orders, load).await.is_err());
        seen[6].push(hiccuped(wals[6].open_log(&orders, load).await.err()));
        seen[7].push(hiccuped(wals[7].stage(&orders, chunk(1)).await.err()));
    }
    for writes in [0, 6, 7] {
        assert_eq!(
            seen[writes], expected[0],
            "write {writes} draws as the generator does"
        );
    }
    for (draws, method) in ["loads", "leftovers", "chunks", "read", "remove_staged"]
        .into_iter()
        .enumerate()
    {
        assert_eq!(seen[draws + 1], expected[draws + 1], "{method} draws apart");
    }
}
