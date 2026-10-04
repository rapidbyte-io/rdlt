//! What a log costs as it ages: the objects it keeps and the requests a commit, a replay's
//! listing and a removal make stay those of its live chunks, however many came before.

use bytes::Bytes;
use rdlt_testkit::objects::{Call, Op, faultless};

use super::{chunk, keys, objects, opened, options, pipeline};
use crate::wal::{Chunk, WalStore};

/// The requests `calls` made since the first `from`.
fn since(calls: &[Call], from: usize) -> Vec<Op> {
    calls[from..].iter().map(|call| call.op).collect()
}

/// Publishes two thousand chunks of `load`'s log of `orders` in `wal`, every other in parts of four
/// bytes, removing each one's predecessor as the writer does: the requests each made.
async fn aged(
    wal: &dyn WalStore,
    objects: &rdlt_testkit::objects::Faulty<object_store::memory::InMemory>,
    orders: &rdlt_connector::PipelineId,
    load: rdlt_connector::LoadId,
) -> Vec<Vec<Op>> {
    wal.open_log(orders, load).await.expect("opens");
    let mut costs = Vec::new();
    for number in 0..2_000 {
        let before = objects.calls().len();
        let mut staged = wal
            .stage(orders, Chunk { load, number })
            .await
            .expect("stages");
        let bytes: &'static [u8] = if number % 2 == 0 { b"abc" } else { b"in parts" };
        staged
            .append(Bytes::from_static(bytes))
            .await
            .expect("appends");
        staged.publish().await.expect("publishes");
        // As the writer does: the chunk before is needed no more once this one is published.
        if let Some(previous) = number.checked_sub(1) {
            wal.remove(
                orders,
                Chunk {
                    load,
                    number: previous,
                },
            )
            .await
            .expect("removes");
        }
        costs.push(since(&objects.calls(), before));
    }
    costs
}

#[tokio::test]
async fn thousands_of_commits_leave_only_the_live_chunk_and_cost_what_the_first_did() {
    let objects = objects(faultless());
    let wal = opened(&objects, options(4)).await;
    let orders = pipeline("aged");
    let load = chunk(1, 0).load;
    let costs = aged(&wal, &objects, &orders, load).await;
    // A commit lists nothing, and costs the same at the last as at the second and third.
    for cost in &costs {
        assert!(!cost.contains(&Op::List), "{cost:?}");
    }
    assert_eq!(
        costs[1_998].len(),
        costs[2].len(),
        "{:?} {:?}",
        costs[2],
        costs[1_998]
    );
    assert_eq!(
        costs[1_999].len(),
        costs[1].len(),
        "{:?} {:?}",
        costs[1],
        costs[1_999]
    );
    // The log keeps its live chunk's head and body, and its mark.
    let kept: Vec<String> = keys(&objects).await;
    assert_eq!(kept.len(), 3, "{kept:?}");
    let before = objects.calls().len();
    assert_eq!(
        wal.chunks(&orders, load).await.expect("lists"),
        [(1_999, 8)]
    );
    let listed = since(&objects.calls(), before);
    assert_eq!(
        listed.iter().filter(|op| **op == Op::List).count(),
        1,
        "{listed:?}"
    );
    assert!(listed.len() <= 2, "{listed:?}");
    let before = objects.calls().len();
    wal.remove_log(&orders, load).await.expect("removes");
    let removed = since(&objects.calls(), before);
    assert!(removed.len() <= 4, "{removed:?}");
    assert_eq!(keys(&objects).await, Vec::<String>::new());
}
