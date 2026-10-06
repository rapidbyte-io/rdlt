//! The store's contract, under each fault a request meets, in chunks whole and in parts.

use std::io;

use bytes::Bytes;
use rdlt_testkit::objects::{Call, Fault, Op, faultless};

use super::{chunk, every, keys, objects, opened, options, pipeline};
use crate::conformance;
use crate::wal::{Chunk, WalStore};

#[tokio::test]
async fn a_log_kept_in_parts_keeps_the_store_s_contract() {
    // Every chunk the contract writes is longer than a part of four bytes.
    let objects = objects(faultless());
    conformance::conforms(&opened(&objects, options(4)).await).await;
}

fn deletes(call: &Call) -> bool {
    call.op == Op::Delete
}

/// Whether `call` creates a log's mark.
fn marks(call: &Call) -> bool {
    call.op == (Op::Put { create: true }) && call.key.contains("/open/")
}

#[tokio::test(start_paused = true)]
async fn the_contract_holds_under_every_fault_a_retry_meets() {
    let faults = [Fault::Fail, Fault::Slow(3), Fault::Hang, Fault::Answerless];
    // A name answered taken at once is another's, which the contract's own races test. A log's
    // mark is created by one attempt, which fails the open where the attempt's outcome is unknown,
    // as the requests' tests show: the contract's opens are not faulted.
    for part in [1 << 20, 4] {
        for fault in faults {
            let objects = objects(faultless());
            let wal = opened(&objects, options(part)).await;
            objects.plan(every(3, fault, |call| !marks(call)));
            conformance::conforms(&wal).await;
        }
        // Deletions that fail, each tried again.
        let objects = objects(faultless());
        let wal = opened(&objects, options(part)).await;
        objects.plan(every(2, Fault::Fail, deletes));
        conformance::conforms(&wal).await;
    }
}

#[tokio::test(start_paused = true)]
async fn a_publish_racing_a_removal_deletes_the_head_it_created_once_its_log_closed() {
    // Each request waits a varying number of turns, so publishes and the removal interleave.
    let mut turn = 0_u32;
    let objects = objects(Box::new(move |_| {
        turn = (turn + 1) % 7;
        Fault::Slow(turn)
    }));
    let wal = opened(&objects, options(1 << 20)).await;
    let orders = pipeline("racing");
    let mut created_then_closed = 0;
    for round in 0..16 {
        let load = chunk(round, 0).load;
        wal.open_log(&orders, load).await.expect("opens");
        let mut stagings = Vec::new();
        for number in 0..4 {
            let mut staged = wal
                .stage(&orders, Chunk { load, number })
                .await
                .expect("stages");
            staged
                .append(Bytes::from_static(b"racing"))
                .await
                .expect("appends");
            stagings.push(staged);
        }
        let mut stagings = stagings.into_iter();
        let mut next = || stagings.next().expect("four stagings").publish();
        let removal = async {
            tokio::task::yield_now().await;
            wal.remove_log(&orders, load).await
        };
        let (zero, one, removed, two, three) =
            tokio::join!(next(), next(), removal, next(), next());
        removed.expect("removes");
        let calls = objects.calls();
        for (number, published) in [zero, one, two, three].into_iter().enumerate() {
            let Err(refused) = published else { continue };
            assert_eq!(refused.kind(), io::ErrorKind::NotFound, "{refused}");
            let head = format!("{number:08}.wal");
            let created = calls.iter().any(|call| {
                call.op == (Op::Put { create: true })
                    && call.key.contains(&load.to_string())
                    && call.key.ends_with(&head)
            });
            created_then_closed += usize::from(created);
        }
        assert!(
            keys(&objects)
                .await
                .iter()
                .all(|key| !key.contains(&load.to_string())),
            "round {round}: {:?}",
            keys(&objects).await
        );
    }
    assert!(
        created_then_closed > 0,
        "no publish created its head and found the log closed"
    );
}
