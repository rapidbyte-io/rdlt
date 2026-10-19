//! What a source is told is committed, against the commits that had landed when it was told,
//! and each commit's horizon, whatever order partitions seal and end in.

use std::sync::Arc;

use proptest::prelude::*;
use rdlt_connector::{Cursor, PartitionId, PartitionState, StateChange, StateEntry};

use super::{Setup, cursor, ended, partition, stream};
use crate::partition::Progress;
use crate::plan::WriteMode;
use crate::wal::memory::MemoryWal;

/// The most partitions a case reads.
const PARTITIONS: usize = 3;

/// What a partition sends the coordinator next.
#[derive(Clone, Debug)]
enum Step {
    /// Partition `.0` seals a segment of `.1` rows at its next cursor.
    Sealed(usize, u64),
    /// Partition `.0` seals its last segment, of `.1` rows, done.
    Done(usize, u64),
}

fn step() -> impl Strategy<Value = Step> {
    prop_oneof![
        4 => (0..PARTITIONS, 0_u64..=600).prop_map(|(at, rows)| Step::Sealed(at, rows)),
        1 => (0..PARTITIONS, 0_u64..=600).prop_map(|(at, rows)| Step::Done(at, rows)),
    ]
}

/// The partition `change` records, and where it records it.
fn recorded(change: &StateChange) -> Option<(PartitionId, PartitionState)> {
    let StateChange::Put(record) = change else {
        return None;
    };
    match StateEntry::from_record(record) {
        Ok(StateEntry::Partition {
            partition, state, ..
        }) => Some((partition, state)),
        _ => None,
    }
}

/// A runtime on the paused clock, for a property's cases.
fn paused() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .start_paused(true)
        .build()
        .expect("a runtime")
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(rdlt_testkit::cases(64)))]

    /// Every cursor a source is told is committed is one its partition sealed, and no later than
    /// where the commits that had landed when it was told leave the partition, a partition done
    /// being past every cursor; and each commit's horizon is no earlier than its predecessor's.
    #[test]
    fn a_source_is_told_only_cursors_a_landed_commit_covers_and_horizons_only_move_on(
        partitions in 1..=PARTITIONS,
        logged in any::<bool>(),
        steps in proptest::collection::vec(step(), 0..16),
    ) {
        let ids: Vec<PartitionId> = (0..partitions)
            .map(|at| PartitionId::parse(format!("p{at}")).expect("a valid partition"))
            .collect();
        let mut setup = Setup::new(
            vec![stream(WriteMode::Append, None, partitions)],
            ids.iter().map(|id| partition(id.as_str(), false)).collect(),
        );
        if logged {
            setup.wal = Some(Arc::new(MemoryWal::default()));
        }
        let (commits, acks, heard, sealed) = paused().block_on(async {
            let (task, harness) = setup.start().await;
            let mut sealed: Vec<Vec<Cursor>> = vec![Vec::new(); partitions];
            let mut done = vec![false; partitions];
            for at in 0..partitions {
                harness.send(Progress::Started { partition: at });
            }
            for (segment, step) in (1_u64..).zip(steps) {
                let (at, rows, last) = match step {
                    Step::Sealed(at, rows) => (at, rows, false),
                    Step::Done(at, rows) => (at, rows, true),
                };
                if at >= partitions || done[at] {
                    continue;
                }
                let state = if last {
                    done[at] = true;
                    PartitionState::Done
                } else {
                    let next = cursor(u64::try_from(sealed[at].len()).expect("few") + 1);
                    sealed[at].push(next.clone());
                    PartitionState::Cursor(next)
                };
                harness.seal(at, segment, rows, state, None);
            }
            for at in 0..partitions {
                harness.end(at, false);
            }
            ended(task).await.expect("the coordinator ends");
            let commits = harness.commits.lock().clone();
            let acks = harness.acks.lock().clone();
            let heard = harness.heard.lock().clone();
            (commits, acks, heard, sealed)
        });
        prop_assert_eq!(acks.len(), heard.len());
        for ((_, told), landed) in acks.iter().zip(&heard) {
            for (id, cursor) in told {
                let at = ids.iter().position(|known| known == id).expect("a partition of the case");
                let told_at = sealed[at].iter().position(|sealed| sealed == cursor);
                prop_assert!(told_at.is_some(), "{id} was told a cursor it never sealed");
                let committed = commits[..*landed]
                    .iter()
                    .flat_map(|commit| commit.state_delta.iter().filter_map(recorded))
                    .filter(|(partition, _)| partition == id)
                    .map(|(_, state)| state)
                    .last();
                match committed {
                    Some(PartitionState::Cursor(standing)) => {
                        let standing_at = sealed[at].iter().position(|sealed| *sealed == standing);
                        prop_assert!(told_at <= standing_at, "{id} was told a cursor ahead of its commit");
                    }
                    Some(PartitionState::Done) => {}
                    None => prop_assert!(false, "{id} was told before any commit recorded it"),
                }
            }
        }
        for pair in commits.windows(2) {
            prop_assert!(pair[0].horizon.is_some() && pair[0].horizon <= pair[1].horizon);
        }
    }
}
