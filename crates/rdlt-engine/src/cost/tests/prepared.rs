//! Preparing a batch in every mode, a merge's compaction, a history table's hashes and a change
//! stream's flags among them, allocates no more at its peak than the write path reserved for
//! lowering it.

use super::lowering::{SLACK, peak};
use crate::fixtures::lowering::{Case, LoweringCase, Storage};

/// Rows of each case's batch: enough that what an array takes beside its values is little of it.
const ROWS: u32 = 2_048;

#[test]
fn preparing_a_batch_in_any_mode_allocates_no_more_than_its_charge() {
    let mut beyond = Vec::new();
    for case in LoweringCase::ALL {
        for storage in [Storage::Native, Storage::Text] {
            let built = Case::stored(case, ROWS, storage);
            let charge = built.charge();
            let (prepared, peak) = peak(|| built.prepare());
            let prepared = prepared.unwrap();
            assert_eq!(prepared.batch.num_rows(), built.kept().rows, "{case:?}");
            if peak > charge + SLACK {
                beyond.push(format!(
                    "{} stored {storage:?}: charged {charge}, allocated {peak}",
                    case.name()
                ));
            }
        }
    }
    assert!(beyond.is_empty(), "{}", beyond.join("\n"));
}
