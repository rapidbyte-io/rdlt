use std::collections::{BTreeMap, BTreeSet};
use std::num::NonZeroUsize;

use super::{Merged, Staging};
use crate::Cores;
use crate::bench::Normalized;

#[test]
fn a_run_s_ids_repeat_hot_keys_and_are_the_same_in_any_process() {
    let pushes = Merged::pushes(120_000);
    assert_eq!(pushes.len(), 3);
    assert_eq!(pushes, Merged::pushes(120_000));
    let ids = ids(&pushes);
    assert_eq!(ids.len(), 120_000);
    assert!(ids.iter().all(|id| *id < Merged::KEYS.get()));
    let distinct: BTreeSet<u64> = ids.iter().copied().collect();
    let merged = Merged::try_new(Cores::new(NonZeroUsize::new(2).unwrap()), 120_000).unwrap();
    assert_eq!(merged.keys(), u64::try_from(distinct.len()).unwrap());
    let bytes: usize = pushes.iter().map(bytes::Bytes::len).sum();
    assert_eq!(merged.bytes(), u64::try_from(bytes).unwrap());
    assert!(merged.keys() < 60_000, "{} keys", merged.keys());
    let hottest = ids.iter().filter(|id| **id == 0).count();
    assert!(hottest > 6_000, "the hottest key {hottest} times");
}

/// The id of each row of `pushes`, in order.
fn ids(pushes: &[bytes::Bytes]) -> Vec<u64> {
    pushes
        .iter()
        .flat_map(|push| push.split(|byte| *byte == b'\n'))
        .filter(|line| !line.is_empty())
        .map(|line| {
            let row: serde_json::Value = serde_json::from_slice(line).unwrap();
            row["id"].as_u64().unwrap()
        })
        .collect()
}

#[test]
fn a_merge_stages_each_keys_last_row_of_a_flush_and_only_its_children() {
    let cores = Cores::new(NonZeroUsize::new(2).unwrap());
    let merged = Merged::try_new(cores, 2_000).unwrap();
    // One flush: each key's last row, and its items with their tags, three child rows an item.
    let mut last = BTreeMap::new();
    for (row, id) in ids(&Merged::pushes(2_000)).into_iter().enumerate() {
        last.insert(id, u64::try_from(row).unwrap());
    }
    let children: u64 = last.values().map(|row| 3 * (row % 4)).sum();
    assert!(children < Normalized::loaded(2_000) - 2_000);
    for _ in 0..2 {
        let (report, staging) = merged.run();
        assert_eq!(report.commits, 1);
        let keys = merged.keys();
        assert!(keys < 2_000, "{keys} keys");
        let expected = Staging {
            roots: keys,
            children,
            keys,
            commits: 1,
        };
        assert_eq!(staging, expected);
        assert_eq!(report.rows, keys + children);
    }
}

#[test]
fn what_a_merge_staged_reads_a_commit_at_a_time() {
    let staging = Staging {
        roots: 30,
        children: 91,
        keys: 20,
        commits: 2,
    };
    assert_eq!(
        staging.to_string(),
        "staged root rows 15, staged child rows 46 and distinct keys 10 a commit"
    );
}
