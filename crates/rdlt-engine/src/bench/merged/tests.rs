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
    let ids: Vec<u64> = pushes
        .iter()
        .flat_map(|push| push.split(|byte| *byte == b'\n'))
        .filter(|line| !line.is_empty())
        .map(|line| {
            let row: serde_json::Value = serde_json::from_slice(line).unwrap();
            row["id"].as_u64().unwrap()
        })
        .collect();
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

/// The rows of each key's last row of each push of the first `rows` orders, each push a flush:
/// the root rows and their child rows, an item and its two tags for each of a row's items.
fn last_rows(rows: u64) -> (u64, u64) {
    let (mut roots, mut children, mut first) = (0, 0, 0);
    for push in Merged::pushes(rows) {
        let mut last = BTreeMap::new();
        let lines = push
            .split(|byte| *byte == b'\n')
            .filter(|line| !line.is_empty());
        for (index, line) in (first..).zip(lines) {
            let row: serde_json::Value = serde_json::from_slice(line).unwrap();
            last.insert(row["id"].as_u64().unwrap(), index);
            first = index + 1;
        }
        roots += u64::try_from(last.len()).unwrap();
        children += last.values().map(|index| index % 4 * 3).sum::<u64>();
    }
    (roots, children)
}

#[test]
fn a_merge_stages_each_keys_last_row_of_a_flush_and_its_children() {
    let cores = Cores::new(NonZeroUsize::new(2).unwrap());
    // A flush of two thousand rows is one piece, and one of twelve thousand several.
    for rows in [2_000, 12_000] {
        let merged = Merged::try_new(cores, rows).unwrap();
        let (roots, children) = last_rows(rows);
        assert_eq!(roots, merged.keys(), "one push");
        assert!(children < Normalized::loaded(rows) - rows);
        let (report, staging) = merged.run();
        let expected = Staging {
            roots,
            children,
            keys: merged.keys(),
            commits: 1,
        };
        assert_eq!(staging, expected, "{rows} rows");
        assert_eq!(report.rows, roots + children);
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
