use std::collections::BTreeSet;
use std::num::NonZeroUsize;

use bytes::Bytes;

use super::{Merged, Staging};
use crate::bench::Normalized;
use crate::shred::Sizing;
use crate::{BatchPolicy, Cores};

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

/// The root rows a merge of `pushes`, each a flush, stages within `cores`: a row for each key of
/// each chunk the engine cuts a flush into, whose batch is a unit of its own.
fn staged_roots(pushes: &[Bytes], cores: Cores) -> u64 {
    let sizing = Sizing::new(BatchPolicy::default().chunk_bytes(), cores.count());
    let mut roots = 0;
    for push in pushes {
        let chunk_bytes = sizing.chunk_bytes(std::slice::from_ref(push));
        // A chunk of lines ends at the first line end at least its bytes past its start.
        let (mut start, mut at, mut keys) = (0, 0, BTreeSet::new());
        for line in push.split_inclusive(|byte| *byte == b'\n') {
            let row: serde_json::Value = serde_json::from_slice(line).unwrap();
            keys.insert(row["id"].as_u64().unwrap());
            at += line.len();
            if at - 1 >= start + chunk_bytes || at == push.len() {
                roots += keys.len() as u64;
                (start, keys) = (at, BTreeSet::new());
            }
        }
    }
    roots
}

#[test]
fn a_merge_stages_a_root_row_a_key_of_each_unit_and_every_child() {
    let cores = Cores::new(NonZeroUsize::new(2).unwrap());
    let merged = Merged::try_new(cores, 2_000).unwrap();
    let roots = staged_roots(&Merged::pushes(2_000), cores);
    for _ in 0..2 {
        let (report, staging) = merged.run();
        assert_eq!(report.commits, 1);
        let keys = merged.keys();
        assert!(keys < 2_000, "{keys} keys");
        assert!(roots >= keys, "{roots} roots of {keys} keys");
        let children = Normalized::loaded(2_000) - 2_000;
        let expected = Staging {
            roots,
            children,
            keys,
            commits: 1,
        };
        assert_eq!(staging, expected);
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
