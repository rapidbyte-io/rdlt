use std::collections::BTreeSet;
use std::num::{NonZeroU64, NonZeroUsize};

use arrow_array::cast::AsArray;
use arrow_array::types::Int64Type;

use super::{Load, Loading, ids};
use crate::Cores;
use crate::bench::{Sinking, sink_factory};

fn loading(load: Load, rows: u64, commit: Option<u64>) -> Loading {
    let cores = Cores::from_count(NonZeroUsize::new(2).unwrap());
    let per_batch = NonZeroU64::new(10).unwrap();
    Loading::try_new(
        cores,
        load,
        rows,
        per_batch,
        commit.and_then(NonZeroU64::new),
    )
    .unwrap()
}

#[test]
fn a_merge_updates_the_first_tenth_and_adds_the_rest_scattered() {
    let merged = ids(Load::Update, 100);
    let set: BTreeSet<i64> = merged.iter().copied().collect();
    let expected: BTreeSet<i64> = (0..10).chain(100..190).collect();
    assert_eq!(set, expected);
    assert_eq!(merged.len(), 100);
    // Each half holds half the updates.
    for half in merged.chunks(50) {
        assert_eq!(half.iter().filter(|id| **id < 10).count(), 5);
    }
    assert_eq!(ids(Load::Append, 5), [0, 1, 2, 3, 4]);
    assert_eq!(ids(Load::Merge, 5), [0, 1, 2, 3, 4]);
}

#[test]
fn a_load_commits_every_so_many_rows_or_once() {
    for (commit, commits) in [(None, 1), (Some(20), 3)] {
        let load = loading(Load::Append, 55, commit);
        assert_eq!(load.batches().len(), 6);
        assert_eq!(load.rows(), 55);
        let sink = load.connect(sink_factory().as_ref(), Sinking::Discard.config());
        let report = load.run(sink);
        assert_eq!(report.rows, 55);
        assert!(report.commits >= commits, "{commit:?}: {}", report.commits);
        if commit.is_none() {
            assert_eq!(report.commits, 1);
        }
    }
}

#[test]
fn an_update_s_rows_change_every_value_but_their_key() {
    let merge = loading(Load::Update, 30, None);
    let append = loading(Load::Append, 30, None);
    let first = &merge.batches()[0];
    let id = first.column(0).as_primitive::<Int64Type>().value(0);
    let a = first.column(1).as_primitive::<Int64Type>().value(0);
    assert_eq!(a, id * 7 + 1);
    let appended = append.batches()[0]
        .column(1)
        .as_primitive::<Int64Type>()
        .value(0);
    assert_eq!(appended, 0);
    let sink = merge.connect(sink_factory().as_ref(), Sinking::Ipc.config());
    assert_eq!(merge.run(sink).rows, 30);
    let fresh = loading(Load::Merge, 30, None);
    let a = fresh.batches()[0]
        .column(1)
        .as_primitive::<Int64Type>()
        .value(1);
    assert_eq!(a, 7);
    let sink = fresh.connect(sink_factory().as_ref(), Sinking::Discard.config());
    assert_eq!(fresh.run(sink).rows, 30);
}
