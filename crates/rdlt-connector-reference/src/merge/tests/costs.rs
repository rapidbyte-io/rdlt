//! What a merge costs, and what it refuses: work in proportion to the rows it is given, whatever
//! they hold, and a typed refusal for a key it cannot merge by.

use std::sync::Arc;
use std::time::{Duration, Instant};

use arrow_array::{ArrayRef, BinaryArray, Int8Array, Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use rdlt_connector::{ChangeOp, Deletion, HistoryColumns, MergeKey, RootKey};

use super::super::Merged;
use super::super::refused::code;
use super::{key, merge, merge_children, soft, stored_schema};

fn position(seq: u64) -> Vec<u8> {
    let mut bytes = vec![0; 16];
    bytes[8..].copy_from_slice(&seq.to_be_bytes());
    bytes
}

/// A written batch of `ids`, each with its `seq`, all of `op`; a truncate's rows name no key.
fn changes(ids: &[Option<i64>], seqs: &[u64], op: ChangeOp) -> RecordBatch {
    let rows = seqs.len();
    let columns: Vec<(&str, ArrayRef)> = vec![
        ("id", Arc::new(Int64Array::from(ids.to_vec()))),
        ("value", Arc::new(StringArray::from(vec![Some("v"); rows]))),
        (
            "seq",
            Arc::new(BinaryArray::from_iter_values(
                seqs.iter().map(|seq| position(*seq)),
            )),
        ),
        ("at", Arc::new(Int64Array::from(vec![Some(7); rows]))),
        ("op", Arc::new(Int8Array::from(vec![op.code(); rows]))),
        (
            "unchanged",
            Arc::new(BinaryArray::from(vec![None::<&[u8]>; rows])),
        ),
    ];
    RecordBatch::try_from_iter(columns).expect("a valid batch")
}

/// The least of three timings of `work`, which a busy machine inflates least.
fn timed<T>(mut work: impl FnMut() -> T) -> Duration {
    (0..3)
        .map(|_| {
            let started = Instant::now();
            std::hint::black_box(work());
            started.elapsed()
        })
        .min()
        .expect("three timings")
}

/// A table of `rows` rows, sequenced from a million up.
fn table(rows: u64, deletion: &Deletion) -> Merged {
    let ids: Vec<Option<i64>> = (0..rows).map(|id| i64::try_from(id).ok()).collect();
    let seqs: Vec<u64> = (0..rows).map(|id| 1_000_000 + id).collect();
    let load = changes(&ids, &seqs, ChangeOp::Insert);
    merge(&stored_schema(), &[], &[], &[load], &key(deletion.clone())).expect("the rows merge")
}

/// How long merging `count` truncates into `published` takes, each sequenced from `first` up.
fn truncating(published: &Merged, deletion: &Deletion, first: u64, count: u64) -> Duration {
    let seqs: Vec<u64> = (0..count).map(|index| first + index).collect();
    let truncates = changes(&vec![None; seqs.len()], &seqs, ChangeOp::Truncate);
    let key = key(deletion.clone());
    timed(|| {
        merge(
            &stored_schema(),
            &published.rows,
            &published.tombstones,
            std::slice::from_ref(&truncates),
            &key,
        )
        .expect("the truncates merge")
    })
}

#[test]
fn truncates_cost_their_count_and_the_rows_not_their_product() {
    const ROWS: u64 = 20_000;
    const TRUNCATES: u64 = 2_000;
    for deletion in [Deletion::Hard, soft()] {
        let published = table(ROWS, &deletion);
        // Truncates before every row change nothing; those past every row remove or mark each.
        for first in [1, 2_000_000] {
            let one = truncating(&published, &deletion, first, 1);
            let many = truncating(&published, &deletion, first, TRUNCATES);
            assert!(
                many < one * 15,
                "{deletion:?} from {first}: one truncate took {one:?}, {TRUNCATES} took {many:?}"
            );
        }
    }
}

/// A schema of the key, the sequence, the deletion time and `width` data columns.
fn wide(width: usize) -> SchemaRef {
    let mut fields = vec![
        Field::new("id", DataType::Int64, true),
        Field::new("seq", DataType::Binary, false),
        Field::new("at", DataType::Int64, true),
    ];
    fields.extend((0..width).map(|column| Field::new(format!("c{column}"), DataType::Int64, true)));
    Arc::new(Schema::new(fields))
}

/// `rows` updates of distinct keys under [`wide`], each flagging every data column unchanged
/// where `flagged`.
fn updates(width: usize, rows: usize, flagged: bool) -> RecordBatch {
    let count = i64::try_from(rows).expect("a small count");
    let seqs = (0..rows).map(|row| position(row as u64 + 1));
    let mut columns: Vec<(String, ArrayRef)> = vec![
        (
            "id".into(),
            Arc::new(Int64Array::from_iter_values(0..count)),
        ),
        ("seq".into(), Arc::new(BinaryArray::from_iter_values(seqs))),
        ("at".into(), Arc::new(Int64Array::from(vec![None; rows]))),
    ];
    for column in 0..width {
        let values = Arc::new(Int64Array::from(vec![1; rows]));
        columns.push((format!("c{column}"), values));
    }
    let mut bitmap = vec![0_u8; (width + 3).div_ceil(8)];
    for field in 3..width + 3 {
        bitmap[field / 8] |= 1 << (field % 8);
    }
    let flags = vec![flagged.then_some(bitmap.as_slice()); rows];
    columns.push((
        "op".into(),
        Arc::new(Int8Array::from(vec![ChangeOp::Update.code(); rows])),
    ));
    columns.push(("unchanged".into(), Arc::new(BinaryArray::from(flags))));
    RecordBatch::try_from_iter(columns).expect("a valid batch")
}

#[test]
fn unchanged_flags_cost_a_mask_a_row_not_a_search_a_column() {
    const WIDTH: usize = 800;
    const ROWS: usize = 400;
    let (schema, key) = (wide(WIDTH), key(Deletion::Hard));
    let merging = |flagged: bool| {
        let batch = updates(WIDTH, ROWS, flagged);
        timed(|| merge(&schema, &[], &[], std::slice::from_ref(&batch), &key).expect("it merges"))
    };
    let (plain, flagged) = (merging(false), merging(true));
    assert!(
        flagged < plain * 12,
        "plain rows took {plain:?}, rows flagging every column {flagged:?}"
    );
}

fn plain_key() -> MergeKey {
    MergeKey {
        changes: None,
        ..key(Deletion::Hard)
    }
}

fn history_key(deletion: Option<Deletion>) -> MergeKey {
    let changes = key(deletion.clone().unwrap_or(Deletion::Hard)).changes;
    MergeKey {
        changes: deletion
            .and(changes)
            .map(|changes| rdlt_connector::ChangeColumns {
                unchanged: None,
                ..changes
            }),
        history: Some(HistoryColumns {
            valid_from: "at".into(),
            valid_to: "at".into(),
            is_current: "value".into(),
            row_hash: "seq".into(),
        }),
        ..plain_key()
    }
}

#[test]
fn a_key_the_rows_cannot_be_merged_by_is_refused() {
    let batch = changes(&[Some(1)], &[1], ChangeOp::Update);
    let keys = [
        plain_key(),
        key(Deletion::Hard),
        key(soft()),
        history_key(None),
        history_key(Some(Deletion::Hard)),
        history_key(Some(soft())),
    ];
    for key in keys {
        let broken = [
            MergeKey {
                columns: Vec::new(),
                ..key.clone()
            },
            MergeKey {
                columns: vec!["id".into(), "missing".into()],
                ..key.clone()
            },
            MergeKey {
                seq: "missing".into(),
                ..key.clone()
            },
        ];
        for key in broken {
            let refused = merge(
                &stored_schema(),
                &[],
                &[],
                std::slice::from_ref(&batch),
                &key,
            );
            let refused = refused.expect_err("the key merges nothing");
            assert_eq!(code(&refused), Some("merge_key_invalid"), "{key:?}");
        }
    }
    // A child table is keyed by its root id, the first of its key's columns.
    let root = RootKey {
        table: "roots".into(),
        id: "seq".into(),
        seq: "seq".into(),
    };
    for columns in [Vec::new(), vec!["missing".into()]] {
        let key = MergeKey {
            columns,
            root: Some(root.clone()),
            ..plain_key()
        };
        let children = std::slice::from_ref(&batch);
        let refused = merge_children(&stored_schema(), &[], children, &key, &root, children);
        let refused = refused.expect_err("the key merges nothing");
        assert_eq!(code(&refused), Some("merge_key_invalid"), "{key:?}");
    }
}

/// The columns of `batch` that are the same array as its column `first`.
fn shared_with(batch: &RecordBatch, first: &str) -> usize {
    let first = batch.column_by_name(first).expect("the column");
    batch
        .columns()
        .iter()
        .filter(|column| Arc::ptr_eq(column, first))
        .count()
}

#[test]
fn columns_a_row_never_had_are_one_shared_array_of_nulls() {
    const WIDTH: usize = 50;
    let schema = wide(WIDTH);
    let narrow = |ids: &[i64], seq: u64, op: Option<ChangeOp>| {
        let mut columns: Vec<(&str, ArrayRef)> = vec![
            ("id", Arc::new(Int64Array::from(ids.to_vec()))),
            (
                "seq",
                Arc::new(BinaryArray::from_iter_values(
                    ids.iter().map(|_| position(seq)),
                )),
            ),
        ];
        if let Some(op) = op {
            columns.push(("op", Arc::new(Int8Array::from(vec![op.code(); ids.len()]))));
        }
        RecordBatch::try_from_iter(columns).expect("a valid batch")
    };
    // Every data column and the deletion time are absent from the rows held and from those
    // merged in: each is the same array, whatever the merge.
    let absent = WIDTH + 1;
    let held = [narrow(&[1, 2, 3], 1, None)];
    let upserted = merge(
        &schema,
        &held,
        &[],
        &[narrow(&[3, 4], 2, None)],
        &plain_key(),
    )
    .unwrap();
    for batch in &upserted.rows {
        assert_eq!(shared_with(batch, "c0"), absent);
        assert_eq!(
            batch.column_by_name("c0").unwrap().null_count(),
            batch.num_rows()
        );
    }
    let key = MergeKey {
        changes: key(Deletion::Hard)
            .changes
            .map(|changes| rdlt_connector::ChangeColumns {
                unchanged: None,
                ..changes
            }),
        ..plain_key()
    };
    let incoming = narrow(&[3, 4], 2, Some(ChangeOp::Update));
    let changed = merge(&schema, &held, &[], &[incoming], &key).unwrap();
    let [batch] = &changed.rows[..] else {
        panic!("{changed:?}")
    };
    assert_eq!(batch.num_rows(), 4);
    assert_eq!(shared_with(batch, "c0"), absent);
}

#[test]
fn a_hard_truncate_costs_nothing_for_each_tombstone_it_leaves() {
    const KEYS: u64 = 20_000;
    const TRUNCATES: u64 = 2_000;
    // A delete of each key, none of which the table held: a tombstone each.
    let ids: Vec<Option<i64>> = (0..KEYS).map(|id| i64::try_from(id).ok()).collect();
    let seqs: Vec<u64> = (0..KEYS).map(|id| 1_000_000 + id).collect();
    let deletes = changes(&ids, &seqs, ChangeOp::Delete);
    let key = key(Deletion::Hard);
    let buried = merge(&stored_schema(), &[], &[], &[deletes], &key).expect("the deletes merge");
    assert_eq!(buried.tombstones[0].num_rows() as u64, KEYS);
    let (one, many) = (
        truncating(&buried, &Deletion::Hard, 1, 1),
        truncating(&buried, &Deletion::Hard, 1, TRUNCATES),
    );
    assert!(
        many < one * 15,
        "one truncate took {one:?}, {TRUNCATES} took {many:?}"
    );
}

#[test]
fn tombstones_a_merge_leaves_as_they_were_are_the_batch_it_was_given() {
    let key = key(Deletion::Hard);
    let deletes = changes(&[Some(1), Some(2)], &[5, 6], ChangeOp::Delete);
    let buried = merge(&stored_schema(), &[], &[], &[deletes], &key).expect("the deletes merge");
    let [stored] = &buried.tombstones[..] else {
        panic!("{buried:?}")
    };
    // A change of another key, one sequenced behind a tombstone, and a truncate behind the bound
    // change no tombstone.
    let unrelated = changes(&[Some(3), Some(1)], &[7, 4], ChangeOp::Insert);
    let merged = merge(
        &stored_schema(),
        &[],
        &buried.tombstones,
        &[unrelated],
        &key,
    )
    .unwrap();
    let [kept] = &merged.tombstones[..] else {
        panic!("{merged:?}")
    };
    for (kept, stored) in kept.columns().iter().zip(stored.columns()) {
        assert!(Arc::ptr_eq(kept, stored));
    }
    // One that lifts a tombstone leaves the others, assembled anew.
    let lifting = changes(&[Some(1)], &[9], ChangeOp::Insert);
    let merged = merge(&stored_schema(), &[], &buried.tombstones, &[lifting], &key).unwrap();
    assert_eq!(merged.tombstones[0].num_rows(), 1);
}

#[test]
fn a_delete_of_a_key_never_held_keeps_its_earlier_changes_from_landing_however_late() {
    let key = key(Deletion::Hard);
    let schema = stored_schema();
    // The insert the delete followed never reached the table: a batch that held both kept only
    // the delete.
    let delete = changes(&[Some(9)], &[5], ChangeOp::Delete);
    let mut state = merge(&schema, &[], &[], &[delete], &key).expect("the delete merges");
    assert!(state.rows.is_empty());
    assert_eq!(state.tombstones[0].num_rows(), 1);
    // However many commits of other keys land since, the tombstone stays.
    for commit in 0..40_u64 {
        let other = changes(&[Some(100)], &[10 + commit], ChangeOp::Update);
        state = merge(&schema, &state.rows, &state.tombstones, &[other], &key).unwrap();
        assert_eq!(state.tombstones[0].num_rows(), 1);
    }
    for stale in [1, 5] {
        let again = changes(&[Some(9)], &[stale], ChangeOp::Insert);
        let merged = merge(&schema, &state.rows, &state.tombstones, &[again], &key).unwrap();
        assert_eq!(merged.rows[0].num_rows(), 1, "sequence {stale}");
        assert_eq!(merged.tombstones[0].num_rows(), 1, "sequence {stale}");
    }
    // A change past the delete lands, and lifts the tombstone.
    let later = changes(&[Some(9)], &[6], ChangeOp::Insert);
    let merged = merge(&schema, &state.rows, &state.tombstones, &[later], &key).unwrap();
    assert_eq!(merged.rows[0].num_rows(), 2);
    assert!(merged.tombstones.is_empty());
}
