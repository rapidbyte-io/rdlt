//! A row costs the cells it holds: many narrow rows and one row of many more columns cost their
//! own cells, in the commit the wide row arrives in and in every commit after.

use std::collections::BTreeMap;
use std::sync::Arc;

use arrow_array::cast::AsArray;
use arrow_array::types::Int64Type;
use arrow_array::{Array, ArrayRef, BinaryArray, BooleanArray, Int8Array, Int64Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use rdlt_connector::{ChangeColumns, Deletion, HistoryColumns, MergeKey, RootKey};

use super::super::{Merged, folded, merge_children_sparse, merge_sparse, read_back};
use crate::limits::{FOLD_CELLS, MAX_SHAPES};

const ROWS: i64 = 5_000;
const WIDTH: usize = 1_000;

/// How a table of these tests merges.
#[derive(Clone, Copy, Debug)]
enum Kind {
    Upsert,
    Changes,
    History,
}

fn key(kind: Kind) -> MergeKey {
    MergeKey {
        columns: vec!["id".into()],
        seq: "seq".into(),
        root: None,
        changes: matches!(kind, Kind::Changes).then(|| ChangeColumns {
            op: "op".into(),
            unchanged: None,
            deletion: Deletion::Hard,
        }),
        history: matches!(kind, Kind::History).then(|| HistoryColumns {
            valid_from: "from".into(),
            valid_to: "to".into(),
            is_current: "current".into(),
            row_hash: "hash".into(),
        }),
    }
}

/// The table's columns once it has gained `width` columns beside those every row holds.
fn schema(kind: Kind, width: usize) -> SchemaRef {
    let mut fields = vec![
        Field::new("id", DataType::Int64, true),
        Field::new("seq", DataType::Binary, false),
    ];
    if matches!(kind, Kind::History) {
        fields.extend([
            Field::new("from", DataType::Int64, false),
            Field::new("to", DataType::Int64, true),
            Field::new("current", DataType::Boolean, false),
            Field::new("hash", DataType::Binary, true),
        ]);
    }
    fields.extend((0..width).map(|column| Field::new(format!("c{column}"), DataType::Int64, true)));
    Arc::new(Schema::new(fields))
}

fn position(seq: u64) -> Vec<u8> {
    let mut bytes = vec![0_u8; 16];
    bytes[8..].copy_from_slice(&seq.to_be_bytes());
    bytes
}

/// Rows of `ids`, sequenced from `first`, each holding 7 in `width` columns beside its own.
fn rows(kind: Kind, ids: &[i64], first: u64, width: usize) -> RecordBatch {
    let count = ids.len();
    let seqs = (0..count as u64).map(|row| position(first + row));
    let mut columns: Vec<(String, ArrayRef)> = vec![
        ("id".into(), Arc::new(Int64Array::from(ids.to_vec()))),
        ("seq".into(), Arc::new(BinaryArray::from_iter_values(seqs))),
    ];
    match kind {
        Kind::Upsert => {}
        Kind::Changes => {
            columns.push(("op".into(), Arc::new(Int8Array::from(vec![1_i8; count]))));
        }
        Kind::History => {
            let hashes = ids
                .iter()
                .map(|id| (id + i64::try_from(width).unwrap()).to_be_bytes());
            columns.extend([
                (
                    "from".into(),
                    Arc::new(Int64Array::from(vec![1; count])) as ArrayRef,
                ),
                ("to".into(), Arc::new(Int64Array::new_null(count))),
                (
                    "current".into(),
                    Arc::new(BooleanArray::from(vec![true; count])),
                ),
                (
                    "hash".into(),
                    Arc::new(BinaryArray::from_iter_values(hashes)),
                ),
            ]);
        }
    }
    for column in 0..width {
        columns.push((
            format!("c{column}"),
            Arc::new(Int64Array::from(vec![7; count])),
        ));
    }
    RecordBatch::try_from_iter(columns).expect("a valid batch")
}

/// The bytes the buffers of `batches` hold, a buffer several columns share counted once.
fn held_bytes(batches: &[RecordBatch]) -> usize {
    let mut seen = BTreeMap::new();
    for batch in batches {
        for column in batch.columns() {
            let data = column.to_data();
            for buffer in data.buffers() {
                seen.insert(buffer.as_ptr() as usize, buffer.capacity());
            }
            if let Some(nulls) = data.nulls() {
                let buffer = nulls.buffer();
                seen.insert(buffer.as_ptr() as usize, buffer.capacity());
            }
        }
    }
    seen.values().sum()
}

/// The rows of `merged` that hold a value in the last of the wide columns, and all its rows.
fn counted(merged: &Merged) -> (usize, usize) {
    let last = format!("c{}", WIDTH - 1);
    let wide: usize = merged
        .rows
        .iter()
        .filter_map(|batch| batch.column_by_name(&last))
        .map(|column| {
            let values = column.as_primitive::<Int64Type>();
            values.iter().filter(|value| *value == Some(7)).count()
        })
        .sum();
    (wide, merged.rows.iter().map(RecordBatch::num_rows).sum())
}

/// What a table of five thousand narrow rows may hold with one wide row among them: far under
/// the forty megabytes of eight bytes each narrow row would pay for each of the thousand columns.
const BOUND: usize = 1024 * 1024;

#[test]
fn one_wide_row_after_many_narrow_rows_costs_its_own_cells_then_and_after() {
    for kind in [Kind::Upsert, Kind::Changes, Kind::History] {
        let key = key(kind);
        let ids: Vec<i64> = (0..ROWS).collect();
        let (narrow, wide) = (schema(kind, 0), schema(kind, WIDTH));
        let first = merge_sparse(&narrow, &[], &[], &[rows(kind, &ids, 1, 0)], &key).unwrap();
        assert!(held_bytes(&first.rows) < BOUND, "{kind:?}");
        // The wide row's own commit.
        let incoming = [rows(kind, &[ROWS], 1_000_000, WIDTH)];
        let second = merge_sparse(&wide, &first.rows, &first.tombstones, &incoming, &key).unwrap();
        assert_eq!(counted(&second), (1, ids.len() + 1), "{kind:?}");
        let held = held_bytes(&second.rows);
        assert!(
            held < BOUND,
            "{kind:?}: the wide row's commit holds {held} bytes"
        );
        // The commit after it, of a narrow row and of a change to a narrow row held.
        let incoming = [rows(kind, &[ROWS + 1, 5], 2_000_000, 0)];
        let third = merge_sparse(&wide, &second.rows, &second.tombstones, &incoming, &key).unwrap();
        assert_eq!(counted(&third), (1, ids.len() + 2), "{kind:?}");
        let held = held_bytes(&third.rows);
        assert!(
            held < BOUND,
            "{kind:?}: the commit after holds {held} bytes"
        );
        // Rows of one shape share a batch: the table is as many batches as its rows have shapes.
        assert_eq!(third.rows.len(), 2, "{kind:?}");
    }
}

#[test]
fn a_wide_row_among_narrow_rows_of_its_own_commit_costs_its_own_cells() {
    for kind in [Kind::Upsert, Kind::Changes, Kind::History] {
        let key = key(kind);
        let ids: Vec<i64> = (0..ROWS).collect();
        let wide = schema(kind, WIDTH);
        let incoming = [
            rows(kind, &ids[..2_500], 1, 0),
            rows(kind, &[ROWS], 1_000_000, WIDTH),
            rows(kind, &ids[2_500..], 20_000, 0),
        ];
        let merged = merge_sparse(&wide, &[], &[], &incoming, &key).unwrap();
        assert_eq!(counted(&merged), (1, ids.len() + 1), "{kind:?}");
        let held = held_bytes(&merged.rows);
        assert!(held < BOUND, "{kind:?}: the commit holds {held} bytes");
    }
}

/// Rows of `ids` that hold 7 in the column `column` alone beside their key and sequence.
fn only(ids: std::ops::Range<i64>, column: usize) -> RecordBatch {
    let count = usize::try_from(ids.end - ids.start).unwrap();
    let seqs = ids.clone().map(|id| position(u64::try_from(id).unwrap()));
    let columns: Vec<(String, ArrayRef)> = vec![
        ("id".into(), Arc::new(Int64Array::from_iter_values(ids))),
        ("seq".into(), Arc::new(BinaryArray::from_iter_values(seqs))),
        (
            format!("c{column}"),
            Arc::new(Int64Array::from(vec![7; count])),
        ),
    ];
    RecordBatch::try_from_iter(columns).expect("a valid batch")
}

/// Checks `merged` holds `shapes` sets of `each` rows, every row its own column and no other,
/// and gives the cells of columns its rows never had, batch by batch.
fn absent_cells(merged: &Merged, schema: &SchemaRef, shapes: i64, each: i64) -> Vec<usize> {
    let whole = read_back(schema, &merged.rows).unwrap();
    let mut found = 0;
    for batch in &whole {
        let ids = batch.column(0).as_primitive::<Int64Type>();
        for (row, id) in ids.values().iter().enumerate() {
            let own = usize::try_from(id / each).unwrap();
            for column in 0..usize::try_from(shapes).unwrap() {
                let cell = batch.column(2 + column).as_primitive::<Int64Type>();
                assert_eq!(
                    cell.is_valid(row),
                    column == own,
                    "row {id}, column {column}"
                );
            }
            found += 1;
        }
    }
    assert_eq!(found, shapes * each);
    let nulls = |batch: &RecordBatch| -> usize {
        let columns = batch.columns().iter();
        columns.map(Array::null_count).sum()
    };
    merged.rows.iter().map(nulls).collect()
}

/// `shapes` batches of `each` rows, each holding a column no other does.
fn shaped(shapes: i64, each: i64) -> Vec<RecordBatch> {
    (0..shapes)
        .map(|shape| {
            only(
                shape * each..(shape + 1) * each,
                usize::try_from(shape).unwrap(),
            )
        })
        .collect()
}

/// A merge of `incoming` into `published`, its rows folded as a destination of files folds
/// them.
fn folding(wide: &SchemaRef, published: &[RecordBatch], incoming: &[RecordBatch]) -> Merged {
    let merged = merge_sparse(wide, published, &[], incoming, &key(Kind::Upsert)).unwrap();
    Merged {
        rows: folded(wide, merged.rows).unwrap(),
        tombstones: merged.tombstones,
    }
}

#[test]
fn a_merge_gives_a_batch_for_each_set_of_columns_and_a_fold_joins_the_smallest() {
    // A hundred batches of ten rows, each holding a column no other does.
    let (shapes, each) = (100, 10);
    let wide = schema(Kind::Upsert, 100);
    let incoming = shaped(shapes, each);
    let merged = merge_sparse(&wide, &[], &[], &incoming, &key(Kind::Upsert)).unwrap();
    assert_eq!(merged.rows.len(), 100);
    assert_eq!(absent_cells(&merged, &wide, shapes, each), vec![0; 100]);
    let merged = folding(&wide, &[], &incoming);
    assert_eq!(merged.rows.len(), MAX_SHAPES);
    let absent = absent_cells(&merged, &wide, shapes, each);
    // All but one batch are as they were written; the smallest joined under their columns.
    let joined = usize::try_from(shapes).unwrap() - (MAX_SHAPES - 1);
    assert_eq!(absent.iter().filter(|cells| **cells != 0).count(), 1);
    assert_eq!(absent.iter().sum::<usize>(), joined * 10 * (joined - 1));
    // The same rows fold into the same batches.
    assert_eq!(folding(&wide, &[], &incoming).rows, merged.rows);
    // What was folded merges again as any rows do, and stays as few.
    let merged = folding(&wide, &merged.rows, &[only(0..each, 0)]);
    assert!(merged.rows.len() <= MAX_SHAPES + 1);
    absent_cells(&merged, &wide, shapes, each);
}

#[test]
fn batches_join_apart_where_one_would_hold_more_absent_cells_than_a_fold_makes() {
    // Forty batches of two thousand rows: the twenty-five smallest, joined as one, would hold
    // over a million cells no row had, twenty-four for each of their rows.
    let (shapes, each) = (40, 2_000);
    let wide = schema(Kind::Upsert, 40);
    let incoming = shaped(shapes, each);
    let merged = folding(&wide, &[], &incoming);
    assert_eq!(merged.rows.len(), MAX_SHAPES + 1);
    let absent = absent_cells(&merged, &wide, shapes, each);
    let limit = usize::try_from(FOLD_CELLS).unwrap();
    assert!(absent.iter().all(|cells| *cells <= limit), "{absent:?}");
    assert_eq!(absent.iter().filter(|cells| **cells != 0).count(), 2);
    // A child table's rows are a batch a set of columns too, and fold as a table's do.
    let child = MergeKey {
        root: Some(RootKey {
            table: "roots".into(),
            id: "id".into(),
            seq: "seq".into(),
        }),
        ..key(Kind::Upsert)
    };
    let root = child.root.clone().unwrap();
    let rows = merge_children_sparse(&wide, &[], &incoming, &child, &root, &incoming).unwrap();
    assert_eq!(rows.len(), 40);
    assert_eq!(folded(&wide, rows).unwrap().len(), MAX_SHAPES + 1);
}

#[test]
fn folding_commit_after_commit_keeps_each_batch_within_a_fold_s_absent_cells() {
    let width = 1000;
    let wide = schema(Kind::Upsert, width);
    // Sixteen shapes of many rows, each holding a column of its own, and a row of every column.
    let (each, few) = (1_100_i64, 1_000_i64);
    let mut incoming = shaped(16, each);
    let base = 16 * each;
    incoming.push(rows(Kind::Upsert, &[base], 1, width));
    let mut published = folding(&wide, &[], &incoming).rows;
    let limit = usize::try_from(FOLD_CELLS).unwrap();
    let nulls = |batch: &RecordBatch| -> usize {
        let columns = batch.columns().iter();
        columns.map(Array::null_count).sum()
    };
    let mut counts = Vec::new();
    for commit in 1..=12_i64 {
        // Rows of their key and sequence alone: joined with the wide row they lack a million
        // cells, just within what one fold makes.
        let ids: Vec<i64> = (base + commit * few..base + (commit + 1) * few).collect();
        let narrow = rows(Kind::Upsert, &ids, 1, 0);
        published = folding(&wide, &published, &[narrow]).rows;
        let absent: Vec<usize> = published.iter().map(nulls).collect();
        assert!(
            absent.iter().all(|cells| *cells <= limit),
            "{commit}: {absent:?}"
        );
        counts.push((published.len(), absent.iter().sum::<usize>()));
    }
    // What the table holds of cells without a value does not grow with its commits, nor do its
    // batches: a batch joined by one commit is measured by the next as what it is.
    assert!(
        counts[0].1 > limit / 2,
        "the first commit joins: {counts:?}"
    );
    for (batches, absent) in &counts {
        assert!(*batches <= MAX_SHAPES + 2, "{counts:?}");
        assert!(*absent <= 2 * limit, "{counts:?}");
    }
    let held: usize = published.iter().map(RecordBatch::num_rows).sum();
    assert_eq!(held, usize::try_from(base + 1 + 12 * few).unwrap());
}

#[test]
fn a_fold_joins_batches_under_columns_the_table_says_every_row_holds() {
    // A table that gained a column it declares never null still holds rows from before it.
    let fields: Vec<Field> = schema(Kind::Upsert, 30)
        .fields()
        .iter()
        .map(|field| field.as_ref().clone().with_nullable(false))
        .collect();
    let strict: SchemaRef = Arc::new(Schema::new(fields));
    let merged = folding(&strict, &[], &shaped(30, 10));
    assert_eq!(merged.rows.len(), MAX_SHAPES);
    absent_cells(&merged, &schema(Kind::Upsert, 30), 30, 10);
}
