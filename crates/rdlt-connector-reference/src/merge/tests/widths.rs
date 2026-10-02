//! A row costs the cells it holds: many narrow rows and one row of many more columns cost their
//! own cells, in the commit the wide row arrives in and in every commit after.

use std::collections::BTreeMap;
use std::sync::Arc;

use arrow_array::cast::AsArray;
use arrow_array::types::Int64Type;
use arrow_array::{Array, ArrayRef, BinaryArray, BooleanArray, Int8Array, Int64Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use rdlt_connector::{ChangeColumns, Deletion, HistoryColumns, MergeKey};

use super::super::{Merged, merge_sparse};

const ROWS: i64 = 20_000;
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

/// What a table of twenty thousand narrow rows may hold with one wide row among them: far
/// under the eight bytes each narrow row would pay for each of the thousand columns.
const BOUND: usize = 4 * 1024 * 1024;

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
            rows(kind, &ids[..10_000], 1, 0),
            rows(kind, &[ROWS], 1_000_000, WIDTH),
            rows(kind, &ids[10_000..], 20_000, 0),
        ];
        let merged = merge_sparse(&wide, &[], &[], &incoming, &key).unwrap();
        assert_eq!(counted(&merged), (1, ids.len() + 1), "{kind:?}");
        let held = held_bytes(&merged.rows);
        assert!(held < BOUND, "{kind:?}: the commit holds {held} bytes");
    }
}
