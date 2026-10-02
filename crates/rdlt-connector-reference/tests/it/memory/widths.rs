//! A memory merge table keeps each row under the columns the row holds: a row of many columns
//! among many rows of few costs the table its own cells.

use std::collections::BTreeMap;
use std::sync::Arc;

use arrow_array::{Array, ArrayRef, BinaryArray, Int64Array, RecordBatch};
use rdlt_connector::{MergeKey, OpenedSession, SegmentId, TableChange, TableRef, TableSchema};
use rdlt_connector_reference::published;

use super::owned::{meta, open, store};

const ROWS: i64 = 20_000;
const WIDTH: usize = 1_000;

fn events() -> TableRef {
    TableRef {
        merge: Some(MergeKey {
            columns: vec!["id".into()],
            seq: "seq".into(),
            root: None,
            changes: None,
            history: None,
        }),
        ..super::owned::table("events", "events", None)
    }
}

/// Rows of `ids`, each holding 7 in `width` columns beside its key and its sequence.
fn rows(ids: std::ops::Range<i64>, width: usize) -> RecordBatch {
    let seqs = ids.clone().map(|id| {
        let mut seq = [0_u8; 16];
        seq[8..].copy_from_slice(&id.to_be_bytes());
        seq
    });
    let count = usize::try_from(ids.end - ids.start).expect("a count");
    let mut columns: Vec<(String, ArrayRef)> = vec![
        ("id".into(), Arc::new(Int64Array::from_iter_values(ids))),
        ("seq".into(), Arc::new(BinaryArray::from_iter_values(seqs))),
    ];
    for column in 0..width {
        let sevens = Int64Array::from(vec![7; count]);
        columns.push((format!("c{column}"), Arc::new(sevens)));
    }
    RecordBatch::try_from_iter(columns).expect("a valid batch")
}

/// Declares the columns of `batch` for the table, stages it in `segment` and commits it.
async fn loaded(session: &mut OpenedSession, segment: u64, batch: RecordBatch) {
    let create = TableChange::Create {
        table: events(),
        schema: TableSchema::from_arrow(&batch.schema()).expect("a schema"),
    };
    session
        .session
        .apply_schema(&create)
        .await
        .expect("the columns are declared");
    let mut writer = session.session.writer(&events()).await.expect("a writer");
    writer
        .write(SegmentId(segment), batch)
        .await
        .expect("the write buffers");
    writer.flush().await.expect("the flush stages");
    let commit = meta(session, 1, segment, &[segment]);
    session
        .session
        .commit(&commit)
        .await
        .expect("the commit lands");
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

#[tokio::test]
async fn one_wide_row_among_many_narrow_rows_costs_the_table_its_own_cells() {
    let destination = store("widths").await;
    let mut session = open(destination.as_ref(), "p", 1).await;
    loaded(&mut session, 1, rows(0..ROWS, 0)).await;
    loaded(&mut session, 2, rows(ROWS..ROWS + 1, WIDTH)).await;
    loaded(&mut session, 3, rows(ROWS + 1..ROWS + 2, 0)).await;
    let held = published("widths", "events");
    let count: usize = held.iter().map(RecordBatch::num_rows).sum();
    assert_eq!(count, usize::try_from(ROWS).expect("a count") + 2);
    // A reader is given every column of every row; the narrow rows share what they never had.
    for batch in &held {
        assert_eq!(batch.num_columns(), WIDTH + 2);
    }
    let wide: usize = held
        .iter()
        .map(|batch| batch.num_rows() - batch.column(WIDTH + 1).null_count())
        .sum();
    assert_eq!(wide, 1);
    // Eight bytes for each of a thousand columns of each narrow row would be 160 MB.
    let bytes = held_bytes(&held);
    assert!(bytes < 4 * 1024 * 1024, "the table holds {bytes} bytes");
}

/// Rows of `ids` that hold 7 in the column `column` alone beside their key and sequence.
fn only(ids: std::ops::Range<i64>, column: usize) -> RecordBatch {
    rows(ids, 400)
        .project(&[0, 1, column + 2])
        .expect("three columns")
}

#[tokio::test]
async fn a_table_of_many_shapes_keeps_each_row_under_its_own_columns_over_many_commits() {
    let destination = store("shapes").await;
    let mut session = open(destination.as_ref(), "p", 1).await;
    loaded(&mut session, 1, rows(0..1, 400)).await;
    // Forty shapes of a thousand rows, each holding a column of its own, then twelve commits
    // of rows that hold their key and sequence alone.
    for shape in 0..40_i64 {
        let from = (shape + 1) * 1_000;
        let column = usize::try_from(shape).expect("a column");
        let segment = u64::try_from(shape).expect("a segment") + 2;
        loaded(&mut session, segment, only(from..from + 1_000, column)).await;
    }
    for commit in 0..12_i64 {
        let from = 100_000 + commit * 5_000;
        let segment = u64::try_from(commit).expect("a segment") + 50;
        loaded(&mut session, segment, rows(from..from + 5_000, 0)).await;
        let held = published("shapes", "events");
        // A table keeps a batch for each set of columns its rows hold and makes no cell: the
        // narrow rows gather in one batch, whatever the wide row beside them holds, and a
        // reader's absent columns are nulls every batch shares.
        assert_eq!(held.len(), 42, "commit {commit}");
        let bytes = held_bytes(&held);
        assert!(bytes < 4 * 1024 * 1024, "commit {commit}: {bytes} bytes");
    }
}

#[tokio::test]
async fn a_row_of_a_shape_of_its_own_costs_a_batch_of_the_columns_it_holds() {
    let destination = store("shapes-of-one").await;
    let mut session = open(destination.as_ref(), "p", 1).await;
    loaded(&mut session, 1, rows(0..1, 400)).await;
    // Four hundred rows, each holding a column no other does, written a batch each: the table
    // keeps a batch for each, of three columns, and joins none.
    let mut writer = session.session.writer(&events()).await.expect("a writer");
    for shape in 0..400_i64 {
        let column = usize::try_from(shape).expect("a column");
        let batch = only(shape + 1..shape + 2, column);
        writer.write(SegmentId(2), batch).await.expect("buffers");
    }
    writer.flush().await.expect("the flush stages");
    let commit = meta(&session, 1, 2, &[2]);
    session.session.commit(&commit).await.expect("the commit");
    let held = published("shapes-of-one", "events");
    assert_eq!(held.len(), 401);
    // Every column of every row would be over a megabyte of cells.
    let bytes = held_bytes(&held);
    assert!(bytes < 256 * 1024, "the table holds {bytes} bytes");
}
