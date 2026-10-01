use std::sync::Arc;

use arrow_array::{ArrayRef, Int16Array, Int32Array, RecordBatch};
use rdlt_connector::{
    Field, LogicalType, MergeKey, SchemaVersion, TablePath, TableRef, TableSchema,
};

use crate::files::FileFormat;
use crate::files::manifest;
use crate::files::session::tests::Sessions;

#[test]
fn a_merge_table_of_more_narrow_rows_than_a_batch_holds_reads_back_and_merges_again() {
    let sessions = Sessions::new(FileFormat::Arrow);
    let table = TableRef {
        path: TablePath::new(["rows"]).unwrap(),
        name: "rows".into(),
        version: SchemaVersion(1),
        generation: None,
        merge: Some(MergeKey {
            columns: vec!["id".into()],
            seq: "seq".into(),
            root: None,
            changes: None,
            history: None,
        }),
    };
    let schema = TableSchema::new(vec![
        Field::new("id", LogicalType::Int32, false),
        Field::new("seq", LogicalType::Int16, false),
    ])
    .unwrap();
    sessions.create(&table, &schema);
    let rows = |from: i32, count: i32| {
        let id: ArrayRef = Arc::new(Int32Array::from_iter_values(from..from + count));
        let seq: ArrayRef = Arc::new(Int16Array::from_iter_values((0..count).map(|_| 1)));
        RecordBatch::try_from_iter([("id", id), ("seq", seq)]).unwrap()
    };
    // The table's rows are merged as one batch of six bytes a row: more rows than a batch a
    // reader accepts holds, in less than the bytes a batch is cut at.
    let half = 800_000;
    sessions.stage(&table, 1, rows(0, half));
    sessions.stage(&table, 2, rows(half, half));
    sessions.commit(&sessions.meta(1, 1, &[1, 2])).unwrap();
    let published = || {
        let latest = manifest::latest(&sessions.location.dir).unwrap().unwrap();
        let arrow = Arc::new(schema.to_arrow());
        let mut read = 0;
        for file in &latest.tables["rows"].files {
            let batches = manifest::read(&sessions.location.dir, &file.path, &arrow)
                .expect("the published file reads back");
            read += batches.iter().map(RecordBatch::num_rows).sum::<usize>();
        }
        read
    };
    assert_eq!(published(), 1_600_000);
    // The next commit reads the table to merge into it.
    sessions.stage(&table, 3, rows(2 * half, 1));
    sessions.commit(&sessions.meta(1, 2, &[3])).unwrap();
    assert_eq!(published(), 1_600_001);
}

/// Rows `from..from + count` of `(id, seq, payload)`, each payload 64 bytes but that of the row
/// `large.0`, which holds `large.1` bytes.
fn payloads(from: i64, count: i64, large: (i64, usize)) -> RecordBatch {
    use arrow_array::{BinaryArray, Int64Array, StringArray};
    let ids = from..from + count;
    let text = |id: i64| "p".repeat(if id == large.0 { large.1 } else { 64 });
    let seq = |id: i64| id.to_be_bytes().repeat(2);
    RecordBatch::try_from_iter([
        (
            "id",
            Arc::new(Int64Array::from_iter_values(ids.clone())) as ArrayRef,
        ),
        (
            "seq",
            Arc::new(BinaryArray::from_iter_values(ids.clone().map(seq))) as ArrayRef,
        ),
        (
            "payload",
            Arc::new(StringArray::from_iter_values(ids.map(text))) as ArrayRef,
        ),
    ])
    .unwrap()
}

/// The length of each payload the latest manifest publishes, by id, and the files it lists.
fn published_payloads(sessions: &Sessions, schema: &TableSchema) -> (Vec<(i64, usize)>, usize) {
    use arrow_array::cast::AsArray as _;
    use arrow_array::types::Int64Type;
    let latest = manifest::latest(&sessions.location.dir).unwrap().unwrap();
    let arrow = Arc::new(schema.to_arrow());
    let files = &latest.tables["rows"].files;
    let mut found = Vec::new();
    for file in files {
        for batch in manifest::read(&sessions.location.dir, &file.path, &arrow).unwrap() {
            let ids = batch.column(0).as_primitive::<Int64Type>();
            let payloads = batch.column(2).as_string::<i32>();
            for (id, payload) in ids.values().iter().zip(payloads.iter().flatten()) {
                found.push((*id, payload.len()));
            }
        }
    }
    found.sort_unstable();
    (found, files.len())
}

#[test]
fn a_table_with_one_row_far_larger_than_the_others_is_written_by_a_merge_and_by_an_append() {
    use rdlt_wire::limits::FRAME_BYTES;
    let schema = TableSchema::new(vec![
        Field::new("id", LogicalType::Int64, false),
        Field::new("seq", LogicalType::Binary, false),
        Field::new("payload", LogicalType::Utf8, false),
    ])
    .unwrap();
    let key = MergeKey {
        columns: vec!["id".into()],
        seq: "seq".into(),
        root: None,
        changes: None,
        history: None,
    };
    // A merge writes the whole table as one batch, an append merges its files batch by batch:
    // a row a few kibibytes short of a frame, or most of one, lands in a batch it fits.
    let frame = usize::try_from(FRAME_BYTES).unwrap();
    for (merge, large) in [(Some(key), frame - 4096), (None, frame / 2)] {
        let sessions = Sessions::new(FileFormat::Arrow);
        let table = TableRef {
            path: TablePath::new(["rows"]).unwrap(),
            name: "rows".into(),
            version: SchemaVersion(1),
            generation: None,
            merge,
        };
        sessions.create(&table, &schema);
        sessions.stage(&table, 1, payloads(0, 2000, (1000, large)));
        sessions.stage(&table, 2, payloads(2000, 2000, (-1, 0)));
        sessions.commit(&sessions.meta(1, 1, &[1, 2])).unwrap();
        let (found, files) = published_payloads(&sessions, &schema);
        let expected: Vec<(i64, usize)> = (0..4000)
            .map(|id| (id, if id == 1000 { large } else { 64 }))
            .collect();
        assert_eq!(found, expected);
        assert_eq!(files, 1, "the commit's files are one");
    }
}
