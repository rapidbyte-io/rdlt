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
