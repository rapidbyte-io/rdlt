//! A files merge table keeps each row under the columns the row holds, on disk and where it is
//! read: a row of many columns among many rows of few costs the table its own cells.

use std::collections::BTreeMap;
use std::sync::Arc;

use arrow_array::cast::AsArray as _;
use arrow_array::types::Int64Type;
use arrow_array::{Array, ArrayRef, Int64Array, RecordBatch};
use rdlt_connector::{
    Field, GenerationId, LogicalType, MergeKey, SchemaVersion, TablePath, TableRef, TableSchema,
};

use crate::files::FileFormat;
use crate::files::manifest::{self, Listed};
use crate::files::session::tests::Sessions;

const ROWS: i64 = 20_000;
const WIDTH: usize = 1_000;
/// Bytes: far under the 160 MB that eight bytes for each of a thousand columns of each narrow
/// row come to.
const BOUND: usize = 4 * 1024 * 1024;

fn table(generation: Option<u64>, merge: bool) -> TableRef {
    TableRef {
        path: TablePath::new(["rows"]).unwrap(),
        name: "rows".into(),
        version: SchemaVersion(1),
        generation: generation.map(GenerationId),
        merge: merge.then(|| MergeKey {
            columns: vec!["id".into()],
            seq: "seq".into(),
            root: None,
            changes: None,
            history: None,
        }),
    }
}

/// The table's schema: its key, its sequence and `width` columns beside them.
fn schema(width: usize) -> TableSchema {
    let mut fields = vec![
        Field::new("id", LogicalType::Int64, false),
        Field::new("seq", LogicalType::Int64, false),
    ];
    fields.extend(
        (0..width).map(|column| Field::new(format!("c{column}"), LogicalType::Int64, true)),
    );
    TableSchema::new(fields).unwrap()
}

/// Rows of `ids` at sequence `seq`, each holding 7 in `width` columns beside its key.
fn rows(ids: std::ops::Range<i64>, seq: i64, width: usize) -> RecordBatch {
    let count = usize::try_from(ids.end - ids.start).unwrap();
    let mut columns: Vec<(String, ArrayRef)> = vec![
        ("id".into(), Arc::new(Int64Array::from_iter_values(ids))),
        ("seq".into(), Arc::new(Int64Array::from(vec![seq; count]))),
    ];
    for column in 0..width {
        let sevens = Int64Array::from(vec![7; count]);
        columns.push((format!("c{column}"), Arc::new(sevens)));
    }
    RecordBatch::try_from_iter(columns).unwrap()
}

/// The bytes the buffers of `batches` hold, an allocation several buffers share counted once: a
/// batch read from an Arrow file holds every column in the block it was read as.
fn held_bytes(batches: &[RecordBatch]) -> usize {
    let mut seen = BTreeMap::new();
    let mut held = |buffer: &arrow_buffer::Buffer| {
        seen.insert(
            buffer.as_ptr() as usize - buffer.ptr_offset(),
            buffer.capacity(),
        );
    };
    for batch in batches {
        for column in batch.columns() {
            let data = column.to_data();
            data.buffers().iter().for_each(&mut held);
            if let Some(nulls) = data.nulls() {
                held(nulls.buffer());
            }
        }
    }
    seen.values().sum()
}

/// The files the latest manifest lists for the table.
fn listed(sessions: &Sessions) -> Vec<Listed> {
    let latest = manifest::latest(&sessions.location.dir).unwrap().unwrap();
    latest.tables["rows"].files.clone()
}

/// What the table's files hold as a merge reads them: every batch, each of its own columns.
fn read(sessions: &Sessions) -> Vec<RecordBatch> {
    let arrow = Arc::new(schema(WIDTH).to_arrow());
    let mut batches = Vec::new();
    for file in listed(sessions) {
        batches.extend(manifest::read_held(&sessions.location.dir, &file.path, &arrow).unwrap());
    }
    batches
}

/// The id of every row the table holds, ascending.
fn ids(sessions: &Sessions) -> Vec<i64> {
    let mut ids: Vec<i64> = read(sessions)
        .iter()
        .flat_map(|batch| {
            batch
                .column(0)
                .as_primitive::<Int64Type>()
                .values()
                .to_vec()
        })
        .collect();
    ids.sort_unstable();
    ids
}

/// Checks the table holds `narrow` rows of two columns and one of every column, in files and
/// batches that cost what those rows hold.
fn assert_costs_its_own_cells(sessions: &Sessions, narrow: usize, what: &str) {
    let written: u64 = listed(sessions).iter().map(|file| file.bytes).sum();
    assert!(
        written < u64::try_from(BOUND).unwrap(),
        "{what}: the files hold {written} bytes"
    );
    let batches = read(sessions);
    let count: usize = batches.iter().map(RecordBatch::num_rows).sum();
    assert_eq!(count, narrow + 1, "{what}");
    // The wide row is in a batch of its own columns, with at most a narrow row or two.
    let wide: Vec<&RecordBatch> = batches
        .iter()
        .filter(|batch| batch.num_columns() == WIDTH + 2)
        .collect();
    assert_eq!(wide.len(), 1, "{what}");
    let last = wide[0].column(WIDTH + 1);
    assert!(wide[0].num_rows() <= 3, "{what}");
    assert_eq!(last.len() - last.null_count(), 1, "{what}");
    let sevens: i64 = last.as_primitive::<Int64Type>().iter().flatten().sum();
    assert_eq!(sevens, 7, "{what}");
    let held = held_bytes(&batches);
    assert!(held < BOUND, "{what}: the batches hold {held} bytes");
    // A reader of the destination is given every column of every row, the narrow rows sharing
    // what they never had.
    let (dir, arrow) = (&sessions.location.dir, Arc::new(schema(WIDTH).to_arrow()));
    let published =
        crate::files::destination::published_by(dir, "rows", &arrow, manifest::latest).unwrap();
    assert!(
        published
            .iter()
            .all(|batch| batch.num_columns() == WIDTH + 2)
    );
    let count: usize = published.iter().map(RecordBatch::num_rows).sum();
    assert_eq!(count, narrow + 1, "{what}");
    let held = held_bytes(&published);
    assert!(held < BOUND, "{what}: a reader holds {held} bytes");
}

#[test]
fn one_wide_row_after_many_narrow_rows_costs_a_merge_table_its_own_cells() {
    let narrow = usize::try_from(ROWS).unwrap();
    for format in [FileFormat::Jsonl, FileFormat::Arrow] {
        let mut sessions = Sessions::new(format);
        let merged = table(None, true);
        sessions.create(&merged, &schema(0));
        sessions.stage(&merged, 1, rows(0..ROWS, 1, 0));
        sessions.commit(&sessions.meta(1, 1, &[1])).unwrap();
        sessions.create(&merged, &schema(WIDTH));
        sessions.stage(&merged, 2, rows(ROWS..ROWS + 1, 1, WIDTH));
        sessions.commit(&sessions.meta(1, 2, &[2])).unwrap();
        assert_costs_its_own_cells(&sessions, narrow, "once merged");
        // Another session merges into what the first left.
        sessions.open(2);
        sessions.stage(&merged, 1, rows(5..6, 2, 0));
        sessions.stage(&merged, 2, rows(ROWS + 1..ROWS + 2, 2, 0));
        sessions.commit(&sessions.meta(2, 1, &[1, 2])).unwrap();
        assert_costs_its_own_cells(&sessions, narrow + 1, "merged again");
        assert_eq!(ids(&sessions), (0..ROWS + 2).collect::<Vec<i64>>());
    }
}

#[test]
fn files_of_differing_columns_are_compacted_and_merged_into_at_their_rows_own_cost() {
    let narrow = usize::try_from(ROWS).unwrap();
    for format in [FileFormat::Jsonl, FileFormat::Arrow] {
        let sessions = Sessions::new(format);
        sessions.create(&table(None, true), &schema(WIDTH));
        // A generation's files, written apart, are compacted as the commit publishes them.
        let filling = table(Some(1), true);
        sessions.stage(&filling, 1, rows(0..ROWS / 2, 1, 0));
        sessions.stage(&filling, 2, rows(ROWS / 2..ROWS / 2 + 1, 1, WIDTH));
        sessions.stage(&filling, 3, rows(ROWS / 2 + 1..ROWS + 1, 1, 0));
        let mut meta = sessions.meta(1, 1, &[1, 2, 3]);
        meta.finish_generations = vec![(filling.path.clone(), GenerationId(1))];
        sessions.commit(&meta).unwrap();
        // Lines of any columns share a file; an Arrow file holds one set of columns, so a file
        // joins only its like and the three stay apart.
        let files = if format == FileFormat::Jsonl { 1 } else { 3 };
        assert_eq!(listed(&sessions).len(), files, "{format:?}");
        assert_costs_its_own_cells(&sessions, narrow, "once compacted");
        assert_eq!(
            ids(&sessions),
            (0..=ROWS).collect::<Vec<i64>>(),
            "{format:?}"
        );
        // A merge reads the compacted files and writes the table again.
        let merged = table(None, true);
        sessions.stage(&merged, 4, rows(5..6, 2, 0));
        sessions.commit(&sessions.meta(1, 2, &[4])).unwrap();
        assert_costs_its_own_cells(&sessions, narrow, "merged after compaction");
        let files = if format == FileFormat::Jsonl { 1 } else { 2 };
        assert_eq!(listed(&sessions).len(), files, "{format:?}");
    }
}
