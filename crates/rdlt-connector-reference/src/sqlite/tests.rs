//! The SQLite destination merges change streams as the reference merge does: the same written
//! batches, committed in turn to both, leave the same rows and the same tombstones.

use std::collections::BTreeSet;
use std::path::Path;
use std::sync::Arc;
use std::time::UNIX_EPOCH;

use arrow_array::cast::AsArray;
use arrow_array::types::Int64Type;
use arrow_array::{Array, ArrayRef, BinaryArray, Int8Array, Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field as ArrowField, Schema, SchemaRef};
use proptest::prelude::*;
use rdlt_connector::{
    ChangeColumns, ChangeOp, CommitMeta, CommitSeq, ConnectContext, DELETED_AT_COLUMN, Deletion,
    Destination, Field, LoadId, LogicalType, MergeKey, OP_COLUMN, OpenContext, PipelineId,
    SEQ_COLUMN, SchemaVersion, SegmentId, TableChange, TablePath, TableRef, TableSchema,
    UNCHANGED_COLUMN, destination_factory,
};
use serde_json::json;

use super::{SqliteDestination, published};
use crate::merge::{Merged, merge};

/// One change: its op, key and region, values, deletion time, and the columns it flags
/// unchanged, bit 0 `value`, bit 1 `n`, bit 2 the deletion time.
#[derive(Clone, Debug)]
struct Change {
    op: ChangeOp,
    key: i64,
    region: i64,
    value: Option<String>,
    n: Option<i64>,
    at: Option<i64>,
    flags: u8,
    seq: u16,
}

/// A table's rows as its key and region, value, counter, sequence and deletion time.
type Rows = BTreeSet<(
    Option<i64>,
    Option<i64>,
    Option<String>,
    Option<i64>,
    Vec<u8>,
    Option<i64>,
)>;

fn change() -> impl Strategy<Value = Change> {
    let op = prop_oneof![
        3 => Just(ChangeOp::Insert),
        3 => Just(ChangeOp::Update),
        2 => Just(ChangeOp::Delete),
        1 => Just(ChangeOp::Truncate),
    ];
    let value = proptest::option::of("[a-c]{1,2}");
    let number = proptest::option::of(0_i64..4);
    let key = (0_i64..5, 0_i64..2);
    (op, key, value, number.clone(), number, 0_u8..8).prop_map(
        |(op, (key, region), value, n, at, flags)| Change {
            op,
            key,
            region,
            value,
            n,
            at,
            flags,
            seq: 0,
        },
    )
}

/// Two or three commits, each of changes in one to three segments, sequenced apart from every
/// other change, but for changes some commit sends again, as they were.
fn commits() -> impl Strategy<Value = Vec<Vec<Vec<Change>>>> {
    let segment = proptest::collection::vec((change(), 0_u16..512, any::<Option<u8>>()), 0..5);
    let commit = proptest::collection::vec(segment, 1..=3);
    proptest::collection::vec(commit, 2..=3).prop_map(|commits| {
        let mut sent: Vec<Change> = Vec::new();
        commits
            .into_iter()
            .map(|commit| {
                commit
                    .into_iter()
                    .map(|segment| sent_once(segment, &mut sent))
                    .collect()
            })
            .collect()
    })
}

/// `segment`'s changes, each sequenced apart from every change `sent` before it, or one of those
/// sent again.
fn sent_once(segment: Vec<(Change, u16, Option<u8>)>, sent: &mut Vec<Change>) -> Vec<Change> {
    segment
        .into_iter()
        .map(|(mut change, position, again)| {
            if let Some(again) = again.filter(|_| !sent.is_empty()) {
                return sent[usize::from(again) % sent.len()].clone();
            }
            // Distinct: at most 64 changes, each with its own low bits.
            change.seq = position * 64 + u16::try_from(sent.len()).unwrap_or(0);
            sent.push(change.clone());
            change
        })
        .collect()
}

/// How a case's table merges: its deletes, and whether its key spans the region too.
#[derive(Clone, Debug)]
struct Shape {
    deletion: Deletion,
    regions: bool,
}

fn key(shape: &Shape) -> MergeKey {
    let mut columns = vec!["id".into()];
    if shape.regions {
        columns.push("region".into());
    }
    let deletion = shape.deletion.clone();
    MergeKey {
        columns,
        seq: SEQ_COLUMN.into(),
        root: None,
        changes: Some(ChangeColumns {
            op: OP_COLUMN.into(),
            unchanged: Some(UNCHANGED_COLUMN.into()),
            deletion,
        }),
        history: None,
    }
}

fn table(shape: &Shape) -> TableRef {
    TableRef {
        path: TablePath::new(["changes"]).expect("a valid table path"),
        name: "changes".into(),
        version: SchemaVersion(1),
        generation: None,
        merge: Some(key(shape)),
    }
}

fn soft(deletion: &Deletion) -> bool {
    matches!(deletion, Deletion::Soft { .. })
}

/// The table's schema: its key and region, value and counter, its sequence, and where deletes
/// are soft, its deletion time.
fn schema(deletion: &Deletion) -> TableSchema {
    let mut fields = vec![
        Field::new("id", LogicalType::Int64, false),
        Field::new("region", LogicalType::Int64, false),
        Field::new("value", LogicalType::Utf8, true),
        Field::new("n", LogicalType::Int64, true),
        Field::new(SEQ_COLUMN, LogicalType::Binary, false),
    ];
    if soft(deletion) {
        fields.push(Field::new(DELETED_AT_COLUMN, LogicalType::Int64, true));
    }
    TableSchema::new(fields).expect("the schema is valid")
}

fn sequence(seq: u16) -> Vec<u8> {
    let mut bytes = vec![0; 16];
    bytes[14..].copy_from_slice(&seq.to_be_bytes());
    bytes
}

/// `changes` as a change stream writes them: the stored columns, then the op and the unchanged
/// flags, a bitmap over the batch's fields.
fn written(changes: &[Change], deletion: &Deletion) -> RecordBatch {
    let keyed = |change: &Change| change.op != ChangeOp::Truncate;
    let mut columns: Vec<(&str, ArrayRef)> = vec![
        (
            "id",
            Arc::new(
                changes
                    .iter()
                    .map(|c| keyed(c).then_some(c.key))
                    .collect::<Int64Array>(),
            ),
        ),
        (
            "region",
            Arc::new(
                changes
                    .iter()
                    .map(|c| keyed(c).then_some(c.region))
                    .collect::<Int64Array>(),
            ),
        ),
        (
            "value",
            Arc::new(
                changes
                    .iter()
                    .map(|c| c.value.clone())
                    .collect::<StringArray>(),
            ),
        ),
        (
            "n",
            Arc::new(changes.iter().map(|c| c.n).collect::<Int64Array>()),
        ),
        (
            SEQ_COLUMN,
            Arc::new(BinaryArray::from_iter_values(
                changes.iter().map(|c| sequence(c.seq)),
            )),
        ),
    ];
    if soft(deletion) {
        columns.push((
            DELETED_AT_COLUMN,
            Arc::new(changes.iter().map(|c| c.at).collect::<Int64Array>()),
        ));
    }
    let ops = Int8Array::from_iter_values(changes.iter().map(|c| c.op.code()));
    columns.push((OP_COLUMN, Arc::new(ops)));
    let flags = changes.iter().map(|change| {
        // Fields 2 and 3 are the value and the counter, 5 the deletion time.
        let at = if soft(deletion) {
            (change.flags & 4) << 3
        } else {
            0
        };
        let bitmap = ((change.flags & 3) << 2) | at;
        (bitmap != 0 && keyed(change) && change.op != ChangeOp::Delete).then(|| vec![bitmap])
    });
    columns.push((UNCHANGED_COLUMN, Arc::new(flags.collect::<BinaryArray>())));
    RecordBatch::try_from_iter(columns).expect("a valid batch")
}

/// The rows of `batches`, read by name, their integers as `Int64`.
fn rows(batches: &[RecordBatch]) -> Rows {
    let mut rows = Rows::new();
    for batch in batches {
        let integers = |name: &str| {
            batch.column_by_name(name).map(|column| {
                arrow_cast::cast(column, &DataType::Int64).expect("an integer column")
            })
        };
        let (ids, n, at) = (integers("id"), integers("n"), integers(DELETED_AT_COLUMN));
        let regions = integers("region");
        let value = |row: usize| {
            let column = batch.column_by_name("value")?;
            let column = arrow_cast::cast(column, &DataType::Utf8).expect("a text column");
            let texts = column.as_string::<i32>();
            (!texts.is_null(row)).then(|| texts.value(row).to_owned())
        };
        let integer = |column: &Option<ArrayRef>, row: usize| {
            let column = column.as_ref()?.as_primitive::<Int64Type>();
            (!column.is_null(row)).then(|| column.value(row))
        };
        let seqs = batch.column_by_name(SEQ_COLUMN).expect("a sequence column");
        let seqs = seqs.as_binary::<i32>();
        for row in 0..batch.num_rows() {
            let seq = seqs.value(row).to_vec();
            rows.insert((
                integer(&ids, row),
                integer(&regions, row),
                value(row),
                integer(&n, row),
                seq,
                integer(&at, row),
            ));
        }
    }
    rows
}

/// The reference's rows and tombstones once `batches` merge into `state`.
fn reference(state: &Merged, batches: &[RecordBatch], shape: &Shape) -> Merged {
    let arrow = Arc::new(schema(&shape.deletion).to_arrow());
    let key = key(shape);
    merge(&arrow, &state.rows, &state.tombstones, batches, &key).expect("the reference merges")
}

/// Commits `batches`, each staged as a segment of its own, to `destination` as commit `seq` of
/// one load.
async fn commit(
    destination: &dyn Destination,
    table: &TableRef,
    seq: u64,
    batches: Vec<RecordBatch>,
) {
    let context = OpenContext {
        pipeline: PipelineId::parse("differential").expect("a valid pipeline id"),
        load_id: LoadId::from_parts(UNIX_EPOCH, 1),
    };
    let mut opened = destination.open(&context).await.expect("a session opens");
    let create = TableChange::Create {
        table: table.clone(),
        schema: TableSchema::from_arrow(&tableless(&batches[0])).expect("a schema"),
    };
    opened
        .session
        .apply_schema(&create)
        .await
        .expect("the table is created");
    let mut writer = opened.session.writer(table).await.expect("a writer opens");
    let segments: Vec<SegmentId> = (0..batches.len() as u64)
        .map(|index| SegmentId(seq * 10 + index))
        .collect();
    for (segment, batch) in segments.iter().zip(batches) {
        writer
            .write(*segment, batch)
            .await
            .expect("the write buffers");
    }
    writer.flush().await.expect("the flush stages");
    let meta = CommitMeta {
        load_id: context.load_id,
        commit_seq: (1..seq).fold(CommitSeq::FIRST, |commit, _| commit.next()),
        epoch: opened.epoch,
        segments: segments.into_iter().collect(),
        state_delta: Vec::new(),
        finish_generations: Vec::new(),
        child_tables: Vec::new(),
        drop_tables: Vec::new(),
    };
    opened
        .session
        .commit(&meta)
        .await
        .expect("the commit lands");
}

/// The stored columns of a written batch's schema.
fn tableless(batch: &RecordBatch) -> SchemaRef {
    let fields: Vec<ArrowField> = batch
        .schema()
        .fields()
        .iter()
        .filter(|field| ![OP_COLUMN, UNCHANGED_COLUMN].contains(&field.name().as_str()))
        .map(|field| {
            let key = ["id", "region"].contains(&field.name().as_str());
            field.as_ref().clone().with_nullable(!key)
        })
        .collect();
    Arc::new(Schema::new(fields))
}

/// Commits `commits` to a SQLite database at `path` and to the reference alike, comparing the
/// rows and tombstones after each.
fn differ(path: &Path, commits: &[Vec<Vec<Change>>], shape: &Shape) -> Result<(), TestCaseError> {
    let runtime = tokio::runtime::Runtime::new().expect("a runtime");
    let destination: Box<dyn Destination> = runtime
        .block_on(
            destination_factory::<SqliteDestination>()
                .connect(json!({ "path": path }), ConnectContext::new()),
        )
        .expect("the destination connects");
    let table = table(shape);
    let mut state = Merged {
        rows: Vec::new(),
        tombstones: Vec::new(),
    };
    for (index, segments) in commits.iter().enumerate() {
        let batches: Vec<RecordBatch> = segments
            .iter()
            .map(|changes| written(changes, &shape.deletion))
            .collect();
        state = reference(&state, &batches, shape);
        let seq = u64::try_from(index).unwrap_or(0) + 1;
        runtime.block_on(commit(destination.as_ref(), &table, seq, batches));
        let stored = published(path, "changes").expect("the table reads");
        prop_assert_eq!(
            rows(&stored),
            rows(&state.rows),
            "rows after commit {}",
            seq
        );
        let buried = published(path, "_rdlt_tombstones__changes").expect("the tombstones read");
        prop_assert_eq!(
            rows(&buried),
            rows(&state.tombstones),
            "tombstones after commit {}",
            seq
        );
    }
    Ok(())
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(rdlt_testkit::cases(256)))]

    #[test]
    fn sqlite_merges_changes_as_the_reference_does(
        commits in commits(),
        soft_deletes in any::<bool>(),
        regions in any::<bool>(),
    ) {
        let deletion = if soft_deletes {
            Deletion::Soft { at: DELETED_AT_COLUMN.into() }
        } else {
            Deletion::Hard
        };
        let shape = Shape { deletion, regions };
        let directory = tempfile::tempdir().expect("a temporary directory");
        differ(&directory.path().join("changes.db"), &commits, &shape)?;
    }
}

#[test]
fn sqlite_lands_a_change_at_a_truncate_s_own_sequence_as_the_reference_does() {
    let change = |op, key, seq| Change {
        op,
        key,
        region: 0,
        value: Some("v".to_owned()),
        n: None,
        at: Some(1),
        flags: 0,
        seq,
    };
    let commits = vec![
        vec![vec![
            change(ChangeOp::Insert, 1, 1),
            change(ChangeOp::Truncate, 0, 4),
            change(ChangeOp::Insert, 2, 4),
        ]],
        vec![vec![change(ChangeOp::Insert, 3, 4)]],
    ];
    for deletion in [
        Deletion::Hard,
        Deletion::Soft {
            at: DELETED_AT_COLUMN.into(),
        },
    ] {
        let shape = Shape {
            deletion,
            regions: false,
        };
        let directory = tempfile::tempdir().expect("a temporary directory");
        differ(&directory.path().join("changes.db"), &commits, &shape).expect("alike");
    }
}
