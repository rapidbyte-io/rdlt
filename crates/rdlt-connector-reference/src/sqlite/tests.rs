//! The SQLite destination merges change streams and history tables as the reference merge does:
//! the same written batches, committed in turn to both, leave the same rows and the same
//! tombstones.

mod cases;

use std::path::Path;
use std::sync::Arc;
use std::time::UNIX_EPOCH;

use arrow_array::{
    ArrayRef, BinaryArray, BooleanArray, Int8Array, Int64Array, RecordBatch, StringArray,
};
use arrow_cast::display::{ArrayFormatter, FormatOptions};
use arrow_schema::{DataType, Field as ArrowField, Schema, SchemaRef};
use proptest::prelude::*;
use rdlt_connector::{
    ChangeColumns, ChangeOp, CommitMeta, CommitSeq, ConnectContext, DELETED_AT_COLUMN, Deletion,
    Destination, HistoryColumns, LoadId, MergeKey, OP_COLUMN, OpenContext, PipelineId, SEQ_COLUMN,
    SchemaVersion, SegmentId, TableChange, TablePath, TableRef, TableSchema, UNCHANGED_COLUMN,
    destination_factory,
};
use serde_json::json;

use super::{SqliteDestination, published};
use crate::merge::Merged;
use crate::merge::tests::merge;

/// One change: its op, key and region, values, deletion time, the columns it flags unchanged
/// (bit 0 `value`, bit 1 `n`, bit 2 the deletion time), its sequence, and for a history table
/// when it happened.
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
    from: i64,
}

/// How a case's table merges.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Mode {
    /// A change stream whose deletes remove.
    Hard,
    /// A change stream whose deletes mark.
    Soft,
    /// A history of rows that are all upserts.
    HistoryPlain,
    /// A history of a change stream whose deletes close.
    HistoryHard,
    /// A history of a change stream whose deletes keep a deleted version.
    HistorySoft,
}

const MODES: [Mode; 5] = [
    Mode::Hard,
    Mode::Soft,
    Mode::HistoryPlain,
    Mode::HistoryHard,
    Mode::HistorySoft,
];

impl Mode {
    fn history(self) -> bool {
        matches!(
            self,
            Self::HistoryPlain | Self::HistoryHard | Self::HistorySoft
        )
    }

    fn soft(self) -> bool {
        matches!(self, Self::Soft | Self::HistorySoft)
    }

    fn changes(self) -> bool {
        self != Self::HistoryPlain
    }
}

/// A case's table: how it merges, and whether its key spans the region too.
#[derive(Clone, Copy, Debug)]
struct Shape {
    mode: Mode,
    regions: bool,
}

fn key(shape: Shape) -> MergeKey {
    let mode = shape.mode;
    let mut columns = vec!["id".into()];
    if shape.regions {
        columns.push("region".into());
    }
    MergeKey {
        columns,
        seq: SEQ_COLUMN.into(),
        root: None,
        changes: mode.changes().then(|| ChangeColumns {
            op: OP_COLUMN.into(),
            unchanged: (!mode.history()).then(|| UNCHANGED_COLUMN.into()),
            deletion: if mode.soft() {
                Deletion::Soft {
                    at: DELETED_AT_COLUMN.into(),
                }
            } else {
                Deletion::Hard
            },
        }),
        history: mode.history().then(|| HistoryColumns {
            valid_from: "valid_from".into(),
            valid_to: "valid_to".into(),
            is_current: "is_current".into(),
            row_hash: "row_hash".into(),
        }),
    }
}

fn table(shape: Shape) -> TableRef {
    TableRef {
        path: TablePath::new(["changes"]).expect("a valid table path"),
        name: "changes".into(),
        version: SchemaVersion(1),
        generation: None,
        merge: Some(key(shape)),
    }
}

/// The table's stored columns: its key and region, value and counter, its sequence, where
/// deletes are soft its deletion time, and a history table's columns.
fn stored(shape: Shape) -> SchemaRef {
    let keyed = |name: &str| name == "id" || (shape.regions && name == "region");
    let mut fields = vec![
        ArrowField::new("id", DataType::Int64, !keyed("id")),
        ArrowField::new("region", DataType::Int64, !keyed("region")),
        ArrowField::new("value", DataType::Utf8, true),
        ArrowField::new("n", DataType::Int64, true),
        ArrowField::new(SEQ_COLUMN, DataType::Binary, false),
    ];
    if shape.mode.soft() {
        fields.push(ArrowField::new(DELETED_AT_COLUMN, DataType::Int64, true));
    }
    if shape.mode.history() {
        fields.extend([
            ArrowField::new("valid_from", DataType::Int64, false),
            ArrowField::new("valid_to", DataType::Int64, true),
            ArrowField::new("is_current", DataType::Boolean, false),
            ArrowField::new("row_hash", DataType::Binary, true),
        ]);
    }
    Arc::new(Schema::new(fields))
}

fn sequence(seq: u16) -> Vec<u8> {
    let mut bytes = vec![0; 16];
    bytes[14..].copy_from_slice(&seq.to_be_bytes());
    bytes
}

/// The key, value, sequence and deletion time columns of `changes` as `mode` writes them.
fn data(changes: &[Change], mode: Mode) -> Vec<(&'static str, ArrayRef)> {
    let keyed = |c: &Change| !mode.changes() || c.op != ChangeOp::Truncate;
    let upsert =
        |c: &Change| !mode.changes() || matches!(c.op, ChangeOp::Insert | ChangeOp::Update);
    let integers = |of: &dyn Fn(&Change) -> Option<i64>| -> ArrayRef {
        Arc::new(changes.iter().map(of).collect::<Int64Array>())
    };
    let values: StringArray = changes
        .iter()
        .map(|c| upsert(c).then(|| c.value.clone()).flatten())
        .collect();
    let seqs = BinaryArray::from_iter_values(changes.iter().map(|c| sequence(c.seq)));
    let mut columns: Vec<(&str, ArrayRef)> = vec![
        ("id", integers(&|c| keyed(c).then_some(c.key))),
        ("region", integers(&|c| keyed(c).then_some(c.region))),
        ("value", Arc::new(values)),
        ("n", integers(&|c| upsert(c).then_some(c.n).flatten())),
        (SEQ_COLUMN, Arc::new(seqs)),
    ];
    if mode.soft() {
        // A history table's upserts never say when they were deleted, and its deletes always
        // do, as the engine writes them.
        let at = |c: &Change| match (mode.history(), upsert(c)) {
            (true, true) => None,
            (true, false) => Some(c.at.unwrap_or(c.from)),
            (false, _) => c.at,
        };
        columns.push((DELETED_AT_COLUMN, integers(&at)));
    }
    columns
}

/// `changes` as `mode` writes them: the stored columns, then for a change stream the op, and
/// where rows may flag columns unchanged, the flags, a bitmap over the batch's fields.
fn written(changes: &[Change], mode: Mode) -> RecordBatch {
    let upsert =
        |c: &Change| !mode.changes() || matches!(c.op, ChangeOp::Insert | ChangeOp::Update);
    let mut columns = data(changes, mode);
    if mode.history() {
        let hashes: BinaryArray = changes
            .iter()
            .map(|c| upsert(c).then(|| format!("{:?}|{:?}", c.value, c.n).into_bytes()))
            .collect();
        let from = Int64Array::from_iter_values(changes.iter().map(|c| c.from));
        columns.extend([
            ("valid_from", Arc::new(from) as ArrayRef),
            ("valid_to", Arc::new(Int64Array::new_null(changes.len()))),
            (
                "is_current",
                Arc::new(BooleanArray::from(vec![true; changes.len()])),
            ),
            ("row_hash", Arc::new(hashes)),
        ]);
    }
    if mode.changes() {
        let ops = Int8Array::from_iter_values(changes.iter().map(|c| c.op.code()));
        columns.push((OP_COLUMN, Arc::new(ops)));
    }
    if !mode.history() {
        // Fields 2 and 3 are the value and the counter, 5 the deletion time.
        let flags = changes.iter().map(|c| {
            let at = if mode.soft() { (c.flags & 4) << 3 } else { 0 };
            let bitmap = ((c.flags & 3) << 2) | at;
            (bitmap != 0 && upsert(c)).then(|| vec![bitmap])
        });
        columns.push((UNCHANGED_COLUMN, Arc::new(flags.collect::<BinaryArray>())));
    }
    RecordBatch::try_from_iter(columns).expect("a valid batch")
}

/// Every row of `batches` as the text of each of `columns`, null where a batch lacks one, with
/// booleans as SQLite keeps them; sorted.
fn rows(batches: &[RecordBatch], columns: &[&str]) -> Vec<Vec<String>> {
    let options = FormatOptions::default().with_null("NULL");
    let mut rows = Vec::new();
    for batch in batches {
        let formatters: Vec<Option<ArrayFormatter<'_>>> = columns
            .iter()
            .map(|name| {
                let column = batch.column_by_name(name)?;
                Some(ArrayFormatter::try_new(column.as_ref(), &options).expect("a formatter"))
            })
            .collect();
        for row in 0..batch.num_rows() {
            let cell = |formatter: &Option<ArrayFormatter<'_>>| match formatter {
                Some(formatter) => formatter.value(row).to_string(),
                None => "NULL".to_owned(),
            };
            let text = |cell: String| match cell.as_str() {
                "true" => "1".to_owned(),
                "false" => "0".to_owned(),
                _ => cell,
            };
            rows.push(formatters.iter().map(cell).map(text).collect());
        }
    }
    rows.sort();
    rows
}

/// Commits `batches`, each staged as a segment of its own, to `destination` as commit `seq` of
/// one load.
async fn commit(
    destination: &dyn Destination,
    (table, schema): (&TableRef, &SchemaRef),
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
        schema: TableSchema::from_arrow(schema).expect("a schema"),
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

/// Commits `commits` to a SQLite database at `path` and to the reference alike, comparing the
/// rows and tombstones after each.
fn differ(path: &Path, commits: &[Vec<Vec<Change>>], shape: Shape) -> Result<(), TestCaseError> {
    let runtime = tokio::runtime::Runtime::new().expect("a runtime");
    let destination: Box<dyn Destination> = runtime
        .block_on(
            destination_factory::<SqliteDestination>()
                .connect(json!({ "path": path }), ConnectContext::new()),
        )
        .expect("the destination connects");
    let (table, key, stored) = (table(shape), key(shape), stored(shape));
    let columns: Vec<&str> = stored.fields().iter().map(|f| f.name().as_str()).collect();
    let mut buried: Vec<&str> = key.columns.iter().map(AsRef::as_ref).collect();
    buried.push(SEQ_COLUMN);
    let mut state = Merged {
        rows: Vec::new(),
        tombstones: Vec::new(),
    };
    for (index, segments) in commits.iter().enumerate() {
        let batches: Vec<RecordBatch> = segments
            .iter()
            .map(|changes| written(changes, shape.mode))
            .collect();
        state = merge(&stored, &state.rows, &state.tombstones, &batches, &key)
            .expect("the reference merges");
        let seq = u64::try_from(index).unwrap_or(0) + 1;
        runtime.block_on(commit(
            destination.as_ref(),
            (&table, &stored),
            seq,
            batches,
        ));
        let held = published(path, "changes").expect("the table reads");
        let (held, expected) = (rows(&held, &columns), rows(&state.rows, &columns));
        prop_assert_eq!(held, expected, "rows after commit {}", seq);
        if !shape.mode.changes() {
            continue;
        }
        // The tombstones are no table read-back serves: a reader of the file itself reads them.
        let reader = super::database::reading(path)
            .expect("the database opens")
            .expect("the database is there");
        let stones =
            super::values::read_table(&reader, &super::Sqlite, "_rdlt_tombstones__changes")
                .expect("the tombstones read");
        let (stones, expected) = (rows(&stones, &buried), rows(&state.tombstones, &buried));
        prop_assert_eq!(stones, expected, "tombstones after commit {}", seq);
    }
    Ok(())
}

fn change() -> impl Strategy<Value = Change> {
    let op = prop_oneof![
        3 => Just(ChangeOp::Insert),
        3 => Just(ChangeOp::Update),
        2 => Just(ChangeOp::Delete),
        2 => Just(ChangeOp::Truncate),
    ];
    let value = proptest::option::of("[a-c]{1,2}");
    let number = proptest::option::of(0_i64..4);
    let key = (0_i64..4, 0_i64..2);
    // Few sequences: changes sit at a truncate's own, and at a row's or a version's.
    let placed = (0_u16..24, 0_i64..50);
    (op, key, value, number.clone(), number, 0_u8..8, placed).prop_map(
        |(op, (key, region), value, n, at, flags, (seq, from))| Change {
            op,
            key,
            region,
            value,
            n,
            at,
            flags,
            seq,
            from,
        },
    )
}

/// Two to four commits of changes in one to three segments, sequenced close together.
///
/// A sequence is one key's change or one truncate, as a source numbers them, but a truncate
/// and a key's change may share one; some changes are sent again as they were, and some commits
/// are an earlier commit whole.
fn commits() -> impl Strategy<Value = Vec<Vec<Vec<Change>>>> {
    let segment = proptest::collection::vec((change(), any::<Option<u8>>()), 0..7);
    let commit = (
        proptest::collection::vec(segment, 1..=3),
        proptest::option::weighted(0.15, any::<u8>()),
    );
    proptest::collection::vec(commit, 2..=4).prop_map(|drawn| {
        let mut sent: Vec<Change> = Vec::new();
        let mut commits: Vec<Vec<Vec<Change>>> = Vec::new();
        for (segments, replay) in drawn {
            if let Some(replay) = replay.filter(|_| !commits.is_empty()) {
                commits.push(commits[usize::from(replay) % commits.len()].clone());
                continue;
            }
            let commit = segments
                .into_iter()
                .map(|segment| sent_once(segment, &mut sent))
                .collect();
            commits.push(commit);
        }
        commits
    })
}

/// `segment`'s changes, each one `sent` before sent again as it was, or a change whose sequence
/// no change of its key, or for a truncate no truncate, in `sent` has.
fn sent_once(segment: Vec<(Change, Option<u8>)>, sent: &mut Vec<Change>) -> Vec<Change> {
    let subject = |change: &Change| match change.op {
        ChangeOp::Truncate => None,
        _ => Some((change.key, change.region)),
    };
    let mut changes = Vec::new();
    for (change, again) in segment {
        if let Some(again) = again.filter(|again| again % 4 == 0 && !sent.is_empty()) {
            changes.push(sent[usize::from(again / 4) % sent.len()].clone());
            continue;
        }
        let taken = sent.iter().any(|earlier| {
            // A key's own region may differ where the table's key does not span it.
            earlier.seq == change.seq
                && subject(earlier).map(|(key, _)| key) == subject(&change).map(|(key, _)| key)
        });
        if !taken {
            sent.push(change.clone());
            changes.push(change);
        }
    }
    changes
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(rdlt_testkit::cases(24)))]

    #[test]
    fn sqlite_merges_hard_changes_as_the_reference_does(
        commits in commits(),
        regions in any::<bool>(),
    ) {
        compared(&commits, Shape { mode: Mode::Hard, regions })?;
    }

    #[test]
    fn sqlite_merges_soft_changes_as_the_reference_does(
        commits in commits(),
        regions in any::<bool>(),
    ) {
        compared(&commits, Shape { mode: Mode::Soft, regions })?;
    }

    #[test]
    fn sqlite_keeps_a_history_of_upserts_as_the_reference_does(
        commits in commits(),
        regions in any::<bool>(),
    ) {
        compared(&commits, Shape { mode: Mode::HistoryPlain, regions })?;
    }

    #[test]
    fn sqlite_keeps_a_history_of_hard_changes_as_the_reference_does(
        commits in commits(),
        regions in any::<bool>(),
    ) {
        compared(&commits, Shape { mode: Mode::HistoryHard, regions })?;
    }

    #[test]
    fn sqlite_keeps_a_history_of_soft_changes_as_the_reference_does(
        commits in commits(),
        regions in any::<bool>(),
    ) {
        compared(&commits, Shape { mode: Mode::HistorySoft, regions })?;
    }
}

/// Commits `commits` to a database of its own and to the reference, comparing them.
fn compared(commits: &[Vec<Vec<Change>>], shape: Shape) -> Result<(), TestCaseError> {
    let directory = tempfile::tempdir().expect("a temporary directory");
    differ(&directory.path().join("changes.db"), commits, shape)
}
