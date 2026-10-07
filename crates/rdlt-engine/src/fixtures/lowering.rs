//! A batch of each kind a lowering plan prepares, with its plan: what the lowering bench times
//! and the heap-peak tests hold to the charge the write path reserves for it.

#[cfg(test)]
mod tests;

use std::sync::Arc;
use std::time::UNIX_EPOCH;

use arrow_array::{
    Array, ArrayRef, BinaryArray, FixedSizeBinaryArray, Int8Array, Int64Array, RecordBatch,
    StringArray, TimestampNanosecondArray, TimestampSecondArray,
};
use arrow_schema::{Field as ArrowField, Schema};
#[cfg(test)]
use rdlt_connector::cost::Rendering;
use rdlt_connector::{
    Capabilities, ChangeOp, ColumnPath, Field, LoadId, LogicalType, OP_COLUMN, ReadMode,
    SEQ_COLUMN, SchemaVersion, SegmentId, StreamName, TablePath, TableRef, TableSchema, TimeUnit,
    TypeKind, UNCHANGED_COLUMN,
};

use super::events;
#[cfg(test)]
use crate::cost::{CHANGE_ROW, rendering};
use crate::error::Error;
use crate::naming::Naming;
use crate::plan::{StreamPlan, WriteMode};
use crate::policy::{SchemaPolicy, SchemaSettings};
#[cfg(test)]
use crate::table::aligned;
use crate::table::{
    ChangeLayout, ChangeRows, Incoming, LineageColumns, LoweringPlan, MetaNames, Model, Prepared,
    Resolver, Settings, Stamp, TableView,
};

/// Columns of the history case's rows, whose whole rows its versions hash.
const HISTORY_COLUMNS: usize = 50;
/// Columns of the change stream case's rows, whose flags name one of them.
const CHANGE_COLUMNS: usize = 200;

/// A kind of batch a lowering plan prepares: each mode, and each costly path within one.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum LoweringCase {
    /// An append of ten mixed columns, each stored as it arrives.
    AppendNative,
    /// The same and a column of UUIDs, into a destination that stores every column as text.
    AppendText,
    /// A merge whose rows' keys are unique.
    MergeUnique,
    /// A merge whose rows each share their key with one other row.
    MergeDuplicates,
    /// A history table of fifty columns whose versions begin at a column of instants.
    History,
    /// A change stream merged by key, of two hundred columns, each update flagging one of them
    /// unchanged.
    Changes,
    /// A column of JSON whose integers its own column holds and the rest its JSON variant.
    SplitJson,
    /// An append whose one instant its column cannot hold, a value the stream discards.
    OneBadValue,
    /// An append of instants in seconds into a column of microseconds, a tenth of them null.
    TemporalWidening,
}

impl LoweringCase {
    /// Every case, in the order the bench reports them.
    pub(crate) const ALL: [Self; 9] = [
        Self::AppendNative,
        Self::AppendText,
        Self::MergeUnique,
        Self::MergeDuplicates,
        Self::History,
        Self::Changes,
        Self::SplitJson,
        Self::OneBadValue,
        Self::TemporalWidening,
    ];

    /// The case's name in benchmark ids.
    pub(crate) const fn name(self) -> &'static str {
        match self {
            Self::AppendNative => "append_native",
            Self::AppendText => "append_text",
            Self::MergeUnique => "merge_unique",
            Self::MergeDuplicates => "merge_duplicates",
            Self::History => "history",
            Self::Changes => "changes",
            Self::SplitJson => "split_json",
            Self::OneBadValue => "one_bad_value",
            Self::TemporalWidening => "temporal_widening",
        }
    }
}

/// A batch as a partition hands it to lowering, and the plan that prepares it.
#[derive(Debug)]
pub(crate) struct Case {
    plan: LoweringPlan,
    pushed: Pushed,
    /// How the destination stores the columns, which its charge measures.
    #[cfg(test)]
    rendering: Rendering,
    kept: Kept,
}

/// What preparing a case's batch keeps of it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct Kept {
    /// The rows of its table it makes.
    pub(crate) rows: usize,
    /// The values the stream discards.
    pub(crate) discarded_values: u64,
}

/// A batch as it was pushed.
#[derive(Debug)]
enum Pushed {
    /// Its data alone.
    Data(RecordBatch),
    /// A change batch: its data and its change columns.
    Changes(RecordBatch),
}

/// How a destination stores what its tables hold.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Storage {
    /// In the types the minimal capabilities name.
    Native,
    /// As text, every column.
    Text,
}

impl Storage {
    /// What a destination storing so declares.
    fn capabilities(self) -> Capabilities {
        let mut capabilities = Capabilities::minimal();
        if self == Self::Text {
            capabilities.types = [TypeKind::Utf8].into();
        }
        capabilities
    }
}

/// How a case's table names its metadata columns.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Meta {
    /// An append's.
    Plain,
    /// A merge's, with its sequence.
    Merged,
    /// A history table's, its versions beginning at the column `at`.
    History,
    /// A change stream's merged by key.
    Changes,
}

impl Meta {
    /// The names under `naming`.
    fn names(self, naming: &Naming) -> MetaNames {
        let merged = self != Self::Plain;
        let layout = (self == Self::Changes).then_some(ChangeLayout::Merge { soft: false });
        let names = MetaNames::assign_changes(naming, merged, LineageColumns::None, layout);
        match self {
            Self::History => names.with_history(naming, Some(Arc::from("at"))),
            _ => names,
        }
    }
}

/// What a case is made of: its stream, its table's metadata, the batch its table is first made
/// of, and the batch it pushes.
struct Made {
    stream: StreamPlan,
    meta: Meta,
    created: RecordBatch,
    pushed: Pushed,
}

impl Made {
    /// An append of `batch` into a table made of it.
    fn append(batch: RecordBatch) -> Self {
        Self::into_table(batch.clone(), batch)
    }

    /// An append of `pushed` into a table made of `created`.
    fn into_table(created: RecordBatch, pushed: RecordBatch) -> Self {
        Self {
            stream: stream(),
            meta: Meta::Plain,
            created,
            pushed: Pushed::Data(pushed),
        }
    }

    /// A stream keyed by `id`, written as `mode`, with its table's `meta`, of `batch`.
    fn keyed(mode: WriteMode, meta: Meta, batch: RecordBatch) -> Self {
        Self {
            stream: stream().write(mode).key(["id"]),
            meta,
            created: batch.clone(),
            pushed: Pushed::Data(batch),
        }
    }
}

impl Case {
    /// The batch of `rows` rows of `case`, and its plan, for the destination the case names: one
    /// storing text for [`LoweringCase::AppendText`], one storing native types for the rest.
    pub(crate) fn new(case: LoweringCase, rows: u32) -> Self {
        let storage = match case {
            LoweringCase::AppendText => Storage::Text,
            _ => Storage::Native,
        };
        Self::stored(case, rows, storage)
    }

    /// The batch of `rows` rows of `case`, and its plan, for a destination storing as `storage`.
    ///
    /// # Panics
    ///
    /// Panics where `rows` is below two, which leaves a merge no duplicate.
    pub(crate) fn stored(case: LoweringCase, rows: u32, storage: Storage) -> Self {
        assert!(rows >= 2, "a case holds two rows at least");
        let mixed = events(0, rows, 10);
        let made = match case {
            LoweringCase::AppendNative => Made::append(mixed),
            LoweringCase::AppendText => {
                Made::append(with_column(&mixed, "uuid", LogicalType::Uuid, uuids(rows)))
            }
            LoweringCase::MergeUnique => Made::keyed(WriteMode::Merge, Meta::Merged, mixed),
            LoweringCase::MergeDuplicates => {
                let ids = (0..i64::from(rows)).map(|row| row % i64::from(rows / 2));
                let ids: ArrayRef = Arc::new(Int64Array::from_iter_values(ids));
                let batch = replaced(&mixed, "id", LogicalType::Int64, ids);
                Made::keyed(WriteMode::Merge, Meta::Merged, batch)
            }
            LoweringCase::History => {
                let batch = events(0, rows, HISTORY_COLUMNS);
                Made::keyed(WriteMode::History, Meta::History, batch)
            }
            LoweringCase::Changes => {
                let batch = events(0, rows, CHANGE_COLUMNS);
                let mut made = Made::keyed(WriteMode::Merge, Meta::Changes, batch.clone());
                made.stream = made.stream.read(ReadMode::Cdc);
                made.pushed = Pushed::Changes(changed(&batch));
                made
            }
            LoweringCase::SplitJson => {
                let batch = replaced(&mixed, "a", LogicalType::Json, json(rows));
                Made::into_table(mixed, batch)
            }
            LoweringCase::OneBadValue => {
                let mut made = bad_instant(&mixed, rows);
                let policy = SchemaSettings::new().policy(SchemaPolicy::DiscardValue);
                made.stream = made.stream.schema(policy);
                made
            }
            LoweringCase::TemporalWidening => {
                let seconds = (0..i64::from(rows)).map(|row| (row % 10 != 0).then_some(row));
                let seconds = TimestampSecondArray::from(seconds.collect::<Vec<_>>());
                let seconds = Arc::new(seconds.with_timezone("UTC"));
                let batch = replaced(&mixed, "at", instant(TimeUnit::Second), seconds);
                Made::into_table(mixed, batch)
            }
        };
        let mut built = Self::planned(storage.capabilities(), made);
        match case {
            LoweringCase::MergeDuplicates => built.kept.rows /= 2,
            LoweringCase::OneBadValue => built.kept.discarded_values = 1,
            _ => {}
        }
        built
    }

    /// The plan of what `made` pushes into its table, for a destination of `capabilities`.
    fn planned(capabilities: Capabilities, made: Made) -> Self {
        let naming = Naming::checked(&capabilities.identifiers)
            .expect("the minimal rules leave every metadata column an identifier");
        let key = made.stream.merge_key().map(<[ColumnPath]>::to_vec);
        #[cfg(test)]
        let rendering = rendering(&capabilities);
        let resolver = Resolver {
            stream: made.stream.name().clone(),
            settings: Settings {
                pipeline: SchemaSettings::default(),
                stream: made.stream,
                key: key.unwrap_or_default(),
                owner: None,
            },
            meta: made.meta.names(&naming),
            naming,
            capabilities: Arc::new(capabilities),
            root: None,
            columns: u64::MAX,
            unwidened: std::collections::BTreeSet::new(),
        };
        let pushed = made.pushed;
        let model = resolver
            .resolve(&Model::default(), &incoming(&made.created))
            .expect("a table takes the columns it is made of")
            .model;
        let rows = match &pushed {
            Pushed::Data(batch) | Pushed::Changes(batch) => batch.num_rows(),
        };
        Self {
            plan: plan(&resolver, &model, &pushed),
            pushed,
            #[cfg(test)]
            rendering,
            kept: Kept {
                rows,
                discarded_values: 0,
            },
        }
    }

    /// The rows the batch holds.
    #[cfg(test)]
    pub(crate) fn rows(&self) -> usize {
        self.batch().num_rows()
    }

    /// The batch as it was pushed.
    #[cfg(test)]
    pub(crate) fn batch(&self) -> &RecordBatch {
        match &self.pushed {
            Pushed::Data(batch) | Pushed::Changes(batch) => batch,
        }
    }

    /// What preparing the batch keeps of it.
    pub(crate) fn kept(&self) -> Kept {
        self.kept
    }

    /// The plan that prepares the batch.
    #[cfg(test)]
    pub(crate) fn plan(&self) -> &LoweringPlan {
        &self.plan
    }

    /// The batch prepared as the write path prepares it: a change batch split into its data and
    /// its change columns first.
    pub(crate) fn prepare(&self) -> Result<Prepared, Error> {
        match &self.pushed {
            Pushed::Data(batch) => self.plan.prepare(batch, None, &stamp(), None),
            Pushed::Changes(batch) => {
                let (data, changes) = ChangeRows::split(batch)
                    .map_err(|error| Error::internal(format!("splitting changes: {error}")))?;
                self.plan.prepare(&data, None, &stamp(), Some(&changes))
            }
        }
    }

    /// Bytes: what the write path reserves for lowering the batch, as one piece.
    #[cfg(test)]
    pub(crate) fn charge(&self) -> u64 {
        let (stored, beside) = match &self.pushed {
            Pushed::Data(_) => (self.plan.stored(), 0),
            Pushed::Changes(batch) => (aligned(batch, &self.plan.stored()), CHANGE_ROW),
        };
        let row = self.plan.row_bytes().saturating_add(beside);
        let mut measure = self.rendering.lowering(self.batch(), stored, row, u64::MAX);
        measure.expanded(0..self.rows())
    }
}

/// What every case's batch was received as: segment 1 of a load, its rows the segment's first.
fn stamp() -> Stamp {
    Stamp {
        load_id: LoadId::from_parts(UNIX_EPOCH, 1),
        loaded_at: UNIX_EPOCH,
        received_at: UNIX_EPOCH,
        segment: SegmentId(1),
        first_row: 0,
    }
}

/// A stream of the table `events`.
fn stream() -> StreamPlan {
    StreamPlan::new(StreamName::new("events").expect("a valid name"))
}

/// The plan that `resolver` makes for `pushed`'s data into a table of `model`.
fn plan(resolver: &Resolver, model: &Model, pushed: &Pushed) -> LoweringPlan {
    let data = match pushed {
        Pushed::Data(batch) => batch.clone(),
        Pushed::Changes(batch) => ChangeRows::data(batch).expect("a change batch has data"),
    };
    let incoming = incoming(&data);
    let resolution = resolver
        .resolve(model, &incoming)
        .expect("the table takes the batch");
    let table = TableRef {
        path: TablePath::new(["events"]).expect("a valid path"),
        name: "events".into(),
        version: SchemaVersion(1),
        generation: None,
        merge: None,
    };
    let view = TableView::new(&table, resolution.model, resolver).expect("a valid view");
    let policies = incoming
        .paths
        .iter()
        .map(|path| resolver.settings.column(path).policy)
        .collect();
    LoweringPlan::new(
        resolver.stream.clone(),
        Arc::new(view),
        incoming,
        resolution.routes,
    )
    .with_policies(policies)
}

/// An append of `mixed`'s rows into a table holding its instants in nanoseconds, the instants
/// pushed in seconds, the middle row's one no nanosecond holds.
fn bad_instant(mixed: &RecordBatch, rows: u32) -> Made {
    let nanos = TimestampNanosecondArray::from_iter_values(0..i64::from(rows));
    let nanos = Arc::new(nanos.with_timezone("UTC"));
    let created = replaced(mixed, "at", instant(TimeUnit::Nanosecond), nanos);
    let far = (0..rows).map(|row| Some(if row == rows / 2 { i64::MAX } else { 1 }));
    let seconds = TimestampSecondArray::from(far.collect::<Vec<_>>());
    let seconds = Arc::new(seconds.with_timezone("UTC"));
    let pushed = replaced(&created, "at", instant(TimeUnit::Second), seconds);
    Made::into_table(created, pushed)
}

/// The columns of `batch` as they arrive, its integers judged.
fn incoming(batch: &RecordBatch) -> Incoming {
    let schema = TableSchema::from_arrow(&batch.schema()).expect("a case's columns have types");
    let paths = schema
        .fields()
        .iter()
        .map(|field| ColumnPath::from(field.name()))
        .collect();
    Incoming::of(schema, paths, std::slice::from_ref(batch))
}

/// A timestamp of `unit` in UTC.
fn instant(unit: TimeUnit) -> LogicalType {
    LogicalType::Timestamp(unit, Some("UTC".into()))
}

/// `batch` and a column `name` of `logical` type holding `values`.
fn with_column(
    batch: &RecordBatch,
    name: &str,
    logical: LogicalType,
    values: ArrayRef,
) -> RecordBatch {
    let schema = batch.schema();
    let mut fields: Vec<ArrowField> = schema
        .fields()
        .iter()
        .map(|field| field.as_ref().clone())
        .collect();
    fields.push(Field::new(name, logical, true).to_arrow());
    let mut columns = batch.columns().to_vec();
    columns.push(values);
    RecordBatch::try_new(Arc::new(Schema::new(fields)), columns)
        .expect("a column of the batch's rows")
}

/// `batch` with its column `name` of `logical` type holding `values` in its place.
fn replaced(
    batch: &RecordBatch,
    name: &str,
    logical: LogicalType,
    values: ArrayRef,
) -> RecordBatch {
    let schema = batch.schema();
    let index = schema.index_of(name).expect("the batch has the column");
    let mut fields: Vec<ArrowField> = schema
        .fields()
        .iter()
        .map(|field| field.as_ref().clone())
        .collect();
    fields[index] = Field::new(name, logical, true).to_arrow();
    let mut columns = batch.columns().to_vec();
    columns[index] = values;
    RecordBatch::try_new(Arc::new(Schema::new(fields)), columns)
        .expect("a column of the batch's rows")
}

/// `rows` distinct UUIDs.
fn uuids(rows: u32) -> ArrayRef {
    let values = (0..u128::from(rows)).map(|row| (row * 0x9E37_79B9_7F4A_7C15).to_be_bytes());
    Arc::new(FixedSizeBinaryArray::try_from_iter(values).expect("sixteen bytes a value"))
}

/// `rows` values of JSON text: integers, but every tenth a string and every hundredth an object.
fn json(rows: u32) -> ArrayRef {
    let value = |row: u32| match row {
        row if row % 100 == 0 => format!(r#"{{"cents":{row}}}"#),
        row if row % 10 == 0 => format!(r#""{row}""#),
        row => row.to_string(),
    };
    Arc::new(StringArray::from_iter_values((0..rows).map(value)))
}

/// `data` as a change batch of updates, each flagging one column but its key unchanged.
fn changed(data: &RecordBatch) -> RecordBatch {
    let rows = data.num_rows();
    let columns = data.num_columns();
    let ops = Int8Array::from(vec![ChangeOp::Update.code(); rows]);
    let seqs = (0..rows).map(|row| (row as u128).to_be_bytes());
    let seqs = BinaryArray::from_iter_values(seqs);
    let bytes = (columns + 3).div_ceil(8);
    let flags = (0..rows).map(|row| {
        let flagged = 1 + row % (columns - 1);
        let mut bitmap = vec![0_u8; bytes];
        bitmap[flagged / 8] |= 1 << (flagged % 8);
        bitmap
    });
    let flags = BinaryArray::from_iter_values(flags);
    let schema = data.schema();
    let mut fields: Vec<ArrowField> = schema
        .fields()
        .iter()
        .map(|field| field.as_ref().clone())
        .collect();
    fields.extend([
        ArrowField::new(OP_COLUMN, ops.data_type().clone(), false),
        ArrowField::new(SEQ_COLUMN, seqs.data_type().clone(), false),
        ArrowField::new(UNCHANGED_COLUMN, flags.data_type().clone(), true),
    ]);
    let mut arrays = data.columns().to_vec();
    arrays.extend([Arc::new(ops) as ArrayRef, Arc::new(seqs), Arc::new(flags)]);
    RecordBatch::try_new(Arc::new(Schema::new(fields)), arrays).expect("a change batch")
}
