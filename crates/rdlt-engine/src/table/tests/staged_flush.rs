//! A normalized merge's flush, cut into pieces, staged as each key's last row of the flush with
//! its children or as each piece's last rows with every child, publishes the same tables through
//! the SDK's merge and counts the same discarded values: what the engine leaves out is what the
//! destination would drop.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use arrow_array::builder::{ListBuilder, StringBuilder};
use arrow_array::{
    Array, ArrayRef, Float64Array, Int64Array, ListArray, RecordBatch, StringArray, StructArray,
    TimestampSecondArray,
};
use arrow_buffer::{BooleanBuffer, NullBuffer, OffsetBuffer};
use arrow_cast::display::{ArrayFormatter, FormatOptions};
use arrow_schema::{DataType, Field as ArrowField, Fields, SchemaRef};
use proptest::prelude::*;
use rdlt_connector::merge::{merge_children_sparse, merge_sparse, read_back};
use rdlt_connector::{
    ColumnPath, Field, LogicalType, MergeKey, RootKey, SchemaVersion, SegmentId, StreamName,
    TablePath, TableRef, TableSchema, TimeUnit,
};

use super::{capabilities, plan, resolver, stamp};
use crate::normalize::{Part, PartSchemas, Pruned, PushKind, Shape, normalize};
use crate::policy::SchemaPolicy;
use crate::table::lowering::LoweringPlan;
use crate::table::resolve::{Incoming, Resolver};
use crate::table::{
    Creation, LineageColumns, MetaNames, Model, Stamp, TableView, key_values, last_across,
    with_columns,
};

/// How a stream's merge key arrives.
#[derive(Clone, Copy, Debug)]
enum Keyed {
    /// One column of integers.
    Int,
    /// One column of floats, among them both zeros, which are one key.
    Float,
    /// One column of text.
    Text,
    /// A column of integers and one of text.
    Composite,
}

impl Keyed {
    fn columns(self) -> &'static [&'static str] {
        match self {
            Self::Composite => &["id", "tag"],
            _ => &["id"],
        }
    }
}

/// A row: its key, whether it holds an instant its column cannot hold, whose value its policy
/// discards, and where it holds an array, its items, each a sku and its tags.
#[derive(Clone, Debug)]
struct Row {
    key: u8,
    beyond: bool,
    items: Option<Vec<(u8, Vec<u8>)>>,
}

fn rows() -> impl Strategy<Value = Vec<Row>> {
    let item = (0_u8..8, proptest::collection::vec(0_u8..4, 0..3));
    let row = (
        0_u8..4,
        proptest::bool::weighted(0.2),
        proptest::option::weighted(0.8, proptest::collection::vec(item, 0..3)),
    );
    proptest::collection::vec(
        row.prop_map(|(key, beyond, items)| Row { key, beyond, items }),
        1..24,
    )
}

/// A flush of rows and where it is cut into pieces: each cut the rows of a piece.
fn flush() -> impl Strategy<Value = (Vec<Row>, Vec<usize>)> {
    (rows(), proptest::collection::vec(1_usize..8, 0..4))
}

/// `rows` as a batch whose key arrives as `keyed` says.
fn batch_of(rows: &[Row], keyed: Keyed) -> RecordBatch {
    let keys = rows.iter().map(|row| row.key);
    let mut columns: Vec<(&str, ArrayRef)> = match keyed {
        Keyed::Int => vec![(
            "id",
            Arc::new(Int64Array::from_iter_values(keys.map(i64::from))),
        )],
        Keyed::Float => {
            let zero = |(index, key): (usize, u8)| match (key, index % 2) {
                (0, 1) => -0.0,
                _ => f64::from(key),
            };
            let floats = keys.enumerate().map(zero);
            vec![("id", Arc::new(Float64Array::from_iter_values(floats)))]
        }
        Keyed::Text => {
            let texts = keys.map(|key| key.to_string());
            vec![("id", Arc::new(StringArray::from_iter_values(texts)))]
        }
        Keyed::Composite => vec![
            (
                "id",
                Arc::new(Int64Array::from_iter_values(
                    keys.clone().map(|key| i64::from(key / 2)),
                )),
            ),
            (
                "tag",
                Arc::new(StringArray::from_iter_values(
                    keys.map(|key| format!("t{}", key % 2)),
                )),
            ),
        ],
    };
    let instants = rows.iter().enumerate().map(|(index, row)| {
        if row.beyond {
            i64::MAX
        } else {
            i64::try_from(index).unwrap()
        }
    });
    let at = TimestampSecondArray::from_iter_values(instants).with_timezone("UTC");
    columns.push(("at", Arc::new(at)));
    columns.push(("items", items(rows)));
    RecordBatch::try_from_iter(columns).unwrap()
}

/// The arrays of `rows`' items, each an object of its sku and its tags.
fn items(rows: &[Row]) -> ArrayRef {
    let (mut skus, mut tags) = (StringBuilder::new(), ListBuilder::new(StringBuilder::new()));
    let (mut offsets, mut held) = (vec![0_i32], Vec::with_capacity(rows.len()));
    let mut count = 0_i32;
    for row in rows {
        for (sku, labels) in row.items.iter().flatten() {
            count += 1;
            skus.append_value(format!("s{sku}"));
            for label in labels {
                tags.values().append_value(format!("l{label}"));
            }
            tags.append(true);
        }
        held.push(row.items.is_some());
        offsets.push(count);
    }
    let tags = tags.finish();
    let fields = Fields::from(vec![
        ArrowField::new("sku", DataType::Utf8, true),
        ArrowField::new("tags", tags.data_type().clone(), true),
    ]);
    let objects = StructArray::new(
        fields.clone(),
        vec![Arc::new(skus.finish()), Arc::new(tags)],
        None,
    );
    let item = Arc::new(ArrowField::new("item", DataType::Struct(fields), true));
    Arc::new(ListArray::new(
        item,
        OffsetBuffer::new(offsets.into()),
        Arc::new(objects),
        Some(NullBuffer::from(held)),
    ))
}

/// `rows` cut into pieces of `cuts` rows each, the last taking the rest: the row each starts at
/// and its rows.
fn pieces(rows: usize, cuts: &[usize]) -> Vec<(usize, usize)> {
    let mut pieces = Vec::new();
    let mut first = 0;
    for cut in cuts {
        let count = (*cut).min(rows - first);
        if count == 0 {
            break;
        }
        pieces.push((first, count));
        first += count;
    }
    if first < rows {
        pieces.push((first, rows - first));
    }
    pieces
}

/// One table's staged rows, or once published its rows, with what merges them.
struct Staged {
    schema: SchemaRef,
    key: MergeKey,
    batches: Vec<RecordBatch>,
}

/// The parts of `piece`, a slice of a flush whose key arrives as `keyed` says, split as the
/// engine splits them: the key columns normalized, each negative zero the zero, then normalized.
fn split(piece: &RecordBatch, keyed: Keyed) -> Vec<Part> {
    let stream = StreamName::new("s").unwrap();
    let mut columns = piece.columns().to_vec();
    for name in keyed.columns() {
        let index = piece.schema().index_of(name).unwrap();
        columns[index] = key_values(&stream, name, piece.column(index), &|_| true).unwrap();
    }
    let batch = with_columns(piece, columns).unwrap();
    let shape = Shape {
        max_depth: 8,
        whole: BTreeSet::new(),
        key: keyed
            .columns()
            .iter()
            .map(|name| Arc::from(*name))
            .collect(),
    };
    normalize(&batch, &shape, &mut PartSchemas::default()).unwrap()
}

/// What a flush stages, each table's under its path below the stream's table, and the values its
/// policy discarded.
struct Flushed {
    tables: BTreeMap<Vec<Arc<str>>, Staged>,
    discarded: u64,
}

impl Flushed {
    /// The rows staged in the tables below the stream's.
    fn children(&self) -> usize {
        self.tables
            .iter()
            .filter(|(path, _)| !path.is_empty())
            .flat_map(|(_, table)| &table.batches)
            .map(RecordBatch::num_rows)
            .sum()
    }

    /// The rows staged in the stream's table.
    fn roots(&self) -> usize {
        self.tables[&Vec::new()]
            .batches
            .iter()
            .map(RecordBatch::num_rows)
            .sum()
    }
}

/// What a flush of `rows`, cut as `cuts` says, whose key arrives as `keyed` says, stages in
/// round `round`: where `across`, each piece's parts carry which of its roots no later row of
/// the flush shares a key with.
fn staged(rows: &[Row], cuts: &[usize], keyed: Keyed, round: u64, across: bool) -> Flushed {
    let flush = batch_of(rows, keyed);
    let pieces = pieces(rows.len(), cuts);
    let last = across.then(|| last_across(&keys(&flush, &pieces, keyed)).unwrap());
    let mut roots = resolver(capabilities(), plan(), keyed.columns());
    roots.meta = MetaNames::assign(&roots.naming, true, LineageColumns::Root);
    let mut flushed = Flushed {
        tables: BTreeMap::new(),
        discarded: 0,
    };
    for (first, count) in pieces {
        let stamp = Stamp {
            segment: SegmentId(round),
            first_row: u64::try_from(first).unwrap(),
            ..stamp()
        };
        let piece_last = last.as_ref().map(|last| last.slice(first, count));
        let mut parts = split(&flush.slice(first, count), keyed).into_iter();
        let root = Pruned::whole(parts.next().unwrap(), PushKind::Arrow).part;
        let root_plan = planned(&roots, &[], &root);
        let root_key = root_key(root_plan.view());
        let mut lower = |path: Vec<Arc<str>>, plan: &LoweringPlan, mut part: Part| {
            part.lineage.last.clone_from(&piece_last);
            let prepared = plan
                .prepare(part.batch, Some(&part.lineage), &stamp, None)
                .unwrap();
            flushed.discarded += prepared.discarded_values;
            let view = plan.view();
            let table = flushed.tables.entry(path).or_insert_with(|| Staged {
                schema: Arc::clone(&view.schema),
                key: view.table.merge.clone().unwrap(),
                batches: Vec::new(),
            });
            assert_eq!(table.schema, view.schema, "a table keeps its columns");
            table.batches.push(prepared.batch);
        };
        lower(Vec::new(), &root_plan, root);
        for part in parts {
            if part.batch.num_rows() == 0 {
                continue;
            }
            let owner = ColumnPath::from(part.path[0].as_ref());
            let children = roots.child(Some(root_key.clone()), owner);
            let plan = planned(&children, &part.path, &part);
            lower(part.path.clone(), &plan, part);
        }
    }
    flushed
}

/// The key columns of `flush`, whose key arrives as `keyed` says, of each of `pieces`, as the
/// split holds them: each negative zero the zero.
fn keys(flush: &RecordBatch, pieces: &[(usize, usize)], keyed: Keyed) -> Vec<Vec<ArrayRef>> {
    let stream = StreamName::new("s").unwrap();
    pieces
        .iter()
        .map(|(first, count)| {
            keyed
                .columns()
                .iter()
                .map(|name| {
                    let column = flush.column_by_name(name).unwrap().slice(*first, *count);
                    key_values(&stream, name, &column, &|_| true).unwrap()
                })
                .collect()
        })
        .collect()
}

/// How the child tables of the stream whose table `view` is follow its merges.
fn root_key(view: &TableView) -> RootKey {
    RootKey {
        table: Arc::clone(&view.table.name),
        id: Arc::clone(view.meta.id.as_ref().unwrap()),
        seq: Arc::clone(&view.table.merge.as_ref().unwrap().seq),
    }
}

/// The plan lowering `part` into its table, at `path` below the stream's, created by
/// `resolver` from the part's columns, the stream's instants in nanoseconds, whose values its
/// columns cannot hold their policy discards.
fn planned(resolver: &Resolver, path: &[Arc<str>], part: &Part) -> LoweringPlan {
    let incoming = |schema: TableSchema| Incoming {
        schema: Arc::new(schema),
        paths: Arc::clone(&part.columns),
        rounding: BTreeSet::new(),
        creation: Creation::AsModeled,
    };
    let schema = TableSchema::from_arrow(&part.batch.schema()).unwrap();
    let nanos = LogicalType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into()));
    let declared: Vec<Field> = schema
        .fields()
        .iter()
        .zip(part.columns.iter())
        .map(|(field, column)| {
            if *column == ColumnPath::from("at") {
                Field::new(field.name(), nanos.clone(), true)
            } else {
                field.clone()
            }
        })
        .collect();
    let declared = incoming(TableSchema::new(declared).unwrap());
    let model = resolver
        .resolve(&Model::default(), &declared)
        .unwrap()
        .model;
    let incoming = incoming(schema);
    let resolution = resolver.resolve(&model, &incoming).unwrap();
    let segments = ["t"].into_iter().chain(path.iter().map(AsRef::as_ref));
    let table_path = TablePath::new(segments).unwrap();
    let table = TableRef {
        name: table_path.segments().collect::<Vec<_>>().join("__").into(),
        path: table_path,
        version: SchemaVersion(0),
        generation: None,
        merge: None,
    };
    let view = Arc::new(TableView::new(&table, resolution.model, resolver).unwrap());
    let policies = vec![SchemaPolicy::DiscardValue; incoming.paths.len()];
    LoweringPlan::new(resolver.stream.clone(), view, incoming, resolution.routes)
        .with_policies(policies)
}

/// Each table's published rows, with what merges them, by its path below the stream's table.
#[derive(Default)]
struct Published {
    tables: BTreeMap<Vec<Arc<str>>, Staged>,
}

impl Published {
    /// Publishes a commit of `staged`: the stream's table merges by key, and each child table,
    /// staged in it or published before, follows the roots it publishes.
    fn commit(&mut self, mut staged: BTreeMap<Vec<Arc<str>>, Staged>) {
        let root = staged.remove(&Vec::new()).unwrap();
        let published = self.rows(&[], &root);
        let merged = merge_sparse(&root.schema, &published, &[], &root.batches, &root.key);
        let rows = read_back(&root.schema, &merged.unwrap().rows).unwrap();
        let mut paths: BTreeSet<Vec<Arc<str>>> = staged.keys().cloned().collect();
        paths.extend(self.tables.keys().filter(|path| !path.is_empty()).cloned());
        for path in paths {
            let child = match staged.remove(&path) {
                Some(child) => child,
                None => self.tables[&path].emptied(),
            };
            let published = self.rows(&path, &child);
            let parent = child.key.root.clone().unwrap();
            let merged = merge_children_sparse(
                &child.schema,
                &published,
                &child.batches,
                &child.key,
                &parent,
                &root.batches,
            );
            let rows = read_back(&child.schema, &merged.unwrap()).unwrap();
            self.tables.insert(path, child.holding(rows));
        }
        self.tables.insert(Vec::new(), root.holding(rows));
    }

    /// The rows published of the table at `path`, which merges as `table` does.
    fn rows(&self, path: &[Arc<str>], table: &Staged) -> Vec<RecordBatch> {
        self.tables.get(path).map_or_else(Vec::new, |published| {
            assert_eq!(published.schema, table.schema, "a table keeps its columns");
            published.batches.clone()
        })
    }

    /// Each table's rows, rendered and sorted, by path.
    fn shown(&self) -> BTreeMap<Vec<Arc<str>>, Vec<String>> {
        self.tables
            .iter()
            .map(|(path, table)| (path.clone(), shown(&table.batches)))
            .collect()
    }
}

impl Staged {
    /// The same table holding `batches`.
    fn holding(self, batches: Vec<RecordBatch>) -> Self {
        Self { batches, ..self }
    }

    /// The same table holding no rows.
    fn emptied(&self) -> Self {
        Self {
            schema: Arc::clone(&self.schema),
            key: self.key.clone(),
            batches: Vec::new(),
        }
    }
}

/// The rows of `batches`, each rendered as its values' text, sorted.
fn shown(batches: &[RecordBatch]) -> Vec<String> {
    let options = FormatOptions::default();
    let mut rows = Vec::new();
    for batch in batches {
        let formatters: Vec<ArrayFormatter<'_>> = batch
            .columns()
            .iter()
            .map(|column| ArrayFormatter::try_new(column.as_ref(), &options).unwrap())
            .collect();
        for row in 0..batch.num_rows() {
            let values: Vec<String> = formatters
                .iter()
                .map(|formatter| formatter.value(row).to_string())
                .collect();
            rows.push(values.join("|"));
        }
    }
    rows.sort();
    rows
}

/// The distinct keys of a flush of `rows`, however a key arrives: each row's key is one value.
fn distinct(rows: &[Row]) -> usize {
    rows.iter()
        .map(|row| row.key)
        .collect::<BTreeSet<_>>()
        .len()
}

#[test]
fn a_child_table_whose_items_hold_no_value_keeps_the_items_of_the_roots_staged() {
    // Two rows of one key, each holding two items with no value: the first row is not staged.
    let ids: ArrayRef = Arc::new(Int64Array::from(vec![1, 1]));
    let item = Arc::new(ArrowField::new(
        "item",
        DataType::Struct(Fields::empty()),
        true,
    ));
    let items: ArrayRef = Arc::new(ListArray::new(
        item,
        OffsetBuffer::new(vec![0, 2, 4].into()),
        Arc::new(StructArray::new_empty_fields(4, None)),
        None,
    ));
    let batch = RecordBatch::try_from_iter([("id", ids), ("items", items)]).unwrap();
    let mut parts = split(&batch, Keyed::Int).into_iter();
    let root = parts.next().unwrap();
    let mut child = parts.next().unwrap();
    assert_eq!((child.batch.num_columns(), child.batch.num_rows()), (0, 4));
    let mut roots = resolver(capabilities(), plan(), Keyed::Int.columns());
    roots.meta = MetaNames::assign(&roots.naming, true, LineageColumns::Root);
    let root_key = root_key(planned(&roots, &[], &root).view());
    let children = roots.child(Some(root_key), ColumnPath::from("items"));
    let plan = planned(&children, &child.path, &child);
    child.lineage.last = Some(BooleanBuffer::from(vec![false, true]));
    let prepared = plan
        .prepare(child.batch, Some(&child.lineage), &stamp(), None)
        .unwrap();
    assert_eq!(prepared.batch.num_rows(), 2);
}

fn keyed() -> impl Strategy<Value = Keyed> {
    prop_oneof![
        Just(Keyed::Int),
        Just(Keyed::Float),
        Just(Keyed::Text),
        Just(Keyed::Composite),
    ]
}

proptest! {
    #[test]
    fn staging_a_flush_s_last_row_of_each_key_publishes_what_staging_each_piece_s_does(
        keyed in keyed(),
        commits in proptest::collection::vec(flush(), 1..4),
    ) {
        let (mut across, mut pieces) = (Published::default(), Published::default());
        for (round, (rows, cuts)) in (1..).zip(&commits) {
            let fewer = staged(rows, cuts, keyed, round, true);
            let every = staged(rows, cuts, keyed, round, false);
            prop_assert_eq!(fewer.roots(), distinct(rows), "commit {}", round);
            prop_assert!(fewer.children() <= every.children());
            prop_assert_eq!(fewer.discarded, every.discarded, "commit {}", round);
            across.commit(fewer.tables);
            pieces.commit(every.tables);
            prop_assert_eq!(across.shown(), pieces.shown(), "commit {}", round);
        }
    }
}
