//! A normalized merge's staging, kept to the rows of each key's last root row or holding every
//! row, publishes the same tables through the SDK's merge: what the engine leaves out is what
//! the destination would drop.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use arrow_array::builder::{ListBuilder, StringBuilder};
use arrow_array::{
    Array, ArrayRef, Float64Array, Int64Array, ListArray, RecordBatch, StringArray, StructArray,
    TimestampSecondArray,
};
use arrow_buffer::{NullBuffer, OffsetBuffer};
use arrow_cast::display::{ArrayFormatter, FormatOptions};
use arrow_schema::{DataType, Field as ArrowField, Fields, SchemaRef};
use proptest::prelude::*;
use rdlt_connector::merge::{merge_children_sparse, merge_sparse, read_back};
use rdlt_connector::{
    ColumnPath, Field, LogicalType, MergeKey, RootKey, SchemaVersion, SegmentId, StreamName,
    TablePath, TableRef, TableSchema, TimeUnit,
};

use super::{capabilities, plan, resolver, stamp};
use crate::normalize::{Dropped, Part, PartSchemas, Pruned, PushKind, Shape, normalize};
use crate::policy::SchemaPolicy;
use crate::table::lowering::LoweringPlan;
use crate::table::resolve::{Incoming, Resolver};
use crate::table::{
    Creation, LineageColumns, MetaNames, Model, Stamp, TableView, key_values, with_columns,
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

/// A row: its key, whether it holds an instant its column cannot hold, which its policy drops it
/// for, and where it holds an array, its items, each a sku and its tags.
#[derive(Clone, Debug)]
struct Row {
    key: u8,
    dropped: bool,
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
        row.prop_map(|(key, dropped, items)| Row {
            key,
            dropped,
            items,
        }),
        1..16,
    )
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
        if row.dropped {
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

/// One table's staged rows, or once published its rows, with what merges them.
struct Staged {
    schema: SchemaRef,
    key: MergeKey,
    rows: RecordBatch,
    batches: Vec<RecordBatch>,
}

/// The parts of a batch of `rows` whose key arrives as `keyed` says, split as the engine splits
/// them: the key columns normalized, each negative zero the zero, then normalized.
fn split(rows: &[Row], keyed: Keyed) -> Vec<Part> {
    let stream = StreamName::new("s").unwrap();
    let batch = batch_of(rows, keyed);
    let mut columns = batch.columns().to_vec();
    for name in keyed.columns() {
        let index = batch.schema().index_of(name).unwrap();
        columns[index] = key_values(&stream, name, batch.column(index), &|_| true).unwrap();
    }
    let batch = with_columns(&batch, columns).unwrap();
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

/// What a batch of `rows` stages for each table in round `round`, the stream's table's under
/// the empty path, each child table's under its own: every row, or only those of the root rows
/// that are the last of their key where `pruned`.
fn staged(rows: &[Row], keyed: Keyed, round: u64, pruned: bool) -> BTreeMap<Vec<Arc<str>>, Staged> {
    let mut parts = split(rows, keyed).into_iter();
    let stamp = Stamp {
        segment: SegmentId(round),
        ..stamp()
    };
    let mut roots = resolver(capabilities(), plan(), keyed.columns());
    roots.meta = MetaNames::assign(&roots.naming, true, LineageColumns::Root);
    let root = Pruned::whole(parts.next().unwrap(), PushKind::Arrow);
    let root_plan = planned(&roots, &[], &root.part, true);
    let kept = root_plan.kept(&root.part.batch).unwrap();
    let last = root_plan
        .last_roots(&root.part.batch, &root.part.lineage, kept.as_ref())
        .unwrap();
    let mut dropped = Dropped::new(PushKind::Arrow);
    if let Some(kept) = &kept {
        dropped.unkept(&root, kept);
    }
    let view = Arc::clone(root_plan.view());
    let root_key = RootKey {
        table: Arc::clone(&view.table.name),
        id: Arc::clone(view.meta.id.as_ref().unwrap()),
        seq: Arc::clone(&view.table.merge.as_ref().unwrap().seq),
    };
    let mut staged = BTreeMap::new();
    let last = last.filter(|_| pruned);
    let mut lower = |path: Vec<Arc<str>>, plan: &LoweringPlan, mut part: Part| {
        part.lineage.last.clone_from(&last);
        let prepared = plan.prepare(part.batch, Some(&part.lineage), &stamp, None);
        let view = plan.view();
        let table = Staged {
            schema: Arc::clone(&view.schema),
            key: view.table.merge.clone().unwrap(),
            rows: prepared.unwrap().batch,
            batches: Vec::new(),
        };
        staged.insert(path, table);
    };
    lower(Vec::new(), &root_plan, root.part);
    for part in parts {
        let part = dropped.prune(part).unwrap().part;
        if part.batch.num_rows() == 0 {
            continue;
        }
        let owner = ColumnPath::from(part.path[0].as_ref());
        let children = roots.child(Some(root_key.clone()), owner);
        let plan = planned(&children, &part.path, &part, false);
        lower(part.path.clone(), &plan, part);
    }
    staged
}

/// The plan lowering `part` into its table, at `path` below the stream's, created by
/// `resolver` from the part's columns, the stream's instants in nanoseconds; its columns drop
/// the rows they cannot hold where `dropping`.
fn planned(resolver: &Resolver, path: &[Arc<str>], part: &Part, dropping: bool) -> LoweringPlan {
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
    let policies = if dropping {
        vec![SchemaPolicy::DiscardRow; incoming.paths.len()]
    } else {
        Vec::new()
    };
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
        let incoming = std::slice::from_ref(&root.rows);
        let merged = merge_sparse(&root.schema, &published, &[], incoming, &root.key).unwrap();
        let rows = read_back(&root.schema, &merged.rows).unwrap();
        let mut paths: BTreeSet<Vec<Arc<str>>> = staged.keys().cloned().collect();
        paths.extend(self.tables.keys().filter(|path| !path.is_empty()).cloned());
        for path in paths {
            let (child, incoming) = match staged.remove(&path) {
                Some(child) => {
                    let rows = vec![child.rows.clone()];
                    (child, rows)
                }
                None => (self.tables[&path].emptied(), Vec::new()),
            };
            let published = self.rows(&path, &child);
            let parent = child.key.root.clone().unwrap();
            let merged = merge_children_sparse(
                &child.schema,
                &published,
                &incoming,
                &child.key,
                &parent,
                std::slice::from_ref(&root.rows),
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
        let options = FormatOptions::default();
        let mut shown = BTreeMap::new();
        for (path, table) in &self.tables {
            let mut rows = Vec::new();
            for batch in &table.batches {
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
            shown.insert(path.clone(), rows);
        }
        shown
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
            rows: RecordBatch::new_empty(Arc::clone(&self.schema)),
            batches: Vec::new(),
        }
    }
}

/// Child rows staged across every table but the stream's own.
fn children(staged: &BTreeMap<Vec<Arc<str>>, Staged>) -> usize {
    staged
        .iter()
        .filter(|(path, _)| !path.is_empty())
        .map(|(_, table)| table.rows.num_rows())
        .sum()
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
    fn staging_only_each_keys_last_roots_children_publishes_what_staging_every_row_does(
        keyed in keyed(),
        commits in proptest::collection::vec(rows(), 1..4),
    ) {
        let (mut pruned, mut whole) = (Published::default(), Published::default());
        for (round, rows) in (1..).zip(&commits) {
            let (fewer, every) = (staged(rows, keyed, round, true), staged(rows, keyed, round, false));
            prop_assert!(children(&fewer) <= children(&every));
            pruned.commit(fewer);
            whole.commit(every);
            prop_assert_eq!(pruned.shown(), whole.shown(), "commit {}", round);
        }
    }
}
