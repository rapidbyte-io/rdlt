//! A normalized merge stream's tables keep only the rows of the root rows that are the last of
//! their key: its own table finds those once its policy has dropped rows, and every table of the
//! stream keeps the rows descending from them, without sorting by key again.

use std::sync::Arc;

use arrow_array::cast::AsArray;
use arrow_array::types::Int64Type;
use arrow_array::{
    ArrayRef, BinaryArray, Int32Array, Int64Array, RecordBatch, RecordBatchOptions, StringArray,
    TimestampSecondArray, UInt32Array,
};
use arrow_buffer::BooleanBuffer;
use arrow_schema::Schema;
use rdlt_connector::{ColumnPath, LogicalType, RootKey, TableSchema, TimeUnit};

use super::{batch, capabilities, created, plan, resolver, seq_offsets, stamp, table};
use crate::normalize::{Lineage, Parent};
use crate::policy::SchemaPolicy;
use crate::table::lowering::LoweringPlan;
use crate::table::resolve::{Incoming, Resolver};
use crate::table::{LineageColumns, MetaNames, TableView};

/// The resolver of a normalized merge stream's own table, keyed by `id`.
fn roots() -> Resolver {
    let mut resolver = resolver(capabilities(), plan(), &["id"]);
    resolver.meta = MetaNames::assign(&resolver.naming, true, LineageColumns::Root);
    resolver
}

/// The plan lowering `rows` into the table `resolver` creates from `fields`, each incoming
/// column dropping a row holding a value its column cannot hold.
fn planned(
    resolver: &Resolver,
    fields: &[(&str, LogicalType)],
    rows: &RecordBatch,
) -> LoweringPlan {
    let model = created(resolver, fields);
    let incoming = Incoming::declared(TableSchema::from_arrow(&rows.schema()).unwrap());
    let resolution = resolver.resolve(&model, &incoming).unwrap();
    let view = Arc::new(TableView::new(&table("t"), resolution.model, resolver).unwrap());
    let policies = vec![SchemaPolicy::DiscardRow; rows.num_columns()];
    LoweringPlan::new(resolver.stream.clone(), view, incoming, resolution.routes)
        .with_policies(policies)
}

fn positions(rows: &[u32]) -> ArrayRef {
    Arc::new(UInt32Array::from(rows.to_vec()))
}

/// The lineage of a merge stream's root rows at `rows` of a batch of `roots` root rows.
fn root_lineage(rows: &[u32], roots: usize) -> Lineage {
    Lineage {
        part: 0,
        row: positions(rows),
        keys: Some(Arc::new(BinaryArray::from_iter_values(
            (0..roots).map(usize::to_be_bytes),
        ))),
        last: None,
        parent: None,
    }
}

/// Key 1's rows at 0, 2 and 3, key 2's at 1 and 4, of which row 3, key 1's newest, holds an
/// instant its column cannot hold.
fn versions() -> RecordBatch {
    let at = TimestampSecondArray::from(vec![10, 20, 30, i64::MAX, 50]).with_timezone("UTC");
    batch(vec![
        ("id", Arc::new(Int64Array::from(vec![1, 2, 1, 1, 2])) as _),
        ("at", Arc::new(at) as _),
    ])
}

fn nanos() -> LogicalType {
    LogicalType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into()))
}

fn set(last: &BooleanBuffer) -> Vec<usize> {
    last.set_indices().collect()
}

#[test]
fn a_keys_last_root_is_found_among_the_rows_its_policy_keeps() {
    let resolver = roots();
    let rows = versions();
    let plan = planned(
        &resolver,
        &[("id", LogicalType::Int64), ("at", nanos())],
        &rows,
    );
    let kept = plan.kept(&rows).unwrap();
    assert!(
        kept.is_some(),
        "row 3 holds an instant nanoseconds cannot hold"
    );
    let lineage = root_lineage(&[0, 1, 2, 3, 4], 5);
    let last = plan.last_roots(&rows, &lineage, kept.as_ref()).unwrap();
    assert_eq!(
        set(&last.unwrap()),
        [2, 4],
        "key 1's newest row is dropped, so the row before it is its last"
    );
}

#[test]
fn a_root_rows_place_is_its_position_among_the_batchs_roots() {
    let resolver = roots();
    let rows = batch(vec![("id", Arc::new(Int64Array::from(vec![1, 2, 1])) as _)]);
    let plan = planned(&resolver, &[("id", LogicalType::Int64)], &rows);
    // The part lost the batch's roots at 0, 2 and 4 before it was planned.
    let lineage = root_lineage(&[1, 3, 5], 7);
    let last = plan.last_roots(&rows, &lineage, None).unwrap().unwrap();
    assert_eq!(last.len(), 7);
    assert_eq!(set(&last), [3, 5]);
}

#[test]
fn every_kept_root_is_last_where_every_key_is_unique() {
    let resolver = roots();
    let rows = batch(vec![("id", Arc::new(Int64Array::from(vec![3, 1, 2])) as _)]);
    let plan = planned(&resolver, &[("id", LogicalType::Int64)], &rows);
    let last = plan.last_roots(&rows, &root_lineage(&[0, 1, 2], 3), None);
    assert_eq!(set(&last.unwrap().unwrap()), [0, 1, 2]);
}

#[test]
fn only_a_table_merging_by_its_own_key_finds_last_roots() {
    let rows = batch(vec![("id", Arc::new(Int64Array::from(vec![1, 1])) as _)]);
    let appended = resolver(capabilities(), plan(), &[]);
    let plan = planned(&appended, &[("id", LogicalType::Int64)], &rows);
    let lineage = root_lineage(&[0, 1], 2);
    assert!(plan.last_roots(&rows, &lineage, None).unwrap().is_none());
    // A table without its key's column refuses the batch as it lowers it.
    let keyless = roots();
    let plan = planned(&keyless, &[("v", LogicalType::Utf8)], &rows_without_key());
    assert!(
        plan.last_roots(&rows_without_key(), &lineage, None)
            .unwrap()
            .is_none()
    );
}

fn rows_without_key() -> RecordBatch {
    batch(vec![(
        "v",
        Arc::new(StringArray::from(vec!["a", "b"])) as _,
    )])
}

#[test]
fn a_root_part_keeps_the_roots_found_last_without_sorting_by_key_again() {
    let resolver = roots();
    let rows = versions();
    let plan = planned(
        &resolver,
        &[("id", LogicalType::Int64), ("at", nanos())],
        &rows,
    );
    // Not the last of each key: a mask that differs from a sort shows the lowering takes it.
    let mut lineage = root_lineage(&[0, 1, 2, 3, 4], 5);
    lineage.last = Some(BooleanBuffer::from(vec![true, true, false, false, false]));
    let prepared = plan.prepare(rows, Some(&lineage), &stamp(), None).unwrap();
    assert_eq!(prepared.discarded_rows, 1, "the policy's drop counts");
    let out = prepared.batch;
    assert_eq!(out.column(0).as_primitive::<Int64Type>().values(), &[1, 2]);
    // The stamp's first row is 10: the batch's rows 0 and 1.
    assert_eq!(seq_offsets(&out), [10, 11]);
}

/// The resolver of a child table of [`roots`]'s stream, below its column `items`.
fn items() -> Resolver {
    let parent = roots();
    let root = RootKey {
        table: "t".into(),
        id: Arc::clone(parent.meta.id.as_ref().unwrap()),
        seq: "_rdlt_seq".into(),
    };
    parent.child(Some(root), ColumnPath::from("items"))
}

/// The lineage of a child table's rows, the first items of roots at `roots` of a batch whose
/// root rows `last` says are the last of their key.
fn child_lineage(roots: &[u32], last: Vec<bool>) -> Lineage {
    let rows = u32::try_from(roots.len()).unwrap();
    let at = positions(roots);
    Lineage {
        part: 1,
        row: Arc::new(UInt32Array::from_iter_values(0..rows)),
        keys: Some(Arc::new(BinaryArray::from_iter_values(
            (0..last.len()).map(|root| format!("r{root}")),
        ))),
        last: Some(BooleanBuffer::from(last)),
        parent: Some(Parent {
            path: Vec::new(),
            part: 0,
            row: Arc::clone(&at),
            root_row: at,
            idx: Arc::new(Int32Array::from_iter_values((0..rows).map(|_| 0))),
        }),
    }
}

#[test]
fn a_child_tables_rows_follow_the_roots_they_descend_from() {
    let child = items();
    let at = TimestampSecondArray::from(vec![10, i64::MAX, 30, 40, 50]).with_timezone("UTC");
    let rows = batch(vec![
        (
            "sku",
            Arc::new(StringArray::from(vec!["a", "b", "c", "d", "e"])) as _,
        ),
        ("at", Arc::new(at) as _),
    ]);
    let plan = planned(
        &child,
        &[("sku", LogicalType::Utf8), ("at", nanos())],
        &rows,
    );
    let lineage = child_lineage(&[0, 0, 1, 1, 3], vec![true, false, true, true]);
    let prepared = plan.prepare(rows, Some(&lineage), &stamp(), None).unwrap();
    assert_eq!(
        prepared.discarded_rows, 1,
        "row 1 holds an instant its column cannot hold"
    );
    let skus: Vec<&str> = prepared
        .batch
        .column(0)
        .as_string::<i32>()
        .iter()
        .flatten()
        .collect();
    assert_eq!(
        skus,
        ["a", "e"],
        "root 1's children go with it; root 0's dropped row is gone"
    );
    let root_ids = prepared.batch.column_by_name("_rdlt_root_id").unwrap();
    let root_ids: Vec<&[u8]> = root_ids.as_binary::<i32>().iter().flatten().collect();
    assert_eq!(root_ids, [b"r0".as_slice(), b"r3".as_slice()]);
    assert_eq!(seq_offsets(&prepared.batch), [10, 13], "each its root's");
}

#[test]
fn a_child_table_of_no_columns_keeps_the_rows_of_the_roots_found_last() {
    // Items holding no value are rows of no column: only their lineage says which to keep.
    let options = RecordBatchOptions::new().with_row_count(Some(3));
    let rows =
        RecordBatch::try_new_with_options(Arc::new(Schema::empty()), Vec::new(), &options).unwrap();
    let plan = planned(&items(), &[], &rows);
    let lineage = child_lineage(&[0, 1, 1], vec![false, true]);
    let prepared = plan.prepare(rows, Some(&lineage), &stamp(), None).unwrap();
    assert_eq!(prepared.batch.num_rows(), 2);
    assert_eq!(seq_offsets(&prepared.batch), [11, 11]);
}
