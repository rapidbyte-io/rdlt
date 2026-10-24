use std::collections::BTreeSet;
use std::sync::Arc;

use arrow_array::builder::{Int64Builder, MapBuilder, StringBuilder};
use arrow_array::cast::AsArray;
use arrow_array::types::{Int32Type, Int64Type, UInt32Type};
use arrow_array::{
    Array, ArrayRef, DictionaryArray, Float64Array, Int32Array, Int64Array, LargeListArray,
    ListArray, NullArray, RecordBatch, RunArray, StringArray, StringViewArray, StructArray,
    UInt8Array, UnionArray,
};
use arrow_buffer::{NullBuffer, OffsetBuffer, ScalarBuffer};
use arrow_schema::{ArrowError, DataType, Field as ArrowField, Fields, UnionFields};
use bytes::Bytes;
use proptest::prelude::*;
use rdlt_connector::cost::Allocations;
use rdlt_connector::{Epoch, SegmentId};
use serde_json::{Value as Json, json};

use super::identity::version_hashes;
use super::reference::compared::{self, Cut};
use super::reference::{self, Record};
use super::{Part, Received, Shape, normalize};
use crate::compute::{Pool, ready};
use crate::shred::shred;
use crate::table::convert::decoded;

pub(super) fn shape(max_depth: u8) -> Shape {
    Shape {
        max_depth,
        whole: BTreeSet::new(),
        key: Vec::new(),
    }
}

/// `records` shredded as one push into batches of about `chunk_bytes` each.
pub(super) fn shredded(records: &[Json], chunk_bytes: usize) -> Vec<RecordBatch> {
    let lines: Vec<String> = records.iter().map(Json::to_string).collect();
    let push = Bytes::from(lines.join("\n"));
    ready(shred(
        &Pool::inline(),
        &[push],
        chunk_bytes,
        crate::shred::ShredLimits::new(rdlt_wire::limits::SCHEMA_COLUMNS),
    ))
    .expect("the records shred")
}

/// The parts of `records`, shredded as one batch and normalized as `shape`.
pub(super) fn parts(records: &[Json], shape: &Shape) -> Vec<Part> {
    shredded(records, 1 << 20)
        .iter()
        .flat_map(|batch| normalize(batch, shape).expect("the batch normalizes"))
        .collect()
}

/// A piece received first in segment 1 of epoch 1.
fn first() -> Received {
    Received {
        epoch: Epoch(1),
        segment: SegmentId(1),
        first_row: 0,
    }
}

/// `batches`, one stream's rows in order, each a piece of segment 1, as the reference writes
/// the tables they normalize into as `shape`.
fn tables_of(batches: &[RecordBatch], shape: &Shape) -> reference::Tables {
    let mut received = first();
    let mut pieces = Vec::new();
    for batch in batches {
        pieces.push((batch.clone(), received));
        received.first_row += batch.num_rows() as u64;
    }
    compared::tables_of(&pieces, shape).expect("the lineage names its rows")
}

/// Ids, each its bytes.
type Ids = Vec<Vec<u8>>;

/// The ids of the rows of `parts`' part `part`, received first in segment 1, and of their
/// parents and roots.
fn ids_of(parts: &[Part], part: usize) -> (Ids, Ids, Ids) {
    let values = |array: &ArrayRef| -> Vec<Vec<u8>> {
        let ids = array.as_binary::<i32>();
        (0..ids.len()).map(|row| ids.value(row).to_vec()).collect()
    };
    let (own, parent) = parts[part].lineage.columns(&first()).unwrap();
    let [parents, roots] = match &parent {
        Some([parents, roots, _]) => [values(parents), values(roots)],
        None => [Vec::new(), Vec::new()],
    };
    (values(&own), parents, roots)
}

fn path(segments: &[&str]) -> Vec<String> {
    segments
        .iter()
        .map(|segment| (*segment).to_owned())
        .collect()
}

#[test]
fn objects_flatten_into_a_column_per_field() {
    let parts = parts(
        &[json!({"id": 1, "meta": {"a": 2, "b": {"c": "x"}}})],
        &shape(8),
    );
    assert_eq!(parts.len(), 1);
    let columns: Vec<String> = parts[0].columns.iter().map(ToString::to_string).collect();
    assert_eq!(columns, ["id", "meta.a", "meta.b.c"]);
    assert!(parts[0].lineage.parent.is_none());
}

#[test]
fn arrays_of_objects_become_child_tables_that_name_their_parents() {
    let parts = parts(
        &[json!({"id": 1, "items": [{"sku": "a"}, {"sku": "b", "tags": ["x", "y"]}]})],
        &shape(8),
    );
    let paths: Vec<Vec<Arc<str>>> = parts.iter().map(|part| part.path.clone()).collect();
    assert_eq!(
        paths,
        [
            Vec::new(),
            vec![Arc::from("items")],
            vec![Arc::from("items"), Arc::from("tags")],
        ]
    );
    let ordinals: Vec<u32> = parts.iter().map(|part| part.lineage.part).collect();
    assert_eq!(ordinals, [0, 1, 2]);
    let items = parts[1]
        .lineage
        .parent
        .as_ref()
        .expect("a child has parents");
    assert_eq!((items.part, positions(&items.row)), (0, vec![0, 0]));
    assert_eq!(items.idx.as_primitive::<Int32Type>().values(), &[0, 1]);
    let tags = parts[2]
        .lineage
        .parent
        .as_ref()
        .expect("a grandchild has parents");
    assert_eq!((tags.part, positions(&tags.row)), (1, vec![1, 1]), "item b");
    assert_eq!(positions(&tags.root_row), [0, 0]);
    let (roots, ..) = ids_of(&parts, 0);
    let (items, item_parents, item_roots) = ids_of(&parts, 1);
    let (_, tag_parents, tag_roots) = ids_of(&parts, 2);
    assert_eq!(item_parents, [roots[0].clone(), roots[0].clone()]);
    assert_eq!(item_roots, item_parents);
    assert_eq!(tag_parents, [items[1].clone(), items[1].clone()]);
    assert_eq!(tag_roots, [roots[0].clone(), roots[0].clone()]);
    let values: Vec<String> = parts[2].columns.iter().map(ToString::to_string).collect();
    assert_eq!(
        values,
        ["value"],
        "arrays of values hold them in a value column"
    );
}

/// The positions `array`, a `UInt32` array, holds.
fn positions(array: &ArrayRef) -> Vec<u32> {
    array.as_primitive::<UInt32Type>().values().to_vec()
}

#[test]
fn null_and_empty_arrays_hold_no_rows_and_null_items_are_rows() {
    let parts = parts(
        &[
            json!({"id": 1, "scores": null}),
            json!({"id": 2, "scores": []}),
            json!({"id": 3, "scores": [null, 4]}),
        ],
        &shape(8),
    );
    let scores = &parts[1];
    assert_eq!(scores.batch.num_rows(), 2);
    let idx = scores.lineage.parent.as_ref().expect("child").idx.clone();
    assert_eq!(idx.as_primitive::<Int32Type>().values(), &[0, 1]);
    assert!(scores.batch.column(0).is_null(0));
}

/// The child table of a batch whose only column, `l`, is `list`: its items, and the bytes it
/// keeps alive beyond the batch's.
fn child_of(list: ListArray) -> (Vec<i64>, u64) {
    let batch = RecordBatch::try_from_iter([("l", Arc::new(list) as ArrayRef)]).unwrap();
    let parts = normalize(&batch, &shape(8)).unwrap();
    let column = parts[1].batch.column(0);
    let items = column
        .as_primitive::<Int64Type>()
        .values()
        .iter()
        .copied()
        .take(16)
        .collect();
    let mut held = Allocations::of(&batch);
    (items, held.add_array(column.as_ref()))
}

#[test]
fn items_that_lie_together_are_their_arrays_values_and_others_are_copied() {
    let item = Arc::new(ArrowField::new("item", DataType::Int64, true));
    let values: ArrayRef = Arc::new(Int64Array::from(vec![1, 2, 3, 4, 5, 6]));
    // Rows [1, 2], [3] and [4, 5, 6].
    let offsets = OffsetBuffer::new(ScalarBuffer::from(vec![0, 2, 3, 6]));
    // The last two rows: their offsets start at 2.
    let sliced = ListArray::new(
        Arc::clone(&item),
        offsets.clone(),
        Arc::clone(&values),
        None,
    );
    assert_eq!(child_of(sliced.slice(1, 2)), (vec![3, 4, 5, 6], 0));
    // A null row whose span holds an item: no row holds 3, so the items are copied.
    let nulls = NullBuffer::from(vec![true, false, true]);
    let (items, grown) = child_of(ListArray::new(item, offsets, values, Some(nulls)));
    assert_eq!(items, [1, 2, 4, 5, 6]);
    assert!(grown > 0, "items apart are copied out of their array");
    // A null row whose span is empty holds no item, so the items still lie together.
    let offsets = OffsetBuffer::new(ScalarBuffer::from(vec![0, 2, 2, 5]));
    let values: ArrayRef = Arc::new(Int64Array::from(vec![1, 2, 3, 4, 5]));
    let nulls = NullBuffer::from(vec![true, false, true]);
    let item = Arc::new(ArrowField::new("item", DataType::Int64, true));
    let (items, grown) = child_of(ListArray::new(item, offsets, values, Some(nulls)));
    assert_eq!(items, [1, 2, 3, 4, 5]);
    assert_eq!(grown, 0);
}

#[test]
fn a_sliced_batch_normalizes_as_the_rows_it_keeps() {
    let records = [
        json!({"id": 1, "items": [{"sku": "a", "tags": ["x"]}]}),
        json!({"id": 2, "items": [{"sku": "b", "tags": ["y", "z"]}, {"sku": "c", "tags": []}]}),
        json!({"id": 3, "items": [{"sku": "d", "tags": ["w"]}]}),
    ];
    let batch = shredded(&records, 1 << 20).remove(0);
    let sliced = batch.slice(1, 2);
    assert_eq!(
        tables_of(std::slice::from_ref(&sliced), &shape(8)),
        tables_of(&shredded(&records[1..], 1 << 20), &shape(8))
    );
    let sliced = normalize(&sliced, &shape(8)).unwrap();
    // Each table's values are the batch's own, at every depth.
    let mut held = Allocations::of(&batch);
    for part in &sliced[1..] {
        assert_eq!(
            held.add(&part.batch),
            rdlt_connector::cost::schema_bytes(&part.batch.schema())
        );
    }
}

#[test]
fn a_roots_items_name_their_root_by_their_parents_row() {
    let parts = parts(
        &[
            json!({"id": 1, "items": [{"sku": "a"}, {"sku": "b"}]}),
            json!({"id": 2, "items": [{"sku": "c"}]}),
        ],
        &shape(8),
    );
    let parent = parts[1]
        .lineage
        .parent
        .as_ref()
        .expect("a child has parents");
    let mut held = Allocations::default();
    held.add_array(parent.row.as_ref());
    assert_eq!(
        held.add_array(parent.root_row.as_ref()),
        0,
        "a root's row is its items' root row"
    );
}

#[test]
fn an_arrays_items_are_listed_in_buffers_of_exactly_their_count() {
    let rows = (0..100_i64).map(|row| Some(vec![Some(row); usize::try_from(row % 4).unwrap()]));
    let list = ListArray::from_iter_primitive::<Int64Type, _, _>(rows);
    let items = super::Items::of(&list).expect("the list holds items");
    // 25 rows each of 0, 1, 2 and 3 items.
    assert_eq!(items.parents.values().inner().capacity(), 150 * 4);
    assert_eq!(items.idx.values().inner().capacity(), 150 * 4);
}

#[test]
fn containers_deeper_than_max_depth_stay_whole() {
    let record = json!({"meta": {"b": {"c": 1}}, "items": [{"sku": "a"}]});
    let flat = parts(std::slice::from_ref(&record), &shape(1));
    let columns: Vec<String> = flat[0].columns.iter().map(ToString::to_string).collect();
    assert_eq!(
        columns,
        ["meta.b"],
        "the object inside meta is past depth 1"
    );
    assert_eq!(flat.len(), 2);
    let item_columns: Vec<String> = flat[1].columns.iter().map(ToString::to_string).collect();
    assert_eq!(item_columns, ["value"], "items are objects past depth 1");
    let none = parts(&[record], &shape(0));
    assert_eq!(none.len(), 1);
    let columns: Vec<String> = none[0].columns.iter().map(ToString::to_string).collect();
    assert_eq!(columns, ["items", "meta"]);
}

#[test]
fn whole_columns_stay_whole() {
    let shape = Shape {
        whole: BTreeSet::from([Arc::from("meta")]),
        ..shape(8)
    };
    let parts = parts(&[json!({"meta": {"a": [1]}, "b": 2})], &shape);
    assert_eq!(parts.len(), 1);
    let columns: Vec<String> = parts[0].columns.iter().map(ToString::to_string).collect();
    assert_eq!(columns, ["b", "meta"]);
}

#[test]
fn a_versions_hash_does_not_depend_on_types_field_order_or_missing_fields() {
    let hashes = |records: &[Json]| -> Vec<Vec<u8>> {
        let hashes = version_hashes(&shredded(records, 1 << 20)[0]).unwrap();
        hashes.iter().map(|hash| hash.unwrap().to_vec()).collect()
    };
    let as_integer = hashes(&[json!({"a": 1, "b": null})]);
    let as_float = hashes(&[json!({"b": null, "a": 1.0}), json!({"a": 2.5})]);
    assert_eq!(as_integer[0], as_float[0]);
    let missing = hashes(&[json!({"a": 1}), json!({"a": 3, "b": "x"})]);
    assert_eq!(as_integer[0], missing[0]);
    assert_ne!(as_float[0], as_float[1]);
}

/// `shape(8)`, merged by `column`.
fn keyed(column: &str) -> Shape {
    Shape {
        key: vec![Arc::from(column)],
        ..shape(8)
    }
}

#[test]
fn a_merge_streams_roots_share_their_keys_id_and_other_streams_roots_have_none() {
    let records = [
        json!({"id": 7, "v": 1, "items": [1]}),
        json!({"id": 7, "v": 2, "items": [2]}),
    ];
    let parts = parts(&records, &keyed("id"));
    let (ids, ..) = ids_of(&parts, 0);
    assert_eq!(ids[0], ids[1], "one key, one id");
    assert_eq!(ids[0].len(), 32, "the 32 bytes of its key's hash");
    let (_, parents, roots) = ids_of(&parts, 1);
    assert_eq!((&parents, &roots), (&ids, &ids));
    let positional = normalize(&shredded(&records, 1 << 20)[0], &shape(8)).unwrap();
    assert!(positional.iter().all(|part| part.lineage.keys.is_none()));
    let (ids, ..) = ids_of(&positional, 0);
    assert_ne!(ids[0], ids[1], "rows apart are identified apart");
}

#[test]
fn maps_and_large_lists_normalize_like_lists() {
    let mut map = MapBuilder::new(None, StringBuilder::new(), Int64Builder::new());
    map.keys().append_value("k");
    map.values().append_value(1);
    map.append(true).unwrap();
    let large = LargeListArray::from_iter_primitive::<Int64Type, _, _>([Some(vec![Some(5)])]);
    let batch = RecordBatch::try_from_iter([
        ("m", Arc::new(map.finish()) as ArrayRef),
        ("l", Arc::new(large) as ArrayRef),
    ])
    .unwrap();
    let parts = normalize(&batch, &shape(8)).unwrap();
    let paths: Vec<String> = parts.iter().map(|part| part.path.join(".")).collect();
    assert_eq!(paths, ["", "m", "l"]);
    let map_columns: Vec<String> = parts[1].columns.iter().map(ToString::to_string).collect();
    assert_eq!(map_columns, ["keys", "values"]);
    assert_eq!(
        parts[2]
            .batch
            .column(0)
            .as_primitive::<Int64Type>()
            .values(),
        &[5]
    );
}

#[test]
fn numbers_hash_alike_whatever_their_type_and_apart_from_text() {
    let batch = |values: ArrayRef| RecordBatch::try_from_iter([("v", values)]).unwrap();
    let ids = |batch: RecordBatch| ids_of(&normalize(&batch, &keyed("v")).unwrap(), 0).0;
    let integer = ids(batch(Arc::new(Int64Array::from(vec![3]))));
    let float = ids(batch(Arc::new(Float64Array::from(vec![3.0]))));
    let text = ids(batch(Arc::new(StringArray::from(vec!["3"]))));
    let byte = ids(batch(Arc::new(UInt8Array::from(vec![3]))));
    assert_eq!(integer, float);
    assert_eq!(integer, byte);
    assert_ne!(integer, text);
}

fn number() -> impl Strategy<Value = Json> {
    prop_oneof![
        (0_i64..5).prop_map(Json::from),
        (0_i32..20).prop_map(|quarters| Json::from(f64::from(quarters) / 4.0)),
    ]
}

fn text() -> impl Strategy<Value = Json> {
    "[a-c]{0,3}".prop_map(Json::from)
}

fn maybe<S: Strategy<Value = Json>>(strategy: S) -> impl Strategy<Value = Option<Json>> {
    prop::option::weighted(0.7, strategy)
}

fn object(fields: Vec<(&'static str, Option<Json>)>) -> Json {
    Json::Object(
        fields
            .into_iter()
            .filter_map(|(name, value)| Some((name.to_owned(), value?)))
            .collect(),
    )
}

/// An array of up to two values of `item`, some of them null.
fn array<S: Strategy<Value = Json>>(item: S, null_share: f64) -> impl Strategy<Value = Json> {
    prop::collection::vec(
        prop::option::weighted((1.0 - null_share).min(0.99), item),
        0..3,
    )
    .prop_map(|items| {
        Json::Array(
            items
                .into_iter()
                .map(|item| item.unwrap_or(Json::Null))
                .collect(),
        )
    })
}

/// An item of an object, with two sibling arrays.
fn item() -> impl Strategy<Value = Json> {
    (
        text(),
        maybe((0_i64..5).prop_map(Json::from)),
        maybe(array(text(), 0.2)),
        maybe(array((0_i64..3).prop_map(Json::from), 0.0)),
    )
        .prop_map(|(sku, qty, tags, codes)| {
            object(vec![
                ("sku", Some(sku)),
                ("qty", qty),
                ("tags", tags),
                ("codes", codes),
            ])
        })
}

/// A value that is an array, an object holding one, or a scalar, from record to record.
fn polymorphic() -> impl Strategy<Value = Json> {
    prop_oneof![
        array((0_i64..3).prop_map(Json::from), 0.2),
        array(text(), 0.0).prop_map(|inner| object(vec![("x", Some(inner))])),
        (0_i64..3).prop_map(Json::from),
    ]
}

fn record() -> impl Strategy<Value = Json> {
    let inner = maybe(text()).prop_map(|c| object(vec![("c", c)]));
    let a = prop_oneof![(0_i64..5).prop_map(Json::from), Just(Json::Null)];
    let meta = (maybe(a), maybe(inner)).prop_map(|(a, b)| object(vec![("a", a), ("b", b)]));
    let matrix = array(array((0_i64..3).prop_map(Json::from), 0.0), 0.0);
    let nested_key = array((0_i64..3).prop_map(Json::from), 0.0)
        .prop_map(|values| object(vec![("b", Some(values))]));
    let scalars = (
        maybe((0_i64..4).prop_map(Json::from)),
        maybe(prop_oneof![text(), Just(Json::Null)]),
        maybe(number()),
        maybe((0_i64..3).prop_map(Json::from)),
        maybe(prop_oneof![(0_i64..3).prop_map(Json::from), text()]),
    );
    let nested = (
        maybe(meta),
        maybe(array(item(), 0.15)),
        maybe(array((0_i64..5).prop_map(Json::from), 0.2)),
        maybe(matrix),
        maybe(nested_key),
        maybe(polymorphic()),
    );
    (scalars, nested).prop_map(
        |((id, name, amount, flat, mixed), (meta, items, scores, matrix, nested, poly))| {
            object(vec![
                ("mixed", mixed),
                ("id", id),
                ("name", name),
                ("amount", amount),
                ("meta", meta),
                ("items", items),
                ("scores", scores),
                ("matrix", matrix),
                ("a__b", flat),
                ("a", nested),
                ("poly", poly),
            ])
        },
    )
}

/// Where a piece begins: most often after the piece before it, in its segment.
fn cut() -> impl Strategy<Value = Cut> {
    prop_oneof![2 => Just(Cut::Piece), 1 => Just(Cut::Segment), 1 => Just(Cut::Session)]
}

/// `records`, shredded into `batches`, each with the fields of the batch holding it.
fn with_fields<'a>(records: &'a [Json], batches: &'a [RecordBatch]) -> Vec<Record<'a>> {
    let mut held = Vec::with_capacity(records.len());
    let mut rows = records.iter();
    for batch in batches {
        let fields = batch.schema_ref().fields();
        held.extend(
            rows.by_ref()
                .take(batch.num_rows())
                .map(|value| Record { value, fields }),
        );
    }
    held
}

/// The reference's tables of `records` shredded into `batches`, normalized as `shape`.
fn expected(records: &[Json], batches: &[RecordBatch], shape: &Shape) -> reference::Tables {
    let key: Vec<&str> = shape.key.iter().map(AsRef::as_ref).collect();
    let whole: Vec<&str> = shape.whole.iter().map(AsRef::as_ref).collect();
    let records = with_fields(records, batches);
    reference::normalize(&records, shape.max_depth, &key, &whole)
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(256))]

    /// Normalizing shredded JSON agrees with the reference normalizer, each row's lineage naming
    /// exactly the rows it comes from and no id naming two rows, however the records are pushed
    /// and chunked into pieces and the pieces into segments and sessions.
    #[test]
    fn normalizing_agrees_with_the_reference(
        records in prop::collection::vec(record(), 1..10),
        max_depth in 0_u8..5,
        keyed in any::<bool>(),
        whole_meta in any::<bool>(),
        chunk_bytes in 16_usize..400,
        pushes in prop::collection::vec(any::<bool>(), 10),
        cuts in prop::collection::vec(cut(), 10),
    ) {
        let mut records = records;
        if keyed {
            // A merge stream's rows each hold a key; distinct ones here, so each root's id
            // names one row.
            for (index, record) in records.iter_mut().enumerate() {
                record["id"] = Json::from(index);
            }
        }
        let shape = Shape {
            max_depth,
            whole: if whole_meta { BTreeSet::from([Arc::from("meta")]) } else { BTreeSet::new() },
            key: if keyed { vec![Arc::from("id")] } else { Vec::new() },
        };
        // Each record may start a push of its own, whose shapes join apart from the others'.
        let mut batches = Vec::new();
        let mut first = 0;
        for (index, starts) in pushes.iter().enumerate().take(records.len()).skip(1) {
            if *starts {
                batches.extend(shredded(&records[first..index], chunk_bytes));
                first = index;
            }
        }
        batches.extend(shredded(&records[first..], chunk_bytes));
        let pieces: Vec<(RecordBatch, Cut)> =
            batches.iter().cloned().zip(cuts.iter().copied().cycle()).collect();
        let actual = compared::tables_of(&compared::stamped(&pieces), &shape)
            .map_err(|disagreed| TestCaseError::fail(disagreed.to_string()))?;
        prop_assert_eq!(actual, expected(&records, &batches, &shape));
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(rdlt_testkit::cases(256)))]

    /// Drawn Arrow batches normalize as the reference normalizes the records they hold, however
    /// a path is an array in one batch and an object or a scalar in another.
    #[test]
    fn drawn_batches_normalize_as_the_reference_normalizes_their_rows(
        drawn in rdlt_testkit::drawn::json::batches(),
        cuts in prop::collection::vec(cut(), 4),
        max_depth in 0_u8..4,
        keyed in any::<bool>(),
    ) {
        let batches: Vec<RecordBatch> =
            drawn.iter().map(rdlt_testkit::drawn::json::batch).collect();
        let batches = if keyed {
            rdlt_testkit::drawn::json::numbered(&batches, "k")
        } else {
            batches
        };
        let pieces: Vec<(RecordBatch, Cut)> = batches.into_iter().zip(cuts).collect();
        let key = keyed.then_some("k");
        compared::normalized_against_reference(&pieces, max_depth, key)
            .map_err(|disagreed| TestCaseError::fail(disagreed.to_string()))?;
    }
}

#[test]
fn the_reference_names_child_tables_by_their_arrays_paths() {
    let records = [json!({"a": {"b": [1]}, "a__b": [2]})];
    let tables = expected(&records, &shredded(&records, 1 << 20), &shape(8));
    let paths: Vec<&Vec<String>> = tables.keys().collect();
    assert_eq!(paths, [&path(&[]), &path(&["a", "b"]), &path(&["a__b"])]);
}

#[test]
fn a_path_that_mixes_kinds_in_a_push_is_a_column_kept_whole_and_apart_a_table() {
    let records = [json!({"p": [1, 2]}), json!({"p": 3}), json!({"p": [4]})];
    let together = shredded(&records, 1);
    assert_eq!(together.len(), 3, "a chunk a record");
    let tables = tables_of(&together, &shape(8));
    assert_eq!(tables.len(), 1, "one push keeps p whole in every chunk");
    assert_eq!(tables, expected(&records, &together, &shape(8)));
    let apart: Vec<RecordBatch> = records
        .iter()
        .flat_map(|record| shredded(std::slice::from_ref(record), 1 << 20))
        .collect();
    let tables = tables_of(&apart, &shape(8));
    assert_eq!(tables[&path(&["p"])].len(), 3, "p's arrays are a table");
    assert_eq!(tables, expected(&records, &apart, &shape(8)));
}

#[test]
fn fields_of_a_null_object_are_null_whatever_the_array_holds_beneath() {
    let values: ArrayRef = Arc::new(Int64Array::from(vec![1, 2]));
    let fields = Fields::from(vec![ArrowField::new("a", DataType::Int64, true)]);
    let nulls = NullBuffer::from(vec![true, false]);
    let object = StructArray::new(fields, vec![values], Some(nulls));
    let batch = RecordBatch::try_from_iter([("o", Arc::new(object) as ArrayRef)]).unwrap();
    let parts = normalize(&batch, &shape(8)).unwrap();
    let a = parts[0].batch.column(0);
    assert_eq!(a.as_primitive::<Int64Type>().value(0), 1);
    assert!(a.is_null(1), "the null object's field is null, not 2");
}

#[test]
fn a_null_field_of_an_object_null_in_other_rows_normalizes() {
    let records = [
        json!({"id": 1, "o": {"a": null, "b": 1}}),
        json!({"id": 2, "o": null}),
    ];
    let parts = parts(&records, &shape(8));
    let columns: Vec<String> = parts[0].columns.iter().map(ToString::to_string).collect();
    assert_eq!(columns, ["id", "o.a", "o.b"]);
}

#[test]
fn columns_keep_the_arrow_types_their_fields_declare() {
    let records = [
        json!({"a": 1, "o": {"m": 1}}),
        json!({"a": "x", "o": {"m": [2]}}),
    ];
    let parts = parts(&records, &shape(8));
    let schema = rdlt_connector::TableSchema::from_arrow(&parts[0].batch.schema()).unwrap();
    let types: Vec<&rdlt_connector::LogicalType> = schema
        .fields()
        .iter()
        .map(rdlt_connector::Field::logical_type)
        .collect();
    assert_eq!(
        types,
        [
            &rdlt_connector::LogicalType::Json,
            &rdlt_connector::LogicalType::Json
        ],
        "mixed values stay JSON, flattened or not"
    );
}

#[test]
fn a_merge_roots_id_does_not_depend_on_the_rows_beside_it() {
    let alone = parts(&[json!({"id": 1, "a": 1, "o": {"b": 2}})], &keyed("id"));
    let beside = parts(
        &[
            json!({"id": 1, "a": 1, "o": {"b": 2}}),
            json!({"id": "x", "a": "x", "o": [3]}),
        ],
        &keyed("id"),
    );
    let id = |parts: &[Part]| ids_of(parts, 0).0[0].clone();
    assert_eq!(
        id(&alone),
        id(&beside),
        "a key widened to JSON hashes as itself"
    );
}

#[test]
fn rows_that_differ_never_encode_alike() {
    let text = format!("{}t}}", "x".repeat(121));
    let key = format!("\u{1}bs{{{}", "x".repeat(121));
    let first = json!({"a": {}, "b": text});
    let second = json!({"a": {key: true}});
    let hashes: Vec<Vec<u8>> = [first, second]
        .iter()
        .map(|record| {
            let batch = &shredded(std::slice::from_ref(record), 1 << 20)[0];
            version_hashes(batch).unwrap().value(0).to_vec()
        })
        .collect();
    assert_ne!(hashes[0], hashes[1]);
}

/// A batch of one column `s`, objects of the field `v` holding `values`, the second row null.
fn under_a_null_object(values: ArrayRef) -> RecordBatch {
    let field = ArrowField::new("v", values.data_type().clone(), true);
    let nulls = Some(NullBuffer::from(vec![true, false]));
    let object = StructArray::try_new(Fields::from(vec![field]), vec![values], nulls).unwrap();
    RecordBatch::try_from_iter([("s", Arc::new(object) as ArrayRef)]).unwrap()
}

/// Two-row fields of every encoding a field under a null object may have.
fn encoded_fields() -> Vec<ArrayRef> {
    let ends = || Int32Array::from(vec![1, 2]);
    let integers = || Int64Array::from(vec![1, 2]);
    let keyed: ArrayRef = Arc::new(
        DictionaryArray::<Int32Type>::try_new(Int32Array::from(vec![0, 1]), Arc::new(integers()))
            .unwrap(),
    );
    vec![
        Arc::new(integers()),
        Arc::new(RunArray::<Int32Type>::try_new(&ends(), &integers()).unwrap()),
        Arc::clone(&keyed),
        Arc::new(RunArray::<Int32Type>::try_new(&ends(), keyed.as_ref()).unwrap()),
        Arc::new(StringViewArray::from(vec!["a", "b"])),
    ]
}

#[test]
fn every_field_under_a_null_object_is_null_whatever_its_encoding() {
    for values in encoded_fields() {
        let kind = values.data_type().clone();
        let parts = normalize(&under_a_null_object(values), &shape(8)).unwrap();
        let column = decoded(parts[0].batch.column(0)).unwrap();
        assert_eq!(
            (column.is_valid(0), column.is_null(1)),
            (true, true),
            "{kind}"
        );
    }
}

#[test]
fn a_null_field_under_a_null_object_stays_null() {
    let parts = normalize(&under_a_null_object(Arc::new(NullArray::new(2))), &shape(8)).unwrap();
    let column = parts[0].batch.column(0);
    assert_eq!(
        (column.data_type(), column.logical_null_count()),
        (&DataType::Null, 2)
    );
}

#[test]
fn every_item_field_under_a_null_object_is_null_whatever_its_encoding() {
    for values in encoded_fields() {
        let kind = values.data_type().clone();
        let field = ArrowField::new("v", kind.clone(), true);
        let nulls = Some(NullBuffer::from(vec![true, false]));
        let items = StructArray::try_new(Fields::from(vec![field]), vec![values], nulls).unwrap();
        let item = Arc::new(ArrowField::new("item", items.data_type().clone(), true));
        let offsets = OffsetBuffer::from_lengths([2]);
        let list = ListArray::try_new(item, offsets, Arc::new(items), None).unwrap();
        let batch = RecordBatch::try_from_iter([("l", Arc::new(list) as ArrayRef)]).unwrap();
        let parts = normalize(&batch, &shape(8)).unwrap();
        let child = parts.iter().find(|part| !part.path.is_empty()).unwrap();
        let column = decoded(child.batch.column(0)).unwrap();
        assert_eq!(
            (column.is_valid(0), column.is_null(1)),
            (true, true),
            "{kind}"
        );
    }
}

#[test]
fn a_union_under_an_object_is_an_internal_error() {
    let fields = UnionFields::try_new(
        vec![0_i8],
        vec![ArrowField::new("i", DataType::Int32, true)],
    )
    .unwrap();
    let children: Vec<ArrayRef> = vec![Arc::new(Int32Array::from(vec![1, 2]))];
    let union =
        UnionArray::try_new(fields, ScalarBuffer::from(vec![0_i8, 0]), None, children).unwrap();
    // The key keeps identity from reading the union, so normalizing meets it first.
    let object = under_a_null_object(Arc::new(union));
    let ids: ArrayRef = Arc::new(Int64Array::from(vec![1, 2]));
    let batch =
        RecordBatch::try_from_iter([("id", ids), ("s", Arc::clone(object.column(0)))]).unwrap();
    let keyed = Shape {
        key: vec![Arc::from("id")],
        ..shape(8)
    };
    let error = normalize(&batch, &keyed).unwrap_err();
    assert!(
        matches!(&error, ArrowError::InvalidArgumentError(message) if message == "a union cannot be a column of a table"),
        "{error}"
    );
}
