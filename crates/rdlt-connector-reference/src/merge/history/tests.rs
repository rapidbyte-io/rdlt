use std::sync::Arc;

use arrow_array::cast::AsArray;
use arrow_array::types::Int64Type;
use arrow_array::{
    Array, ArrayRef, BinaryArray, BooleanArray, Int8Array, Int64Array, RecordBatch, StringArray,
    new_null_array,
};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use proptest::prelude::*;
use rdlt_connector::{ChangeColumns, ChangeOp, Deletion, HistoryColumns, MergeKey};
use rdlt_testkit::drawn::{self, Drawn, Encoding, Shape};

use crate::merge::{Merged, merge};

/// How a history table is written: rows that are all upserts, or a change stream's.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Kind {
    Plain,
    Hard,
    Soft,
}

struct Row {
    op: ChangeOp,
    id: Option<i64>,
    name: Option<&'static str>,
    seq: u8,
    at: i64,
}

fn upsert(id: i64, name: &'static str, seq: u8, at: i64) -> Row {
    Row {
        op: ChangeOp::Update,
        id: Some(id),
        name: Some(name),
        seq,
        at,
    }
}

fn delete(id: i64, seq: u8, at: i64) -> Row {
    Row {
        op: ChangeOp::Delete,
        id: Some(id),
        name: None,
        seq,
        at,
    }
}

fn truncate(seq: u8, at: i64) -> Row {
    Row {
        op: ChangeOp::Truncate,
        id: None,
        name: None,
        seq,
        at,
    }
}

fn sequence(seq: u8) -> Vec<u8> {
    let mut bytes = vec![0; 16];
    bytes[15] = seq;
    bytes
}

fn hash(name: &str) -> Vec<u8> {
    let mut hash = vec![0; 16];
    for (byte, source) in hash.iter_mut().zip(name.bytes()) {
        *byte = source;
    }
    hash
}

fn key(kind: Kind) -> MergeKey {
    let deletion = match kind {
        Kind::Soft => Deletion::Soft { at: "at".into() },
        _ => Deletion::Hard,
    };
    MergeKey {
        columns: vec!["id".into()],
        seq: "seq".into(),
        root: None,
        changes: (kind != Kind::Plain).then(|| ChangeColumns {
            op: "op".into(),
            unchanged: None,
            deletion,
        }),
        history: Some(HistoryColumns {
            valid_from: "from".into(),
            valid_to: "to".into(),
            is_current: "current".into(),
            row_hash: "hash".into(),
        }),
    }
}

/// The stored columns: `data`, the key, the sequence, where deletes are soft the deletion time,
/// then the history columns.
fn stored(data: &[Field], kind: Kind) -> SchemaRef {
    let mut fields = vec![Field::new("id", DataType::Int64, true)];
    fields.extend(data.iter().cloned());
    fields.push(Field::new("seq", DataType::Binary, false));
    if kind == Kind::Soft {
        fields.push(Field::new("at", DataType::Int64, true));
    }
    fields.extend([
        Field::new("from", DataType::Int64, false),
        Field::new("to", DataType::Int64, true),
        Field::new("current", DataType::Boolean, false),
        Field::new("hash", DataType::Binary, true),
    ]);
    Arc::new(Schema::new(fields))
}

fn names() -> Vec<Field> {
    vec![Field::new("name", DataType::Utf8, true)]
}

/// Rows of `ids`, `data` and the rest as a written batch of a table of `kind`.
fn batch(
    ids: Vec<Option<i64>>,
    data: Vec<(Field, ArrayRef)>,
    rows: &[Row],
    kind: Kind,
) -> RecordBatch {
    let fields: Vec<Field> = data.iter().map(|(field, _)| field.clone()).collect();
    let schema = stored(&fields, kind);
    let mut columns: Vec<(String, ArrayRef)> = vec![("id".into(), Arc::new(Int64Array::from(ids)))];
    columns.extend(
        data.into_iter()
            .map(|(field, array)| (field.name().clone(), array)),
    );
    let seqs = BinaryArray::from_iter_values(rows.iter().map(|row| sequence(row.seq)));
    let at: Int64Array = rows
        .iter()
        .map(|row| (row.op != ChangeOp::Update).then_some(row.at))
        .collect();
    let hashes: BinaryArray = rows.iter().map(|row| row.name.map(hash)).collect();
    columns.extend([
        ("seq".into(), Arc::new(seqs) as ArrayRef),
        ("at".into(), Arc::new(at)),
        (
            "from".into(),
            Arc::new(Int64Array::from_iter_values(rows.iter().map(|row| row.at))),
        ),
        ("to".into(), Arc::new(Int64Array::new_null(rows.len()))),
        (
            "current".into(),
            Arc::new(BooleanArray::from(vec![true; rows.len()])),
        ),
        ("hash".into(), Arc::new(hashes)),
        (
            "op".into(),
            Arc::new(Int8Array::from_iter_values(
                rows.iter().map(|row| row.op.code()),
            )),
        ),
    ]);
    let kept =
        |name: &str| schema.field_with_name(name).is_ok() || (name == "op" && kind != Kind::Plain);
    let columns: Vec<(String, ArrayRef)> =
        columns.into_iter().filter(|(name, _)| kept(name)).collect();
    RecordBatch::try_from_iter(columns).expect("a valid batch")
}

fn written(rows: &[Row], kind: Kind) -> RecordBatch {
    let values: StringArray = rows.iter().map(|row| row.name).collect();
    let data = vec![(names()[0].clone(), Arc::new(values) as ArrayRef)];
    batch(rows.iter().map(|row| row.id).collect(), data, rows, kind)
}

fn apply(published: &Merged, batches: &[&[Row]], kind: Kind) -> Merged {
    let incoming: Vec<RecordBatch> = batches.iter().map(|rows| written(rows, kind)).collect();
    merge(
        &stored(&names(), kind),
        &published.rows,
        &published.tombstones,
        &incoming,
        &key(kind),
    )
    .expect("the rows merge")
}

/// Applies each commit in turn to an empty table.
fn history(commits: &[&[Row]], kind: Kind) -> Merged {
    let empty = Merged {
        rows: Vec::new(),
        tombstones: Vec::new(),
    };
    commits
        .iter()
        .fold(empty, |merged, rows| apply(&merged, &[rows], kind))
}

/// A version: its key, name, sequence's last byte, deletion time, validity and current flag.
type Version = (i64, Option<String>, u8, Option<i64>, i64, Option<i64>, bool);

fn current(id: i64, name: &str, seq: u8, from: i64) -> Version {
    (id, Some(name.into()), seq, None, from, None, true)
}

fn closed(id: i64, name: &str, seq: u8, from: i64, to: i64) -> Version {
    (id, Some(name.into()), seq, None, from, Some(to), false)
}

/// A version deleted at `from`, current until `to` closes it.
fn deleted(id: i64, name: &str, seq: u8, from: i64, to: Option<i64>) -> Version {
    (
        id,
        Some(name.into()),
        seq,
        Some(from),
        from,
        to,
        to.is_none(),
    )
}

fn versions(merged: &Merged) -> Vec<Version> {
    let mut versions = Vec::new();
    for batch in &merged.rows {
        let int = |name| {
            batch
                .column_by_name(name)
                .unwrap()
                .as_primitive::<Int64Type>()
        };
        let (ids, from, to) = (int("id"), int("from"), int("to"));
        let at = batch
            .column_by_name("at")
            .map(AsArray::as_primitive::<Int64Type>);
        let names = batch.column_by_name("name").unwrap().as_string::<i32>();
        let seqs = batch.column_by_name("seq").unwrap().as_binary::<i32>();
        let current = batch.column_by_name("current").unwrap().as_boolean();
        for row in 0..batch.num_rows() {
            versions.push((
                ids.value(row),
                names.is_valid(row).then(|| names.value(row).to_owned()),
                seqs.value(row)[15],
                at.filter(|at| at.is_valid(row)).map(|at| at.value(row)),
                from.value(row),
                to.is_valid(row).then(|| to.value(row)),
                current.value(row),
            ));
        }
    }
    versions.sort_unstable();
    versions
}

fn sorted(mut expected: Vec<Version>) -> Vec<Version> {
    expected.sort_unstable();
    expected
}

/// Each tombstone as its key, none for the bound, and its sequence's last byte.
fn tombstones(merged: &Merged) -> Vec<(Option<i64>, u8)> {
    let mut tombstones = Vec::new();
    for batch in &merged.tombstones {
        let ids = batch
            .column_by_name("id")
            .unwrap()
            .as_primitive::<Int64Type>();
        let seqs = batch.column_by_name("seq").unwrap().as_binary::<i32>();
        for row in 0..batch.num_rows() {
            tombstones.push((
                ids.is_valid(row).then(|| ids.value(row)),
                seqs.value(row)[15],
            ));
        }
    }
    tombstones.sort_unstable();
    tombstones
}

#[test]
fn an_upsert_equal_to_the_current_version_changes_nothing() {
    for kind in [Kind::Plain, Kind::Hard, Kind::Soft] {
        let merged = history(&[&[upsert(1, "a", 1, 10)], &[upsert(1, "a", 2, 20)]], kind);
        assert_eq!(versions(&merged), [current(1, "a", 1, 10)]);
    }
}

#[test]
fn a_changed_upsert_closes_the_current_version_where_it_begins() {
    for kind in [Kind::Plain, Kind::Hard, Kind::Soft] {
        let merged = history(&[&[upsert(1, "a", 1, 10)], &[upsert(1, "b", 2, 20)]], kind);
        assert_eq!(
            versions(&merged),
            [closed(1, "a", 1, 10, 20), current(1, "b", 2, 20)]
        );
    }
}

#[test]
fn versions_of_a_key_in_one_commit_chain_in_sequence_order_whichever_batch_carries_them() {
    for kind in [Kind::Plain, Kind::Hard] {
        let empty = history(&[], kind);
        let merged = apply(
            &empty,
            &[
                &[upsert(1, "c", 3, 30), upsert(2, "x", 4, 40)],
                &[
                    upsert(1, "a", 1, 10),
                    upsert(1, "b", 2, 20),
                    upsert(1, "c", 5, 50),
                ],
            ],
            kind,
        );
        let expected = vec![
            closed(1, "a", 1, 10, 20),
            closed(1, "b", 2, 20, 30),
            current(1, "c", 3, 30),
            current(2, "x", 4, 40),
        ];
        assert_eq!(versions(&merged), sorted(expected));
    }
}

#[test]
fn a_plain_table_applies_each_commit_after_the_last_whatever_its_sequences() {
    let merged = history(
        &[
            &[upsert(1, "a", 9, 10)],
            &[upsert(1, "b", 1, 20)],
            &[upsert(1, "b", 1, 30)],
        ],
        Kind::Plain,
    );
    assert_eq!(
        versions(&merged),
        [closed(1, "a", 9, 10, 20), current(1, "b", 1, 20)]
    );
    assert_eq!(tombstones(&merged), []);
}

#[test]
fn a_change_applies_only_past_its_key_s_newest_version() {
    let published = history(
        &[&[upsert(1, "a", 2, 20), upsert(1, "b", 4, 40)]],
        Kind::Hard,
    );
    let deleted = apply(&published, &[&[delete(1, 5, 50)]], Kind::Hard);
    let replayed = apply(
        &published,
        &[&[upsert(1, "old", 3, 30), upsert(1, "same", 4, 45)]],
        Kind::Hard,
    );
    assert_eq!(versions(&replayed), versions(&published));
    let later = apply(&deleted, &[&[upsert(1, "c", 6, 60)]], Kind::Hard);
    assert_eq!(
        versions(&later),
        [
            closed(1, "a", 2, 20, 40),
            closed(1, "b", 4, 40, 50),
            current(1, "c", 6, 60)
        ]
    );
}

#[test]
fn an_equal_upsert_leaves_the_guard_where_it_was() {
    let merged = history(
        &[
            &[upsert(1, "a", 1, 10), upsert(1, "a", 5, 50)],
            &[upsert(1, "b", 3, 30)],
        ],
        Kind::Hard,
    );
    assert_eq!(
        versions(&merged),
        [closed(1, "a", 1, 10, 30), current(1, "b", 3, 30)]
    );
}

#[test]
fn a_hard_delete_closes_the_current_version_and_a_later_upsert_opens_another() {
    let merged = history(
        &[
            &[upsert(1, "a", 1, 10), delete(1, 2, 20), delete(7, 3, 30)],
            &[upsert(1, "a", 4, 40)],
        ],
        Kind::Hard,
    );
    assert_eq!(
        versions(&merged),
        [closed(1, "a", 1, 10, 20), current(1, "a", 4, 40)]
    );
    // A delete buries its key whether or not a version was current, until a version sequenced
    // past it holds the key again.
    assert_eq!(tombstones(&merged), [(Some(7), 3)]);
    let buried = history(
        &[&[upsert(1, "a", 1, 10), delete(1, 2, 20), delete(7, 3, 30)]],
        Kind::Hard,
    );
    assert_eq!(tombstones(&buried), [(Some(1), 2), (Some(7), 3)]);
}

#[test]
fn a_change_replayed_after_a_hard_delete_or_truncate_changes_nothing() {
    let published = history(
        &[
            &[upsert(1, "a", 1, 10), upsert(2, "b", 2, 20)],
            &[delete(1, 5, 50), upsert(3, "c", 6, 60), truncate(7, 70)],
        ],
        Kind::Hard,
    );
    assert_eq!(tombstones(&published), [(None, 7)]);
    let replayed = apply(
        &published,
        &[&[
            upsert(1, "x", 4, 40),
            upsert(9, "i", 6, 65),
            upsert(2, "y", 3, 30),
        ]],
        Kind::Hard,
    );
    assert_eq!(versions(&replayed), versions(&published));
    let buried = history(&[&[delete(1, 5, 50)], &[upsert(1, "x", 5, 55)]], Kind::Hard);
    assert_eq!(versions(&buried), []);
}

#[test]
fn a_soft_delete_opens_a_deleted_version_that_an_equal_upsert_closes() {
    let merged = history(
        &[
            &[upsert(1, "a", 1, 10)],
            &[delete(1, 2, 20)],
            &[upsert(1, "a", 3, 30)],
        ],
        Kind::Soft,
    );
    let expected = vec![
        closed(1, "a", 1, 10, 20),
        deleted(1, "a", 2, 20, Some(30)),
        current(1, "a", 3, 30),
    ];
    assert_eq!(versions(&merged), sorted(expected));
    assert_eq!(tombstones(&merged), []);
}

#[test]
fn a_delete_of_a_deleted_or_missing_key_changes_nothing() {
    let published = history(&[&[upsert(1, "a", 1, 10)], &[delete(1, 2, 20)]], Kind::Soft);
    let merged = apply(
        &published,
        &[&[delete(1, 3, 30), delete(9, 4, 40)]],
        Kind::Soft,
    );
    assert_eq!(versions(&merged), versions(&published));
    let merged = apply(&published, &[&[truncate(5, 50)]], Kind::Soft);
    assert_eq!(versions(&merged), versions(&published));
    let merged = history(&[&[delete(9, 4, 40)]], Kind::Hard);
    assert_eq!(versions(&merged), []);
}

#[test]
fn a_hard_truncate_closes_every_current_version_before_it_and_raises_the_bound() {
    let merged = history(
        &[
            &[
                upsert(1, "a", 1, 10),
                upsert(2, "b", 2, 20),
                delete(3, 3, 30),
            ],
            &[truncate(4, 40)],
        ],
        Kind::Hard,
    );
    assert_eq!(
        versions(&merged),
        [closed(1, "a", 1, 10, 40), closed(2, "b", 2, 20, 40)]
    );
    assert_eq!(tombstones(&merged), [(None, 4)]);
    let later = apply(
        &merged,
        &[&[upsert(4, "d", 5, 50), upsert(5, "e", 3, 35)]],
        Kind::Hard,
    );
    let mut expected = versions(&merged);
    expected.push(current(4, "d", 5, 50));
    assert_eq!(versions(&later), sorted(expected));
}

#[test]
fn a_soft_truncate_deletes_every_current_version_before_it() {
    let merged = history(
        &[
            &[upsert(1, "a", 1, 10), upsert(2, "b", 2, 20)],
            &[delete(2, 3, 30), truncate(4, 40)],
        ],
        Kind::Soft,
    );
    assert_eq!(
        versions(&merged),
        [
            closed(1, "a", 1, 10, 40),
            deleted(1, "a", 4, 40, None),
            closed(2, "b", 2, 20, 30),
            deleted(2, "b", 3, 30, None),
        ]
    );
    assert_eq!(tombstones(&merged), []);
}

#[test]
fn rows_after_a_truncate_in_its_commit_stay() {
    for kind in [Kind::Hard, Kind::Soft] {
        let merged = history(
            &[&[
                upsert(1, "a", 1, 10),
                truncate(4, 40),
                upsert(2, "b", 4, 45),
                upsert(1, "c", 5, 50),
            ]],
            kind,
        );
        let mut expected = vec![closed(1, "a", 1, 10, 40), current(2, "b", 4, 45)];
        if kind == Kind::Soft {
            expected.extend([deleted(1, "a", 4, 40, Some(50)), current(1, "c", 5, 50)]);
        } else {
            expected.push(current(1, "c", 5, 50));
        }
        assert_eq!(versions(&merged), sorted(expected));
    }
}

#[test]
fn truncates_of_one_commit_each_close_what_was_current_before_them() {
    let commits: [&[Row]; 2] = [
        &[upsert(1, "a", 1, 10), upsert(2, "b", 2, 20)],
        &[
            truncate(3, 30),
            upsert(1, "c", 4, 40),
            truncate(5, 50),
            truncate(6, 60),
            upsert(2, "d", 7, 70),
        ],
    ];
    let hard = history(&commits, Kind::Hard);
    assert_eq!(
        versions(&hard),
        sorted(vec![
            closed(1, "a", 1, 10, 30),
            closed(1, "c", 4, 40, 50),
            closed(2, "b", 2, 20, 30),
            current(2, "d", 7, 70),
        ])
    );
    assert_eq!(tombstones(&hard), [(None, 6)]);
    // Where deletes are soft, the first truncate past a version deletes it and the next finds it
    // deleted already.
    let soft = history(&commits, Kind::Soft);
    assert_eq!(
        versions(&soft),
        sorted(vec![
            closed(1, "a", 1, 10, 30),
            deleted(1, "a", 3, 30, Some(40)),
            closed(1, "c", 4, 40, 50),
            deleted(1, "c", 5, 50, None),
            closed(2, "b", 2, 20, 30),
            deleted(2, "b", 3, 30, Some(70)),
            current(2, "d", 7, 70),
        ])
    );
    assert_eq!(tombstones(&soft), []);
}

/// The least of three timings of `work`, which a busy machine inflates least.
fn timed<T>(mut work: impl FnMut() -> T) -> std::time::Duration {
    (0..3)
        .map(|_| {
            let started = std::time::Instant::now();
            std::hint::black_box(work());
            started.elapsed()
        })
        .min()
        .expect("three timings")
}

#[test]
fn truncates_cost_their_count_and_the_keys_not_their_product() {
    const KEYS: i64 = 10_000;
    const TRUNCATES: usize = 2_000;
    for kind in [Kind::Hard, Kind::Soft] {
        let rows: Vec<Row> = (0..KEYS).map(|id| upsert(id, "a", 100, 10)).collect();
        let published = history(&[&rows], kind);
        // Truncates before every version change nothing; those past them close each.
        for seq in [1, 200] {
            let truncating = |count: usize| {
                let truncates: Vec<Row> = (0..count).map(|_| truncate(seq, 50)).collect();
                timed(|| apply(&published, &[&truncates], kind))
            };
            let (one, many) = (truncating(1), truncating(TRUNCATES));
            assert!(
                many < one * 15,
                "at {seq}: one truncate took {one:?}, {TRUNCATES} took {many:?}"
            );
        }
    }
}

#[test]
fn a_change_stream_s_history_stores_no_op() {
    let merged = history(&[&[upsert(1, "a", 1, 10)]], Kind::Hard);
    let schema = merged.rows[0].schema();
    let stored: Vec<&str> = schema
        .fields()
        .iter()
        .map(|field| field.name().as_str())
        .collect();
    assert_eq!(
        stored,
        ["id", "name", "seq", "from", "to", "current", "hash"]
    );
}

/// `shape` in the plain Arrow types of its logical types, as a table stores them.
fn plain(mut shape: Shape) -> Shape {
    shape.encoding = Encoding::Plain;
    shape.children = shape.children.into_iter().map(plain).collect();
    shape
}

/// One to three columns of any type, in the plain Arrow types a table stores, and up to six rows.
fn stored_values() -> impl Strategy<Value = Drawn> {
    proptest::collection::vec(drawn::values::shape(2).prop_map(plain), 1..=3).prop_flat_map(
        |shapes| {
            let row: Vec<_> = shapes
                .iter()
                .map(|shape| drawn::values::value(shape, true))
                .collect();
            let columns = shapes
                .into_iter()
                .enumerate()
                .map(|(index, shape)| (format!("c{index}"), shape))
                .collect();
            (Just(columns), proptest::collection::vec(row, 0..6))
        },
    )
}

/// The drawn columns as fields and arrays.
fn data((columns, rows): &Drawn) -> Vec<(Field, ArrayRef)> {
    columns
        .iter()
        .enumerate()
        .map(|(index, (name, shape))| {
            let values: Vec<_> = rows.iter().map(|row| &row[index]).collect();
            let array = drawn::array(shape, &values);
            (drawn::field(name, shape, &array, true), array)
        })
        .collect()
}

/// Upserts of every drawn row, each its own key, hashed as `name`, or with `name` none, deletes
/// of them; sequenced from `first` and beginning at `at`.
fn commit(
    data: &[(Field, ArrayRef)],
    count: usize,
    (name, first, at): (Option<&'static str>, usize, i64),
    kind: Kind,
) -> RecordBatch {
    let ids: Vec<i64> = (0..count).map(|id| i64::try_from(id).unwrap()).collect();
    let rows: Vec<Row> = ids
        .iter()
        .map(|&id| {
            let seq = u8::try_from(first).unwrap() + u8::try_from(id).unwrap();
            name.map_or_else(|| delete(id, seq, at), |name| upsert(id, name, seq, at))
        })
        .collect();
    let data = data
        .iter()
        .map(|(field, array)| match name {
            Some(_) => (field.clone(), Arc::clone(array)),
            None => (field.clone(), new_null_array(field.data_type(), count)),
        })
        .collect();
    batch(ids.into_iter().map(Some).collect(), data, &rows, kind)
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(rdlt_testkit::cases(128)))]

    #[test]
    fn every_type_keeps_its_values_through_each_version(
        drawn in stored_values(),
        soft in any::<bool>(),
    ) {
        let kind = if soft { Kind::Soft } else { Kind::Plain };
        let data = data(&drawn);
        let count = drawn.1.len();
        let mut commits = vec![
            commit(&data, count, (Some("a"), 1, 10), kind),
            commit(&data, count, (Some("b"), 1 + count, 20), kind),
        ];
        if soft {
            commits.push(commit(&data, count, (None, 1 + 2 * count, 30), kind));
        }
        let fields: Vec<Field> = data.iter().map(|(field, _)| field.clone()).collect();
        let schema = stored(&fields, kind);
        let mut merged = Merged { rows: Vec::new(), tombstones: Vec::new() };
        for incoming in &commits {
            let incoming = std::slice::from_ref(incoming);
            merged = merge(&schema, &merged.rows, &merged.tombstones, incoming, &key(kind))
                .expect("the rows merge");
        }
        let versions: usize = merged.rows.iter().map(RecordBatch::num_rows).sum();
        prop_assert_eq!(versions, commits.len() * count);
        for batch in &merged.rows {
            let ids = batch.column_by_name("id").unwrap().as_primitive::<Int64Type>();
            for row in 0..batch.num_rows() {
                let id = usize::try_from(ids.value(row)).unwrap();
                for (field, array) in &data {
                    let kept = batch.column_by_name(field.name()).unwrap().slice(row, 1);
                    prop_assert_eq!(kept.to_data(), array.slice(id, 1).to_data(), "{}", field.name());
                }
            }
        }
    }
}
