//! A child table's rows are matched to their roots by the value of the root's id, however each
//! side holds it.

use std::sync::Arc;

use arrow_array::cast::AsArray;
use arrow_array::types::Int64Type;
use arrow_array::{
    ArrayRef, BinaryArray, Int16Array, Int32Array, Int64Array, RecordBatch, StringArray,
    UInt8Array, UInt32Array, UInt64Array,
};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use rdlt_connector::{MergeKey, RootKey};

use super::super::merge_children_sparse;
use super::super::refused::code;
use super::super::retype::compared;

fn root() -> RootKey {
    RootKey {
        table: "roots".into(),
        id: "id".into(),
        seq: "seq".into(),
    }
}

fn key() -> MergeKey {
    MergeKey {
        columns: vec!["owner".into(), "item".into()],
        seq: "seq".into(),
        root: Some(root()),
        changes: None,
        history: None,
    }
}

/// A child table whose owner column and sequence are of the types of `owner` and `seq`.
fn children(owner: &ArrayRef, seq: &ArrayRef) -> (SchemaRef, RecordBatch) {
    let items: ArrayRef = Arc::new(Int64Array::from_iter_values(
        (0..owner.len()).map(|item| i64::try_from(item).unwrap()),
    ));
    let schema = Arc::new(Schema::new(vec![
        Field::new("owner", owner.data_type().clone(), true),
        Field::new("item", DataType::Int64, true),
        Field::new("seq", seq.data_type().clone(), true),
    ]));
    let columns = vec![Arc::clone(owner), items, Arc::clone(seq)];
    let batch = RecordBatch::try_new(Arc::clone(&schema), columns).unwrap();
    (schema, batch)
}

fn roots(id: ArrayRef, seq: ArrayRef) -> RecordBatch {
    RecordBatch::try_from_iter([("id", id), ("seq", seq)]).unwrap()
}

/// `values` held as a dictionary whose keys are of `keys`.
fn keyed(values: &ArrayRef, keys: &DataType) -> ArrayRef {
    let kind = DataType::Dictionary(Box::new(keys.clone()), Box::new(values.data_type().clone()));
    arrow_cast::cast(values, &kind).unwrap()
}

/// The items the child table holds once `incoming` follows `roots`.
fn followed(schema: &SchemaRef, incoming: &RecordBatch, roots: &RecordBatch) -> Vec<i64> {
    let incoming = std::slice::from_ref(incoming);
    let roots = std::slice::from_ref(roots);
    let rows = merge_children_sparse(schema, &[], incoming, &key(), &root(), roots).unwrap();
    let mut items: Vec<i64> = rows
        .iter()
        .flat_map(|batch| {
            let items = batch.column_by_name("item").unwrap();
            items.as_primitive::<Int64Type>().values().to_vec()
        })
        .collect();
    items.sort_unstable();
    items
}

#[test]
fn an_id_or_a_sequence_in_a_dictionary_compares_as_the_values_it_stands_for() {
    use DataType as T;
    let plain: Vec<ArrayRef> = vec![
        Arc::new(Int64Array::from(vec![Some(-5), None, Some(5), Some(-5)])),
        Arc::new(Int32Array::from(vec![
            Some(7),
            Some(7),
            None,
            Some(i32::MIN),
        ])),
        Arc::new(UInt64Array::from(vec![
            Some(u64::MAX),
            Some(0),
            None,
            Some(0),
        ])),
        Arc::new(StringArray::from(vec![
            Some("b"),
            Some(""),
            None,
            Some("b"),
        ])),
        Arc::new(BinaryArray::from(vec![
            Some(&b"\x00"[..]),
            None,
            Some(b"zz"),
            Some(b"zz"),
        ])),
    ];
    let keys = [
        T::Int8,
        T::Int16,
        T::Int32,
        T::Int64,
        T::UInt8,
        T::UInt16,
        T::UInt32,
        T::UInt64,
    ];
    for values in &plain {
        let expected = compared(values).unwrap();
        for key in &keys {
            let held = keyed(values, key);
            let found = compared(&held).unwrap();
            assert_eq!(&found, &expected, "{}", held.data_type());
        }
    }
}

#[test]
fn a_root_s_id_and_its_children_s_owner_match_by_value_whatever_integers_hold_them() {
    // Roots 1, 2 and 300; the children of 2 at its winning sequence and of 300 follow.
    let owners: ArrayRef = Arc::new(Int16Array::from(vec![1, 2, 2, 300, 4]));
    let seqs: ArrayRef = Arc::new(UInt8Array::from(vec![9, 3, 2, 7, 1]));
    let (schema, incoming) = children(&owners, &seqs);
    let ids: Vec<ArrayRef> = vec![
        Arc::new(Int64Array::from(vec![2, 300, 2])),
        Arc::new(UInt32Array::from(vec![2, 300, 2])),
        keyed(
            &(Arc::new(UInt64Array::from(vec![2, 300, 2])) as ArrayRef),
            &DataType::Int8,
        ),
    ];
    let won: Vec<ArrayRef> = vec![
        Arc::new(Int64Array::from(vec![3, 7, 2])),
        Arc::new(Int32Array::from(vec![3, 7, 2])),
        keyed(
            &(Arc::new(Int16Array::from(vec![3, 7, 2])) as ArrayRef),
            &DataType::UInt16,
        ),
    ];
    for id in &ids {
        for seq in &won {
            let roots = roots(Arc::clone(id), Arc::clone(seq));
            let what = format!("{} and {}", id.data_type(), seq.data_type());
            assert_eq!(followed(&schema, &incoming, &roots), [1, 3], "{what}");
        }
    }
}

#[test]
fn a_root_s_id_or_sequence_of_another_kind_than_its_children_s_is_refused() {
    let number: ArrayRef = Arc::new(Int64Array::from(vec![2]));
    let text: ArrayRef = Arc::new(StringArray::from(vec!["2"]));
    let bytes: ArrayRef = Arc::new(BinaryArray::from_iter_values([b"2"]));
    // Text and bytes are both the bytes they are, and match.
    let (schema, incoming) = children(&text, &text);
    let found = followed(
        &schema,
        &incoming,
        &roots(Arc::clone(&bytes), Arc::clone(&bytes)),
    );
    assert_eq!(found, [0]);
    // A number is no text: neither side's rows are matched to the other's as if none were there.
    let mixed = [
        (&number, &number, &text, &number),
        (&text, &number, &number, &number),
        (&number, &number, &number, &bytes),
        (&number, &text, &number, &number),
        (&number, &number, &keyed(&text, &DataType::Int8), &number),
    ];
    for (owner, seq, id, won) in mixed {
        let (schema, incoming) = children(owner, seq);
        let roots = roots(Arc::clone(id), Arc::clone(won));
        let refused = merge_children_sparse(
            &schema,
            &[],
            std::slice::from_ref(&incoming),
            &key(),
            &root(),
            std::slice::from_ref(&roots),
        );
        let refused = refused.expect_err("the kinds differ");
        assert_eq!(code(&refused), Some("merge_key_invalid"), "{refused}");
    }
}
