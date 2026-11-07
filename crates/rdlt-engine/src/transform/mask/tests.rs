use std::sync::Arc;

use arrow_array::cast::AsArray as _;
use arrow_array::types::Int32Type;
use arrow_array::{
    Array, ArrayRef, BinaryArray, BinaryViewArray, DictionaryArray, FixedSizeBinaryArray,
    Int64Array, LargeBinaryArray, LargeStringArray, ListArray, RecordBatch, StringArray,
    StringViewArray, StructArray,
};
use arrow_buffer::NullBuffer;
use arrow_schema::{DataType, Field, Fields};
use proptest::prelude::*;
use rdlt_connector::{ColumnPath, Secret};

use super::Mask;
use crate::transform::{Op, Transform, allocated_beyond};

fn key(text: &str) -> Mask {
    Mask::hmac_sha256(Secret::new(text.to_owned()))
}

fn path(dotted: &str) -> ColumnPath {
    ColumnPath::new(dotted.split('.')).unwrap()
}

fn one(name: &str, column: ArrayRef) -> RecordBatch {
    RecordBatch::try_from_iter([(name, column)]).unwrap()
}

fn hex(batch: &RecordBatch, name: &str) -> Vec<Option<String>> {
    let column = batch.column_by_name(name).unwrap();
    column
        .as_string::<i32>()
        .iter()
        .map(|v| v.map(str::to_owned))
        .collect()
}

#[test]
fn an_hmac_mask_matches_hmac_sha256_computed_elsewhere() {
    // RFC 4231's second test case.
    let input = one(
        "s",
        Arc::new(StringArray::from(vec!["what do ya want for nothing?"])),
    );
    let output = Op::mask(["s"], key("Jefe")).apply(input).unwrap();
    assert_eq!(
        hex(&output, "s"),
        [Some(
            "5bdcc146bf60754e6a042426089575c75a003f089d2739839dec58b964ec3843".to_owned()
        )]
    );
}

#[test]
fn an_hmac_mask_hashes_every_layout_of_text_and_bytes_alike_and_keeps_nulls() {
    let values = [Some("a"), None, Some("b"), Some("a")];
    let dictionary: DictionaryArray<Int32Type> = values.iter().copied().collect();
    let bytes: Vec<Option<&[u8]>> = values.iter().map(|v| v.map(str::as_bytes)).collect();
    let layouts: [ArrayRef; 8] = [
        Arc::new(StringArray::from(values.to_vec())),
        Arc::new(LargeStringArray::from(values.to_vec())),
        Arc::new(StringViewArray::from(values.to_vec())),
        Arc::new(BinaryArray::from(
            values
                .iter()
                .map(|v| v.map(str::as_bytes))
                .collect::<Vec<_>>(),
        )),
        Arc::new(dictionary),
        Arc::new(LargeBinaryArray::from(bytes.clone())),
        Arc::new(BinaryViewArray::from(bytes.clone())),
        Arc::new(
            FixedSizeBinaryArray::try_from_sparse_iter_with_size(bytes.into_iter(), 1).unwrap(),
        ),
    ];
    let expected = hex(
        &Op::mask(["s"], key("k"))
            .apply(one("s", Arc::clone(&layouts[0])))
            .unwrap(),
        "s",
    );
    assert_eq!(expected[1], None);
    assert_eq!(expected[0], expected[3]);
    for layout in layouts {
        let data_type = layout.data_type().clone();
        let output = Op::mask(["s"], key("k")).apply(one("s", layout)).unwrap();
        assert_eq!(hex(&output, "s"), expected, "{data_type:?}");
    }
}

#[test]
fn a_redaction_replaces_every_value_and_keeps_nulls() {
    let input = one(
        "n",
        Arc::new(Int64Array::from(vec![Some(1), None, Some(3)])),
    );
    let output = Op::mask(["n"], Mask::redact("***")).apply(input).unwrap();
    assert_eq!(
        hex(&output, "n"),
        [Some("***".to_owned()), None, Some("***".to_owned())]
    );
}

#[test]
fn a_null_mask_leaves_a_column_of_the_null_type() {
    let input = one("s", Arc::new(StringArray::from(vec!["x", "y"])));
    let output = Op::mask(["s"], Mask::Null).apply(input).unwrap();
    let column = output.column(0);
    assert_eq!((column.data_type(), column.len()), (&DataType::Null, 2));
}

#[test]
fn a_nested_field_is_masked_with_its_siblings_and_its_parents_nulls_shared() {
    let secret: ArrayRef = Arc::new(StringArray::from(vec!["a", "b", "c"]));
    let count: ArrayRef = Arc::new(Int64Array::from(vec![1, 2, 3]));
    let fields = Fields::from(vec![
        Field::new("secret", DataType::Utf8, false),
        Field::new("count", DataType::Int64, false),
    ]);
    let nulls = NullBuffer::from(vec![true, false, true]);
    let parent = StructArray::new(
        fields,
        vec![secret, Arc::clone(&count)],
        Some(nulls.clone()),
    );
    let input = one("s", Arc::new(parent));
    let output = Op::mask([path("s.secret")], Mask::redact("x"))
        .apply(input)
        .unwrap();
    let parent = output.column(0).as_struct();
    assert!(Arc::ptr_eq(parent.column(1), &count));
    assert_eq!(parent.nulls(), Some(&nulls));
    assert_eq!(
        parent
            .column(0)
            .as_string::<i32>()
            .iter()
            .collect::<Vec<_>>(),
        [Some("x"); 3]
    );
}

#[test]
fn a_mask_two_structs_deep_through_a_required_middle_struct_builds_what_output_says() {
    let leaf: ArrayRef = Arc::new(StringArray::from(vec!["a", "b"]));
    let inner_fields = Fields::from(vec![Field::new("c", DataType::Utf8, false)]);
    let inner = StructArray::new(inner_fields.clone(), vec![leaf], None);
    let outer_fields = Fields::from(vec![Field::new("b", DataType::Struct(inner_fields), false)]);
    let outer = StructArray::new(outer_fields, vec![Arc::new(inner)], None);
    let input = one("a", Arc::new(outer));
    let op = Op::mask([path("a.b.c")], Mask::redact("x"));
    let output = op.apply(input.clone()).unwrap();
    assert_eq!(output.schema(), op.output(input.schema_ref()).unwrap());
    let middle = output.column(0).as_struct().column(0).as_struct();
    assert_eq!(
        middle
            .column(0)
            .as_string::<i32>()
            .iter()
            .collect::<Vec<_>>(),
        [Some("x"); 2]
    );
}

#[test]
fn a_masked_column_no_longer_names_an_extension_type() {
    let marked = |name: &str, column: ArrayRef| {
        let field = Field::new(name, column.data_type().clone(), true).with_metadata(
            [
                ("ARROW:extension:name".to_owned(), "arrow.json".to_owned()),
                ("ARROW:extension:metadata".to_owned(), String::new()),
                ("other".to_owned(), "kept".to_owned()),
            ]
            .into(),
        );
        RecordBatch::try_new(
            Arc::new(arrow_schema::Schema::new(vec![field])),
            vec![column],
        )
        .unwrap()
    };
    for mask in [key("k"), Mask::redact("x"), Mask::Null] {
        let input = marked("j", Arc::new(StringArray::from(vec!["{}"])));
        let op = Op::mask(["j"], mask);
        let from_output = op.output(input.schema_ref()).unwrap();
        let from_apply = op.apply(input).unwrap().schema();
        for schema in [from_output, from_apply] {
            let metadata = schema.field(0).metadata();
            assert!(!metadata.contains_key("ARROW:extension:name"));
            assert!(!metadata.contains_key("ARROW:extension:metadata"));
            assert_eq!(metadata.get("other").map(String::as_str), Some("kept"));
        }
    }
}

#[test]
fn masks_the_kernels_cannot_take_are_refused_naming_the_column() {
    let numbers = one("n", Arc::new(Int64Array::from(vec![1])));
    let refused = Op::mask(["n"], key("k"))
        .output(numbers.schema_ref())
        .unwrap_err();
    assert_eq!(refused.column(), Some("n"));
    let item = Arc::new(Field::new("item", DataType::Int64, true));
    let list = ListArray::new_null(item, 1);
    let lists = one("l", Arc::new(list));
    let refused = Op::mask([path("l.item")], Mask::Null)
        .output(lists.schema_ref())
        .unwrap_err();
    assert_eq!(refused.column(), Some("l.item"));
}

#[test]
fn a_mask_of_a_column_the_batch_lacks_passes_it_on() {
    let input = one("s", Arc::new(StringArray::from(vec!["x"])));
    let output = Op::mask([path("absent"), path("absent.below")], key("k"))
        .apply(input.clone())
        .unwrap();
    assert_eq!(output.schema(), input.schema());
}

#[test]
fn a_masks_key_never_shows() {
    let shown = format!("{:?}", Op::mask(["s"], key("k3y-held-back")));
    assert!(!shown.contains("k3y-held-back"), "{shown}");
}

proptest! {
    #[test]
    fn masks_allocate_within_their_bounds(values in proptest::collection::vec(proptest::option::of("\\PC{0,40}"), 0..2000)) {
        let strings: ArrayRef = Arc::new(StringArray::from(values.clone()));
        let dictionary: DictionaryArray<Int32Type> = values.iter().map(Option::as_deref).collect();
        for column in [strings, Arc::new(dictionary) as ArrayRef] {
            let input = one("s", column);
            for mask in [key("k"), Mask::redact("redacted"), Mask::Null] {
                let op = Op::mask(["s"], mask);
                let bound = op.bound(input.schema_ref()).expect("an op declares its bound");
                let output = op.apply(input.clone()).unwrap();
                prop_assert!(allocated_beyond(&output, &input) <= bound.of(&input));
            }
        }
    }
}

#[test]
fn an_hmac_mask_bounds_a_row_at_its_hex_digits_and_dictionary_values_twice() {
    let values = ["a", "b", "a", "b"];
    let plain: ArrayRef = Arc::new(StringArray::from(values.to_vec()));
    let dictionary: DictionaryArray<Int32Type> = values.iter().copied().map(Some).collect();
    let dictionary: ArrayRef = Arc::new(dictionary);
    let bound = |column: ArrayRef| {
        let input = one("s", column);
        let op = Op::mask(["s"], key("k"));
        (op.bound(input.schema_ref()).unwrap().of(&input), input)
    };
    assert_eq!(bound(plain).0, 256 + 69 * 4);
    let (bytes, input) = bound(dictionary);
    let held = crate::transform::column_bytes(input.column(0).as_ref());
    assert_eq!(bytes, 512 + 69 * 4 + 69 * held);
}

#[test]
fn a_redaction_making_more_text_than_a_column_holds_is_refused_naming_it() {
    let rows = RecordBatch::try_from_iter([(
        "n",
        Arc::new(arrow_array::NullArray::new(600_000)) as ArrayRef,
    )])
    .unwrap();
    let refused = Op::mask(["n"], Mask::redact("x".repeat(4096)))
        .apply(rows)
        .unwrap_err();
    assert_eq!(refused.column(), Some("n"));
}
