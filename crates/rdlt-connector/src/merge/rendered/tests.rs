use std::sync::Arc;

use arrow_array::builder::StringDictionaryBuilder;
use arrow_array::cast::AsArray;
use arrow_array::types::Int32Type;
use arrow_array::{
    Array, ArrayRef, BinaryArray, BooleanArray, Date32Array, Decimal128Array,
    DurationNanosecondArray, FixedSizeBinaryArray, Float64Array, Int8Array, Int64Array,
    IntervalYearMonthArray, ListArray, StringArray, StructArray, TimestampSecondArray, UInt64Array,
};
use arrow_schema::{DataType, Field as ArrowField, Fields};

use super::super::refused::code;
use super::rendered;
use crate::{Field, LogicalType};

/// The JSON text `rendered` gives each value of `array`, under a field of its own type, or of
/// `logical` where one is given.
fn json(array: &ArrayRef, logical: Option<LogicalType>) -> Vec<Option<String>> {
    let field = match logical {
        Some(logical) => Field::new("v", logical, true).to_arrow(),
        None => ArrowField::new("v", array.data_type().clone(), true),
    };
    let rendered = rendered(array, &field).unwrap();
    let rendered = rendered.as_string::<i32>();
    (0..rendered.len())
        .map(|row| {
            rendered
                .is_valid(row)
                .then(|| rendered.value(row).to_owned())
        })
        .collect()
}

fn some(texts: &[&str]) -> Vec<Option<String>> {
    texts.iter().map(|text| Some((*text).to_owned())).collect()
}

#[test]
fn text_becomes_a_json_string_and_json_stays_as_it_is() {
    let text: ArrayRef = Arc::new(StringArray::from(vec![Some("a\"b\\c\u{1}/é"), None]));
    assert_eq!(
        json(&text, None),
        [Some(r#""a\"b\\c\u0001/é""#.to_owned()), None]
    );
    let held: ArrayRef = Arc::new(StringArray::from(vec![r#"{"x":[1,"y"]}"#]));
    assert_eq!(
        json(&held, Some(LogicalType::Json)),
        some(&[r#"{"x":[1,"y"]}"#])
    );
    let mut dictionary = StringDictionaryBuilder::<Int32Type>::new();
    dictionary.append_value("false");
    dictionary.append_null();
    dictionary.append_value("false");
    let dictionary: ArrayRef = Arc::new(dictionary.finish());
    assert_eq!(
        json(&dictionary, None),
        [
            Some(r#""false""#.to_owned()),
            None,
            Some(r#""false""#.to_owned())
        ]
    );
}

#[test]
fn booleans_and_numbers_become_their_json_text_and_decimals_their_digits_as_written() {
    let booleans: ArrayRef = Arc::new(BooleanArray::from(vec![true, false]));
    assert_eq!(json(&booleans, None), some(&["true", "false"]));
    let small: ArrayRef = Arc::new(Int8Array::from(vec![-5]));
    assert_eq!(json(&small, None), some(&["-5"]));
    let wide: ArrayRef = Arc::new(UInt64Array::from(vec![u64::MAX]));
    assert_eq!(json(&wide, None), some(&["18446744073709551615"]));
    let integers: ArrayRef = Arc::new(Int64Array::from(vec![i64::MIN]));
    assert_eq!(json(&integers, None), some(&["-9223372036854775808"]));
    let decimals: ArrayRef = Arc::new(
        Decimal128Array::from(vec![12_340, -5])
            .with_precision_and_scale(7, 2)
            .unwrap(),
    );
    assert_eq!(json(&decimals, None), some(&["123.40", "-0.05"]));
    let floats: ArrayRef = Arc::new(Float64Array::from(vec![1.5, 1e300, f64::NAN, -0.0]));
    assert_eq!(
        json(&floats, None),
        some(&["1.5", "1e+300", r#""NaN""#, "-0.0"])
    );
}

#[test]
fn bytes_become_hex_strings_and_a_uuid_its_hyphenated_string() {
    let bytes: ArrayRef = Arc::new(BinaryArray::from_iter_values([[0x00_u8, 0xff].as_slice()]));
    assert_eq!(json(&bytes, None), some(&[r#""00ff""#]));
    let id: Vec<u8> = (0..16).collect();
    let uuid: ArrayRef = Arc::new(FixedSizeBinaryArray::try_from_iter([id].into_iter()).unwrap());
    assert_eq!(
        json(&uuid, Some(LogicalType::Uuid)),
        some(&[r#""00010203-0405-0607-0809-0a0b0c0d0e0f""#])
    );
    assert_eq!(
        json(&uuid, None),
        some(&[r#""000102030405060708090a0b0c0d0e0f""#])
    );
}

#[test]
fn temporal_values_become_strings_of_their_exact_text() {
    let dates: ArrayRef = Arc::new(Date32Array::from(vec![0, 2_932_897]));
    assert_eq!(
        json(&dates, None),
        some(&[r#""1970-01-01""#, r#""+10000-01-01""#])
    );
    let durations: ArrayRef = Arc::new(DurationNanosecondArray::from(vec![0, -500_000_000]));
    assert_eq!(json(&durations, None), some(&[r#""PT0S""#, r#""-PT0.5S""#]));
    let zoned: ArrayRef = Arc::new(TimestampSecondArray::from(vec![0]).with_timezone("+05:00"));
    assert_eq!(
        json(&zoned, None),
        some(&[r#""1970-01-01T05:00:00+05:00""#])
    );
}

#[test]
fn structs_become_objects_of_every_field_and_lists_arrays() {
    let fields = Fields::from(vec![
        Field::new("y", LogicalType::Binary, true).to_arrow(),
        Field::new("z", LogicalType::Json, true).to_arrow(),
        Field::new("q\"", LogicalType::Int64, true).to_arrow(),
    ]);
    let structs: ArrayRef = Arc::new(StructArray::new(
        fields,
        vec![
            Arc::new(BinaryArray::from_iter_values([[1_u8].as_slice(), &[2]])) as ArrayRef,
            Arc::new(StringArray::from(vec!["[1]", "null"])),
            Arc::new(Int64Array::from(vec![None, Some(3)])),
        ],
        Some(vec![true, false].into()),
    ));
    assert_eq!(
        json(&structs, None),
        [Some(r#"{"y":"01","z":[1],"q\"":null}"#.to_owned()), None]
    );
    let lists: ArrayRef = Arc::new(ListArray::from_iter_primitive::<
        arrow_array::types::Int64Type,
        _,
        _,
    >(vec![Some(vec![Some(1), None]), None, Some(vec![])]));
    assert_eq!(
        json(&lists, None),
        [Some("[1,null]".to_owned()), None, Some("[]".to_owned())]
    );
}

#[test]
fn a_type_no_json_text_holds_is_refused() {
    let months: ArrayRef = Arc::new(IntervalYearMonthArray::from(vec![1]));
    let field = ArrowField::new("v", months.data_type().clone(), true);
    let refused = rendered(&months, &field).unwrap_err();
    assert_eq!(code(&refused), Some("type_unconvertible"));
}

/// A fold's bound of `shapes` batches.
fn bound(shapes: usize) -> std::num::NonZeroUsize {
    std::num::NonZeroUsize::new(shapes).unwrap()
}

/// A text field naming `logical`, the type its values were lowered from.
fn lowered(logical: &LogicalType) -> ArrowField {
    let named = serde_json::to_string(logical).unwrap();
    ArrowField::new("v", DataType::Utf8, true)
        .with_metadata([(crate::LOGICAL_TYPE_KEY.to_owned(), named)].into())
}

/// How many encodings of text [`encodings`] gives.
const ENCODINGS: usize = 5;

/// `texts` in each encoding of text (plain; under a dictionary of `Int8` keys, of `Utf8` and of
/// `LargeUtf8` values; and run-end encoded, over `Utf8` and `Utf8View` values), each with a field
/// naming `logical`, the type its text was lowered from.
fn encodings(texts: &[&str], logical: &LogicalType) -> [(ArrowField, ArrayRef); ENCODINGS] {
    let count = texts.len();
    let keys = || Int8Array::from_iter_values(0..i8::try_from(count).unwrap());
    let runs = |values: &dyn Array| -> ArrayRef {
        let ends = arrow_array::Int32Array::from_iter_values(1..=i32::try_from(count).unwrap());
        Arc::new(arrow_array::RunArray::try_new(&ends, values).unwrap())
    };
    let plain = || StringArray::from(texts.to_vec());
    let arrays: [ArrayRef; ENCODINGS] = [
        Arc::new(plain()),
        Arc::new(arrow_array::DictionaryArray::try_new(keys(), Arc::new(plain())).unwrap()),
        Arc::new(
            arrow_array::DictionaryArray::try_new(
                keys(),
                Arc::new(arrow_array::LargeStringArray::from(texts.to_vec())),
            )
            .unwrap(),
        ),
        runs(&plain()),
        runs(&arrow_array::StringViewArray::from(texts.to_vec())),
    ];
    arrays.map(|array| {
        let field = lowered(logical).with_data_type(array.data_type().clone());
        (field, array)
    })
}

#[test]
fn text_lowered_from_a_type_becomes_the_json_of_a_value_of_that_type() {
    use crate::TimeUnit;
    let cases = [
        (LogicalType::Int16, "4450", "4450"),
        (LogicalType::Bool, "true", "true"),
        (
            LogicalType::Decimal(crate::DecimalType::new(7, 2).unwrap()),
            "123.40",
            "123.40",
        ),
        (LogicalType::Float64, "1e300", "1e+300"),
        (LogicalType::Float64, "NaN", r#""NaN""#),
        (
            LogicalType::Timestamp(TimeUnit::Microsecond, None),
            "1970-01-01T00:00:00",
            r#""1970-01-01T00:00:00""#,
        ),
        (
            LogicalType::Uuid,
            "00010203-0405-0607-0809-0a0b0c0d0e0f",
            r#""00010203-0405-0607-0809-0a0b0c0d0e0f""#,
        ),
        (LogicalType::Json, r#"{"x":[1]}"#, r#"{"x":[1]}"#),
    ];
    for (logical, text, json) in cases {
        let array: ArrayRef = Arc::new(StringArray::from(vec![Some(text), None]));
        let rendered = rendered(&array, &lowered(&logical)).unwrap();
        let rendered = rendered.as_string::<i32>();
        assert_eq!(rendered.value(0), json, "{logical:?}");
        assert!(rendered.is_null(1));
    }
}

#[test]
fn a_merged_row_keeps_the_type_its_text_was_lowered_from_whatever_encodes_the_text() {
    use super::super::{merge_sparse, read_back};
    let key = crate::MergeKey {
        columns: vec!["id".into()],
        seq: "seq".into(),
        root: None,
        changes: None,
        history: None,
    };
    let schema = Arc::new(arrow_schema::Schema::new(vec![
        ArrowField::new("id", DataType::Int64, false),
        ArrowField::new("n", DataType::Utf8, true),
        ArrowField::new("seq", DataType::Binary, false),
    ]));
    let widened = Arc::new(arrow_schema::Schema::new(vec![
        ArrowField::new("id", DataType::Int64, false),
        Field::new("n", LogicalType::Json, true).to_arrow(),
        ArrowField::new("seq", DataType::Binary, false),
    ]));
    for (field, texts) in encodings(&["4450"], &LogicalType::Int16) {
        let kind = field.data_type().clone();
        let written = arrow_array::RecordBatch::try_new(
            Arc::new(arrow_schema::Schema::new(vec![
                ArrowField::new("id", DataType::Int64, false),
                field.with_name("n"),
                ArrowField::new("seq", DataType::Binary, false),
            ])),
            vec![
                Arc::new(Int64Array::from(vec![1])),
                texts,
                Arc::new(BinaryArray::from_iter_values([[0_u8; 16]])),
            ],
        )
        .unwrap();
        let merged = merge_sparse(&schema, &[], &[], &[written], &key).unwrap();
        let read = read_back(&widened, &merged.rows).unwrap();
        let held = read[0]
            .column_by_name("n")
            .unwrap()
            .as_string::<i32>()
            .value(0)
            .to_owned();
        assert_eq!(held, "4450", "the JSON number, from {kind}");
    }
}

#[test]
fn only_text_names_the_type_it_was_lowered_from_whatever_encodes_it() {
    use super::{holds_text, rendered_from};
    for (field, _) in encodings(&["1"], &LogicalType::Int16) {
        assert!(holds_text(&field), "{field:?}");
        assert!(rendered_from(&field).is_some(), "{field:?}");
        // JSON is no text, however it is stored.
        let json = Field::carrying_extension(field, &LogicalType::Json);
        assert!(!holds_text(&json), "{json:?}");
        assert!(rendered_from(&json).is_none(), "{json:?}");
    }
    let numbers = lowered(&LogicalType::Int16).with_data_type(DataType::Dictionary(
        Box::new(DataType::Int8),
        Box::new(DataType::Int64),
    ));
    assert!(!holds_text(&numbers));
    assert!(rendered_from(&numbers).is_none());
}

/// The schema of the folding tests' table: an id and a text column.
fn folding() -> arrow_schema::SchemaRef {
    Arc::new(arrow_schema::Schema::new(vec![
        ArrowField::new("id", DataType::Int64, false),
        ArrowField::new("n", DataType::Utf8, true),
    ]))
}

/// Rows of `ids`, each holding its id as text rendered from `logical`.
fn rows(ids: Vec<i64>, logical: &LogicalType) -> arrow_array::RecordBatch {
    let texts: Vec<String> = ids.iter().map(ToString::to_string).collect();
    arrow_array::RecordBatch::try_new(
        Arc::new(arrow_schema::Schema::new(vec![
            ArrowField::new("id", DataType::Int64, false),
            lowered(logical).with_name("n"),
        ])),
        vec![
            Arc::new(Int64Array::from(ids)),
            Arc::new(StringArray::from(texts)),
        ],
    )
    .unwrap()
}

/// The type the text of `batch`'s column `n` names.
fn named(batch: &arrow_array::RecordBatch) -> Option<LogicalType> {
    let field = batch.schema_ref().field_with_name("n").unwrap().clone();
    Field::lowered_from(&field)
}

/// A row of `id` whose column `n` holds its id as text lowered from `logical`, in the
/// `encoding`th of [`encodings`].
fn encoded_row(id: i64, logical: &LogicalType, encoding: usize) -> arrow_array::RecordBatch {
    let text = id.to_string();
    let (field, texts) = encodings(&[text.as_str()], logical)
        .into_iter()
        .nth(encoding)
        .expect("an encoding of that number");
    arrow_array::RecordBatch::try_new(
        Arc::new(arrow_schema::Schema::new(vec![
            ArrowField::new("id", DataType::Int64, false),
            field.with_name("n"),
        ])),
        vec![Arc::new(Int64Array::from(vec![id])), texts],
    )
    .unwrap()
}

/// Every row of `batches`, read back under the folding tests' table with `n` widened to `Json`:
/// its id and the JSON text of its `n`, by id.
fn read_as_json(batches: &[arrow_array::RecordBatch]) -> Vec<(i64, String)> {
    let widened = Arc::new(arrow_schema::Schema::new(vec![
        ArrowField::new("id", DataType::Int64, false),
        Field::new("n", LogicalType::Json, true).to_arrow(),
    ]));
    let read = super::super::read_back(&widened, batches).unwrap();
    let mut rows = Vec::new();
    for batch in &read {
        let ids = batch
            .column(0)
            .as_primitive::<arrow_array::types::Int64Type>();
        let texts = batch.column(1).as_string::<i32>();
        for row in 0..batch.num_rows() {
            rows.push((ids.value(row), texts.value(row).to_owned()));
        }
    }
    rows.sort_unstable();
    rows
}

#[test]
fn a_fold_keeps_the_type_encoded_text_was_lowered_from() {
    use super::super::folded;
    let int = LogicalType::Int16;
    for encoding in 0..ENCODINGS {
        let batches: Vec<_> = (1..=3).map(|id| encoded_row(id, &int, encoding)).collect();
        // One batch stays whole, and the other two join.
        let joined = folded(&folding(), batches, bound(2), 1 << 20).unwrap();
        assert_eq!(joined.len(), 2, "encoding {encoding}");
        assert!(
            joined.iter().all(|batch| named(batch) == Some(int.clone())),
            "encoding {encoding}"
        );
        let expected: Vec<_> = (1..=3).map(|id| (id, id.to_string())).collect();
        assert_eq!(read_as_json(&joined), expected, "encoding {encoding}");
    }
}

#[test]
fn a_fold_joins_rows_naming_one_type_and_keeps_those_naming_others_apart() {
    use super::super::folded;
    let schema = folding();
    let (int, bool) = (LogicalType::Int16, LogicalType::Bool);
    let alike = vec![
        rows(vec![1, 2], &int),
        rows(vec![3], &bool),
        rows(vec![4], &bool),
    ];
    let joined = folded(&schema, alike, bound(2), 1 << 20).unwrap();
    assert_eq!(joined.len(), 2);
    assert_eq!(joined[1].num_rows(), 2);
    assert_eq!(named(&joined[1]), Some(bool.clone()));
    let apart = vec![
        rows(vec![1, 2], &int),
        rows(vec![3], &int),
        rows(vec![4], &bool),
    ];
    let kept = folded(&schema, apart, bound(2), 1 << 20).unwrap();
    let mut kinds: Vec<_> = kept
        .iter()
        .map(|batch| (named(batch), batch.num_rows()))
        .collect();
    kinds.sort_by_key(|(_, rows)| *rows);
    assert_eq!(kinds, [(Some(bool.clone()), 1), (Some(int.clone()), 3)]);
}

#[test]
fn encoded_text_and_lists_become_the_json_of_their_values() {
    use arrow_array::{LargeListArray, LargeStringArray, RunArray, StringViewArray};
    let large: ArrayRef = Arc::new(LargeStringArray::from(vec!["a"]));
    assert_eq!(json(&large, None), some(&[r#""a""#]));
    let view: ArrayRef = Arc::new(StringViewArray::from(vec!["b"]));
    assert_eq!(json(&view, None), some(&[r#""b""#]));
    let runs: ArrayRef = Arc::new(
        RunArray::try_new(
            &arrow_array::Int32Array::from(vec![2]),
            &StringArray::from(vec!["c"]),
        )
        .unwrap(),
    );
    assert_eq!(json(&runs, None), some(&[r#""c""#, r#""c""#]));
    let lists: ArrayRef = Arc::new(LargeListArray::from_iter_primitive::<
        arrow_array::types::Int64Type,
        _,
        _,
    >(vec![Some(vec![Some(1), None])]));
    assert_eq!(json(&lists, None), some(&["[1,null]"]));
}

#[test]
fn a_value_that_became_json_names_no_type_nor_does_one_already_json() {
    use super::super::merge_sparse;
    let key = crate::MergeKey {
        columns: vec!["id".into()],
        seq: "seq".into(),
        root: None,
        changes: None,
        history: None,
    };
    let row = |field: ArrowField, text: &str| {
        arrow_array::RecordBatch::try_new(
            Arc::new(arrow_schema::Schema::new(vec![
                ArrowField::new("id", DataType::Int64, false),
                field,
                ArrowField::new("seq", DataType::Binary, false),
            ])),
            vec![
                Arc::new(Int64Array::from(vec![1])),
                Arc::new(StringArray::from(vec![text])),
                Arc::new(BinaryArray::from_iter_values([[0_u8; 16]])),
            ],
        )
        .unwrap()
    };
    let schema = Arc::new(arrow_schema::Schema::new(vec![
        ArrowField::new("id", DataType::Int64, false),
        Field::new("n", LogicalType::Json, true).to_arrow(),
        ArrowField::new("seq", DataType::Binary, false),
    ]));
    let named = |field: ArrowField| {
        let merged = merge_sparse(&schema, &[], &[], &[row(field, "4450")], &key).unwrap();
        let field = merged.rows[0]
            .schema_ref()
            .field_with_name("n")
            .unwrap()
            .clone();
        Field::lowered_from(&field)
    };
    assert_eq!(named(lowered(&LogicalType::Int16).with_name("n")), None);
    let fields = vec![Field::new("x", LogicalType::Int64, true)];
    let nested = LogicalType::Struct(fields.try_into().unwrap());
    let json = Field::new("n", LogicalType::Json, true).to_arrow();
    let mut metadata = json.metadata().clone();
    metadata.insert(
        crate::LOGICAL_TYPE_KEY.to_owned(),
        serde_json::to_string(&nested).unwrap(),
    );
    assert_eq!(named(json.with_metadata(metadata)), None);
}

#[test]
fn batches_alternating_between_two_names_fold_into_a_group_a_name_within_the_bound() {
    use super::super::folded;
    let (int, bool) = (LogicalType::Int16, LogicalType::Bool);
    let alternating: Vec<_> = (0..40)
        .map(|id| {
            let logical = if id % 2 == 0 { &int } else { &bool };
            encoded_row(id, logical, usize::try_from(id).unwrap() % ENCODINGS)
        })
        .collect();
    let bounded = folded(&folding(), alternating, bound(4), 1 << 20).unwrap();
    // Two of the largest stay whole, and a group a name holds the others: the bound exactly.
    assert_eq!(bounded.len(), 4);
    let held: usize = bounded.iter().map(arrow_array::RecordBatch::num_rows).sum();
    assert_eq!(held, 40);
    for batch in &bounded {
        let ids = batch
            .column(0)
            .as_primitive::<arrow_array::types::Int64Type>();
        let even = ids.value(0) % 2 == 0;
        assert!(ids.iter().all(|id| (id.unwrap() % 2 == 0) == even));
        assert_eq!(
            named(batch),
            Some(if even { int.clone() } else { bool.clone() })
        );
    }
}

#[test]
fn a_fold_keeps_whole_as_many_of_the_largest_batches_as_leave_a_group_a_name() {
    use super::super::folded;
    let kinds = [LogicalType::Int16, LogicalType::Bool, LogicalType::Float64];
    let batches: Vec<_> = (0..60)
        .map(|id| rows(vec![id], &kinds[usize::try_from(id % 3).unwrap()]))
        .collect();
    let folded = folded(&folding(), batches, bound(6), 1 << 20).unwrap();
    // Three stay whole and three groups, one a name, hold the other fifty-seven rows.
    assert_eq!(folded.len(), 6);
    let mut sizes: Vec<usize> = folded
        .iter()
        .map(arrow_array::RecordBatch::num_rows)
        .collect();
    sizes.sort_unstable();
    assert_eq!(sizes, [1, 1, 1, 19, 19, 19]);
}

#[test]
fn a_fold_of_more_names_than_its_bound_gives_back_a_group_a_name() {
    use super::super::folded;
    let kinds = [LogicalType::Int16, LogicalType::Bool, LogicalType::Float64];
    let batches: Vec<_> = (0..9)
        .map(|id| rows(vec![id], &kinds[usize::try_from(id % 3).unwrap()]))
        .collect();
    let folded = folded(&folding(), batches, bound(2), 1 << 20).unwrap();
    assert_eq!(folded.len(), 3);
    assert!(folded.iter().all(|batch| batch.num_rows() == 3));
}
