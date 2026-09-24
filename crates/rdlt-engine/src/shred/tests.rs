use std::num::NonZeroUsize;
use std::sync::Arc;

use arrow_array::cast::AsArray;
use arrow_array::types::{Decimal128Type, Float64Type, Int64Type};
use arrow_array::{Array, RecordBatch};
use bytes::Bytes;
use rdlt_connector::limits::{MAX_COLUMNS, MAX_NESTING_DEPTH};
use rdlt_connector::{DecimalType, Field, LogicalType, TableSchema};

use super::differential::shredded;
use super::reference::Code;
use super::{Records, chunks, parse, shred};
use crate::compute::RayonPool;

/// The batch `pushes` shred into, in chunks of `chunk_bytes`.
fn batch_of(pushes: &[&str], chunk_bytes: usize) -> RecordBatch {
    let pushes: Vec<Bytes> = pushes
        .iter()
        .map(|push| Bytes::from(push.to_string()))
        .collect();
    shredded(&pushes, chunk_bytes).unwrap().expect("some rows")
}

/// The code `push` fails to shred with.
fn refused(push: &str) -> &'static str {
    shredded(&[Bytes::from(push.to_owned())], 1 << 20)
        .unwrap_err()
        .0
}

/// The logical type of each column of `batch`.
fn types(batch: &RecordBatch) -> Vec<(String, LogicalType)> {
    TableSchema::from_arrow(&batch.schema())
        .unwrap()
        .fields()
        .iter()
        .map(|field| (field.name().to_string(), field.logical_type().clone()))
        .collect()
}

fn texts(batch: &RecordBatch, column: usize) -> Vec<Option<String>> {
    batch
        .column(column)
        .as_string::<i32>()
        .iter()
        .map(|text| text.map(str::to_owned))
        .collect()
}

#[test]
fn json_lines_and_arrays_hold_the_same_records() {
    let lines = batch_of(&["{\"a\":1}\r\n\n  {\"a\":2}\n"], 1 << 20);
    let array = batch_of(&[" [ {\"a\":1} ,\n{\"a\":2} ] "], 1 << 20);
    assert_eq!(lines, array);
    assert_eq!(lines.num_rows(), 2);
}

#[test]
fn pushes_with_no_records_shred_to_nothing() {
    for push in ["", " \n\n", "[]", " [ ] "] {
        assert_eq!(
            shredded(&[Bytes::from(push)], 1 << 20),
            Ok(None),
            "{push:?}"
        );
    }
}

#[test]
fn brackets_and_commas_inside_strings_do_not_split_an_array() {
    let batch = batch_of(&[r#"[{"a":"],[{\"x\":1},"},{"a":"\\"}]"#], 1 << 20);
    assert_eq!(
        texts(&batch, 0),
        [Some("],[{\"x\":1},".to_owned()), Some("\\".to_owned())]
    );
}

#[test]
fn malformed_pushes_are_invalid_json() {
    for push in [
        "[{\"a\":1},]",
        "[,{\"a\":1}]",
        "[{\"a\":1}",
        "[{\"a\":1}] {}",
        "[{\"a\":1} {\"a\":2}]",
        "{\"a\":1} {\"a\":2}",
        "{\"a\":[1\n2]}",
        "{\"a\":\"\u{1}\"}",
        "{\"a\":1e400}",
        "[{\"a\":1}]}",
    ] {
        assert_eq!(refused(push), "json_invalid", "{push:?}");
    }
    let invalid_utf8 = Bytes::from_static(b"{\"a\":\"\xff\"}");
    assert_eq!(
        shredded(&[invalid_utf8], 1 << 20),
        Err(Code("json_invalid"))
    );
}

#[test]
fn records_that_are_not_objects_are_refused() {
    for push in [
        "null",
        "true",
        "-1",
        "1",
        "1.5",
        "\"a\"",
        "[1]",
        "[{\"a\":1},[]]",
    ] {
        assert_eq!(refused(push), "json_not_object", "{push:?}");
    }
}

#[test]
fn a_repeated_key_is_refused_at_any_depth_and_however_it_is_escaped() {
    // `a` as the unicode escape of code point 0x61, built with an explicit backslash so no
    // tool decodes it on the way in.
    let escaped = format!("{}u0061", '\\');
    for push in [
        r#"{"a":1,"a":2}"#.to_owned(),
        format!(r#"{{"a":1,"{escaped}":2}}"#),
        r#"{"o":{"b":1,"b":1}}"#.to_owned(),
        format!(r#"{{"o":{{"a":1,"{escaped}":1}}}}"#),
        r#"{"l":[{"b":1,"b":1}]}"#.to_owned(),
        "{\"j\":1}\n{\"j\":{\"b\":1,\"b\":1}}".to_owned(),
        format!("{{\"j\":1}}\n{{\"j\":{{\"a\":1,\"{escaped}\":1}}}}"),
    ] {
        assert!(push.is_ascii(), "{push}");
        assert_eq!(refused(&push), "json_duplicate_key", "{push:?}");
    }
}

/// A record whose value `a` nests `levels` levels deep, the record counting as the first.
fn nested(levels: u64, open: &str, close: &str) -> String {
    let inner = levels - 1;
    format!(
        "{{\"a\":{}1{}}}",
        open.repeat(usize::try_from(inner - 1).unwrap()),
        close.repeat(usize::try_from(inner - 1).unwrap())
    )
}

#[test]
fn values_nest_up_to_the_limit_and_no_deeper() {
    // After an integer, the column stops building and only checks the deep values' nesting.
    for before in ["", "{\"a\":1}\n"] {
        for (open, close) in [("[", "]"), ("{\"b\":", "}")] {
            let deepest = format!("{before}{}", nested(MAX_NESTING_DEPTH, open, close));
            assert_eq!(
                batch_of(&[&deepest], 1 << 20).num_rows(),
                1 + before.len().min(1)
            );
            let deeper = format!("{before}{}", nested(MAX_NESTING_DEPTH + 1, open, close));
            assert_eq!(refused(&deeper), "limit_exceeded", "{before:?} {open}");
        }
    }
}

#[test]
fn a_value_nested_far_past_the_limit_is_refused_without_exhausting_the_stack() {
    let deep = format!(
        "[{{\"a\":{}{}}}]",
        "[".repeat(1_000_000),
        "]".repeat(1_000_000)
    );
    assert_eq!(refused(&deep), "limit_exceeded");
}

#[test]
fn records_with_more_columns_than_the_limit_are_refused() {
    let columns = usize::try_from(MAX_COLUMNS).unwrap();
    let record = |count: usize| {
        let fields: Vec<String> = (0..count).map(|index| format!("\"c{index}\":1")).collect();
        format!("{{{}}}", fields.join(","))
    };
    assert_eq!(
        batch_of(&[&record(columns)], 1 << 20).num_columns(),
        columns
    );
    assert_eq!(refused(&record(columns + 1)), "limit_exceeded");
}

#[test]
fn columns_take_the_narrowest_type_every_value_fits() {
    let batch = batch_of(
        &[
            r#"{"int":1,"float":1,"wide":1,"text":"x","mixed":1,"none":null,"flag":true}
{"int":-2,"float":2.5,"wide":18446744073709551615,"text":"y","mixed":"1","flag":false}"#,
        ],
        1 << 20,
    );
    let decimal = LogicalType::Decimal(DecimalType::new(20, 0).unwrap());
    assert_eq!(
        types(&batch),
        [
            ("int".to_owned(), LogicalType::Int64),
            ("float".to_owned(), LogicalType::Float64),
            ("wide".to_owned(), decimal),
            ("text".to_owned(), LogicalType::Utf8),
            ("mixed".to_owned(), LogicalType::Json),
            ("none".to_owned(), LogicalType::Null),
            ("flag".to_owned(), LogicalType::Bool),
        ]
    );
    assert_eq!(
        batch.column(1).as_primitive::<Float64Type>().values(),
        &[1.0, 2.5]
    );
    assert_eq!(
        batch.column(2).as_primitive::<Decimal128Type>().values(),
        &[1, i128::from(u64::MAX)]
    );
    assert_eq!(
        texts(&batch, 4),
        [Some("1".to_owned()), Some("\"1\"".to_owned())]
    );
}

#[test]
fn integers_a_float_cannot_hold_exactly_keep_a_column_of_integers_and_floats_as_json() {
    let batch = batch_of(&["{\"a\":9007199254740993}\n{\"a\":0.5}"], 1 << 20);
    assert_eq!(types(&batch)[0].1, LogicalType::Json);
    let after_exact = batch_of(
        &["{\"a\":1}\n{\"a\":9007199254740993}\n{\"a\":0.5}"],
        1 << 20,
    );
    assert_eq!(types(&after_exact)[0].1, LogicalType::Json);
    let across_chunks = batch_of(&["{\"a\":1}", "{\"a\":9007199254740993}", "{\"a\":0.5}"], 1);
    assert_eq!(types(&across_chunks)[0].1, LogicalType::Json);
    assert_eq!(
        texts(&batch, 0),
        [Some("9007199254740993".to_owned()), Some("0.5".to_owned())]
    );
    let exact = batch_of(&["{\"a\":9007199254740992}\n{\"a\":0.5}"], 1 << 20);
    assert_eq!(types(&exact)[0].1, LogicalType::Float64);
}

#[test]
fn integers_beyond_the_unsigned_range_read_as_floats_and_negative_zero_as_zero() {
    let batch = batch_of(
        &["{\"big\":123456789012345678901234567890,\"zero\":-0.0}"],
        1 << 20,
    );
    assert_eq!(types(&batch)[0].1, LogicalType::Float64);
    assert_eq!(
        batch
            .column(0)
            .as_primitive::<Float64Type>()
            .value(0)
            .to_bits(),
        1.234_567_890_123_456_8e29_f64.to_bits()
    );
    assert_eq!(
        batch
            .column(1)
            .as_primitive::<Float64Type>()
            .value(0)
            .to_bits(),
        0.0_f64.to_bits()
    );
}

#[test]
fn nested_objects_and_arrays_become_structs_and_lists_with_their_items_joined() {
    let batch = batch_of(
        &[r#"{"o":{"x":1,"l":[1,2.5]},"e":{}}
{"o":{"y":"t","l":[]},"e":{}}"#],
        1 << 20,
    );
    let items = Field::new("item", LogicalType::Float64, true);
    let o = LogicalType::Struct(
        rdlt_connector::Fields::new(vec![
            Field::new("x", LogicalType::Int64, true),
            Field::new("l", LogicalType::List(Box::new(items)), true),
            Field::new("y", LogicalType::Utf8, true),
        ])
        .unwrap(),
    );
    let e = LogicalType::Struct(rdlt_connector::Fields::new(Vec::new()).unwrap());
    assert_eq!(types(&batch), [("o".to_owned(), o), ("e".to_owned(), e)]);
    let o = batch.column(0).as_struct();
    assert_eq!(
        o.column(0)
            .as_primitive::<Int64Type>()
            .iter()
            .collect::<Vec<_>>(),
        [Some(1), None]
    );
    let lists = o.column(1).as_list::<i32>();
    assert_eq!(lists.value_offsets(), &[0, 2, 2]);
    assert_eq!(
        lists.values().as_primitive::<Float64Type>().values(),
        &[1.0, 2.5]
    );
    assert_eq!(batch.column(1).len(), 2);
}

#[test]
fn a_column_that_changes_type_in_a_later_chunk_is_built_again_as_their_join() {
    let batch = batch_of(&["{\"a\":1}\n{\"a\":2}", "{\"a\":\"x\",\"b\":true}"], 1);
    assert_eq!(types(&batch)[0].1, LogicalType::Json);
    assert_eq!(
        texts(&batch, 0),
        [
            Some("1".to_owned()),
            Some("2".to_owned()),
            Some("\"x\"".to_owned())
        ]
    );
    assert_eq!(
        batch.column(1).as_boolean().iter().collect::<Vec<_>>(),
        [None, None, Some(true)]
    );
}

#[test]
fn a_column_that_changes_type_within_a_chunk_is_built_again_as_their_join() {
    let batch = batch_of(&["{\"a\":[1]}\n{\"a\":{\"b\":1}}\n{\"a\":2}"], 1 << 20);
    assert_eq!(types(&batch)[0].1, LogicalType::Json);
    assert_eq!(
        texts(&batch, 0),
        [
            Some("[1]".to_owned()),
            Some("{\"b\":1}".to_owned()),
            Some("2".to_owned())
        ]
    );
}

#[tokio::test]
async fn parallel_shredding_keeps_the_pushes_order() {
    let pool = RayonPool::new(NonZeroUsize::new(4).unwrap()).unwrap();
    let pushes: Vec<Bytes> = (0..40)
        .map(|push| {
            let lines: Vec<String> = (0..50)
                .map(|row| format!("{{\"n\":{}}}", push * 50 + row))
                .collect();
            Bytes::from(lines.join("\n"))
        })
        .collect();
    let batches = shred(&pool, &pushes, 256).await.unwrap();
    assert!(batches.len() > 40);
    let numbers: Vec<i64> = batches
        .iter()
        .flat_map(|batch| {
            batch
                .column(0)
                .as_primitive::<Int64Type>()
                .values()
                .to_vec()
        })
        .collect();
    assert_eq!(numbers, (0..2000).collect::<Vec<_>>());
    assert!(
        batches
            .iter()
            .all(|batch| batch.schema() == Arc::clone(&batches[0].schema()))
    );
}

#[test]
fn strings_with_lone_surrogates_or_bad_escapes_are_invalid_json() {
    for push in [
        r#"{"a":"\ud800"}"#,
        r#"{"a":"\udc00x"}"#,
        r#"{"a":"\x"}"#,
        r#"{"a":"\u12"}"#,
    ] {
        assert_eq!(refused(push), "json_invalid", "{push:?}");
    }
    // The escapes are built with an explicit backslash so no tool decodes them on the way in.
    let pair = format!(r#"{{"a":"{0}ud83d{0}ude00"}}"#, '\\');
    assert!(pair.is_ascii(), "{pair}");
    assert_eq!(
        texts(&batch_of(&[&pair], 1 << 20), 0),
        [Some("\u{1f600}".to_owned())]
    );
}

#[test]
fn rows_repeating_their_keys_order_find_every_key_without_a_search() {
    let lines: Vec<String> = (0..100)
        .map(|n| format!(r#"{{"a":{n},"b":"x","c":{{"d":{n}}}}}"#))
        .collect();
    let records = Records::of(Bytes::from(lines.join("\n"))).unwrap();
    let parsed = parse(chunks(&[records], 1 << 20).remove(0)).unwrap();
    // Only the first row searches, for the keys it adds.
    assert_eq!(parsed.record.searches(), 3);
}

#[test]
fn every_kind_keeps_its_type_across_chunks() {
    let rows = r#"{"b":true,"t":"x","i":1,"w":18446744073709551615,"f":0.5,"n":null,"o":{"k":1},"l":[1],"j":1}"#;
    let again = r#"{"b":false,"t":"y","i":2,"w":1,"f":1.5,"n":null,"o":{"k":2},"l":[2],"j":"x"}"#;
    let batch = batch_of(&[rows, again], 1);
    let kinds: Vec<LogicalType> = types(&batch)
        .into_iter()
        .map(|(_, logical)| logical)
        .collect();
    let item = Field::new("item", LogicalType::Int64, true);
    let object =
        rdlt_connector::Fields::new(vec![Field::new("k", LogicalType::Int64, true)]).unwrap();
    assert_eq!(
        kinds,
        [
            LogicalType::Bool,
            LogicalType::Utf8,
            LogicalType::Int64,
            LogicalType::Decimal(DecimalType::new(20, 0).unwrap()),
            LogicalType::Float64,
            LogicalType::Null,
            LogicalType::Struct(object),
            LogicalType::List(Box::new(item)),
            LogicalType::Json,
        ]
    );
}

#[test]
fn only_a_chunk_whose_columns_stopped_building_is_parsed_again() {
    let parsed = |text: &str| {
        let records = Records::of(Bytes::from(text.to_owned())).unwrap();
        parse(chunks(&[records], 1 << 20).remove(0)).unwrap()
    };
    assert!(!parsed("{\"a\":1}\n{\"a\":2.5}\n{\"b\":[1]}").spoiled);
    assert!(parsed("{\"a\":1}\n{\"a\":\"x\"}").spoiled);
    assert!(parsed("{\"a\":{\"b\":true}}\n{\"a\":{\"b\":[]}}").spoiled);
}

#[test]
fn each_pair_of_kinds_joins_as_the_lattice_says_within_and_across_chunks() {
    let decimal = LogicalType::Decimal(DecimalType::new(20, 0).unwrap());
    let inexact = "9007199254740993";
    let wide = "18446744073709551615";
    let cases = [
        ("null", "1", LogicalType::Int64),
        ("1", "0.5", LogicalType::Float64),
        ("0.5", "1", LogicalType::Float64),
        (inexact, "0.5", LogicalType::Json),
        ("0.5", inexact, LogicalType::Json),
        ("1", wide, decimal.clone()),
        (wide, "1", decimal),
        (wide, "0.5", LogicalType::Json),
        ("0.5", wide, LogicalType::Json),
        ("true", "1", LogicalType::Json),
        ("\"x\"", "1", LogicalType::Json),
        ("1", "\"x\"", LogicalType::Json),
        ("{}", "1", LogicalType::Json),
        ("1", "{}", LogicalType::Json),
        ("[]", "1", LogicalType::Json),
        ("1", "[]", LogicalType::Json),
        ("[]", "{}", LogicalType::Json),
        ("{}", "[]", LogicalType::Json),
    ];
    for (first, second, joined) in cases {
        let (first, second) = (format!("{{\"a\":{first}}}"), format!("{{\"a\":{second}}}"));
        let within = batch_of(&[&format!("{first}\n{second}")], 1 << 20);
        assert_eq!(
            types(&within)[0].1,
            joined,
            "{first} then {second} in one chunk"
        );
        let across = batch_of(&[&first, &second], 1);
        assert_eq!(
            types(&across)[0].1,
            joined,
            "{first} then {second} in two chunks"
        );
    }
}

/// An object of `count` distinct keys from `first` on, each holding 1.
fn wide_object(first: usize, count: usize) -> String {
    let fields: Vec<String> = (first..first + count)
        .map(|index| format!("\"c{index}\":1"))
        .collect();
    format!("{{{}}}", fields.join(","))
}

#[test]
fn a_record_over_the_column_limit_is_refused_as_soon_as_it_is_read() {
    let columns = usize::try_from(MAX_COLUMNS).unwrap();
    // The invalid record after it is never reached.
    let push = format!("{}\n{{\"a\":", wide_object(0, columns + 1));
    assert_eq!(refused(&push), "limit_exceeded");
}

#[test]
fn nested_objects_are_bound_by_the_column_limit_too() {
    let columns = usize::try_from(MAX_COLUMNS).unwrap();
    let nested = format!("{{\"o\":{}}}", wide_object(0, columns + 1));
    assert_eq!(refused(&nested), "limit_exceeded");
    let half = columns / 2 + 1;
    let first = format!("{{\"o\":{}}}", wide_object(0, half));
    let second = format!("{{\"o\":{}}}", wide_object(half, half));
    let pushes = [Bytes::from(first), Bytes::from(second)];
    assert_eq!(shredded(&pushes, 1).unwrap_err(), Code("limit_exceeded"));
    let fits = format!("{{\"o\":{}}}", wide_object(0, columns));
    assert_eq!(batch_of(&[&fits], 1 << 20).num_rows(), 1);
}

#[test]
fn sparse_wide_records_are_refused_before_they_are_built() {
    // Over five thousand rows under almost ten thousand columns: fifty million cells, from 114 KB.
    let push = format!("{}{}", "{}\n".repeat(5000), wide_object(0, 9999));
    assert_eq!(refused(&push), "limit_exceeded");
}

#[test]
fn cells_are_bounded_at_the_limit() {
    assert!(super::within_cells(1 << 12, 1 << 13));
    assert!(!super::within_cells(1 << 12, (1 << 13) + 1));
    assert!(!super::within_cells((1 << 12) + 1, 1 << 13));
    assert!(super::within_cells(u64::MAX, 0));
    assert!(!super::within_cells(u64::MAX, 2));
}

#[test]
fn only_columns_holding_values_count_toward_the_cells() {
    let shape = |text: &str| {
        let records = Records::of(Bytes::from(text.to_owned())).unwrap();
        parse(chunks(&[records], 1 << 20).remove(0)).unwrap().shape
    };
    assert_eq!(
        shape(r#"{"a":1,"b":null,"c":{"d":"x","e":null},"l":[true],"x":{}}"#).leaves(),
        3
    );
}

#[test]
fn chunks_that_lack_columns_or_hold_them_in_another_order_are_fitted_without_parsing_again() {
    let parsed = |text: &str| {
        let records = Records::of(Bytes::from(text.to_owned())).unwrap();
        parse(chunks(&[records], 1 << 20).remove(0)).unwrap()
    };
    let chunks = [
        parsed(r#"{"a":1,"o":{"x":1}}"#),
        parsed(r#"{"b":"t","a":2.5,"l":[]}"#),
        parsed(r#"{"a":null,"o":{"y":true,"x":2},"l":[1]}"#),
        parsed(r#"{"a":"x"}"#),
    ];
    let joined = super::join(&chunks[..3]).unwrap();
    for chunk in &chunks[..3] {
        assert!(!chunk.spoiled && super::conform::shape_fits(&chunk.shape, &joined));
    }
    let with_text = super::join(&chunks).unwrap();
    assert!(!super::conform::shape_fits(&chunks[0].shape, &with_text));
}

#[test]
fn fitted_chunks_hold_the_values_the_reference_does() {
    let pushes: Vec<Bytes> = [
        r#"{"a":1,"o":{"x":1},"w":1}"#,
        r#"{"b":"t","a":2.5,"l":[],"w":18446744073709551615}"#,
        r#"{"a":null,"o":{"y":true,"x":2},"l":[1],"n":null}"#,
        r#"{"o":null,"l":[[]],"e":{}}"#,
    ]
    .into_iter()
    .map(Bytes::from)
    .collect();
    let expected = super::reference::shred(&pushes).unwrap();
    let actual = shredded(&pushes, 1).unwrap().unwrap();
    assert_eq!(
        super::differential::normalized(&actual),
        super::differential::normalized(&expected)
    );
}

#[test]
fn a_refusal_names_where_the_json_broke_without_quoting_the_data() {
    let secret = "hunter2-card-4111111111111111";
    let push = format!("{{\"a\":1}}\n{{\"password\":\"{secret}\",\"b\":tru}}");
    let error = crate::compute::ready(shred(
        &crate::compute::Inline,
        &[Bytes::from(push)],
        1 << 20,
    ))
    .unwrap_err();
    let message = error.to_string();
    assert_eq!(error.code(), "json_invalid");
    assert!(!message.contains(secret), "{message}");
    assert!(
        !message.contains('\n') && !message.contains('\t'),
        "{message:?}"
    );
    assert!(message.contains("record 2"), "{message}");
    // Numbered across chunks and pushes alike.
    let pushes = [
        Bytes::from("{\"a\":1}\n{\"a\":2}"),
        Bytes::from("{\"a\":3}\n{\"a\":}"),
    ];
    let later = crate::compute::ready(shred(&crate::compute::Inline, &pushes, 1)).unwrap_err();
    assert!(later.to_string().contains("record 4:"), "{later}");
}
