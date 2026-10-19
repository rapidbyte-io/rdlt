use std::num::NonZeroUsize;
use std::sync::Arc;

use arrow_array::cast::AsArray;
use arrow_array::types::{Decimal128Type, Decimal256Type, Float64Type, Int64Type};
use arrow_array::{Array, RecordBatch};
use bytes::Bytes;
use rdlt_connector::{DecimalType, Field, LogicalType, TableSchema};
use rdlt_wire::limits::{NESTING_DEPTH, SCHEMA_COLUMNS, count};

use super::differential::shredded;
use super::meter::{KEY, OBJECT_SHAPE};
use super::reference::Code;
use super::{Parsed, ShredLimits, chunks, parse, shred};
use crate::compute::Cores;
use crate::report::ShredCounts;

/// The limits of pushes whose records may hold as many columns as the wire's schemas.
pub(crate) fn limits() -> ShredLimits {
    ShredLimits::new(SCHEMA_COLUMNS)
}

/// The first chunk of `text`, parsed with room to build whatever it holds.
fn parsed(text: &str) -> Parsed {
    let records = Bytes::from(text.to_owned());
    let roomy = ShredLimits {
        admitted: 1 << 20,
        ..limits()
    };
    parse(
        chunks(&[records], 1 << 20).unwrap().remove(0),
        roomy,
        &roomy.beyond(),
    )
    .unwrap()
}

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
    // Alone, and after an integer that sends its chunk through the exact parse.
    for invalid_utf8 in [
        &b"{\"a\":\"\xff\"}"[..],
        b"{\"a\":99999999999999999999999999999999999999999}\n{\"a\":\"\xff\"}",
    ] {
        assert_eq!(
            shredded(&[Bytes::from_static(invalid_utf8)], 1 << 20),
            Err(Code("json_invalid"))
        );
    }
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
    // Integers beyond 38 digits, which only the exact parse reads.
    for digits in [39, 400] {
        assert_eq!(refused(&"9".repeat(digits)), "json_not_object", "{digits}");
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
    // After an integer, the column stops building and only checks the deep values' nesting;
    // after an integer beyond 64 bits, the chunk is parsed exactly; after an integer and a
    // string, the column renders as JSON text where it is parsed, fast or exactly.
    let roomy = ShredLimits {
        admitted: 1 << 20,
        ..limits()
    };
    for before in [
        "",
        "{\"a\":1}\n",
        "{\"a\":18446744073709551616}\n",
        "{\"a\":1}\n{\"a\":\"x\"}\n",
        "{\"n\":18446744073709551616}\n{\"a\":1}\n{\"a\":\"x\"}\n",
    ] {
        for (open, close) in [("[", "]"), ("{\"b\":", "}")] {
            let deepest = format!("{before}{}", nested(NESTING_DEPTH, open, close));
            let rows = 1 + before.lines().count();
            assert_eq!(batch_of(&[&deepest], 1 << 20).num_rows(), rows);
            assert_eq!(roomy_batch(&deepest, 1 << 20).1.num_rows(), rows);
            let deeper = format!("{before}{}", nested(NESTING_DEPTH + 1, open, close));
            assert_eq!(refused(&deeper), "limit_exceeded", "{before:?} {open}");
            let pushes = [Bytes::from(deeper)];
            let refused_roomy = super::differential::shredded_within(&pushes, 1 << 20, roomy);
            assert_eq!(
                refused_roomy.unwrap_err().0,
                "limit_exceeded",
                "{before:?} {open}"
            );
        }
        // Far past the limit, as the shredder refuses it before it builds anything.
        let far = format!("{before}{}", nested(200, "{\"b\":", "}"));
        assert_eq!(refused(&far), "limit_exceeded", "{before:?}");
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
fn a_value_nested_past_the_limit_is_refused_even_where_the_chunk_is_parsed_exactly() {
    let depth = usize::try_from(NESTING_DEPTH).unwrap();
    // Each prefix sends the record, or its chunk, through the exact parse.
    let prefixes = [
        format!("{{\"n\":{},\"a\":", "9".repeat(400)),
        "{\"n\":18446744073709551616}\n{\"a\":".to_owned(),
    ];
    for prefix in &prefixes {
        for levels in [depth + 1, 1_000_000] {
            let deep = format!("{prefix}{}{}}}", "[".repeat(levels), "]".repeat(levels));
            assert_eq!(refused(&deep), "limit_exceeded", "{prefix:?} at {levels}");
        }
    }
    // A record is refused for what it holds first: here a number beyond a float's range.
    for levels in [depth + 1, 1_000_000] {
        let invalid = format!(
            "{{\"n\":1e400,\"a\":{}{}}}",
            "[".repeat(levels),
            "]".repeat(levels)
        );
        assert_eq!(refused(&invalid), "json_invalid", "at {levels}");
        let pushes = [Bytes::from(invalid)];
        let reference = super::reference::shred(&pushes).unwrap_err();
        assert_eq!(
            reference,
            Code("json_invalid"),
            "the reference, at {levels}"
        );
    }
    let beside = format!(
        "{{\"n\":18446744073709551616}}\n{{\"a\":{}{}}}",
        "[".repeat(1_000_000),
        "]".repeat(1_000_000)
    );
    assert_eq!(refused(&beside), "limit_exceeded");
    // An empty container at the limit is a value at the limit.
    let levels = depth - 1;
    let at_limit = format!(
        "{{\"n\":18446744073709551616,\"a\":{}{}}}",
        "[".repeat(levels),
        "]".repeat(levels)
    );
    assert_eq!(batch_of(&[&at_limit], 1 << 20).num_rows(), 1);
    // Containers side by side nest no deeper than one.
    let siblings = format!(
        "{{\"n\":18446744073709551616,\"a\":[{}]}}",
        vec!["[]"; 1_000].join(",")
    );
    assert_eq!(batch_of(&[&siblings], 1 << 20).num_rows(), 1);
    // Brackets inside strings, even escaped quotes before them, nest nothing.
    let text = format!("\\\"{}", "[".repeat(1_000));
    let texts = batch_of(
        &[&format!("{{\"n\":18446744073709551616,\"a\":\"{text}\"}}")],
        1 << 20,
    );
    assert_eq!(texts.num_rows(), 1);
}

#[test]
fn a_number_whose_exponent_is_too_long_is_refused_naming_it_where_the_chunk_is_parsed_exactly() {
    let number = "1e-1234567890123456789";
    let records = Bytes::from(format!("{{\"n\":18446744073709551616,\"x\":[{number}]}}"));
    let chunk = chunks(&[records], 1 << 20).unwrap().remove(0);
    let refused = parse(chunk, limits(), &limits().beyond()).err();
    assert_eq!(
        refused,
        Some(super::ShredError::Exponent(number.to_owned()))
    );
}

#[test]
fn records_with_more_columns_than_the_limit_are_refused() {
    let columns = usize::try_from(SCHEMA_COLUMNS).unwrap();
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
fn integers_beyond_the_unsigned_range_read_exactly_as_decimals_and_negative_zero_as_zero() {
    // The two keys differ by one, which a float would round away.
    let batch = batch_of(
        &["{\"big\":18446744073709551616,\"zero\":-0.0}\n{\"big\":-18446744073709551617}"],
        1 << 20,
    );
    let huge = LogicalType::Decimal(DecimalType::new(38, 0).unwrap());
    assert_eq!(types(&batch)[0].1, huge);
    let values: Vec<Option<i128>> = batch
        .column(0)
        .as_primitive::<Decimal128Type>()
        .iter()
        .collect();
    assert_eq!(
        values,
        [
            Some(18_446_744_073_709_551_616),
            Some(-18_446_744_073_709_551_617)
        ]
    );
    assert_eq!(
        batch
            .column(1)
            .as_primitive::<Float64Type>()
            .value(0)
            .to_bits(),
        0.0_f64.to_bits()
    );
    // Beside integers of every width, across chunks, and exact among floats as JSON text.
    let joined = batch_of(
        &[
            "{\"a\":1}",
            "{\"a\":18446744073709551615}",
            "{\"a\":123456789012345678901234567890}",
        ],
        1,
    );
    assert_eq!(types(&joined)[0].1, huge);
    let mixed = batch_of(
        &["{\"a\":123456789012345678901234567890}\n{\"a\":0.5}"],
        1 << 20,
    );
    assert_eq!(types(&mixed)[0].1, LogicalType::Json);
    assert_eq!(
        texts(&mixed, 0),
        [
            Some("123456789012345678901234567890".to_owned()),
            Some("0.5".to_owned())
        ]
    );
    // A float as large stays a float.
    let float = batch_of(&["{\"a\":1e20}"], 1 << 20);
    assert_eq!(types(&float)[0].1, LogicalType::Float64);
}

#[test]
fn integers_beyond_38_digits_read_exactly_as_76_digit_decimals_and_beyond_those_as_json_text() {
    let digits = |count: usize| "9".repeat(count);
    let vast = LogicalType::Decimal(DecimalType::new(76, 0).unwrap());
    for (pushed, joined) in [
        (vec![format!("1{}", "0".repeat(38))], vast.clone()),
        (vec![digits(76), format!("-{}", digits(76))], vast.clone()),
        (vec!["1".to_owned(), digits(39)], vast.clone()),
        (
            vec![digits(39), "18446744073709551615".to_owned()],
            vast.clone(),
        ),
        (vec![digits(39), digits(30)], vast),
        (vec![digits(77)], LogicalType::Json),
        (vec![digits(400), "-1".to_owned()], LogicalType::Json),
        (vec![digits(39), "0.5".to_owned()], LogicalType::Json),
    ] {
        let records: Vec<String> = pushed
            .iter()
            .map(|value| format!("{{\"a\":{value}}}"))
            .collect();
        for chunk_bytes in [1, 1 << 20] {
            let batch = batch_of(&[records.join("\n").as_str()], chunk_bytes);
            assert_eq!(types(&batch)[0].1, joined, "{pushed:?}");
            let read: Vec<String> = if joined == LogicalType::Json {
                texts(&batch, 0).into_iter().flatten().collect()
            } else {
                batch
                    .column(0)
                    .as_primitive::<Decimal256Type>()
                    .iter()
                    .flatten()
                    .map(|value| value.to_string())
                    .collect()
            };
            assert_eq!(read, pushed, "each integer reads back exactly");
        }
    }
}

#[test]
fn a_number_reads_alike_whether_or_not_its_chunk_is_parsed_exactly() {
    // The integer beyond 64 bits sends its chunk, from its record on, through the exact parse.
    let exact = "18446744073709551616";
    for number in [
        "0",
        "-0",
        "-0.0",
        "-0e0",
        "1.5",
        "1E2",
        "1e19",
        "-1e19",
        "1e-400",
        "4.9e-324",
        "1e400",
        "-1e400",
        "1.8e308",
        "12345678901234567890",
        "-9223372036854775809",
    ] {
        let fast = shredded(&[Bytes::from(format!("{{\"n\":{number}}}"))], 1 << 20);
        let beside = shredded(
            &[Bytes::from(format!(
                "{{\"big\":{exact}}}\n{{\"n\":{number}}}"
            ))],
            1 << 20,
        );
        match (fast, beside) {
            (Ok(fast), Ok(beside)) => {
                let (fast, beside) = (fast.expect("a row"), beside.expect("a row"));
                assert_eq!(types(&fast)[0].1, types(&beside)[1].1, "{number}");
                assert_eq!(
                    fast.column(0).to_data(),
                    beside.column(1).slice(1, 1).to_data(),
                    "{number}"
                );
            }
            (fast, beside) => assert_eq!(fast.err(), beside.err(), "{number}"),
        }
    }
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
fn a_column_that_changes_type_in_a_later_chunk_is_fitted_to_their_join() {
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
    let cores = Cores::new(NonZeroUsize::new(4).unwrap());
    let env = crate::env::SystemEnv::try_new(cores).unwrap();
    let pool = crate::compute::Pool::new(Arc::new(env), Arc::default());
    let pushes: Vec<Bytes> = (0..40)
        .map(|push| {
            let lines: Vec<String> = (0..50)
                .map(|row| format!("{{\"n\":{}}}", push * 50 + row))
                .collect();
            Bytes::from(lines.join("\n"))
        })
        .collect();
    let batches = shred(&pool, &pushes, 256, limits()).await.unwrap();
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
    let parsed = parsed(&lines.join("\n"));
    // Only the first row searches, for the keys it adds.
    assert_eq!(parsed.record.unwrap().searches(), 3);
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
    assert!(!parsed("{\"a\":1}\n{\"a\":2.5}\n{\"b\":[1]}").spoiled);
    assert!(!parsed("{\"a\":1}\n{\"a\":\"x\"}").spoiled);
    assert!(parsed("{\"a\":\"x\"}\n{\"a\":2.5}").spoiled);
    assert!(parsed("{\"a\":2.5}\n{\"a\":\"x\"}").spoiled);
    assert!(parsed("{\"a\":{\"b\":true}}\n{\"a\":{\"b\":[]}}").spoiled);
    // Integers widen in place, however wide, parsed exactly once one may be beyond 64 bits.
    let wide = parsed("{\"a\":1}\n{\"a\":18446744073709551615}\n{\"a\":18446744073709551616}");
    assert!(!wide.spoiled && wide.exact);
    // A chunk whose floats are small or not whole is parsed once, fast.
    assert!(!parsed("{\"a\":1}\n{\"a\":2.0}\n{\"a\":0.5}").exact);
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
    let columns = usize::try_from(SCHEMA_COLUMNS).unwrap();
    // The invalid record after it, which adds no field, is never reached.
    let push = format!("{}\n{{\"c0\":", wide_object(0, columns + 1));
    let error = crate::compute::ready(shred(
        &crate::compute::Pool::inline(),
        &[Bytes::from(push)],
        1 << 20,
        limits(),
    ));
    assert_eq!(
        error.unwrap_err(),
        super::ShredError::TooManyColumns(SCHEMA_COLUMNS + 1, SCHEMA_COLUMNS)
    );
}

#[test]
fn nested_objects_are_bound_by_the_column_limit_too() {
    let columns = usize::try_from(SCHEMA_COLUMNS).unwrap();
    let nested = format!("{{\"o\":{}}}", wide_object(0, columns + 1));
    assert_eq!(refused(&nested), "limit_exceeded");
    let half = columns / 2 + 1;
    let first = format!("{{\"o\":{}}}", wide_object(0, half));
    let second = format!("{{\"o\":{}}}", wide_object(half, half));
    let pushes = [Bytes::from(first), Bytes::from(second)];
    assert_eq!(shredded(&pushes, 1).unwrap_err(), Code("limit_exceeded"));
    let first = format!("{{\"l\":[{}]}}", wide_object(0, half));
    let second = format!("{{\"l\":[{}]}}", wide_object(half, half));
    let in_lists = [Bytes::from(first), Bytes::from(second)];
    assert_eq!(shredded(&in_lists, 1).unwrap_err(), Code("limit_exceeded"));
    // The object's own column and its fields together are the limit.
    let fits = format!("{{\"o\":{}}}", wide_object(0, columns - 1));
    assert_eq!(batch_of(&[&fits], 1 << 20).num_rows(), 1);
}

#[test]
fn sparse_wide_records_are_refused_before_they_are_built() {
    // Over five thousand rows under almost ten thousand columns: fifty million cells, from 114 KB.
    let push = format!("{}{}", "{}\n".repeat(5000), wide_object(0, 9999));
    assert_eq!(refused(&push), "limit_exceeded");
}

#[test]
fn cells_are_bounded_at_the_limit_before_anything_is_built() {
    // 4096 rows under 8192 columns are the limit's 2^25 cells; one row more is past it. The
    // chunk's builders would take more than its text was admitted for, so it is only observed.
    let at = |rows: usize| {
        let push = format!("{}{}", "{}\n".repeat(rows - 1), wide_object(0, 8192));
        let chunk = chunks(&[Bytes::from(push)], 1 << 30).unwrap().remove(0);
        let parsed = parse(chunk, limits(), &limits().beyond()).unwrap();
        assert!(parsed.record.is_none(), "observed, not built");
        super::join(&[parsed], limits()).map(|_| ())
    };
    assert_eq!(at(4096), Ok(()));
    assert_eq!(at(4097), Err(super::ShredError::TooManyCells(4097 * 8192)));
}

#[test]
fn cells_count_a_list_s_items_at_their_own_level() {
    let cells = |text: &str| {
        let parsed = parsed(text);
        let rows = u64::try_from(parsed.rows).unwrap();
        super::cost::built(&parsed.shape, &parsed.shape, rows).cells
    };
    assert_eq!(
        cells(r#"{"a":1,"b":null,"c":{"d":"x","e":null},"l":[true],"x":{}}"#),
        3
    );
    assert_eq!(cells(r#"{"l":[{"p":1,"q":"x","r":true}],"m":[null]}"#), 3);
    // Three items under three fields, and the record's own column.
    assert_eq!(
        cells(r#"{"n":1,"l":[{"p":1},{"q":"x"},{"r":true}]}"#),
        1 + 3 * 3
    );
}

#[test]
fn chunks_that_lack_columns_or_hold_them_in_another_order_are_fitted_without_parsing_again() {
    let chunks = [
        parsed(r#"{"a":1,"o":{"x":1}}"#),
        parsed(r#"{"b":"t","a":2.5,"l":[]}"#),
        parsed(r#"{"a":null,"o":{"y":true,"x":2},"l":[1]}"#),
        parsed(r#"{"a":{"z":1}}"#),
    ];
    let joined = super::join(&chunks[..3], limits()).unwrap().0;
    for chunk in &chunks[..3] {
        assert!(!chunk.spoiled && super::conform::shape_fits(&chunk.shape, &joined));
    }
    // Integers render into a column of JSON as they were read; floats need their text again.
    let with_object = super::join(&chunks, limits()).unwrap().0;
    assert!(super::conform::shape_fits(&chunks[0].shape, &with_object));
    assert!(!super::conform::shape_fits(&chunks[1].shape, &with_object));
}

#[test]
fn a_chunk_planned_to_fit_takes_the_columns_it_was_parsed_into_not_its_bytes_again() {
    // The chunk's bytes are swapped for another's of the same shape, so the columns parsed
    // and a build from the bytes hold different values.
    let mut swapped = parsed(r#"{"a":1}"#);
    swapped.chunk = parsed(r#"{"a":2}"#).chunk;
    let (shape, plans, _) = super::join(std::slice::from_ref(&swapped), limits()).unwrap();
    assert!(!plans[0].again);
    let schema = TableSchema::new(shape.logical_fields()).unwrap().to_arrow();
    let fitted = super::batch(swapped, plans[0], &shape, Arc::new(schema.clone())).unwrap();
    assert_eq!(fitted.column(0).as_primitive::<Int64Type>().values(), &[1]);

    let mut again = parsed(r#"{"a":1}"#);
    again.chunk = parsed(r#"{"a":2}"#).chunk;
    let plan = super::Plan {
        again: true,
        ..plans[0]
    };
    let rebuilt = super::batch(again, plan, &shape, Arc::new(schema)).unwrap();
    assert_eq!(rebuilt.column(0).as_primitive::<Int64Type>().values(), &[2]);
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
    let push = format!("{{\"a\":1}}\n{{\"password\":\"{secret}\",\"b\":}}");
    let error = crate::compute::ready(shred(
        &crate::compute::Pool::inline(),
        &[Bytes::from(push)],
        1 << 20,
        limits(),
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
    let later = crate::compute::ready(shred(&crate::compute::Pool::inline(), &pushes, 1, limits()))
        .unwrap_err();
    assert!(later.to_string().contains("record 4:"), "{later}");
}

#[test]
fn a_bad_record_is_numbered_in_its_push_whichever_chunk_holds_it() {
    for array in [false, true] {
        let mut rows: Vec<String> = (0..300).map(|id| format!(r#"{{"id":{id}}}"#)).collect();
        rows[250] = r#"{"id":}"#.to_owned();
        let push = if array {
            format!("[{}]", rows.join(","))
        } else {
            rows.join("\n")
        };
        let pushes = [Bytes::from(push)];
        let cut = chunks(&pushes, 1024).unwrap().len();
        assert!(cut >= 3, "{cut} chunks");
        let refused = crate::compute::ready(shred(
            &crate::compute::Pool::inline(),
            &pushes,
            1024,
            limits(),
        ))
        .unwrap_err();
        assert_eq!(refused.code(), "json_invalid");
        assert!(
            matches!(refused, super::ShredError::Record { index: 250, .. }),
            "{refused:?}"
        );
    }
}

#[test]
fn a_record_a_job_refuses_is_numbered_past_the_rows_of_the_jobs_before() {
    let refused = super::ShredError::Record {
        index: 1,
        what: "x".to_owned(),
    };
    let results = [Ok(2), Ok(3), Ok(0), Err(refused), Ok(4)];
    let numbered = super::numbered(results, |rows| *rows);
    assert!(
        matches!(numbered, Err(super::ShredError::Record { index: 6, .. })),
        "{numbered:?}"
    );
    assert_eq!(
        super::numbered([Ok(2), Ok(3)], |rows| *rows),
        Ok(vec![2, 3])
    );
    let other = [Ok(2), Err(super::ShredError::NotObject)];
    assert_eq!(
        super::numbered(other, |rows| *rows),
        Err(super::ShredError::NotObject)
    );
}

#[test]
fn blank_lines_among_records_cost_passes_but_not_their_batch() {
    // Ten blank lines a record: a chunk presized for every line takes more than its text was
    // admitted for, and is observed and built again.
    let dense: Vec<String> = (0..200)
        .map(|id| format!("{{\"id\":{id},{}", &wide_object(0, 10)[1..]))
        .collect();
    let padded = dense.join(&format!("\n{}", " \r\n".repeat(10)));
    let dense = dense.join("\n");
    let counts = |push: &str| {
        let pushes = vec![Bytes::from(push.to_owned())];
        crate::compute::ready(super::observe(
            &crate::compute::Pool::inline(),
            pushes,
            1 << 20,
            limits(),
        ))
        .unwrap()
        .counts()
    };
    assert_eq!(counts(&dense).tripped, 0);
    assert_eq!(counts(&padded).tripped, 1);
    assert_eq!(batch_of(&[&padded], 1 << 20), batch_of(&[&dense], 1 << 20));
}

#[test]
fn only_json_whitespace_surrounds_records() {
    let form_feed = '\x0C';
    let no_break_space = char::from_u32(0xA0).unwrap();
    for push in [
        format!("{form_feed}{{\"a\":1}}\n"),
        format!("{{\"a\":1}}{form_feed}"),
        format!("[{form_feed}{{\"a\":1}}]"),
        format!("[{{\"a\":1}}]{form_feed}"),
        format!("{no_break_space}{{\"a\":1}}"),
    ] {
        assert_eq!(refused(&push), "json_invalid", "{push:?}");
        let reference = super::reference::shred(&[Bytes::from(push.clone())]);
        assert_eq!(reference.unwrap_err(), Code("json_invalid"), "{push:?}");
    }
    assert_eq!(
        batch_of(&[" \t\r\n{\"a\":1} \t\r\n"], 1 << 20).num_rows(),
        1
    );
}

#[test]
fn values_at_the_nesting_limit_shred_on_a_small_stack_in_any_build() {
    let shred_here = |push: String| {
        crate::compute::ready(shred(
            &crate::compute::Pool::inline(),
            &[Bytes::from(push)],
            1 << 20,
            limits(),
        ))
    };
    let shredded = std::thread::Builder::new()
        .stack_size(256 << 10)
        .spawn(move || {
            for before in ["", "{\"a\":1}\n"] {
                for (open, close) in [("[", "]"), ("{\"b\":", "}"), ("[{\"b\":", "}]")] {
                    let levels = if open.len() > 5 {
                        NESTING_DEPTH / 2
                    } else {
                        NESTING_DEPTH
                    };
                    let deepest = format!("{before}{}", nested(levels, open, close));
                    assert!(shred_here(deepest).is_ok());
                    let deeper = format!("{before}{}", nested(NESTING_DEPTH + 1, open, close));
                    assert_eq!(shred_here(deeper).unwrap_err().code(), "limit_exceeded");
                }
            }
        })
        .unwrap()
        .join();
    assert!(shredded.is_ok());
}

#[test]
fn a_refusal_quotes_a_key_or_a_number_cut_to_its_limit() {
    let key = "k".repeat(64 << 10);
    let digits = "9".repeat(64 << 10);
    for push in [
        format!("{{\"{key}\":1,\"{key}\":2}}"),
        format!("{{\"a\":1}}\n{{\"a\":{{\"{key}\":1,\"{key}\":2}}}}"),
        format!("{{\"a\":{digits}e999999999}}"),
    ] {
        let error = crate::compute::ready(shred(
            &crate::compute::Pool::inline(),
            &[Bytes::from(push)],
            1 << 30,
            limits(),
        ))
        .unwrap_err();
        assert!(
            error.to_string().len() <= 2 * crate::limits::QUOTED_BYTES,
            "{} bytes",
            error.to_string().len()
        );
    }
}

#[test]
fn numbers_in_a_column_of_json_keep_the_text_they_were_written_as() {
    let written = [
        "0.12345678901234567891",
        "1234567890123456.78",
        "1.50",
        "1E2",
        "-0.0",
        "2.5e-3",
        "18446744073709551616",
    ];
    let records: Vec<String> = written
        .iter()
        .map(|number| format!("{{\"a\":{number}}}"))
        .chain([
            r#"{"a":"x"}"#.to_owned(),
            r#"{"a":{"p":[1.25e-3,0.10]}}"#.to_owned(),
        ])
        .collect();
    let expected: Vec<Option<String>> = written
        .iter()
        .map(|number| Some((*number).to_owned()))
        .chain([
            Some("\"x\"".to_owned()),
            Some(r#"{"p":[1.25e-3,0.10]}"#.to_owned()),
        ])
        .collect();
    // In one chunk, where the column stops building; and a chunk a record, where only the join
    // makes it JSON.
    for chunk_bytes in [1 << 20, 1] {
        let refs: Vec<&str> = records.iter().map(String::as_str).collect();
        let batch = batch_of(&[&refs.join("\n")], chunk_bytes);
        assert_eq!(types(&batch)[0].1, LogicalType::Json);
        assert_eq!(texts(&batch, 0), expected, "chunks of {chunk_bytes}");
    }
    // A column of floats keeps reading them as floats.
    let floats = batch_of(&["{\"f\":1.50}\n{\"f\":-0.0}"], 1 << 20);
    let values = floats.column(0).as_primitive::<Float64Type>().values();
    let bits: Vec<u64> = values.iter().map(|value| value.to_bits()).collect();
    assert_eq!(bits, [1.5_f64.to_bits(), 0.0_f64.to_bits()]);
}

#[test]
fn a_push_takes_more_than_it_was_admitted_for_only_where_its_batches_hold_more() {
    let excess = |text: String| {
        let pushes = vec![Bytes::from(text)];
        crate::compute::ready(super::observe(
            &crate::compute::Pool::inline(),
            pushes,
            1 << 20,
            limits(),
        ))
        .unwrap()
        .excess()
    };
    // Records of a few fields each hold less than twice their text.
    let dense: Vec<String> = (0..1_000)
        .map(|n| format!(r#"{{"id":{n},"name":"user-{n}","score":{n}.5,"o":{{"a":true}}}}"#))
        .collect();
    assert_eq!(excess(dense.join("\n")), 0);
    // Three thousand rows each naming one of three hundred keys: every row a cell in each.
    let sparse: Vec<String> = (0..3_000)
        .map(|n| format!("{{\"k{}\":1}}", n % 300))
        .collect();
    let taken = excess(sparse.join("\n"));
    let cells = 3_000 * 300;
    assert!((cells * 8..cells * 9).contains(&taken), "{taken}");
}

#[test]
fn the_column_limit_counts_every_column_a_chunk_holds_and_the_join_of_all() {
    let shredded = |pushes: &[&str], columns: u64| {
        let pushes: Vec<Bytes> = pushes
            .iter()
            .map(|push| Bytes::from(push.to_string()))
            .collect();
        let limits = ShredLimits {
            columns,
            ..limits()
        };
        crate::compute::ready(shred(&crate::compute::Pool::inline(), &pushes, 1, limits))
            .map(|_| ())
    };
    // An object and its fields, a list and its items, at any depth.
    let nested = r#"{"o":{"a":1,"l":[{"b":1}]}}"#;
    assert_eq!(shredded(&[nested], 5), Ok(()));
    assert_eq!(
        shredded(&[nested], 4),
        Err(super::ShredError::TooManyColumns(5, 4))
    );
    // Chunks within the limit each may join into a shape past it.
    assert_eq!(shredded(&[r#"{"a":1,"b":1}"#, r#"{"c":1}"#], 3), Ok(()));
    assert_eq!(
        shredded(&[r#"{"a":1,"b":1}"#, r#"{"c":1,"d":1}"#], 3),
        Err(super::ShredError::TooManyColumns(4, 3))
    );
}

#[test]
fn what_a_chunk_s_columns_of_scalars_hold_is_no_more_than_they_are_charged() {
    held_within_charges(&["1", "\"ab\"", "1.5"]);
}

#[test]
fn what_a_chunk_s_columns_of_lists_hold_is_no_more_than_they_are_charged() {
    held_within_charges(&["[1]", "[{\"q\":\"x\"}]"]);
}

#[test]
fn what_a_chunk_s_columns_of_structs_hold_is_no_more_than_they_are_charged() {
    held_within_charges(&["{\"q\":1}"]);
}

/// Asserts what a chunk of one record of columns each holding `values` in turn holds built,
/// observed and as a batch is no more than it is charged, at widths just past two of the sizes
/// the vectors holding the columns' entries double at, and between them.
fn held_within_charges(values: &[&str]) {
    for keys in [1_025, 2_049, 3_000] {
        for value in values {
            held_within_charge(keys, value);
        }
    }
}

/// Asserts what a chunk of one record of `keys` columns each holding `value` holds built,
/// observed and as a batch is no more than it is charged.
fn held_within_charge(keys: usize, value: &str) {
    let heap = &crate::cost::tests::HEAP;
    let fields: Vec<String> = (0..keys).map(|key| format!("\"c{key}\":{value}")).collect();
    let records = Bytes::from(format!("{{{}}}", fields.join(",")));
    let chunk = || {
        chunks(std::slice::from_ref(&records), 1 << 20)
            .unwrap()
            .remove(0)
    };
    // Built as it is parsed, with room; observed, with none.
    for admitted in [1 << 20, 0] {
        let limits = ShredLimits {
            admitted,
            ..limits()
        };
        heap.reset_peak_usage();
        let before = heap.current_usage();
        let parsed = parse(chunk(), limits, &limits.beyond()).unwrap();
        let held = u64::try_from(heap.peak_usage() - before).unwrap();
        assert_eq!(parsed.record.is_some(), admitted > 0, "{keys} {value}");
        assert!(
            held <= parsed.spent,
            "{keys} {value}: held {held}, charged {}",
            parsed.spent
        );
    }
    // The batch: its columns' arrays, beside the values they hold.
    let parsed = parse(chunk(), limits(), &limits().beyond()).unwrap();
    let shape = parsed.shape.clone();
    drop(parsed);
    heap.reset_peak_usage();
    let before = heap.current_usage();
    let batches = crate::compute::ready(shred(
        &crate::compute::Pool::inline(),
        std::slice::from_ref(&records),
        1 << 20,
        limits(),
    ))
    .unwrap();
    let held = u64::try_from(heap.current_usage() - before).unwrap();
    drop(batches);
    let charged = super::cost::arrays(&shape, None)
        + super::cost::built(&shape, &shape, 1).bytes
        + 2 * records.len() as u64;
    assert!(
        held <= charged,
        "{keys} {value}: a batch held {held}, charged {charged}"
    );
}

#[test]
fn a_batch_holds_its_columns_at_their_count_not_the_builders_capacity() {
    let heap = &crate::cost::tests::HEAP;
    let keys = 1_025;
    let fields: Vec<String> = (0..keys).map(|key| format!("\"c{key}\":1")).collect();
    let records = Bytes::from(format!("{{{}}}", fields.join(",")));
    let before = heap.current_usage();
    let batches = crate::compute::ready(shred(
        &crate::compute::Pool::inline(),
        std::slice::from_ref(&records),
        1 << 20,
        limits(),
    ))
    .unwrap();
    let held = heap.current_usage() - before;
    let batch = batches.first().unwrap();
    assert_eq!(batch.num_columns(), keys);
    let arrays: usize = batch
        .columns()
        .iter()
        .take(keys)
        .map(Array::get_array_memory_size)
        .sum();
    let schema: usize = batch
        .schema()
        .fields()
        .iter()
        .take(keys)
        .map(|field| size_of_val(field.as_ref()) + field.name().len() + 16)
        .sum();
    // What the batch holds beside its arrays and schema: the vector of columns, the arrays'
    // handles and the batch itself; the builders' columns alone would take sixteen times that.
    let extra = held.saturating_sub(arrays + schema);
    let vector = keys * size_of::<arrow_array::ArrayRef>();
    assert!(extra <= vector + 128 * 1024, "held {held}, extra {extra}");
}

/// An integer beyond 64 bits, which sends the chunk holding it through the exact parse.
const BEYOND_64_BITS: &str = "18446744073709551616";

/// Records `{"v":1}`, `{"v":true}` and `{"v":value}`: a column of integers that drifts to JSON
/// at the second record, so the third is rendered as it is parsed.
fn drifting(value: &str) -> String {
    drifting_from("1", value)
}

/// Records `{"v":first}`, `{"v":true}` and `{"v":value}`, `first` an integer.
fn drifting_from(first: &str, value: &str) -> String {
    format!("{{\"v\":{first}}}\n{{\"v\":true}}\n{{\"v\":{value}}}")
}

/// An object of `keys` keys, each holding 0.
fn flat_object(keys: usize) -> String {
    let fields: Vec<String> = (0..keys).map(|key| format!("\"k{key}\":0")).collect();
    format!("{{{}}}", fields.join(","))
}

/// How parsing a chunk went, and what it held.
struct ParsePeak {
    /// Bytes allocated at the peak.
    peak: u64,
    /// Whether the chunk built as it was parsed.
    built: bool,
    /// Whether it was parsed exactly.
    exact: bool,
    /// Bytes the parse charged.
    spent: u64,
    /// Bytes its meter allows: its allowance and the flush's room beyond.
    allowed: u64,
}

/// How parsing `records`, one chunk, under a limit of `columns` columns went.
fn parse_peak(records: &str, columns: u64) -> ParsePeak {
    let limits = ShredLimits::new(columns);
    let records = Bytes::from(records.to_owned());
    let chunk = chunks(std::slice::from_ref(&records), records.len())
        .unwrap()
        .remove(0);
    let allowed = count(chunk.bytes()) * limits.admitted + limits.beyond_bytes();
    let mut chunk = Some(chunk);
    let (peak, (built, exact, spent)) = crate::cost::tests::peak_alone(|| {
        let parsed = parse(chunk.take().unwrap(), limits, &limits.beyond()).unwrap();
        let parse = (parsed.record.is_some(), parsed.exact, parsed.spent);
        chunk = Some(parsed.chunk);
        parse
    });
    ParsePeak {
        peak,
        built,
        exact,
        spent,
        allowed,
    }
}

#[test]
fn rendering_a_wide_object_into_a_column_that_drifted_to_json_stays_within_its_meter() {
    // Parsed fast, and exactly.
    for first in ["1", BEYOND_64_BITS] {
        let records = drifting_from(first, &flat_object(262_145));
        for columns in [10_000, 1] {
            let parse = parse_peak(&records, columns);
            assert_eq!(parse.exact, first == BEYOND_64_BITS, "{first}");
            assert!(
                !parse.built,
                "{first}, {columns} columns: the rendering found no room, so the chunk is observed"
            );
            assert!(
                parse.peak <= parse.allowed,
                "{first}, {columns} columns: peaked at {}, allowed {}",
                parse.peak,
                parse.allowed
            );
        }
    }
}

#[test]
fn a_record_parsed_exactly_past_the_column_limit_is_refused_within_its_allowance() {
    let records = Bytes::from(format!(
        "{{\"n\":{BEYOND_64_BITS}}}\n{}",
        flat_object(262_145)
    ));
    let limits = ShredLimits::new(1_000);
    let chunk = || {
        chunks(std::slice::from_ref(&records), 1 << 20)
            .unwrap()
            .remove(0)
    };
    let allowed = count(chunk().bytes()) * limits.admitted + limits.beyond_bytes();
    let (peak, refused) =
        crate::cost::tests::peak_alone(|| parse(chunk(), limits, &limits.beyond()).err());
    assert_eq!(
        refused,
        Some(super::ShredError::TooManyColumns(1_001, 1_000))
    );
    assert!(peak <= allowed, "peaked at {peak}, allowed {allowed}");
}

/// What parsing four mebibytes of short notes, then `{"v":first}`, `{"v":true}` and `{"v":value}`,
/// allocates at its peak and what the parse charged, the chunk built as it was parsed, exactly
/// where `first` is beyond 64 bits.
fn rendered(first: &str, value: &str) -> (u64, u64) {
    let notes = format!("{{\"n\":\"{}\"}}\n", "x".repeat(1 << 10)).repeat(1 << 12);
    let records = format!("{notes}{}", drifting_from(first, value));
    let parse = parse_peak(&records, 10_000);
    assert!(parse.built, "{value:.32}: the chunk had room to build");
    assert_eq!(parse.exact, first == BEYOND_64_BITS, "{value:.32}");
    (parse.peak, parse.spent)
}

/// Asserts that rendering `value` into a column that drifted to JSON from `first` holds no more
/// than it charged: what the parse takes beside its builders and renderings, as a value of `1`
/// shows, is not the rendering's.
fn rendered_as_charged(first: &str, value: &str) {
    let (base_peak, base_spent) = rendered(first, "1");
    let (peak, spent) = rendered(first, value);
    assert!(
        peak.saturating_sub(base_peak) <= spent.saturating_sub(base_spent),
        "{value:.32}: held {peak}, charged {spent}, where a scalar held {base_peak}, charged {base_spent}"
    );
}

#[test]
fn rendering_values_into_a_column_that_drifted_to_json_holds_no_more_than_it_charged() {
    rendered_as_charged("1", &flat_object(1_000));
    // Objects nested to sixty levels below the column, within the limit.
    rendered_as_charged("1", &format!("{}0{}", "{\"a\":".repeat(60), "}".repeat(60)));
    rendered_as_charged(
        "1",
        &format!("[{}]", vec![r#""\u0001\u0002""#; 2_000].join(",")),
    );
}

#[test]
fn parsing_a_record_exactly_holds_no_more_than_it_charged() {
    // Parsed exactly, a value is walked as its text is read, never held whole beside it.
    rendered_as_charged(BEYOND_64_BITS, &flat_object(100_000));
    rendered_as_charged(
        BEYOND_64_BITS,
        &format!("[{}]", vec!["0.5"; 100_000].join(",")),
    );
}

#[test]
fn a_chunk_whose_rendering_finds_no_room_is_built_again_to_the_same_batch() {
    let text = drifting(&flat_object(10_000));
    let (rebuilt, batch) = built_again(&text);
    assert_eq!(rebuilt, 1, "the rendering tripped the chunk's meter");
    let roomy = ShredLimits {
        admitted: 1 << 20,
        ..limits()
    };
    let pool = crate::compute::Pool::inline();
    let observed = crate::compute::ready(super::observe(
        &pool,
        vec![Bytes::from(text)],
        1 << 20,
        roomy,
    ))
    .unwrap();
    assert_eq!(
        observed.counts().rebuilt,
        0,
        "the roomy chunk built as parsed"
    );
    let built = crate::compute::ready(observed.build(&pool)).unwrap();
    assert_eq!(built, [batch]);
}

#[test]
fn an_object_repeating_a_key_in_a_drifted_column_is_refused_as_before() {
    assert_eq!(refused(&drifting(r#"{"a":1,"a":2}"#)), "json_duplicate_key");
}

/// How many chunks shredding `push` in one chunk builds again, and the batch.
fn built_again(push: &str) -> (u64, RecordBatch) {
    let pool = crate::compute::Pool::inline();
    let pushes = vec![Bytes::from(push.to_owned())];
    let observed = crate::compute::ready(super::observe(&pool, pushes, 1 << 20, limits()));
    let observed = observed.unwrap();
    let rebuilt = observed.counts().rebuilt;
    let mut batches = crate::compute::ready(observed.build(&pool)).unwrap();
    assert_eq!(batches.len(), 1, "one chunk");
    (rebuilt, batches.remove(0))
}

/// A hundred records of `first` and a hundred of `then`, each `{"a": value}` beside a note:
/// enough text that the chunk's allowance holds its builders, the widened ones too.
fn hundreds(first: &str, then: &str) -> String {
    let note = "x".repeat(64);
    let records = |value: &str| vec![format!("{{\"a\":{value},\"n\":\"{note}\"}}"); 100].join("\n");
    format!("{}\n{}", records(first), records(then))
}

#[test]
fn an_observation_counts_its_passes_apart_from_the_chunks_building_will_build_again() {
    let pool = crate::compute::Pool::inline();
    let pushes = vec![Bytes::from(hundreds("1", r#"{"b":1}"#))];
    let observed = crate::compute::ready(super::observe(&pool, pushes, 1 << 20, limits()));
    let observed = observed.unwrap();
    let passes = ShredCounts {
        parsed: 1,
        ..ShredCounts::default()
    };
    assert_eq!(observed.passes(), passes);
    let counts = ShredCounts {
        rebuilt: 1,
        ..passes
    };
    assert_eq!(observed.counts(), counts);
}

#[test]
fn a_chunk_whose_columns_fit_is_built_once_and_one_whose_column_spoiled_twice() {
    // Integers widen to floats and to 256-bit decimals where they are built.
    let (again, batch) = built_again(&hundreds("1", "1.5"));
    assert_eq!(again, 0);
    let floats = batch.column(0).as_primitive::<Float64Type>();
    assert_eq!((floats.value(0), floats.value(199)), (1.0, 1.5));
    let vast = "1234567890123456789012345678901234567890123";
    let (again, batch) = built_again(&hundreds("1", vast));
    assert_eq!(again, 0);
    let decimals = batch.column(0).as_primitive::<Decimal256Type>();
    assert_eq!(decimals.value_as_string(0), "1");
    assert_eq!(decimals.value_as_string(199), vast);
    // A string after integers makes the column one of JSON text where it is built.
    let (again, batch) = built_again(&hundreds("-1", "\"x\""));
    assert_eq!(again, 0);
    let texts = batch.column(0).as_string::<i32>();
    assert_eq!((texts.value(0), texts.value(199)), ("-1", "\"x\""));
    // An object after integers spoils the column, which is built again as JSON text.
    let (again, _) = built_again(&hundreds("1", r#"{"b":1}"#));
    assert_eq!(again, 1);
}

#[test]
fn a_chunk_is_parsed_exactly_only_where_a_zero_may_be_a_vast_exponent() {
    let lookalike = "\"1e1234567890123456789\"";
    assert!(!parsed("{\"a\":0.0}").exact);
    assert!(!parsed(&format!("{{\"a\":1.5,\"n\":{lookalike}}}")).exact);
    assert!(parsed(&format!("{{\"a\":0.0,\"n\":{lookalike}}}")).exact);
}

/// The passes shredding `push` in one chunk takes.
fn counted(push: &'static [u8]) -> ShredCounts {
    let pool = crate::compute::Pool::inline();
    let pushes = vec![Bytes::from_static(push)];
    let observed = crate::compute::ready(super::observe(&pool, pushes, 1 << 20, limits()));
    observed.unwrap().counts()
}

#[test]
fn a_float_beyond_two_to_the_sixty_third_written_with_an_exponent_parses_once() {
    let counts = counted(b"{\"f\":1.5e30}\n{\"f\":2.5e40}\n");
    assert_eq!((counts.parsed, counts.exact), (1, 0));
    // A record holding such a float and a run of digits as long as an integer beyond 64 bits is
    // parsed exactly, even where the run is a string's; a run in another record is not.
    assert!(counted(b"{\"f\":1.5e30,\"s\":\"1234567890123456789\"}\n").exact > 0);
    assert_eq!(
        counted(b"{\"f\":1.5e30}\n{\"n\":1234567890123456789}\n").exact,
        0
    );
    // An integer just beyond 64 bits the fast parse reads as -2⁶³, a float at the bound.
    assert!(counted(b"{\"f\":1.5e30}\n{\"n\":-9223372036854775809}\n").exact > 0);
}

#[test]
fn a_record_is_parsed_exactly_only_for_a_float_the_fast_parse_may_have_misread() {
    // A whole float below 2⁶³ beside a string as long as an integer beyond 64 bits: read right.
    assert_eq!(
        counted(b"{\"f\":2.0,\"s\":\"1234567890123456789\"}\n").exact,
        0
    );
    // A float that is not zero beside a string shaped like a vast exponent: read right too.
    assert_eq!(
        counted(b"{\"f\":1.5,\"s\":\"1e1234567890123456789\"}\n").exact,
        0
    );
    // A zero beside an exponent too short to read as zero: read right as well.
    assert!(!parsed("{\"a\":0.0,\"n\":\"1e5\"}").exact);
}

/// The first chunk of `text`, observed: parsed with no room to build anything.
fn observed_only(text: &str) -> Result<Parsed, super::ShredError> {
    let records = Bytes::from(text.to_owned());
    let tight = ShredLimits {
        admitted: 0,
        ..limits()
    };
    parse(
        chunks(&[records], 1 << 20).unwrap().remove(0),
        tight,
        &tight.beyond(),
    )
}

#[test]
fn each_pass_over_a_chunk_is_counted_as_what_it_was() {
    let counts = |parsed: Parsed| parsed.counts;
    let vast = "1".repeat(30);
    let cases = [
        // Built as it is parsed, once.
        (
            counts(parsed("{\"a\":1}")),
            ShredCounts {
                parsed: 1,
                ..ShredCounts::default()
            },
        ),
        // Parsed again exactly, for an integer beyond 64 bits.
        (
            counts(parsed(&format!("{{\"a\":{vast}}}"))),
            ShredCounts {
                parsed: 2,
                exact: 1,
                ..ShredCounts::default()
            },
        ),
        // Tripped at once, then only observed.
        (
            counts(observed_only("{\"a\":1}").unwrap()),
            ShredCounts {
                parsed: 1,
                tripped: 1,
                observed: 1,
                ..ShredCounts::default()
            },
        ),
    ];
    for (counted, expected) in cases {
        assert_eq!(counted, expected);
    }
}

#[test]
fn an_observation_refuses_records_that_are_not_objects_and_notes_floats_only_in_json() {
    // An object first, which trips the build: the records after it are observed.
    assert!(observed_only("{\"a\":1}").unwrap().record.is_none());
    let kinds = [
        "null",
        "true",
        "1",
        "-1",
        "18446744073709551615",
        "1.5",
        "\"s\"",
        "[1]",
        "[]",
    ];
    for record in kinds {
        assert_eq!(
            observed_only(&format!("{{\"a\":1}}\n{record}")).err(),
            Some(super::ShredError::NotObject),
            "{record}"
        );
    }
    // An integer beyond 64 bits has the chunk observed exactly, which reads an integer beyond 38
    // digits as its digits.
    let vast = "1".repeat(42);
    assert_eq!(
        observed_only(&format!("{{\"a\":18446744073709551616}}\n{vast}")).err(),
        Some(super::ShredError::NotObject)
    );
    assert!(!observed_only("{\"a\":1.5}").unwrap().json_floats);
    assert!(
        observed_only("{\"a\":1.5}\n{\"a\":\"x\"}")
            .unwrap()
            .json_floats
    );
}

#[test]
fn an_observation_refuses_a_key_one_object_repeats_at_any_depth() {
    // An object first, which trips the build: the records after it are observed.
    for record in [
        r#"{"b":1,"b":2}"#,
        r#"{"o":{"b":1,"b":1}}"#,
        r#"{"l":[{"b":1,"b":1}]}"#,
    ] {
        assert!(
            matches!(
                observed_only(&format!("{{\"a\":1}}\n{record}")),
                Err(super::ShredError::DuplicateKey(_))
            ),
            "{record}"
        );
    }
    // Each object may name a key once, whatever other objects name.
    let distinct = "{\"a\":1}\n{\"b\":1}\n{\"b\":2,\"o\":{\"b\":1},\"l\":[{\"b\":1},{\"b\":2}]}";
    assert!(observed_only(distinct).is_ok());
}

#[test]
fn observed_shapes_are_equal_by_their_fields_whatever_objects_named_them() {
    // A second record names the same fields from other objects, which the shape marks.
    let shape = |text: &str| observed_only(text).unwrap().shape;
    let once = shape(r#"{"a":1}"#);
    assert_eq!(once, shape("{\"a\":1}\n{\"a\":2}"));
    assert_ne!(once, shape(r#"{"b":1}"#));
}

#[test]
fn an_observation_past_its_chunk_s_room_and_the_flush_s_is_refused_naming_the_flush_s() {
    // Ten columns of 400-byte names take more than the room of ten columns' shapes.
    let fields: Vec<String> = (0..10)
        .map(|key| format!("\"{}{key}\":1", "k".repeat(399)))
        .collect();
    let records = Bytes::from(format!("{{{}}}", fields.join(",")));
    let tight = ShredLimits {
        columns: 10,
        admitted: 0,
    };
    let chunk = chunks(&[records], 1 << 20).unwrap().remove(0);
    let error = parse(chunk, tight, &tight.beyond()).err();
    assert_eq!(
        error,
        Some(super::ShredError::ColumnsBeyondText(
            10 * (KEY + OBJECT_SHAPE)
        ))
    );
}

#[test]
fn a_shape_counts_a_column_for_each_list_s_items_at_every_depth() {
    // `l` and its items, `m`, its items and theirs, `o` and `x`.
    let shape = parsed("{\"l\":[1],\"m\":[[1]],\"o\":{\"x\":1}}").shape;
    assert_eq!(shape.columns(), 7);
}

#[test]
fn a_chunk_built_again_where_it_needs_exact_numbers_but_was_not_planned_so_is_refused_unbuilt() {
    // A whole float beyond 64 bits written with as many digits as an integer the fast parse
    // rounds: its chunk is parsed exactly, and a build planned otherwise is a fault, not a guess.
    let text = "{\"a\":100000000000000000000.0}";
    let shape = parsed(text).shape;
    let chunk = || chunks(&[Bytes::from(text)], 1 << 20).unwrap().remove(0);
    let built = super::again(&chunk(), 1, &shape, true).unwrap();
    assert_eq!(
        built[0].as_primitive::<Float64Type>().value(0).to_bits(),
        1e20_f64.to_bits()
    );
    assert!(matches!(
        super::again(&chunk(), 1, &shape, false),
        Err(super::ShredError::Internal(_))
    ));
}

#[test]
fn a_build_refuses_arrays_nested_past_the_limit_as_it_parses_them() {
    let parse_once = |text: String| {
        let roomy = ShredLimits {
            admitted: 1 << 20,
            ..limits()
        };
        let chunk = chunks(&[Bytes::from(text)], 1 << 20).unwrap().remove(0);
        parse(chunk, roomy, &roomy.beyond()).map(|parsed| parsed.record.is_some())
    };
    assert_eq!(parse_once(nested(NESTING_DEPTH, "[", "]")), Ok(true));
    assert_eq!(
        parse_once(nested(NESTING_DEPTH + 1, "[", "]")).err(),
        Some(super::ShredError::TooDeep)
    );
}

/// A JSON value as it is written, and the text a column of JSON holds for it: `None` where an
/// object in it repeats a key, which refuses its record.
#[derive(Clone, Debug)]
pub(super) struct Written {
    pub(super) text: String,
    pub(super) rendered: Option<String>,
}

/// `text` with every character written as its unicode escape.
fn all_escaped(text: &str) -> String {
    use std::fmt::Write as _;
    let mut escaped = String::from("\"");
    for unit in text.encode_utf16() {
        write!(escaped, "\\u{unit:04X}").unwrap();
    }
    escaped.push('"');
    escaped
}

/// A string, written with the escapes JSON text renders it with or with every character escaped.
fn written_string() -> impl proptest::strategy::Strategy<Value = (String, String)> {
    use proptest::prelude::*;
    let text = prop_oneof![
        "[a-c]{0,3}",
        "\\PC{0,6}",
        Just("q\"\\/\n\u{1}\u{1f}\u{7f}é😀".to_owned()),
    ];
    (text, any::<bool>()).prop_map(|(text, escaped)| {
        let rendered = serde_json::to_string(&text).unwrap();
        let written = if escaped {
            all_escaped(&text)
        } else {
            rendered.clone()
        };
        (written, rendered)
    })
}

/// A number, written as JSON text may write it: integers within and beyond 64 bits, `-0`, and
/// floats.
fn written_number() -> impl proptest::strategy::Strategy<Value = Written> {
    use proptest::prelude::*;
    prop_oneof![
        Just("0"),
        Just("-7"),
        Just("9223372036854775807"),
        Just("-9223372036854775808"),
        Just("18446744073709551616"),
        Just("-170141183460469231731687303715884105728"),
        Just("1234567890123456789012345678901234567890123456789012345678901234567890123456789"),
        Just("-0"),
        Just("-0.0"),
        Just("1.50"),
        Just("1E2"),
        Just("2.5e-3"),
        Just("0.12345678901234567891"),
    ]
    .prop_map(|number| Written {
        text: number.to_owned(),
        rendered: Some(number.to_owned()),
    })
}

/// An object of up to three fields of `value`, their keys escaped or not, some repeating a key.
fn written_object(
    value: impl proptest::strategy::Strategy<Value = Written>,
) -> impl proptest::strategy::Strategy<Value = Written> {
    use proptest::prelude::*;
    let key = prop_oneof![
        "[a-c]".prop_map(|key| (serde_json::to_string(&key).unwrap(), key)),
        "[a-c]".prop_map(|key| (all_escaped(&key), key)),
    ];
    prop::collection::vec((key, value), 0..4).prop_map(|fields| {
        let text = fields
            .iter()
            .map(|((written, _), value)| format!("{written}:{}", value.text))
            .collect::<Vec<_>>()
            .join(",");
        let mut keys: Vec<&String> = fields.iter().map(|((_, key), _)| key).collect();
        keys.sort();
        let unique = keys.windows(2).all(|pair| pair[0] != pair[1]);
        let rendered = fields
            .iter()
            .map(|((_, key), value)| {
                let key = serde_json::to_string(key).unwrap();
                value
                    .rendered
                    .as_ref()
                    .map(|value| format!("{key}:{value}"))
            })
            .collect::<Option<Vec<_>>>()
            .filter(|_| unique)
            .map(|fields| format!("{{{}}}", fields.join(",")));
        Written {
            text: format!("{{{text}}}"),
            rendered,
        }
    })
}

/// A value nesting up to four levels deep: scalars of every kind, and arrays and objects.
pub(super) fn written() -> impl proptest::strategy::Strategy<Value = Written> {
    use proptest::prelude::*;
    let scalar = prop_oneof![
        Just(Written {
            text: "null".to_owned(),
            rendered: Some("null".to_owned()),
        }),
        any::<bool>().prop_map(|value| Written {
            text: value.to_string(),
            rendered: Some(value.to_string()),
        }),
        written_number(),
        written_string().prop_map(|(text, rendered)| Written {
            text,
            rendered: Some(rendered),
        }),
    ];
    scalar.prop_recursive(4, 24, 4, |inner| {
        let array = prop::collection::vec(inner.clone(), 0..4).prop_map(|items| {
            let text: Vec<&str> = items.iter().map(|item| item.text.as_str()).collect();
            Written {
                text: format!("[{}]", text.join(",")),
                rendered: items
                    .iter()
                    .map(|item| item.rendered.clone())
                    .collect::<Option<Vec<_>>>()
                    .map(|items| format!("[{}]", items.join(","))),
            }
        });
        prop_oneof![array, written_object(inner)]
    })
}

proptest::proptest! {
    #[test]
    fn a_drifted_column_holds_what_render_writes_whichever_parse_built_it(
        value in written(),
        exact in proptest::bool::ANY,
        chunk_bytes in proptest::sample::select(vec![1_usize, 1 << 20]),
    ) {
        // `v` is a column of integers in the first record and an object in the last; the last
        // holds a float where `exact` says, so its chunk is built again with the exact parse.
        let last = if exact { r#"{"x":0.1}"# } else { r#"{"x":0}"# };
        let push = format!("{{\"v\":1}}\n{{\"v\":{},\"f\":1.5}}\n{{\"v\":{last}}}\n", value.text);
        let shredded = shredded(&[Bytes::from(push)], chunk_bytes);
        match value.rendered {
            None => proptest::prop_assert_eq!(shredded.unwrap_err().0, "json_duplicate_key"),
            Some(rendered) => {
                let batch = shredded.unwrap().unwrap();
                let column = batch.column_by_name("v").unwrap().as_string::<i32>();
                if rendered == "null" {
                    proptest::prop_assert!(column.is_null(1));
                } else {
                    proptest::prop_assert_eq!(column.value(1), rendered);
                }
            }
        }
    }
}

#[test]
fn a_string_holding_a_raw_control_character_is_invalid_json() {
    // A value of JSON text renders no longer than it was written only because of this.
    for control in ['\u{0}', '\u{1}', '\n', '\u{1f}'] {
        assert_eq!(
            refused(&format!("{{\"a\":\"x{control}y\"}}")),
            "json_invalid"
        );
    }
}

/// The batch `push` shreds into in chunks of `chunk_bytes`, every chunk with room for its
/// builders, and how many chunks building it parses again.
fn roomy_batch(push: &str, chunk_bytes: usize) -> (u64, RecordBatch) {
    let roomy = ShredLimits {
        admitted: 1 << 20,
        ..limits()
    };
    let pool = crate::compute::Pool::inline();
    let pushes = vec![Bytes::from(push.to_owned())];
    let observed = crate::compute::ready(super::observe(&pool, pushes, chunk_bytes, roomy));
    let observed = observed.unwrap();
    let rebuilt = observed.counts().rebuilt;
    let batches = crate::compute::ready(observed.build(&pool)).unwrap();
    let batch = arrow_select::concat::concat_batches(&batches[0].schema(), &batches).unwrap();
    (rebuilt, batch)
}

#[test]
fn a_float_after_scalars_rendered_as_json_has_its_chunk_parsed_again_exactly() {
    // An integer beyond 64 bits has the chunk parsed exactly before it is built, which renders
    // its digits: it is built once.
    for (float, again) in [("1.50", 1), ("-0", 1), ("18446744073709551616", 0)] {
        let push = format!("{{\"v\":1}}\n{{\"v\":\"x\"}}\n{{\"v\":{float}}}\n{{\"v\":true}}");
        let (rebuilt, batch) = roomy_batch(&push, 1 << 20);
        assert_eq!(rebuilt, again, "{float}");
        let expected = ["1", "\"x\"", float, "true"].map(|text| Some(text.to_owned()));
        assert_eq!(texts(&batch, 0), expected, "{float}");
    }
}

proptest::proptest! {
    #[test]
    fn scalars_of_any_kind_render_into_a_column_of_json_as_they_were_read(
        text in "\\PC{0,24}|[\\x00-\\x7f]{0,8}",
        int in proptest::num::i64::ANY,
        wide in (i64::MAX as u64 + 1)..=u64::MAX,
        huge in proptest::num::i128::ANY,
        flag in proptest::bool::ANY,
        order in proptest::strategy::Strategy::prop_shuffle(proptest::strategy::Just(
            (0..7).collect::<Vec<usize>>(),
        )),
        chunk_bytes in proptest::sample::select(vec![1_usize, 1 << 20]),
    ) {
        let huge = huge % 10_i128.pow(38);
        let kinds = [
            serde_json::to_string(&text).unwrap(),
            int.to_string(),
            wide.to_string(),
            huge.to_string(),
            "-1234567890123456789012345678901234567890".to_owned(),
            flag.to_string(),
            "null".to_owned(),
        ];
        let written: Vec<&String> = order.iter().map(|kind| &kinds[*kind]).collect();
        let push: Vec<String> = written.iter().map(|value| format!("{{\"v\":{value}}}")).collect();
        // Built where it is parsed, or fitted to the join: no chunk is parsed again.
        let (rebuilt, batch) = roomy_batch(&push.join("\n"), chunk_bytes);
        proptest::prop_assert_eq!(rebuilt, 0);
        let expected: Vec<Option<String>> = written
            .iter()
            .map(|value| (value.as_str() != "null").then(|| (*value).clone()))
            .collect();
        proptest::prop_assert_eq!(texts(&batch, 0), expected);
    }
}

/// Asserts that building `text`, once observed under a limit of `columns` columns, allocates at
/// its peak no more than its observation reckoned: what its push was admitted for beside its
/// text, and the excess beyond.
fn built_within_reckoning(text: &str, columns: u64) {
    let heap = &crate::cost::tests::HEAP;
    let pool = crate::compute::Pool::inline();
    let limits = ShredLimits::new(columns);
    let pushes = vec![Bytes::from(text.to_owned())];
    let observed = crate::compute::ready(super::observe(&pool, pushes, 1 << 20, limits)).unwrap();
    let reckoned = count(text.len()) * limits.admitted + observed.excess();
    heap.reset_peak_usage();
    let before = heap.current_usage();
    let batches = crate::compute::ready(observed.build(&pool)).unwrap();
    let peak = u64::try_from(heap.peak_usage().saturating_sub(before)).unwrap();
    drop(batches);
    assert!(
        peak <= reckoned,
        "{columns} columns: built at a peak of {peak}, reckoned {reckoned}"
    );
}

#[test]
fn a_chunk_built_again_renders_its_values_of_json_within_what_its_build_was_reckoned() {
    let text = drifting(&flat_object(262_145));
    for columns in [10_000, 1] {
        built_within_reckoning(&text, columns);
    }
}

/// Asserts that what observing `texts`, one push each, keeps alive until their batches are built
/// is within what the pushes were admitted for beside their texts and the excess reserved beyond.
///
/// That is what a partition still holds once its observing reservation shrinks to what building
/// wants. The pushes are cut into chunks of `chunk_bytes`. Returns whether each chunk is to be
/// built again.
fn kept_within_admission_and_excess(texts: &[&str], chunk_bytes: usize) -> Vec<bool> {
    let heap = &crate::cost::tests::HEAP;
    let pool = crate::compute::Pool::inline();
    let pushes: Vec<Bytes> = texts
        .iter()
        .map(|text| Bytes::from((*text).to_owned()))
        .collect();
    let before = heap.current_usage();
    let observed =
        crate::compute::ready(super::observe(&pool, pushes, chunk_bytes, limits())).unwrap();
    let kept = u64::try_from(heap.current_usage().saturating_sub(before)).unwrap();
    let total: usize = texts.iter().map(|text| text.len()).sum();
    let allowed = count(total) * limits().admitted + observed.excess();
    assert!(kept <= allowed, "kept {kept}, allowed {allowed}");
    observed.plans.iter().map(|plan| plan.again).collect()
}

#[test]
fn what_an_observation_keeps_until_its_build_is_within_its_admission_and_its_excess() {
    let keyed: Vec<String> = (0..600)
        .map(|record| format!("{{\"k{record}\":{record}}}"))
        .collect();
    let keyed = keyed.join("\n");
    let hundreds = hundreds("1", "2");
    let tripped = drifting(&wide_object(0, 10_000));
    let wide = wide_object(0, 3_000);
    let cases: [(&str, Vec<&str>, Vec<bool>); 4] = [
        // Records of a key each, observed past the chunk's allowance.
        ("keyed", vec![&keyed], vec![true]),
        // A chunk that keeps the columns it built.
        ("hundreds", vec![&hundreds], vec![false]),
        // A chunk whose rendering tripped its meter.
        ("tripped", vec![&tripped], vec![true]),
        // One record of three thousand columns, built again.
        ("wide", vec![&wide], vec![true]),
    ];
    for (name, texts, expected) in cases {
        let again = kept_within_admission_and_excess(&texts, 1 << 20);
        assert_eq!(again, expected, "{name}");
    }
    // Chunks of unlike shapes, one's surplus beside another's deficit.
    let pushes = [
        hundreds.as_str(),
        keyed.as_str(),
        hundreds.as_str(),
        wide.as_str(),
    ];
    let again = kept_within_admission_and_excess(&pushes, 1 << 14);
    assert!(again.len() > 1, "several: {again:?}");
}

#[test]
fn a_chunks_parse_is_held_until_its_build_whether_it_is_built_again_or_fitted() {
    let fitted = hundreds("1", "2");
    let again = r#"{"a":"x","n":1}"#.to_owned();
    let limits = ShredLimits {
        admitted: 0,
        ..limits()
    };
    let chunks = |extra: [u64; 2]| {
        let mut chunks = [parsed(&fitted), parsed(&again)];
        chunks[1].record = None;
        for (chunk, extra) in chunks.iter_mut().zip(extra) {
            chunk.spent += extra;
        }
        chunks
    };
    let (_, plans, base) = super::join(&chunks([0, 0]), limits).unwrap();
    let paths: Vec<bool> = plans.iter().map(|plan| plan.again).collect();
    assert_eq!(paths, [false, true]);
    for (spent, held) in [
        ([1_000, 0], 1_000),
        ([0, 7_000], 7_000),
        ([1_000, 7_000], 8_000),
    ] {
        let (_, _, excess) = super::join(&chunks(spent), limits).unwrap();
        assert_eq!(excess, base + held, "{spent:?}");
    }
}
