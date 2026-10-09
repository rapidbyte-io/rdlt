use std::num::NonZeroUsize;
use std::sync::Arc;

use arrow_array::cast::AsArray;
use arrow_array::types::{Decimal128Type, Decimal256Type, Float64Type, Int64Type};
use arrow_array::{Array, RecordBatch};
use bytes::Bytes;
use rdlt_connector::limits::{MAX_COLUMNS, MAX_NESTING_DEPTH};
use rdlt_connector::{DecimalType, Field, LogicalType, TableSchema};

use super::differential::shredded;
use super::meter::{KEY, OBJECT_SHAPE};
use super::reference::Code;
use super::{Parsed, ShredLimits, chunks, parse, shred};
use crate::compute::{Cores, RayonPool};

thread_local! {
    /// How many chunks this thread built again.
    pub(super) static BUILT_AGAIN: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

/// The limits of pushes whose records may hold as many columns as the wire's schemas.
pub(crate) fn limits() -> ShredLimits {
    ShredLimits::new(MAX_COLUMNS)
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
    // Alone, and after a float that sends its chunk through the exact parse.
    for invalid_utf8 in [
        &b"{\"a\":\"\xff\"}"[..],
        b"{\"a\":1e30}\n{\"a\":\"\xff\"}",
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
    // after a float that may be a rounded integer, the chunk is parsed exactly.
    for before in ["", "{\"a\":1}\n", "{\"a\":1e30}\n"] {
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
fn a_value_nested_past_the_limit_is_refused_even_where_the_chunk_is_parsed_exactly() {
    let depth = usize::try_from(MAX_NESTING_DEPTH).unwrap();
    // Each prefix sends the record, or its chunk, through the exact parse.
    let prefixes = [
        format!("{{\"n\":{},\"a\":", "9".repeat(400)),
        "{\"n\":1e400,\"a\":".to_owned(),
        "{\"n\":1e30}\n{\"a\":".to_owned(),
    ];
    for prefix in &prefixes {
        for levels in [depth + 1, 1_000_000] {
            let deep = format!("{prefix}{}{}}}", "[".repeat(levels), "]".repeat(levels));
            assert_eq!(refused(&deep), "limit_exceeded", "{prefix:?} at {levels}");
        }
    }
    let beside = format!(
        "{{\"n\":1e30}}\n{{\"a\":{}{}}}",
        "[".repeat(1_000_000),
        "]".repeat(1_000_000)
    );
    assert_eq!(refused(&beside), "limit_exceeded");
    // An empty container at the limit is a value at the limit.
    let levels = depth - 1;
    let at_limit = format!(
        "{{\"n\":1e30,\"a\":{}{}}}",
        "[".repeat(levels),
        "]".repeat(levels)
    );
    assert_eq!(batch_of(&[&at_limit], 1 << 20).num_rows(), 1);
    // Containers side by side nest no deeper than one.
    let siblings = format!("{{\"n\":1e30,\"a\":[{}]}}", vec!["[]"; 1_000].join(","));
    assert_eq!(batch_of(&[&siblings], 1 << 20).num_rows(), 1);
    // Brackets inside strings, even escaped quotes before them, nest nothing.
    let text = format!("\\\"{}", "[".repeat(1_000));
    let texts = batch_of(&[&format!("{{\"n\":1e30,\"a\":\"{text}\"}}")], 1 << 20);
    assert_eq!(texts.num_rows(), 1);
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
    let pool =
        RayonPool::try_new(Cores::new(NonZeroUsize::new(5).unwrap(), NonZeroUsize::MIN)).unwrap();
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
    assert!(parsed("{\"a\":1}\n{\"a\":\"x\"}").spoiled);
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
    let columns = usize::try_from(MAX_COLUMNS).unwrap();
    // The invalid record after it, which adds no field, is never reached.
    let push = format!("{}\n{{\"c0\":", wide_object(0, columns + 1));
    let error = crate::compute::ready(shred(
        &crate::compute::Inline,
        &[Bytes::from(push)],
        1 << 20,
        limits(),
    ));
    assert_eq!(
        error.unwrap_err(),
        super::ShredError::TooManyColumns(MAX_COLUMNS + 1, MAX_COLUMNS)
    );
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
        let rows = u64::try_from(parsed.chunk.rows).unwrap();
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
        parsed(r#"{"a":"x"}"#),
    ];
    let joined = super::join(&chunks[..3], limits()).unwrap().0;
    for chunk in &chunks[..3] {
        assert!(!chunk.spoiled && super::conform::shape_fits(&chunk.shape, &joined));
    }
    let with_text = super::join(&chunks, limits()).unwrap().0;
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
    let push = format!("{{\"a\":1}}\n{{\"password\":\"{secret}\",\"b\":}}");
    let error = crate::compute::ready(shred(
        &crate::compute::Inline,
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
    let later =
        crate::compute::ready(shred(&crate::compute::Inline, &pushes, 1, limits())).unwrap_err();
    assert!(later.to_string().contains("record 4:"), "{later}");
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
            &crate::compute::Inline,
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
                        MAX_NESTING_DEPTH / 2
                    } else {
                        MAX_NESTING_DEPTH
                    };
                    let deepest = format!("{before}{}", nested(levels, open, close));
                    assert!(shred_here(deepest).is_ok());
                    let deeper = format!("{before}{}", nested(MAX_NESTING_DEPTH + 1, open, close));
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
            &crate::compute::Inline,
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
        let pushes = [Bytes::from(text)];
        crate::compute::ready(super::observe(
            &crate::compute::Inline,
            &pushes,
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
        crate::compute::ready(shred(&crate::compute::Inline, &pushes, 1, limits)).map(|_| ())
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
        &crate::compute::Inline,
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

/// How many chunks shredding `push` in one chunk builds again, and the batch.
fn built_again(push: &str) -> (usize, RecordBatch) {
    let before = BUILT_AGAIN.with(std::cell::Cell::get);
    let batch = batch_of(&[push], 1 << 20);
    (BUILT_AGAIN.with(std::cell::Cell::get) - before, batch)
}

/// A hundred records of `first` and a hundred of `then`, each `{"a": value}` beside a note:
/// enough text that the chunk's allowance holds its builders, the widened ones too.
fn hundreds(first: &str, then: &str) -> String {
    let note = "x".repeat(64);
    let records = |value: &str| vec![format!("{{\"a\":{value},\"n\":\"{note}\"}}"); 100].join("\n");
    format!("{}\n{}", records(first), records(then))
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
    // A string after integers spoils the column, which is built again as JSON text.
    let (again, _) = built_again(&hundreds("1", "\"x\""));
    assert_eq!(again, 1);
}

#[test]
fn a_chunk_is_parsed_exactly_only_where_a_zero_may_be_a_vast_exponent() {
    let lookalike = "\"1e1234567890123456789\"";
    assert!(!parsed("{\"a\":0.0}").exact);
    assert!(!parsed(&format!("{{\"a\":1.5,\"n\":{lookalike}}}")).exact);
    assert!(parsed(&format!("{{\"a\":0.0,\"n\":{lookalike}}}")).exact);
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
    // A float that may be a rounded integer has the chunk observed exactly, which reads an
    // integer beyond 38 digits as its digits.
    let vast = "1".repeat(42);
    assert_eq!(
        observed_only(&format!("{{\"a\":1e30}}\n{vast}")).err(),
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
    // A whole float beyond 64 bits, which may be an integer the fast parse rounded: its chunk is
    // parsed exactly, and a build planned otherwise is a fault, not a guess.
    let text = "{\"a\":1e20}";
    let shape = parsed(text).shape;
    let chunk = || chunks(&[Bytes::from(text)], 1 << 20).unwrap().remove(0);
    let built = super::again(&chunk(), &shape, true).unwrap();
    assert_eq!(
        built[0].as_primitive::<Float64Type>().value(0).to_bits(),
        1e20_f64.to_bits()
    );
    assert!(matches!(
        super::again(&chunk(), &shape, false),
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
    assert_eq!(parse_once(nested(MAX_NESTING_DEPTH, "[", "]")), Ok(true));
    assert_eq!(
        parse_once(nested(MAX_NESTING_DEPTH + 1, "[", "]")).err(),
        Some(super::ShredError::TooDeep)
    );
}
