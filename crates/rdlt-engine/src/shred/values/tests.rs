use std::sync::Arc;

use arrow_array::cast::AsArray;
use arrow_array::types::{Decimal128Type, Float64Type, Int64Type};
use arrow_array::{Array, ArrayRef, RecordBatch, StringArray};
use rdlt_connector::cost::{Rendering, Stored};
use rdlt_connector::{DecimalType, Field, Fields, LogicalType};

use super::{fitting, read};
use crate::shred::observe::{Observed, Shape};

fn texts(values: &[Option<&str>]) -> StringArray {
    StringArray::from(values.to_vec())
}

fn fit(values: &[Option<&str>], column: &LogicalType) -> Vec<bool> {
    fitting(&texts(values), column)
        .0
        .iter()
        .map(|fits| fits.unwrap_or(false))
        .collect()
}

#[test]
fn a_value_fits_a_column_whose_type_holds_its_own_as_it_is() {
    let decimal = |precision| LogicalType::Decimal(DecimalType::new(precision, 0).unwrap());
    let values = [
        Some("1"),
        Some("\"x\""),
        Some("18446744073709551615"),
        Some("100000000000000000000"),
        Some("0.5"),
        Some("9007199254740993"),
        Some("true"),
        Some("null"),
        None,
        Some("{\"a\":1}"),
        Some("[1,2]"),
    ];
    let cases = [
        (
            LogicalType::Int64,
            [
                true, false, false, false, false, true, false, true, false, false, false,
            ],
        ),
        (
            LogicalType::Utf8,
            [
                false, true, false, false, false, false, false, true, false, false, false,
            ],
        ),
        (
            decimal(20),
            [
                true, false, true, false, false, true, false, true, false, false, false,
            ],
        ),
        (
            decimal(38),
            [
                true, false, true, true, false, true, false, true, false, false, false,
            ],
        ),
        // Floats hold an integer only where it is exact as one; every column holds a null.
        (
            LogicalType::Float64,
            [
                true, false, false, false, true, false, false, true, false, false, false,
            ],
        ),
        (
            LogicalType::Bool,
            [
                false, false, false, false, false, false, true, true, false, false, false,
            ],
        ),
    ];
    for (column, expected) in cases {
        assert_eq!(fit(&values, &column), expected, "{column:?}");
    }
}

#[test]
fn an_object_or_array_fits_a_column_whose_type_holds_each_of_its_values() {
    let object = LogicalType::Struct(
        Fields::new(vec![
            Field::new("a", LogicalType::Int64, true),
            Field::new("b", LogicalType::Utf8, true),
        ])
        .unwrap(),
    );
    assert_eq!(
        fit(
            &[Some("{\"a\":1}"), Some("{\"a\":\"x\"}"), Some("{\"c\":1}")],
            &object
        ),
        [true, false, false]
    );
    let list = LogicalType::List(Box::new(Field::new("item", LogicalType::Int64, true)));
    assert_eq!(
        fit(&[Some("[1,2]"), Some("[1,\"x\"]"), Some("[]")], &list),
        [true, false, true]
    );
    // Fields in any order, those lacking null; a field that may not be null must be there.
    let ordered = LogicalType::Struct(
        Fields::new(vec![
            Field::new("b", LogicalType::Float64, true),
            Field::new("a", LogicalType::Int64, false),
        ])
        .unwrap(),
    );
    assert_eq!(
        fit(
            &[
                Some("{\"a\":1,\"b\":2}"),
                Some("{\"a\":1}"),
                Some("{\"b\":2.5}"),
                Some("{\"a\":null,\"b\":2}"),
                Some("{\"a\":1,\"b\":9007199254740993}"),
            ],
            &ordered
        ),
        [true, true, false, false, false]
    );
    // Items that may not be null hold no array but an empty one, whose items are not known.
    let strict = LogicalType::List(Box::new(Field::new("item", LogicalType::Int64, false)));
    assert_eq!(fit(&[Some("[1]"), Some("[]")], &strict), [false, true]);
}

#[test]
fn values_read_into_their_join_exactly_and_the_rest_are_null() {
    let values = texts(&[
        Some("1"),
        Some("\"x\""),
        Some("100000000000000000000"),
        None,
    ]);
    let decimal = LogicalType::Decimal(DecimalType::new(38, 0).unwrap());
    let fit = fitting(&values, &decimal);
    let (column, joined) = read(&values, &fit.0, &fit.1).unwrap();
    assert_eq!(joined, decimal);
    let decimals = column.as_primitive::<Decimal128Type>();
    assert_eq!(decimals.value(0), 1);
    assert!(decimals.is_null(1) && decimals.is_null(3));
    assert_eq!(decimals.value(2), 100_000_000_000_000_000_000);
    let floats = texts(&[Some("0.5"), Some("2")]);
    let fit = fitting(&floats, &LogicalType::Float64);
    let (column, joined) = read(&floats, &fit.0, &fit.1).unwrap();
    assert_eq!(joined, LogicalType::Float64);
    assert_eq!(
        column.as_primitive::<Float64Type>().values().to_vec(),
        [0.5, 2.0]
    );
    let fit = fitting(&floats, &LogicalType::Bool);
    let (column, joined) = read(&floats, &fit.0, &fit.1).unwrap();
    assert_eq!(
        (joined, column.logical_null_count()),
        (LogicalType::Null, 2)
    );
    let ints = texts(&[Some("[1,2]")]);
    let of_ints = LogicalType::List(Box::new(Field::new("item", LogicalType::Int64, true)));
    let fit = fitting(&ints, &of_ints);
    let (column, _) = read(&ints, &fit.0, &fit.1).unwrap();
    let list = column.as_list::<i32>();
    assert_eq!(
        list.values().as_primitive::<Int64Type>().values().to_vec(),
        [1, 2]
    );
}

#[test]
fn a_value_the_shredder_refuses_or_that_repeats_a_key_fits_no_column() {
    // An object repeating a key fits no struct: its text stays JSON.
    let object =
        LogicalType::Struct(Fields::new(vec![Field::new("a", LogicalType::Int64, true)]).unwrap());
    let repeated = texts(&[Some("{\"a\":1,\"a\":2}"), Some("[{\"a\":1},{\"a\":2}]")]);
    assert!(!fitting(&repeated, &object).0.value(0));
    let list = LogicalType::List(Box::new(Field::new("item", object, true)));
    assert!(fitting(&repeated, &list).0.value(1));
    // Items of mixed kinds are not observed as objects, so only the key check sees a repeat.
    let mixed = LogicalType::List(Box::new(Field::new("item", LogicalType::Json, true)));
    let values = [
        Some("[1,{\"b\":1}]"),
        Some("[1,{\"b\":1,\"b\":2}]"),
        Some("[1,{\"b\":{\"c\":1,\"c\":2}}]"),
        Some("[1,{\"b\":{\"b\":1},\"c\":{\"b\":2}}]"),
    ];
    // A key an object nested in another names again repeats nothing.
    assert_eq!(fit(&values, &mixed), [true, false, false, true]);
    // A number beyond a float's range, text that is not JSON and nesting past the limit.
    let deep = format!("{}{}", "[".repeat(65), "]".repeat(65));
    let deep_item = LogicalType::List(Box::new(Field::new("item", LogicalType::Null, true)));
    for (text, column) in [
        ("1e400", LogicalType::Float64),
        ("-1e400", LogicalType::Float64),
        ("[1,", LogicalType::Int64),
        (deep.as_str(), deep_item),
    ] {
        assert_eq!(fit(&[Some(text)], &column), [false], "{text}");
    }
}

#[test]
fn a_number_the_shredder_refuses_inside_a_part_held_as_json_fits_nothing() {
    let column = struct_of(vec![Field::new("j", LogicalType::Json, true)]);
    let far = format!("{{\"j\":1e{}}}", "9".repeat(19));
    let values = [
        Some(r#"{"j":1e400}"#),
        Some(r#"{"j":[-1e400]}"#),
        Some(far.as_str()),
        Some(r#"{"j":1e300}"#),
    ];
    assert_eq!(fit(&values, &column), [false, false, false, true]);
}

#[test]
fn values_nested_to_the_limit_are_fitted_and_read_on_a_small_stack() {
    let fitted = std::thread::Builder::new()
        .stack_size(256 << 10)
        .spawn(|| {
            let mut fits = Vec::new();
            for depth in [63_usize, 64, 65] {
                let text = format!("{}1{}", "[".repeat(depth), "]".repeat(depth));
                let mut list = LogicalType::Int64;
                for _ in 0..depth {
                    list = LogicalType::List(Box::new(Field::new("item", list, true)));
                }
                let values = texts(&[Some(text.as_str())]);
                let fit = fitting(&values, &list);
                if fit.0.value(0) {
                    read(&values, &fit.0, &fit.1).unwrap();
                }
                fits.push(fit.0.value(0));
            }
            fits
        })
        .unwrap()
        .join()
        .unwrap();
    // The record counts as the first level: a value nests one level less than a record may.
    assert_eq!(fitted, [true, false, false]);
}

#[test]
fn a_key_repeated_after_a_list_is_found() {
    let lists = LogicalType::Struct(
        Fields::new(vec![Field::new(
            "l",
            LogicalType::List(Box::new(Field::new("item", LogicalType::Int64, true))),
            true,
        )])
        .unwrap(),
    );
    assert_eq!(fit(&[Some("{\"l\":[1],\"l\":[2]}")], &lists), [false]);
    assert_eq!(fit(&[Some("{\"l\":[1]}")], &lists), [true]);
}

/// Each value that is not null is observed once, to fit it; reading the fitting ones observes
/// none of them again.
#[test]
fn each_value_is_observed_once_from_fitting_to_reading() {
    let values = texts(&[Some("1"), Some("\"x\""), Some("{\"a\":1}"), None, Some("2")]);
    super::OBSERVED.with(|observed| observed.set(0));
    let fit = fitting(&values, &LogicalType::Int64);
    read(&values, &fit.0, &fit.1).unwrap();
    assert_eq!(super::OBSERVED.with(std::cell::Cell::get), 4);
}

fn struct_of(fields: Vec<Field>) -> LogicalType {
    LogicalType::Struct(Fields::new(fields).unwrap())
}

/// An object whose fields `fields` names, each observed as it says.
fn object(fields: Vec<(&str, Observed)>) -> Observed {
    let mut shape = Shape::default();
    for (name, observed) in fields {
        shape.push(name.into(), observed);
    }
    Observed::Object(shape)
}

#[test]
fn a_part_its_column_holds_as_json_is_observed_as_json_with_nothing_below_it() {
    let column = struct_of(vec![Field::new("j", LogicalType::Json, true)]);
    let digits = "1".repeat(40);
    let values = [
        r#"{"x":1,"y":[2]}"#,
        "[1]",
        "true",
        "1",
        "-1.5",
        r#""s""#,
        "18446744073709551615",
        "-9223372036854775809",
        "12345678901234567890123",
        digits.as_str(),
    ];
    for value in values {
        let text = format!("{{\"j\":{value}}}");
        let observed = super::observed(&text, &column, &super::context()).unwrap();
        assert_eq!(observed, object(vec![("j", Observed::Json)]), "{value}");
    }
    // A null stays null, which a field of JSON that may not be null still refuses.
    let observed = super::observed(r#"{"j":null}"#, &column, &super::context()).unwrap();
    assert_eq!(observed, object(vec![("j", Observed::Null)]));
    let strict = struct_of(vec![Field::new("j", LogicalType::Json, false)]);
    assert_eq!(
        fit(&[Some(r#"{"j":null}"#), Some(r#"{"j":{}}"#)], &strict),
        [false, true]
    );
}

#[test]
fn a_value_is_observed_no_further_than_a_key_its_struct_lacks_or_a_container_its_type_holds_none_of()
 {
    let a = struct_of(vec![Field::new("a", LogicalType::Int64, true)]);
    let of_ints = LogicalType::List(Box::new(Field::new("item", LogicalType::Int64, true)));
    let cases = [
        (r#"{"z":1,"a":1}"#, &a),
        (r#"{"a":1,"z":{"b":1}}"#, &a),
        (r#"{"a":[1]}"#, &a),
        (r#"{"a":1}"#, &LogicalType::Int64),
        ("[1]", &LogicalType::Int64),
        (r#"[1,{"a":1}]"#, &of_ints),
        ("[[1]]", &of_ints),
    ];
    for (text, column) in cases {
        let observed = super::observed(text, column, &super::context());
        assert!(observed.is_err(), "{text}: {observed:?}");
    }
    // What the type holds is observed whole.
    assert!(super::observed(r#"{"a":1}"#, &a, &super::context()).is_ok());
    assert!(super::observed("[1,2]", &of_ints, &super::context()).is_ok());
}

#[test]
fn a_part_its_column_holds_as_json_is_read_as_its_text_was_written() {
    let column = struct_of(vec![Field::new("a", LogicalType::Json, true)]);
    let cases: [(&[&str], &[&str]); 3] = [
        (
            &[r#"{"a":{"y":1}}"#, r#"{"a":{"x":2,"y":3}}"#],
            &[r#"{"y":1}"#, r#"{"x":2,"y":3}"#],
        ),
        (&[r#"{"a":1.50}"#, r#"{"a":1e2}"#], &["1.50", "1e2"]),
        // Alone, so nothing else it is joined with makes the field JSON.
        (
            &[r#"{"a":12345678901234567890123}"#],
            &["12345678901234567890123"],
        ),
    ];
    for (values, written) in cases {
        let values: Vec<_> = values.iter().copied().map(Some).collect();
        let values = texts(&values);
        let (fits, joined) = fitting(&values, &column);
        let (built, built_as) = read(&values, &fits, &joined).unwrap();
        let own = crate::table::convert::convert(&built, &built_as, &column).unwrap();
        let a = own.as_struct().column_by_name("a").unwrap();
        let a = a.as_string::<i32>();
        assert_eq!(a.null_count(), 0, "{values:?}");
        let held: Vec<_> = (0..a.len()).map(|row| a.value(row)).collect();
        assert_eq!(held, written);
    }
}

#[test]
fn a_value_observed_no_further_than_it_fits_adds_nothing_to_the_join() {
    let column = struct_of(vec![Field::new("a", LogicalType::Float64, true)]);
    let values = texts(&[Some(r#"{"a":1.5,"z":1}"#), Some(r#"{"a":1}"#)]);
    let (fits, joined) = fitting(&values, &column);
    assert_eq!(
        fits.iter().take(2).collect::<Vec<_>>(),
        [Some(false), Some(true)]
    );
    let (_, built_as) = read(&values, &fits, &joined).unwrap();
    assert_eq!(
        built_as,
        struct_of(vec![Field::new("a", LogicalType::Int64, true)])
    );
}

/// Bytes: what a lowering piece reserves for the rows of `texts`, JSON text whose own column is
/// of `column`, nothing beside, as the cost model measures it.
fn reserved(texts: &StringArray, column: &LogicalType) -> u64 {
    let values: ArrayRef = Arc::new(texts.clone());
    let batch = RecordBatch::try_from_iter([("c", values)]).unwrap();
    let stored = Stored {
        column: column.clone(),
        text: false,
        read: true,
    };
    let mut measure = Rendering::native().lowering(&batch, vec![Some(stored)], 0, u64::MAX);
    measure.expanded(0..batch.num_rows())
}

/// What fitting `texts` to `column` allocates at its peak, and which of them fit.
fn fitted_peak(texts: &StringArray, column: &LogicalType) -> (u64, Vec<bool>) {
    let heap = &crate::cost::tests::HEAP;
    heap.reset_peak_usage();
    let before = heap.current_usage();
    let (fits, joined) = fitting(texts, column);
    let peak = u64::try_from(heap.peak_usage().saturating_sub(before)).unwrap();
    drop(joined);
    let fits = fits
        .iter()
        .take(texts.len())
        .map(|fit| fit.unwrap_or(false));
    (peak, fits.collect())
}

/// An object of `keys` keys, each `prefix` and its number, each holding 0.
fn wide(prefix: &str, keys: usize) -> String {
    let fields: Vec<String> = (0..keys)
        .map(|key| format!("\"{prefix}{key}\":0"))
        .collect();
    format!("{{{}}}", fields.join(","))
}

/// Asserts that fitting a single value `text` to `column` fits as `fit` says, and allocates no
/// more than its piece reserves for reading it.
fn fitted_within_reservation(text: &str, column: &LogicalType, fit: bool) {
    let texts = texts(&[Some(text)]);
    let reserved = reserved(&texts, column);
    let (peak, fits) = fitted_peak(&texts, column);
    assert_eq!(fits, [fit], "{text:.32}");
    assert!(
        peak <= reserved,
        "{text:.32}: fitted at a peak of {peak}, reserved {reserved}"
    );
}

#[test]
fn a_value_that_cannot_fit_its_column_is_fitted_within_what_its_piece_reserves() {
    let a = struct_of(vec![Field::new("a", LogicalType::Int64, true)]);
    let object = wide("z", 100_000);
    // Keys the struct lacks before the key it holds, and an object where an integer is due.
    let lacking = format!("{},\"a\":1}}", object.trim_end_matches('}'));
    fitted_within_reservation(&lacking, &a, false);
    fitted_within_reservation(&object, &LogicalType::Int64, false);
}

#[test]
fn a_part_held_as_json_nested_to_the_limit_is_fitted_on_a_small_stack() {
    let fitted = std::thread::Builder::new()
        .stack_size(256 << 10)
        .spawn(|| {
            let mut fits = Vec::new();
            for depth in [63_usize, 64, 65] {
                let text = format!("{}1{}", "[".repeat(depth), "]".repeat(depth));
                // Lists around a part held as JSON, the innermost array.
                let mut column = LogicalType::Json;
                for _ in 1..depth {
                    column = LogicalType::List(Box::new(Field::new("item", column, true)));
                }
                let values = texts(&[Some(text.as_str())]);
                let fit = fitting(&values, &column);
                if fit.0.value(0) {
                    read(&values, &fit.0, &fit.1).unwrap();
                }
                fits.push(fit.0.value(0));
            }
            fits
        })
        .unwrap()
        .join()
        .unwrap();
    assert_eq!(fitted, [true, false, false]);
}

#[test]
fn a_key_written_escaped_repeats_the_same_key_written_plain() {
    let a = struct_of(vec![Field::new("a", LogicalType::Int64, true)]);
    assert_eq!(
        fit(&[Some(r#"{"\u0061":1,"a":2}"#), Some(r#"{"a":2}"#)], &a),
        [false, true]
    );
}

#[test]
fn a_wide_object_in_a_part_held_as_json_is_fitted_within_what_its_piece_reserves() {
    let j = struct_of(vec![Field::new("j", LogicalType::Json, true)]);
    let object = wide("k", 100_000);
    let repeating = format!("{},\"k0\":1}}", object.trim_end_matches('}'));
    // A string of colons, each of which might follow a key.
    let colons = format!("\"{}\"", ":".repeat(10_000));
    // The shortest keys, one more than a power of two of them, where a list grown by doubling
    // would hold twice what the object needs.
    let shortest = format!("{{{}}}", vec!["\"\":0"; 65_537].join(","));
    for (part, fit) in [
        (&object, true),
        (&repeating, false),
        (&colons, true),
        (&shortest, false),
    ] {
        fitted_within_reservation(&format!("{{\"j\":{part}}}"), &j, fit);
    }
}

#[test]
fn a_value_is_checked_apart_for_a_repeated_key_only_where_its_column_holds_a_part_as_json() {
    let checked = |values: &[Option<&str>], column: &LogicalType| {
        super::CHECKED.with(|checked| checked.set(0));
        fitting(&texts(values), column);
        super::CHECKED.with(std::cell::Cell::get)
    };
    let objects = [Some(r#"{"a":1}"#), Some(r#"{"a":2}"#)];
    let a = struct_of(vec![Field::new("a", LogicalType::Int64, true)]);
    assert_eq!(checked(&objects, &a), 0);
    let json_a = struct_of(vec![Field::new("a", LogicalType::Json, true)]);
    assert_eq!(checked(&objects, &json_a), 2);
    let of_json = LogicalType::List(Box::new(Field::new("item", LogicalType::Json, true)));
    assert_eq!(checked(&[Some("[1]")], &of_json), 1);
    let items = LogicalType::List(Box::new(Field::new("item", json_a, true)));
    let deep = struct_of(vec![Field::new("l", items, true)]);
    assert_eq!(checked(&[Some(r#"{"l":[{"a":1}]}"#)], &deep), 1);
}

#[test]
fn a_part_held_as_json_still_refuses_a_key_its_objects_repeat_at_any_depth() {
    let j = struct_of(vec![Field::new("j", LogicalType::Json, true)]);
    let values = [
        Some(r#"{"j":{"a":1,"a":2}}"#),
        Some(r#"{"j":{"a":1}}"#),
        Some(r#"{"j":[{"a":1},{"a":2}]}"#),
        Some(r#"{"j":{"b":{"a":1},"c":{"a":2}}}"#),
        Some(r#"{"j":{"b":{"a":1},"a":{"a":2},"b":3}}"#),
        Some(r#"{"j":{"a":1,"\u0061":2}}"#),
        Some(r#"{"j":{"b":1,"a":2}}"#),
    ];
    // Each value is checked afresh, whatever the value before it found.
    assert_eq!(
        fit(&values, &j),
        [false, true, true, true, false, false, true]
    );
    let items = LogicalType::List(Box::new(Field::new("item", j, true)));
    assert_eq!(
        fit(
            &[Some(r#"[{"j":{"b":1,"b":2}}]"#), Some(r#"[{"j":{"b":1}}]"#)],
            &items
        ),
        [false, true]
    );
    let json = Field::new("item", LogicalType::Json, true);
    let lists = LogicalType::List(Box::new(Field::new(
        "item",
        LogicalType::List(Box::new(json)),
        true,
    )));
    assert_eq!(
        fit(
            &[Some(r#"[[{"c":1,"c":2}]]"#), Some(r#"[[{"c":1}]]"#)],
            &lists
        ),
        [false, true]
    );
}

#[test]
fn fitting_a_part_held_as_json_holds_no_more_than_twice_its_text_and_a_hundred_bytes() {
    let column = LogicalType::Json;
    let object = wide("k", 100_000);
    // Keys written escaped, each of which is copied to be compared.
    let escaped = format!(
        "{{{}}}",
        (0..20_000)
            .map(|key| format!("\"\\u006b{key}\":0"))
            .collect::<Vec<_>>()
            .join(",")
    );
    let repeating = format!("{},\"k0\":1}}", object.trim_end_matches('}'));
    let deep = format!("{}1{}", "[".repeat(60), "]".repeat(60));
    for text in [&object, &escaped, &repeating, &deep] {
        let (peak, _) = fitted_peak(&texts(&[Some(text.as_str())]), &column);
        let bound = 2 * u64::try_from(text.len()).unwrap() + 100;
        assert!(peak <= bound, "{text:.32}: peak {peak}, bound {bound}");
    }
}
