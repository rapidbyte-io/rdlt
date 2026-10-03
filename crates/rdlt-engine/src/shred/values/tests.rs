use arrow_array::cast::AsArray;
use arrow_array::types::{Decimal128Type, Float64Type, Int64Type};
use arrow_array::{Array, BooleanArray, StringArray};
use rdlt_connector::{DecimalType, Field, Fields, LogicalType};

use super::{fitting, read};

fn texts(values: &[Option<&str>]) -> StringArray {
    StringArray::from(values.to_vec())
}

fn fit(values: &[Option<&str>], column: &LogicalType) -> Vec<bool> {
    fitting(&texts(values), column)
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
                true, false, false, false, false, true, false, false, false, false, false,
            ],
        ),
        (
            LogicalType::Utf8,
            [
                false, true, false, false, false, false, false, false, false, false, false,
            ],
        ),
        (
            decimal(20),
            [
                true, false, true, false, false, true, false, false, false, false, false,
            ],
        ),
        (
            decimal(38),
            [
                true, false, true, true, false, true, false, false, false, false, false,
            ],
        ),
        // Floats hold an integer only where it is exact as one.
        (
            LogicalType::Float64,
            [
                true, false, false, false, true, false, false, false, false, false, false,
            ],
        ),
        (
            LogicalType::Bool,
            [
                false, false, false, false, false, false, true, false, false, false, false,
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
}

#[test]
fn values_read_into_their_join_exactly_and_the_rest_are_null() {
    let values = texts(&[
        Some("1"),
        Some("\"x\""),
        Some("100000000000000000000"),
        None,
    ]);
    let taken = BooleanArray::from(vec![true, false, true, true]);
    let (column, joined) = read(&values, &taken).unwrap();
    assert_eq!(
        joined,
        LogicalType::Decimal(DecimalType::new(38, 0).unwrap())
    );
    let decimals = column.as_primitive::<Decimal128Type>();
    assert_eq!(decimals.value(0), 1);
    assert!(decimals.is_null(1) && decimals.is_null(3));
    assert_eq!(decimals.value(2), 100_000_000_000_000_000_000);
    let floats = texts(&[Some("0.5"), Some("2")]);
    let (column, joined) = read(&floats, &BooleanArray::from(vec![true, true])).unwrap();
    assert_eq!(joined, LogicalType::Float64);
    assert_eq!(
        column.as_primitive::<Float64Type>().values().to_vec(),
        [0.5, 2.0]
    );
    let (column, joined) = read(&floats, &BooleanArray::from(vec![false, false])).unwrap();
    assert_eq!(
        (joined, column.logical_null_count()),
        (LogicalType::Null, 2)
    );
    let ints = texts(&[Some("[1,2]")]);
    let (column, _) = read(&ints, &BooleanArray::from(vec![true])).unwrap();
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
    assert!(!fitting(&repeated, &object).value(0));
    let list = LogicalType::List(Box::new(Field::new("item", object, true)));
    assert!(fitting(&repeated, &list).value(1));
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
