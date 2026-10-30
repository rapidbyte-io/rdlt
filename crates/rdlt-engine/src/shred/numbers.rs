//! Numbers as JSON text writes them, generated with what each one is read from its text alone,
//! and a column of them held to that whichever way it is shredded.

use arrow_array::RecordBatch;
use arrow_array::cast::AsArray;
use arrow_array::types::{Decimal128Type, Decimal256Type, Float64Type, Int64Type};
use arrow_buffer::i256;
use bytes::Bytes;
use proptest::prelude::*;
use rdlt_connector::{DecimalType, LogicalType, TableSchema};

use super::Sizing;
use super::differential::shredded_within;
use crate::error::{Code, codes};

/// What a written number is, read from its text alone.
#[derive(Clone, Debug, PartialEq)]
pub(super) enum Model {
    /// An integer within a signed 64-bit one.
    Int(i64),
    /// An integer only an unsigned 64-bit one holds.
    Wide(u64),
    /// An integer beyond 64 bits, of at most 38 digits.
    Huge(i128),
    /// An integer of 39 to 76 digits, as written.
    Vast(String),
    /// An integer beyond 76 digits.
    Beyond,
    /// A number with a fraction or an exponent, or a negative zero: the float `str::parse` reads,
    /// zero without its sign.
    Float(f64),
    /// A number its record is refused for, with the refusal's code.
    Refused(Code),
}

/// A number as JSON text writes it, and what it is.
#[derive(Clone, Debug)]
pub(super) struct Number {
    pub(super) text: String,
    pub(super) model: Model,
}

impl Number {
    /// The number written `text`, with what it is.
    fn new(text: String) -> Self {
        let model = model(&text);
        Self { text, model }
    }

    /// Whether its record is refused.
    pub(super) fn refused(&self) -> bool {
        matches!(self.model, Model::Refused(_))
    }
}

/// What the JSON number `text` is.
///
/// An exponent of more than 18 digits beside its leading zeros exceeds the limit, and a float
/// beyond the finite ones is not JSON the engine reads; an integer is read by its width.
fn model(text: &str) -> Model {
    let unsigned = text.strip_prefix('-').unwrap_or(text);
    let (mantissa, exponent) = unsigned
        .split_once(['e', 'E'])
        .map_or((unsigned, None), |(mantissa, exponent)| {
            (mantissa, Some(exponent))
        });
    let significant = exponent.map_or(0, |exponent| {
        exponent
            .trim_start_matches(['+', '-'])
            .trim_start_matches('0')
            .len()
    });
    if significant > 18 {
        return Model::Refused(codes::LIMIT_EXCEEDED);
    }
    if exponent.is_some() || mantissa.contains('.') || text == "-0" {
        let float: f64 = text.parse().expect("a JSON number reads as a float");
        if float.is_infinite() {
            return Model::Refused(codes::JSON_INVALID);
        }
        return Model::Float(if float == 0.0 { 0.0 } else { float });
    }
    if let Ok(integer) = text.parse::<i64>() {
        return Model::Int(integer);
    }
    if let Ok(integer) = text.parse::<u64>() {
        return Model::Wide(integer);
    }
    match unsigned.len() {
        ..=38 => Model::Huge(text.parse().expect("38 digits fit a 128-bit integer")),
        39..=76 => Model::Vast(text.to_owned()),
        _ => Model::Beyond,
    }
}

/// The digits of an integer within two of a bound where its type changes: where floats stop
/// holding every integer, past a signed and an unsigned 64-bit one, and past 38 and 76 digits.
fn near_a_bound() -> impl Strategy<Value = String> {
    let bounds = [
        "9007199254740992".to_owned(),
        "9223372036854775808".to_owned(),
        "18446744073709551616".to_owned(),
        format!("1{}", "0".repeat(38)),
        format!("1{}", "0".repeat(76)),
    ];
    (prop::sample::select(Vec::from(bounds)), -2_i128..=2).prop_map(|(bound, offset)| {
        let bound = i256::from_string(&bound).expect("a bound is an integer");
        bound.wrapping_add(i256::from_i128(offset)).to_string()
    })
}

/// The digits of an integer as JSON writes them: a lone zero, 1 to 80 digits led by one that is
/// not, or digits near a bound.
fn digits() -> impl Strategy<Value = String> {
    prop_oneof![
        3 => "0|[1-9][0-9]{0,79}",
        1 => near_a_bound(),
    ]
}

/// An integer as JSON may write it, with or without a sign.
fn integer() -> impl Strategy<Value = Number> {
    (any::<bool>(), digits()).prop_map(|(negative, digits)| {
        Number::new(format!("{}{digits}", if negative { "-" } else { "" }))
    })
}

/// A number as JSON may write it: an integer, then perhaps a fraction, its digits led by zeros
/// or not, and perhaps an exponent of 1 to 30 digits, signed or not, led by zeros or not.
pub(super) fn number() -> impl Strategy<Value = Number> {
    let fraction = prop_oneof![
        3 => Just(String::new()),
        1 => "\\.[0-9]{1,24}",
    ];
    let exponent = prop_oneof![
        3 => Just(String::new()),
        2 => "[eE][+-]?[0-9]{1,3}",
        1 => "[eE][+-]?0{1,27}[1-9][0-9]{0,2}",
        1 => "[eE][+-]?[1-9][0-9]{16,29}",
    ];
    (integer(), fraction, exponent).prop_map(|(integer, fraction, exponent)| {
        Number::new(format!("{}{fraction}{exponent}", integer.text))
    })
}

/// A decimal type of `precision` digits and no scale.
fn decimal(precision: u8) -> LogicalType {
    LogicalType::Decimal(DecimalType::new(precision, 0).expect("a decimal type"))
}

/// The type a column of `numbers` takes: the narrowest that holds every integer by its width,
/// floats with integers a float holds exactly, and JSON text otherwise.
fn joined(numbers: &[Number]) -> LogicalType {
    let floats = numbers
        .iter()
        .any(|number| matches!(number.model, Model::Float(_)));
    if floats {
        let exact = numbers.iter().all(|number| match number.model {
            Model::Float(_) => true,
            Model::Int(integer) => integer.unsigned_abs() <= 1 << 53,
            _ => false,
        });
        return if exact {
            LogicalType::Float64
        } else {
            LogicalType::Json
        };
    }
    let widest = numbers
        .iter()
        .map(|number| match number.model {
            Model::Int(_) => 0,
            Model::Wide(_) => 1,
            Model::Huge(_) => 2,
            Model::Vast(_) => 3,
            _ => 4,
        })
        .max();
    match widest {
        Some(0) => LogicalType::Int64,
        Some(1) => decimal(20),
        Some(2) => decimal(38),
        Some(3) => decimal(76),
        _ => LogicalType::Json,
    }
}

/// The float `value` as a column's value is written for comparison: the float and its bits.
fn float(value: f64) -> String {
    format!("{value:?} {:#x}", value.to_bits())
}

/// The value `number` holds in a column of `logical`, written as [`column`] writes it.
fn value(number: &Number, logical: &LogicalType) -> String {
    match (logical, &number.model) {
        (LogicalType::Json, _) => number.text.clone(),
        (LogicalType::Float64, _) => {
            let read: f64 = number.text.parse().expect("a JSON number reads as a float");
            float(if read == 0.0 { 0.0 } else { read })
        }
        (_, Model::Int(integer)) => integer.to_string(),
        (_, Model::Wide(integer)) => integer.to_string(),
        (_, Model::Huge(integer)) => integer.to_string(),
        (_, Model::Vast(digits)) => digits.clone(),
        (_, model) => panic!("{model:?} has no value in a column of {logical:?}"),
    }
}

/// What a column of `numbers`, one a record, shreds into: its type and each value, or the code of
/// the first refused number's refusal.
fn expected(numbers: &[Number]) -> Result<(LogicalType, Vec<String>), Code> {
    let refused = numbers.iter().find_map(|number| match number.model {
        Model::Refused(code) => Some(code),
        _ => None,
    });
    if let Some(code) = refused {
        return Err(code);
    }
    let logical = joined(numbers);
    let values = numbers
        .iter()
        .map(|number| value(number, &logical))
        .collect();
    Ok((logical, values))
}

/// The type of `batch`'s one column and each value it holds, as [`expected`] writes them.
fn column(batch: &RecordBatch) -> (LogicalType, Vec<String>) {
    let schema = TableSchema::from_arrow(&batch.schema()).expect("a shredded schema");
    let logical = schema
        .fields()
        .iter()
        .next()
        .expect("a column")
        .logical_type()
        .clone();
    let column = batch.column(0);
    let values = match &logical {
        LogicalType::Int64 => (column.as_primitive::<Int64Type>().values().iter())
            .map(ToString::to_string)
            .collect(),
        LogicalType::Float64 => (column.as_primitive::<Float64Type>().values().iter())
            .map(|value| float(*value))
            .collect(),
        LogicalType::Decimal(decimal) if decimal.precision() <= 38 => {
            (column.as_primitive::<Decimal128Type>().values().iter())
                .map(ToString::to_string)
                .collect()
        }
        LogicalType::Decimal(_) => (column.as_primitive::<Decimal256Type>().values().iter())
            .map(ToString::to_string)
            .collect(),
        LogicalType::Json => (column.as_string::<i32>().iter())
            .map(|text| text.expect("a number is no null").to_owned())
            .collect(),
        other => panic!("a column of numbers is {other:?}"),
    };
    (logical, values)
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(rdlt_testkit::cases(512)))]

    #[test]
    fn a_column_of_numbers_takes_the_type_and_values_its_numbers_name(
        // Half the columns hold integers alone, as a column of any numbers rarely does.
        numbers in prop_oneof![
            prop::collection::vec(integer(), 1..6),
            prop::collection::vec(number(), 1..6),
        ],
    ) {
        let records: Vec<String> = numbers
            .iter()
            .map(|number| format!("{{\"n\":{}}}", number.text))
            .collect();
        let push = records.join("\n");
        let expected = expected(&numbers);
        for (chunk_bytes, limits) in super::tests::ways() {
            let shredded = shredded_within(&[Bytes::from(push.clone())], Sizing::fixed(chunk_bytes), limits)
                .map(|batch| column(&batch.expect("a record a number")));
            prop_assert_eq!(&shredded, &expected, "{} in chunks of {}", push, chunk_bytes);
        }
    }
}
