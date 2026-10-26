//! What a history stream's rows carry after their change time, each a value of the row's data
//! alone: a JSON document, written in a way the row picks, a float, a flag, two decimals, a date,
//! a time of day, a duration and bytes.
//!
//! The document's numbers are written in plain or scientific notation, with or without zeros a
//! value does not need, beyond 64 bits and beyond what plain notation keeps short; its names and
//! strings escaped or not, its members in either order, a null member written or left out, and
//! spaces between tokens or none. Every way reads as one value, so a row sending its key's data
//! again, written another way, opens no version.

#[cfg(test)]
mod tests;

use std::fmt::Write as _;
use std::sync::Arc;

use arrow_array::{
    ArrayRef, BinaryArray, BooleanArray, Date32Array, Decimal128Array, Decimal256Array,
    DurationMicrosecondArray, Float64Array, StringArray, Time64MicrosecondArray,
};
use arrow_buffer::i256;
use rdlt_connector::{DecimalType, Field, LogicalType, TimeUnit};
use serde_json::Value;

/// The data of one history row, and the way its document is written.
#[derive(Clone, Copy, Debug)]
pub(super) struct Data<'a> {
    pub(super) value: Option<&'a str>,
    pub(super) n: u64,
    pub(super) way: u64,
}

/// The fields of what a history stream's rows carry after their change time.
pub(super) fn fields() -> Vec<Field> {
    let decimal = |precision, scale| {
        LogicalType::Decimal(DecimalType::new(precision, scale).expect("a valid decimal"))
    };
    vec![
        Field::new("document", LogicalType::Json, true),
        Field::new("ratio", LogicalType::Float64, true),
        Field::new("flag", LogicalType::Bool, true),
        Field::new("amount", decimal(20, 4), true),
        Field::new("huge", decimal(60, 2), true),
        Field::new("day", LogicalType::Date, true),
        Field::new("clock", LogicalType::Time(TimeUnit::Microsecond), true),
        Field::new("span", LogicalType::Duration(TimeUnit::Microsecond), true),
        Field::new("bytes", LogicalType::Binary, true),
    ]
}

/// The columns of what `rows` carry, in the order of [`fields`]; nulls for a row without data.
pub(super) fn columns(rows: &[Option<Data<'_>>]) -> Vec<ArrayRef> {
    let amounts: Vec<Option<i128>> = rows
        .iter()
        .map(|row| row.map(|row| amount(row.n)))
        .collect();
    let amounts = Decimal128Array::from(amounts)
        .with_precision_and_scale(20, 4)
        .expect("amounts fit their decimal");
    let wide: Vec<Option<i256>> = rows.iter().map(|row| row.map(|row| huge(row.n))).collect();
    let wide = Decimal256Array::from(wide)
        .with_precision_and_scale(60, 2)
        .expect("huge amounts fit their decimal");
    let day = |n: u64| i32::try_from(n).map_or(i32::MAX, |n| n.saturating_add(19_000));
    let small = |n: u64| i64::try_from(n).unwrap_or(i64::MAX);
    vec![
        Arc::new(StringArray::from_iter(
            rows.iter()
                .map(|row| row.map(|row| document(row.value, row.n, row.way))),
        )),
        Arc::new(Float64Array::from_iter(
            rows.iter().map(|row| row.map(|row| ratio(row.n))),
        )),
        Arc::new(BooleanArray::from_iter(
            rows.iter().map(|row| row.map(|row| row.n % 2 == 1)),
        )),
        Arc::new(amounts),
        Arc::new(wide),
        Arc::new(Date32Array::from_iter(
            rows.iter().map(|row| row.map(|row| day(row.n))),
        )),
        Arc::new(Time64MicrosecondArray::from_iter(rows.iter().map(|row| {
            row.map(|row| small(row.n).saturating_mul(1_000_003) % 86_400_000_000)
        }))),
        Arc::new(DurationMicrosecondArray::from_iter(rows.iter().map(
            |row| row.map(|row| small(row.n).saturating_mul(3_600_000_001)),
        ))),
        Arc::new(BinaryArray::from_iter(
            rows.iter()
                .map(|row| row.and_then(|row| row.value).map(str::as_bytes)),
        )),
    ]
}

/// The amount of a row of counter `n`, unscaled at four places: the counter and none to three
/// quarters after its point, as the counter picks, negative for every third.
fn amount(n: u64) -> i128 {
    let quarters = i128::from(n % 4) * 2_500;
    let amount = i128::from(n) * 10_000 + quarters;
    if n.is_multiple_of(3) { -amount } else { amount }
}

/// The huge amount of a row of counter `n`, unscaled: the counter beyond 128 bits and again in
/// its last digits, negative for every fifth.
fn huge(n: u64) -> i256 {
    let counter = i256::from_i128(i128::from(n));
    let huge = counter
        .wrapping_mul(i256::from_i128(10_i128.pow(38)))
        .wrapping_mul(i256::from_i128(10))
        .wrapping_add(counter);
    if n.is_multiple_of(5) {
        huge.wrapping_neg()
    } else {
        huge
    }
}

/// The document of a row of `value` and counter `n`, written the way `way` picks.
fn document(value: Option<&str>, n: u64, way: u64) -> String {
    let written = |digits: u64, place: i32, negative: bool| number(digits, place, negative, way);
    let escaped = way & 0b1000 != 0;
    let string =
        |text: Option<&str>| text.map_or_else(|| "null".to_owned(), |text| quoted(text, escaped));
    let (colon, comma) = if way & 0b1_0000 == 0 {
        (":", ",")
    } else {
        (" : ", " ,\n\t")
    };
    let list = [
        written(n, 0, false),
        written(n, 0, true),
        "true".to_owned(),
        "null".to_owned(),
        string(value),
    ]
    .join(comma);
    let mut members = vec![
        ("n", written(n, 0, false)),
        ("half", written(n.saturating_mul(5), -1, false)),
        ("big", written(n, 24, false)),
        ("small", written(n, -30, false)),
        ("tiny", written(n, -420, false)),
        ("list", format!("[{list}]")),
        (
            "nested",
            format!("{{{}{colon}{}}}", quoted("value", escaped), string(value)),
        ),
    ];
    if way & 0b10_0000 != 0 {
        members.push(("gone", "null".to_owned()));
    }
    if way & 0b100 != 0 {
        members.reverse();
    }
    let members: Vec<String> = members
        .into_iter()
        .map(|(name, member)| format!("{}{colon}{member}", quoted(name, escaped)))
        .collect();
    format!(" {{{}}}\n", members.join(comma))
}

/// The float of a row of counter `n`: an eighth of it, beyond 2^64, below 2^-32 or with a half,
/// as the counter picks; negative zero for a counter of zero.
fn ratio(n: u64) -> f64 {
    #[expect(clippy::cast_precision_loss, reason = "counters are small")]
    let counter = n as f64;
    if n == 0 {
        return -0.0;
    }
    match n % 4 {
        0 => counter / 8.0,
        1 => -counter * 2_f64.powi(70),
        2 => counter * 2_f64.powi(-40),
        _ => counter + 0.5,
    }
}

/// `digits` times ten to `place`, negative where `negative` says, in the notation `way` picks.
fn number(digits: u64, place: i32, negative: bool, way: u64) -> String {
    let sign = if negative { "-" } else { "" };
    let digits = digits.to_string();
    match way % 4 {
        0 => format!("{sign}{digits}e{place}"),
        1 => {
            let (first, rest) = digits.split_at(1);
            let exponent = place + i32::try_from(rest.len()).unwrap_or(0);
            format!("{sign}{first}.{rest}0E{exponent:+}")
        }
        2 => format!("{sign}{}", plain(&digits, place, 2)),
        _ => format!("{sign}{}", plain(&digits, place, 0)),
    }
}

/// `digits` times ten to `place` in plain notation, with `zeros` more after the point.
fn plain(digits: &str, place: i32, zeros: usize) -> String {
    let after = |text: String| match zeros {
        0 => text,
        _ if text.contains('.') => format!("{text}{}", "0".repeat(zeros)),
        _ => format!("{text}.{}", "0".repeat(zeros)),
    };
    if digits == "0" {
        return after("0".to_owned());
    }
    let shift = usize::try_from(place.unsigned_abs()).unwrap_or(usize::MAX);
    if place >= 0 {
        return after(format!("{digits}{}", "0".repeat(shift)));
    }
    match digits.len().checked_sub(shift) {
        Some(whole) if whole > 0 => after(format!("{}.{}", &digits[..whole], &digits[whole..])),
        _ => after(format!("0.{}{digits}", "0".repeat(shift - digits.len()))),
    }
}

/// `text` as a JSON string, every character escaped by its code where `escaped` says.
fn quoted(text: &str, escaped: bool) -> String {
    if !escaped {
        return Value::from(text).to_string();
    }
    let mut quoted = String::from("\"");
    for unit in text.encode_utf16() {
        // Writing into a `String` cannot fail.
        let _ = write!(quoted, "\\u{unit:04x}");
    }
    quoted.push('"');
    quoted
}
