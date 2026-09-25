//! Stored cells read back as what they mean: native values, their text, and JSON, read with the
//! type they were written from.

mod json;
#[cfg(test)]
mod tests;

use arrow_array::Array;
use arrow_array::cast::AsArray;
use arrow_array::types::{
    Date32Type, Decimal128Type, Decimal256Type, DurationMicrosecondType, DurationMillisecondType,
    DurationNanosecondType, DurationSecondType, Float32Type, Float64Type, Int8Type, Int16Type,
    Int32Type, Int64Type, Time32MillisecondType, Time32SecondType, Time64MicrosecondType,
    Time64NanosecondType, TimestampMicrosecondType, TimestampMillisecondType,
    TimestampNanosecondType, TimestampSecondType,
};
use arrow_schema::{DataType, TimeUnit as ArrowUnit};
use rdlt_connector::{Field, Fields, LogicalType};

use self::json::{Json, parse};
use crate::canon::{Canon, DAY, decimal, float, hex, uuid};

/// How to read the values of a column of `column` that received values of `source`: the column's
/// type, but the source's wherever the column holds `Json`, which the source's values were
/// rendered into.
pub fn hint(column: &LogicalType, source: Option<&LogicalType>) -> LogicalType {
    use LogicalType as T;
    match (column, source) {
        (T::Json, Some(source)) => source.clone(),
        (T::Struct(fields), Some(T::Struct(sources))) => T::Struct(
            Fields::new(
                fields
                    .iter()
                    .map(|field| {
                        let source = sources
                            .iter()
                            .find(|candidate| candidate.name() == field.name())
                            .map(Field::logical_type);
                        Field::new(field.name(), hint(field.logical_type(), source), true)
                    })
                    .collect(),
            )
            .expect("distinct names"),
        ),
        (T::List(item), Some(T::List(source))) => T::List(Box::new(Field::new(
            item.name(),
            hint(item.logical_type(), Some(source.logical_type())),
            true,
        ))),
        _ => column.clone(),
    }
}

/// `text`, JSON a source pushed, as a column of `logical` holds it: its numbers as that type's
/// values, so a number in a float column means the float it parses to.
pub fn json_as(text: &str, logical: &LogicalType) -> Canon {
    typed(&parse(text), logical)
}

/// The cell at `row` of `array`, a column of `logical` stored as `lowered`, read as `hint`
/// says.
pub fn cell(
    array: &dyn Array,
    row: usize,
    logical: &LogicalType,
    lowered: &LogicalType,
    hint: &LogicalType,
) -> Canon {
    if array
        .logical_nulls()
        .is_some_and(|nulls| nulls.is_null(row))
    {
        return Canon::Null;
    }
    // Destinations receive the load id and load start as dictionaries of one value.
    if let DataType::Dictionary(_, values) = array.data_type() {
        let plain = arrow_cast::cast(&arrow_array::make_array(array.to_data()), values)
            .expect("a dictionary casts to its values' type");
        return cell(plain.as_ref(), row, logical, lowered, hint);
    }
    let nested = matches!(
        logical,
        LogicalType::Struct(_) | LogicalType::List(_) | LogicalType::Json
    );
    if nested && (lowered != logical || *logical == LogicalType::Json) {
        return typed(&parse(array.as_string::<i32>().value(row)), hint);
    }
    if lowered != logical {
        return text(array.as_string::<i32>().value(row), logical);
    }
    native(array, row, logical, hint)
}

/// The native value at `row` of `array`, of `logical`, its JSON parts read as `hint` says.
fn native(array: &dyn Array, row: usize, logical: &LogicalType, hint: &LogicalType) -> Canon {
    let number = |value: String| Canon::Number(decimal(&value));
    let ns = |value: i64, unit: ArrowUnit| i128::from(value) * unit_nanos(unit);
    match array.data_type() {
        DataType::Boolean => Canon::Bool(array.as_boolean().value(row)),
        DataType::Int8 => number(array.as_primitive::<Int8Type>().value(row).to_string()),
        DataType::Int16 => number(array.as_primitive::<Int16Type>().value(row).to_string()),
        DataType::Int32 => number(array.as_primitive::<Int32Type>().value(row).to_string()),
        DataType::Int64 => number(array.as_primitive::<Int64Type>().value(row).to_string()),
        DataType::Float32 => Canon::Number(float(f64::from(
            array.as_primitive::<Float32Type>().value(row),
        ))),
        DataType::Float64 => Canon::Number(float(array.as_primitive::<Float64Type>().value(row))),
        DataType::Decimal128(_, scale) => number(format!(
            "{}e-{scale}",
            array.as_primitive::<Decimal128Type>().value(row)
        )),
        DataType::Decimal256(_, scale) => number(format!(
            "{}e-{scale}",
            array.as_primitive::<Decimal256Type>().value(row)
        )),
        DataType::Utf8 => Canon::Text(array.as_string::<i32>().value(row).to_owned()),
        DataType::Binary => Canon::Bytes(hex(array.as_binary::<i32>().value(row))),
        DataType::FixedSizeBinary(16) => Canon::Text(uuid(array.as_fixed_size_binary().value(row))),
        DataType::Date32 => {
            Canon::Instant(i128::from(array.as_primitive::<Date32Type>().value(row)) * DAY)
        }
        DataType::Time32(unit) => Canon::TimeOfDay(ns(time32(array, row, *unit), *unit)),
        DataType::Time64(unit) => Canon::TimeOfDay(ns(time64(array, row, *unit), *unit)),
        DataType::Timestamp(unit, _) => Canon::Instant(ns(timestamp(array, row, *unit), *unit)),
        DataType::Duration(unit) => Canon::Elapsed(ns(duration(array, row, *unit), *unit)),
        DataType::Struct(_) | DataType::List(_) => nested(array, row, logical, hint),
        other => panic!("no destination stores {other}"),
    }
}

/// The native struct or list at `row` of `array`, of `logical`, read as `hint` says.
fn nested(array: &dyn Array, row: usize, logical: &LogicalType, hint: &LogicalType) -> Canon {
    match (logical, hint) {
        (LogicalType::Struct(fields), LogicalType::Struct(hints)) => Canon::Object(
            fields
                .iter()
                .zip(hints.iter())
                .zip(array.as_struct().columns())
                .map(|((field, hint), child)| {
                    let inner = field.logical_type();
                    let value = cell(child.as_ref(), row, inner, inner, hint.logical_type());
                    (field.name().to_owned(), value)
                })
                .filter(|(_, member)| *member != Canon::Null)
                .collect(),
        ),
        (LogicalType::List(item), LogicalType::List(hint)) => {
            let items = array.as_list::<i32>().value(row);
            let (inner, hint) = (item.logical_type(), hint.logical_type());
            Canon::List(
                (0..items.len())
                    .map(|index| cell(items.as_ref(), index, inner, inner, hint))
                    .collect(),
            )
        }
        (logical, hint) => panic!("a native {logical} read as {hint}"),
    }
}

fn unit_nanos(unit: ArrowUnit) -> i128 {
    match unit {
        ArrowUnit::Second => 1_000_000_000,
        ArrowUnit::Millisecond => 1_000_000,
        ArrowUnit::Microsecond => 1_000,
        ArrowUnit::Nanosecond => 1,
    }
}

fn time32(array: &dyn Array, row: usize, unit: ArrowUnit) -> i64 {
    i64::from(match unit {
        ArrowUnit::Second => array.as_primitive::<Time32SecondType>().value(row),
        _ => array.as_primitive::<Time32MillisecondType>().value(row),
    })
}

fn time64(array: &dyn Array, row: usize, unit: ArrowUnit) -> i64 {
    match unit {
        ArrowUnit::Microsecond => array.as_primitive::<Time64MicrosecondType>().value(row),
        _ => array.as_primitive::<Time64NanosecondType>().value(row),
    }
}

fn timestamp(array: &dyn Array, row: usize, unit: ArrowUnit) -> i64 {
    match unit {
        ArrowUnit::Second => array.as_primitive::<TimestampSecondType>().value(row),
        ArrowUnit::Millisecond => array.as_primitive::<TimestampMillisecondType>().value(row),
        ArrowUnit::Microsecond => array.as_primitive::<TimestampMicrosecondType>().value(row),
        ArrowUnit::Nanosecond => array.as_primitive::<TimestampNanosecondType>().value(row),
    }
}

fn duration(array: &dyn Array, row: usize, unit: ArrowUnit) -> i64 {
    match unit {
        ArrowUnit::Second => array.as_primitive::<DurationSecondType>().value(row),
        ArrowUnit::Millisecond => array.as_primitive::<DurationMillisecondType>().value(row),
        ArrowUnit::Microsecond => array.as_primitive::<DurationMicrosecondType>().value(row),
        ArrowUnit::Nanosecond => array.as_primitive::<DurationNanosecondType>().value(row),
    }
}

/// `text`, the text a column of `logical` stores its values as, read back.
fn text(text: &str, logical: &LogicalType) -> Canon {
    use LogicalType as T;
    match logical {
        T::Bool => Canon::Bool(text.parse().expect("a boolean's text")),
        T::Int8 | T::Int16 | T::Int32 | T::Int64 | T::Decimal(_) => Canon::Number(decimal(text)),
        T::Float32 => Canon::Number(float(f64::from(text.parse::<f32>().expect("a float")))),
        T::Float64 => Canon::Number(float(text.parse::<f64>().expect("a float"))),
        T::Binary => Canon::Bytes(text.to_owned()),
        T::Utf8 | T::Uuid => Canon::Text(text.to_owned()),
        T::Date => Canon::Instant(i128::from(date(text)) * DAY),
        T::Time(_) => Canon::TimeOfDay(time(text)),
        T::Timestamp(..) => Canon::Instant(instant(text)),
        T::Duration(_) => Canon::Elapsed(elapsed(text)),
        other => panic!("no column of {other} is stored as text"),
    }
}

/// JSON, as the JSON writer renders a value of `hint`, read back.
fn typed(json: &Json, hint: &LogicalType) -> Canon {
    use LogicalType as T;
    match (json, hint) {
        (Json::Null, _) => Canon::Null,
        (Json::Bool(value), _) => Canon::Bool(*value),
        (Json::Number(text), T::Float32) => {
            Canon::Number(float(f64::from(text.parse::<f32>().expect("a float"))))
        }
        (Json::Number(text), T::Float64) => {
            Canon::Number(float(text.parse::<f64>().expect("a float")))
        }
        (Json::Number(text), _) => Canon::Number(decimal(text)),
        (Json::String(text), T::Binary) => Canon::Bytes(text.clone()),
        (Json::String(text), T::Date) => Canon::Instant(i128::from(date(text)) * DAY),
        (Json::String(text), T::Time(_)) => Canon::TimeOfDay(time(text)),
        (Json::String(text), T::Timestamp(..)) => Canon::Instant(instant(text)),
        (Json::String(text), T::Duration(_)) => Canon::Elapsed(elapsed(text)),
        (Json::String(text), T::Float32 | T::Float64) => {
            Canon::Number(float(text.parse::<f64>().expect("a float's name")))
        }
        (Json::String(text), _) => Canon::Text(text.clone()),
        (Json::Array(items), T::List(item)) => Canon::List(
            items
                .iter()
                .map(|inner| typed(inner, item.logical_type()))
                .collect(),
        ),
        (Json::Array(items), _) => {
            Canon::List(items.iter().map(|inner| typed(inner, hint)).collect())
        }
        (Json::Object(members), T::Struct(fields)) => Canon::Object(
            members
                .iter()
                .map(|(name, member)| {
                    let inner = fields
                        .iter()
                        .find(|field| field.name() == name)
                        .map_or(&T::Json, |field| field.logical_type());
                    (name.clone(), typed(member, inner))
                })
                .filter(|(_, member)| *member != Canon::Null)
                .collect(),
        ),
        (Json::Object(members), _) => Canon::Object(
            members
                .iter()
                .map(|(name, member)| (name.clone(), typed(member, hint)))
                .filter(|(_, member)| *member != Canon::Null)
                .collect(),
        ),
    }
}

/// Days since the epoch of `text`, `YYYY-MM-DD`.
fn date(text: &str) -> i64 {
    let (sign, text) = text.strip_prefix('-').map_or((1, text), |rest| (-1, rest));
    let mut parts = text
        .splitn(3, '-')
        .map(|part| part.parse::<i64>().expect("a date part"));
    let (year, month, day) = (
        sign * parts.next().expect("a year"),
        parts.next().expect("a month"),
        parts.next().expect("a day"),
    );
    // Howard Hinnant's days_from_civil.
    let year = if month <= 2 { year - 1 } else { year };
    let era = year.div_euclid(400);
    let of_era = year - era * 400;
    let of_year = (153 * (if month > 2 { month - 3 } else { month + 9 }) + 2) / 5 + day - 1;
    let of_era_days = of_era * 365 + of_era / 4 - of_era / 100 + of_year;
    era * 146_097 + of_era_days - 719_468
}

/// Nanoseconds since midnight of `text`, `HH:MM:SS` with an optional fraction.
fn time(text: &str) -> i128 {
    let (sign, text) = match text.strip_prefix('-') {
        Some(text) => (-1, text),
        None => (1, text),
    };
    let mut parts = text.splitn(3, ':');
    let hours: i128 = parts.next().expect("hours").parse().expect("hours");
    let minutes: i128 = parts.next().expect("minutes").parse().expect("minutes");
    let seconds = seconds(parts.next().expect("seconds"));
    sign * ((hours * 3600 + minutes * 60) * 1_000_000_000 + seconds)
}

/// Nanoseconds in `text`, seconds with an optional fraction.
fn seconds(text: &str) -> i128 {
    let (whole, fraction) = text.split_once('.').unwrap_or((text, ""));
    let whole: i128 = whole.parse().expect("whole seconds");
    let digits = format!("{fraction:0<9}");
    whole * 1_000_000_000 + digits[..9].parse::<i128>().expect("a fraction")
}

/// Nanoseconds since the epoch of `text`: a date, `T`, a time and an optional `Z` or offset.
fn instant(text: &str) -> i128 {
    let (day, rest) = text.split_once('T').expect("a timestamp's T");
    let (clock, offset) = match rest.find(['Z', '+', '-']) {
        Some(at) => (&rest[..at], &rest[at..]),
        None => (rest, ""),
    };
    let offset = match offset {
        "" | "Z" => 0,
        offset => {
            let sign = if offset.starts_with('-') { -1 } else { 1 };
            let (hours, minutes) = offset[1..].split_once(':').expect("an offset");
            let minutes = hours.parse::<i128>().expect("hours") * 60
                + minutes.parse::<i128>().expect("minutes");
            sign * minutes * 60 * 1_000_000_000
        }
    };
    i128::from(date(day)) * DAY + time(clock) - offset
}

/// Nanoseconds of `text`, an ISO 8601 duration of seconds such as `-PT1.5S`.
fn elapsed(text: &str) -> i128 {
    let (sign, text) = text.strip_prefix('-').map_or((1, text), |rest| (-1, rest));
    let seconds_text = text
        .strip_prefix("PT")
        .and_then(|rest| rest.strip_suffix('S'))
        .unwrap_or_else(|| panic!("a duration of seconds, not {text}"));
    sign * seconds(seconds_text)
}
