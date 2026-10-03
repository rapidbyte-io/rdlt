//! Every temporal value of every type, unit and kind of zone renders, as text and in JSON, to
//! text that reads back as exactly that value, at the ends of each type's range above all.

use std::sync::Arc;

use arrow_array::cast::AsArray;
use arrow_array::{
    Array, ArrayRef, Date32Array, Date64Array, DurationMicrosecondArray, DurationMillisecondArray,
    DurationNanosecondArray, DurationSecondArray, Time32MillisecondArray, Time32SecondArray,
    Time64MicrosecondArray, Time64NanosecondArray, TimestampMicrosecondArray,
    TimestampMillisecondArray, TimestampNanosecondArray, TimestampSecondArray,
};
use arrow_schema::{DataType, TimeUnit};
use rdlt_connector::LogicalType;

use super::super::text;
use crate::table::convert::json;

const NANOS: i128 = 1_000_000_000;
const DAY: i128 = 86_400 * NANOS;

/// Nanoseconds in one `unit`.
fn per(unit: TimeUnit) -> i128 {
    match unit {
        TimeUnit::Second => NANOS,
        TimeUnit::Millisecond => 1_000_000,
        TimeUnit::Microsecond => 1_000,
        TimeUnit::Nanosecond => 1,
    }
}

/// Values of `unit` at the ends of what an `i64` holds, of what `chrono` holds, around a day
/// and around 2^32 seconds, each a step to either side, and as far as `reach` nanoseconds past
/// chrono's ends.
fn edges(unit: TimeUnit) -> Vec<i64> {
    let per = per(unit);
    let chrono_max = i128::from(chrono::NaiveDateTime::MAX.and_utc().timestamp()) * NANOS
        + i128::from(
            chrono::NaiveDateTime::MAX
                .and_utc()
                .timestamp_subsec_nanos(),
        );
    let chrono_min = i128::from(chrono::NaiveDateTime::MIN.and_utc().timestamp()) * NANOS;
    let reach = [0, 60 * NANOS, 14 * 3_600 * NANOS, 15 * 3_600 * NANOS];
    let mut nanos = vec![
        0,
        DAY,
        -DAY,
        (1_i128 << 32) * NANOS,
        -(1_i128 << 32) * NANOS,
    ];
    for past in reach {
        nanos.extend([
            chrono_max + past,
            chrono_max - past,
            chrono_min + past,
            chrono_min - past,
        ]);
    }
    let mut values = vec![i64::MIN, i64::MIN + 1, i64::MAX - 1, i64::MAX, -1, 1];
    for at in nanos {
        for step in [-1, 0, 1] {
            if let Ok(value) = i64::try_from(at / per + step) {
                values.push(value);
            }
        }
    }
    values.sort_unstable();
    values.dedup();
    values
}

/// The zones a timestamp may name: none, UTC, fixed offsets of whole and odd minutes either
/// side, and named zones of whole, half and three-quarter hours and of offsets in seconds.
const ZONES: [Option<&str>; 13] = [
    None,
    Some("UTC"),
    Some("+00:00"),
    Some("+00:01"),
    Some("-00:01"),
    Some("+14:00"),
    Some("-12:00"),
    Some("+05:45"),
    Some("Asia/Kolkata"),
    Some("Europe/Amsterdam"),
    Some("America/St_Johns"),
    Some("Asia/Kathmandu"),
    Some("Pacific/Kiritimati"),
];

/// The array of `data_type` holding `values`.
fn array(data_type: &DataType, values: Vec<i64>) -> ArrayRef {
    let narrow = || {
        values
            .iter()
            .map(|value| i32::try_from(*value).ok())
            .collect::<Vec<_>>()
    };
    match data_type {
        DataType::Date32 => Arc::new(Date32Array::from(narrow())),
        DataType::Date64 => Arc::new(Date64Array::from(values)),
        DataType::Time32(TimeUnit::Second) => Arc::new(Time32SecondArray::from(narrow())),
        DataType::Time32(_) => Arc::new(Time32MillisecondArray::from(narrow())),
        DataType::Time64(TimeUnit::Microsecond) => Arc::new(Time64MicrosecondArray::from(values)),
        DataType::Time64(_) => Arc::new(Time64NanosecondArray::from(values)),
        DataType::Duration(TimeUnit::Second) => Arc::new(DurationSecondArray::from(values)),
        DataType::Duration(TimeUnit::Millisecond) => {
            Arc::new(DurationMillisecondArray::from(values))
        }
        DataType::Duration(TimeUnit::Microsecond) => {
            Arc::new(DurationMicrosecondArray::from(values))
        }
        DataType::Duration(_) => Arc::new(DurationNanosecondArray::from(values)),
        DataType::Timestamp(unit, zone) => {
            let zone = zone.clone();
            match unit {
                TimeUnit::Second => {
                    Arc::new(TimestampSecondArray::from(values).with_timezone_opt(zone))
                }
                TimeUnit::Millisecond => {
                    Arc::new(TimestampMillisecondArray::from(values).with_timezone_opt(zone))
                }
                TimeUnit::Microsecond => {
                    Arc::new(TimestampMicrosecondArray::from(values).with_timezone_opt(zone))
                }
                TimeUnit::Nanosecond => {
                    Arc::new(TimestampNanosecondArray::from(values).with_timezone_opt(zone))
                }
            }
        }
        other => panic!("{other} is not temporal"),
    }
}

/// The days since the epoch of the civil date `text`, `YYYY-MM-DD` with a signed year beyond
/// `0000`–`9999` (Howard Hinnant's `days_from_civil`).
fn days(text: &str) -> i128 {
    let (sign, rest) = match text.as_bytes()[0] {
        b'+' => (1, &text[1..]),
        b'-' => (-1, &text[1..]),
        _ => (1, text),
    };
    let mut parts = rest.split('-');
    let mut next = || {
        parts
            .next()
            .expect("a date part")
            .parse::<i128>()
            .expect("digits")
    };
    let (year, month, day) = (sign * next(), next(), next());
    let year = if month <= 2 { year - 1 } else { year };
    let era = year.div_euclid(400);
    let of_era = year.rem_euclid(400);
    let of_year = (153 * (if month > 2 { month - 3 } else { month + 9 }) + 2) / 5 + day - 1;
    let of_era_days = of_era * 365 + of_era / 4 - of_era / 100 + of_year;
    era * 146_097 + of_era_days - 719_468
}

/// The nanoseconds of the signed clock `text`, `[-]H+:MM:SS[.fraction]`.
fn clock(text: &str) -> i128 {
    let (sign, rest) = text.strip_prefix('-').map_or((1, text), |rest| (-1, rest));
    let (whole, fraction) = rest.split_once('.').unwrap_or((rest, ""));
    let mut parts = whole
        .split(':')
        .map(|part| part.parse::<i128>().expect("digits"));
    let (hours, minutes, seconds) = (
        parts.next().expect("hours"),
        parts.next().expect("minutes"),
        parts.next().expect("seconds"),
    );
    assert!(minutes < 60 && seconds < 60, "{text}");
    assert!(matches!(fraction.len(), 0 | 3 | 6 | 9), "{text}");
    let fraction = format!("{fraction:0<9}").parse::<i128>().expect("digits");
    sign * (((hours * 60 + minutes) * 60 + seconds) * NANOS + fraction)
}

/// The nanoseconds since the epoch of the instant or wall-clock time `text`, with its offset
/// (`Z` or `±HH:MM`) where it has one.
fn instant(text: &str) -> i128 {
    let (date, rest) = text.split_once('T').expect("a date and a time");
    let (time, offset) = if let Some(time) = rest.strip_suffix('Z') {
        (time, 0)
    } else if rest.len() > 6 && matches!(rest.as_bytes()[rest.len() - 6], b'+' | b'-') {
        let (time, offset) = rest.split_at(rest.len() - 6);
        let sign = if offset.starts_with('-') { -1 } else { 1 };
        let hours: i128 = offset[1..3].parse().expect("hours");
        let minutes: i128 = offset[4..6].parse().expect("minutes");
        (time, sign * (hours * 3_600 + minutes * 60) * NANOS)
    } else {
        (rest, 0)
    };
    days(date) * DAY + clock(time) - offset
}

/// The value `text` reads back as, in the nanoseconds `data_type`'s values count: since the
/// epoch for dates, a day's whole, and timestamps, since midnight for times, elapsed for
/// durations.
fn read_back(data_type: &DataType, text: &str) -> i128 {
    match data_type {
        DataType::Date32 | DataType::Date64 => days(text) * DAY,
        DataType::Time32(_) | DataType::Time64(_) => clock(text),
        DataType::Timestamp(..) => instant(text),
        DataType::Duration(_) => {
            let (sign, rest) = text.strip_prefix('-').map_or((1, text), |rest| (-1, rest));
            let seconds = rest
                .strip_prefix("PT")
                .and_then(|rest| rest.strip_suffix('S'))
                .expect("a duration of seconds");
            let (whole, fraction) = seconds.split_once('.').unwrap_or((seconds, ""));
            let fraction = format!("{fraction:0<9}").parse::<i128>().expect("digits");
            sign * (whole.parse::<i128>().expect("digits") * NANOS + fraction)
        }
        other => panic!("{other} is not temporal"),
    }
}

/// The nanoseconds `value` of `data_type` is: a `Date64` the day it is within.
fn exact(data_type: &DataType, value: i64) -> i128 {
    let value = i128::from(value);
    match data_type {
        DataType::Date32 => value * DAY,
        DataType::Date64 => value.div_euclid(86_400_000) * DAY,
        DataType::Time32(unit)
        | DataType::Time64(unit)
        | DataType::Timestamp(unit, _)
        | DataType::Duration(unit) => value * per(*unit),
        other => panic!("{other} is not temporal"),
    }
}

/// Every temporal type, with every unit and zone.
fn types() -> Vec<DataType> {
    use TimeUnit as U;
    let mut types = vec![
        DataType::Date32,
        DataType::Date64,
        DataType::Time32(U::Second),
        DataType::Time32(U::Millisecond),
        DataType::Time64(U::Microsecond),
        DataType::Time64(U::Nanosecond),
    ];
    for unit in [U::Second, U::Millisecond, U::Microsecond, U::Nanosecond] {
        types.push(DataType::Duration(unit));
        for zone in ZONES {
            types.push(DataType::Timestamp(unit, zone.map(Arc::from)));
        }
    }
    types
}

/// The logical type a column of `data_type` holds.
fn logical(data_type: &DataType) -> LogicalType {
    let field = arrow_schema::Field::new("value", data_type.clone(), true);
    let field = rdlt_connector::Field::from_arrow(&field).expect("a temporal type is logical");
    field.logical_type().clone()
}

#[test]
fn every_temporal_value_renders_as_text_and_json_reading_back_as_itself() {
    for data_type in types() {
        let unit = match &data_type {
            DataType::Time32(unit)
            | DataType::Time64(unit)
            | DataType::Timestamp(unit, _)
            | DataType::Duration(unit) => *unit,
            _ => TimeUnit::Millisecond,
        };
        let values = edges(unit);
        let column = array(&data_type, values.clone());
        let as_text = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| text(&column)))
            .unwrap_or_else(|_| panic!("{data_type}: rendering text panicked"))
            .unwrap_or_else(|error| panic!("{data_type}: {error}"));
        let as_json = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            json(&column, &logical(&data_type))
        }))
        .unwrap_or_else(|_| panic!("{data_type}: rendering JSON panicked"))
        .unwrap_or_else(|error| panic!("{data_type}: {error}"));
        let (as_text, as_json) = (as_text.as_string::<i32>(), as_json.as_string::<i32>());
        for (row, value) in values.iter().enumerate() {
            if column.is_null(row) {
                continue;
            }
            let rendered = as_text.value(row);
            let read = read_back(&data_type, rendered);
            assert_eq!(
                read,
                exact(&data_type, *value),
                "{data_type} {value}: {rendered}"
            );
            let quoted = as_json.value(row);
            assert_eq!(quoted, format!("\"{rendered}\""), "{data_type} {value}");
        }
    }
}

#[test]
fn a_time_of_day_past_two_to_the_thirty_two_seconds_renders_whole() {
    let micros: ArrayRef = Arc::new(Time64MicrosecondArray::from(vec![
        4_294_967_297_000_000_i64,
        -4_294_967_291_000_000,
        1_000_000,
    ]));
    let rendered = text(&micros).unwrap();
    let texts: Vec<Option<&str>> = rendered.as_string::<i32>().iter().collect();
    assert_eq!(
        texts,
        [
            Some("1193046:28:17"),
            Some("-1193046:28:11"),
            Some("00:00:01")
        ]
    );
}

#[test]
fn a_zoned_instant_whose_local_time_chrono_cannot_hold_renders_in_utc() {
    let near: ArrayRef =
        Arc::new(TimestampSecondArray::from(vec![8_210_266_876_799_i64]).with_timezone("+00:01"));
    let rendered = text(&near).unwrap();
    assert_eq!(
        rendered.as_string::<i32>().value(0),
        "+262142-12-31T23:59:59Z"
    );
}
