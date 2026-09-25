//! What values mean, comparable across the types and texts that hold them: every conversion the
//! engine makes is exact, so a stored cell must mean exactly the value the source sent.

use std::collections::BTreeMap;
use std::sync::Arc;

use rdlt_connector::{Capabilities, LogicalType, TimeUnit};
use serde_json::Value;

use crate::drawn::Scalar;

/// What a value means, comparable across the types and texts that hold it.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum Canon {
    /// No value.
    Null,
    /// A boolean.
    Bool(bool),
    /// A number, as its shortest exact decimal text.
    Number(String),
    /// Text.
    Text(String),
    /// Bytes, in lower-case hex.
    Bytes(String),
    /// An instant or a date, in nanoseconds since the epoch.
    Instant(i128),
    /// A time of day, in nanoseconds since midnight.
    TimeOfDay(i128),
    /// A duration, in nanoseconds.
    Elapsed(i128),
    /// A list's items.
    List(Vec<Canon>),
    /// An object's non-null members.
    Object(BTreeMap<String, Canon>),
}

/// Nanoseconds in one `unit`.
pub fn nanos(unit: TimeUnit) -> i128 {
    match unit {
        TimeUnit::Second => 1_000_000_000,
        TimeUnit::Millisecond => 1_000_000,
        TimeUnit::Microsecond => 1_000,
        TimeUnit::Nanosecond => 1,
    }
}

/// Nanoseconds in a day.
pub const DAY: i128 = 86_400 * 1_000_000_000;

/// What `value`, of `logical`, means.
pub fn canonical(value: &Scalar, logical: &LogicalType) -> Canon {
    use LogicalType as T;
    match (value, logical) {
        (Scalar::Null, _) => Canon::Null,
        (Scalar::Bool(value), _) => Canon::Bool(*value),
        (Scalar::Int(value), _) => Canon::Number(value.to_string()),
        (Scalar::Float32(value), _) => Canon::Number(float(f64::from(*value))),
        (Scalar::Float64(value), _) => Canon::Number(float(*value)),
        (Scalar::Decimal(digits), T::Decimal(decimal)) => {
            Canon::Number(scaled(digits, decimal.scale()))
        }
        (Scalar::Utf8(text), _) => Canon::Text(text.clone()),
        (Scalar::Binary(bytes), _) => Canon::Bytes(hex(bytes)),
        (Scalar::Date(days), _) => Canon::Instant(i128::from(*days) * DAY),
        (Scalar::Temporal(value), T::Time(unit)) => {
            Canon::TimeOfDay(i128::from(*value) * nanos(*unit))
        }
        (Scalar::Temporal(value), T::Timestamp(unit, _)) => {
            Canon::Instant(i128::from(*value) * nanos(*unit))
        }
        (Scalar::Temporal(value), T::Duration(unit)) => {
            Canon::Elapsed(i128::from(*value) * nanos(*unit))
        }
        (Scalar::Uuid(bytes), _) => Canon::Text(uuid(bytes)),
        (Scalar::Json(value), _) => json(value),
        (Scalar::Struct(fields), T::Struct(types)) => Canon::Object(
            fields
                .iter()
                .filter_map(|(name, inner)| {
                    let field = types.iter().find(|field| field.name() == name)?;
                    let inner = canonical(inner, field.logical_type());
                    (inner != Canon::Null).then(|| (name.clone(), inner))
                })
                .collect(),
        ),
        (Scalar::List(items), T::List(item)) => Canon::List(
            items
                .iter()
                .map(|inner| canonical(inner, item.logical_type()))
                .collect(),
        ),
        (value, logical) => panic!("the differential draws no {value:?} of {logical}"),
    }
}

/// What `value`, of `from`, means once converted to `to`: what it means, but a date in a column of
/// zoned timestamps is midnight in the column's zone.
pub fn canonical_into(value: &Scalar, from: &LogicalType, to: &LogicalType) -> Canon {
    use LogicalType as T;
    match (value, from, to) {
        (Scalar::Date(days), T::Date, T::Timestamp(_, Some(zone))) => {
            let local = i128::from(*days) * DAY;
            Canon::Instant(local - offset(zone, local).unwrap_or(0))
        }
        (Scalar::Temporal(value), T::Timestamp(unit, None), T::Timestamp(_, Some(zone))) => {
            let local = i128::from(*value) * nanos(*unit);
            Canon::Instant(local - offset(zone, local).unwrap_or(0))
        }
        (Scalar::Struct(fields), T::Struct(from_fields), T::Struct(to_fields)) => Canon::Object(
            fields
                .iter()
                .filter_map(|(name, inner)| {
                    let from = from_fields.iter().find(|field| field.name() == name)?;
                    let to = to_fields.iter().find(|field| field.name() == name)?;
                    let inner = canonical_into(inner, from.logical_type(), to.logical_type());
                    (inner != Canon::Null).then(|| (name.clone(), inner))
                })
                .collect(),
        ),
        (Scalar::List(items), T::List(from_item), T::List(to_item)) => Canon::List(
            items
                .iter()
                .map(|inner| {
                    canonical_into(inner, from_item.logical_type(), to_item.logical_type())
                })
                .collect(),
        ),
        _ => canonical(value, from),
    }
}

/// Whether `to` holds `value`, of `from`: a time converted to a finer unit, or placed in a zone,
/// may leave the integer that stores it (a `Date32`'s days, a `Time32`'s `i32`, an `i64`), and a
/// named zone's offsets are known only for the years `chrono` holds.
pub fn holds(value: &Scalar, from: &LogicalType, to: &LogicalType) -> bool {
    use LogicalType as T;
    let within = |nanos: i128, unit: TimeUnit| i64::try_from(nanos / self::nanos(unit)).is_ok();
    let placed = |local: i128, unit: TimeUnit, zone: &Option<Arc<str>>| {
        within(local, unit)
            && zone.as_deref().is_none_or(|zone| {
                offset(zone, local).is_some_and(|offset| within(local - offset, unit))
            })
    };
    match (value, from, to) {
        (Scalar::Date(days), _, T::Date) => i32::try_from(*days).is_ok(),
        (Scalar::Temporal(value), T::Time(from), T::Time(to)) => {
            let value = i128::from(*value) * nanos(*from) / nanos(*to);
            match to {
                TimeUnit::Second | TimeUnit::Millisecond => i32::try_from(value).is_ok(),
                _ => i64::try_from(value).is_ok(),
            }
        }
        (Scalar::Date(days), _, T::Timestamp(unit, zone)) => {
            placed(i128::from(*days) * DAY, *unit, zone)
        }
        (Scalar::Temporal(value), T::Timestamp(from, None), T::Timestamp(to, zone @ Some(_))) => {
            let local = i128::from(*value) * nanos(*from);
            within(local, *to) && placed(local, *to, zone)
        }
        (
            Scalar::Temporal(value),
            T::Time(from) | T::Timestamp(from, _) | T::Duration(from),
            T::Time(to) | T::Timestamp(to, _) | T::Duration(to),
        ) => within(i128::from(*value) * nanos(*from), *to),
        (Scalar::Struct(fields), T::Struct(from_fields), T::Struct(to_fields)) => {
            fields.iter().all(|(name, inner)| {
                let from = from_fields.iter().find(|field| field.name() == name);
                let to = to_fields.iter().find(|field| field.name() == name);
                match (from, to) {
                    (Some(from), Some(to)) => holds(inner, from.logical_type(), to.logical_type()),
                    _ => true,
                }
            })
        }
        (Scalar::List(items), T::List(from_item), T::List(to_item)) => items
            .iter()
            .all(|item| holds(item, from_item.logical_type(), to_item.logical_type())),
        _ => true,
    }
}

/// Nanoseconds `zone` is ahead of UTC at the wall-clock time `local`: a fixed offset, or a named
/// zone's where its clocks show `local` (the earlier where they show it twice, and where they
/// skip it the offset before they did, so the time moves forward by the gap); `None` beyond the
/// years a named zone's offsets are known for.
pub fn offset(zone: &str, local: i128) -> Option<i128> {
    use chrono::{LocalResult, Offset as _, TimeZone as _};
    const SECOND: i128 = 1_000_000_000;
    if zone == "UTC" {
        return Some(0);
    }
    let sign = match zone.as_bytes().first() {
        Some(b'+') => Some(1),
        Some(b'-') => Some(-1),
        _ => None,
    };
    if let Some(sign) = sign {
        let (hours, minutes) = zone[1..].split_once(':').expect("a fixed offset");
        let minutes =
            hours.parse::<i128>().expect("hours") * 60 + minutes.parse::<i128>().expect("minutes");
        return Some(sign * minutes * 60 * SECOND);
    }
    let tz: arrow_array::timezone::Tz = zone.parse().expect("a known zone");
    let seconds = i64::try_from(local.div_euclid(SECOND)).ok()?;
    let nanos = u32::try_from(local.rem_euclid(SECOND)).expect("a fraction");
    let local = chrono::DateTime::from_timestamp(seconds, nanos)?.naive_utc();
    let offset = match tz.offset_from_local_datetime(&local) {
        LocalResult::Single(offset) | LocalResult::Ambiguous(offset, _) => {
            offset.fix().local_minus_utc()
        }
        LocalResult::None => {
            // The shift lies within a day of `local` read as UTC: find its second by bisection.
            let at = |seconds: i64| {
                let utc = chrono::DateTime::from_timestamp(seconds, 0).expect("in range");
                tz.offset_from_utc_datetime(&utc.naive_utc())
                    .fix()
                    .local_minus_utc()
            };
            let (mut before, mut after) = (seconds - 86_400, seconds + 86_400);
            let shifted = at(after);
            while after - before > 1 {
                let middle = before + (after - before) / 2;
                if at(middle) == shifted {
                    after = middle;
                } else {
                    before = middle;
                }
            }
            at(before)
        }
    };
    Some(i128::from(offset) * SECOND)
}

/// What a JSON value means.
pub fn json(value: &Value) -> Canon {
    match value {
        Value::Null => Canon::Null,
        Value::Bool(value) => Canon::Bool(*value),
        Value::Number(number) => Canon::Number(decimal(&number.to_string())),
        Value::String(text) => Canon::Text(text.clone()),
        Value::Array(items) => Canon::List(items.iter().map(json).collect()),
        Value::Object(members) => Canon::Object(
            members
                .iter()
                .map(|(name, member)| (name.clone(), json(member)))
                .filter(|(_, member)| *member != Canon::Null)
                .collect(),
        ),
    }
}

/// `value` as its shortest decimal text that reads back as it; non-finite values by name.
pub fn float(value: f64) -> String {
    if value.is_finite() {
        decimal(&format!("{value:?}"))
    } else {
        value.to_string()
    }
}

/// `digits`, an unscaled value, with `scale` digits after the point, as a decimal.
pub fn scaled(digits: &str, scale: u8) -> String {
    decimal(&format!("{digits}e-{scale}"))
}

/// A decimal number's text, `-12.50`, `1e3` or `0.0`, as its shortest exact form: no leading or
/// trailing zeros, no exponent, `0` for zero.
pub fn decimal(text: &str) -> String {
    let (negative, text) = text
        .strip_prefix('-')
        .map_or((false, text), |rest| (true, rest));
    let (mantissa, exponent) = match text.find(['e', 'E']) {
        Some(at) => (
            &text[..at],
            text[at + 1..].parse::<i64>().expect("a decimal exponent"),
        ),
        None => (text, 0),
    };
    let (whole, fraction) = mantissa.split_once('.').unwrap_or((mantissa, ""));
    let digits: String = whole.chars().chain(fraction.chars()).collect();
    // The number is `digits` with the point `point` places from their end.
    let point = i64::try_from(fraction.len()).expect("a short fraction") - exponent;
    let digits = digits.trim_start_matches('0');
    if digits.is_empty() {
        return "0".to_owned();
    }
    let (mut digits, mut point) = (digits.to_owned(), point);
    while point > 0 && digits.ends_with('0') {
        digits.pop();
        point -= 1;
    }
    let rendered = if point <= 0 {
        let zeros = usize::try_from(-point).expect("a small exponent");
        format!("{digits}{}", "0".repeat(zeros))
    } else {
        let point = usize::try_from(point).expect("a small scale");
        let padded = format!("{}{digits}", "0".repeat(point.saturating_sub(digits.len())));
        let split = padded.len() - point;
        let whole = if split == 0 { "0" } else { &padded[..split] };
        format!("{whole}.{}", &padded[split..])
    };
    if negative {
        format!("-{rendered}")
    } else {
        rendered
    }
}

/// `bytes` in lower-case hex.
pub fn hex(bytes: &[u8]) -> String {
    use std::fmt::Write as _;
    bytes.iter().fold(String::new(), |mut text, byte| {
        write!(text, "{byte:02x}").expect("writing to a string cannot fail");
        text
    })
}

/// A UUID's 16 bytes in their hyphenated form.
pub fn uuid(bytes: &[u8]) -> String {
    let hex = hex(bytes);
    format!(
        "{}-{}-{}-{}-{}",
        &hex[..8],
        &hex[8..12],
        &hex[12..16],
        &hex[16..20],
        &hex[20..]
    )
}

/// How a destination with `capabilities` stores a column of `logical`, nested values kept native
/// where `native_nested`: natively where it stores the type and, for nested values, everything
/// inside it; nested values and JSON otherwise as JSON, or text without a JSON type; anything else as
/// text.
pub fn storage(
    logical: &LogicalType,
    native_nested: bool,
    capabilities: &Capabilities,
) -> LogicalType {
    let json = if capabilities.types.contains(&LogicalType::Json.kind()) || capabilities.nested.json
    {
        LogicalType::Json
    } else {
        LogicalType::Utf8
    };
    match logical {
        LogicalType::Struct(_) | LogicalType::List(_) => {
            if native_nested && native(logical, capabilities) {
                logical.clone()
            } else {
                json
            }
        }
        LogicalType::Json => json,
        other if capabilities.types.contains(&other.kind()) => other.clone(),
        _ => LogicalType::Utf8,
    }
}

/// Whether a destination with `capabilities` stores `logical`, and everything inside it, natively.
fn native(logical: &LogicalType, capabilities: &Capabilities) -> bool {
    match logical {
        LogicalType::Null => true,
        LogicalType::Struct(fields) => {
            capabilities.nested.structs
                && fields
                    .iter()
                    .all(|field| native(field.logical_type(), capabilities))
        }
        LogicalType::List(item) => {
            capabilities.nested.lists && native(item.logical_type(), capabilities)
        }
        other => capabilities.types.contains(&other.kind()),
    }
}
