//! A column as the wider type its table took since, exactly: every value as it is, or the
//! conversion refused.
//!
//! Arrow's own casts null a value the wider type cannot hold, multiply times of day without a
//! check, and place a wall-clock time in a zone through a subtraction that panics at the edge of
//! the calendar. Here temporal values convert through checked integer arithmetic, a wall-clock
//! time in a zone is the instant it names there (where the zone's clocks repeat it, the earlier
//! one; where they skip it, the instant the offset in force before gives), and every other
//! conversion is one that loses nothing or fails.

#[cfg(test)]
mod tests;

use std::sync::Arc;

use arrow_array::cast::AsArray;
use arrow_array::temporal_conversions::as_datetime;
use arrow_array::timezone::Tz;
use arrow_array::types::{
    ArrowPrimitiveType, Date32Type, Date64Type, DurationMicrosecondType, DurationMillisecondType,
    DurationNanosecondType, DurationSecondType, Time32MillisecondType, Time32SecondType,
    Time64MicrosecondType, Time64NanosecondType, TimestampMicrosecondType,
    TimestampMillisecondType, TimestampNanosecondType, TimestampSecondType,
};
use arrow_array::{Array, ArrayRef, ListArray, PrimitiveArray, StructArray, new_null_array};
use arrow_cast::CastOptions;
use arrow_schema::{ArrowError, DataType, FieldRef, Fields, TimeUnit};
use chrono::{LocalResult, NaiveDateTime, Offset as _, TimeDelta, TimeZone as _};

/// `array` as `to`: every value exactly as it is, or an error naming the conversion.
pub(super) fn retyped(array: &ArrayRef, to: &DataType) -> Result<ArrayRef, ArrowError> {
    let from = array.data_type();
    if from == to {
        return Ok(Arc::clone(array));
    }
    match (from, to) {
        (DataType::Null, _) => Ok(new_null_array(to, array.len())),
        // An encoded column converts as the values it encodes.
        (DataType::Dictionary(_, values), _) => retyped(&checked(array, values)?, to),
        (DataType::RunEndEncoded(_, values), _) => {
            retyped(&checked(array, values.data_type())?, to)
        }
        (DataType::Date32 | DataType::Date64, DataType::Timestamp(unit, zone)) => {
            let per_day = 86_400 * per_second(*unit);
            let local = scaled(days(array)?, per_day)?;
            let values = placed(local, *unit, zone.as_deref())?;
            Ok(timestamps(values, *unit, zone.clone()))
        }
        (DataType::Timestamp(from_unit, from_zone), DataType::Timestamp(unit, zone)) => {
            let values = scaled(raw(array), finer(*from_unit, *unit, from, to)?)?;
            let values = match (from_zone, zone) {
                (None, Some(zone)) => placed(values, *unit, Some(zone))?,
                // An instant is no wall-clock time: only its zone could say which.
                (Some(_), None) => return Err(inexact(from, to)),
                _ => values,
            };
            Ok(timestamps(values, *unit, zone.clone()))
        }
        (
            DataType::Time32(from_unit) | DataType::Time64(from_unit),
            DataType::Time32(unit) | DataType::Time64(unit),
        ) => times(scaled(raw(array), finer(*from_unit, *unit, from, to)?)?, to),
        (DataType::Duration(from_unit), DataType::Duration(unit)) => {
            let values = scaled(raw(array), finer(*from_unit, *unit, from, to)?)?;
            Ok(durations(values, *unit))
        }
        (DataType::Struct(_), DataType::Struct(fields)) => structs(array.as_struct(), fields),
        (DataType::List(_), DataType::List(item)) => lists(array.as_list::<i32>(), item),
        (
            DataType::LargeList(item)
            | DataType::FixedSizeList(item, _)
            | DataType::ListView(item)
            | DataType::LargeListView(item),
            DataType::List(_),
        ) => retyped(&checked(array, &DataType::List(Arc::clone(item)))?, to),
        _ if lossless(from, to) => checked(array, to),
        _ => Err(inexact(from, to)),
    }
}

/// Whether Arrow's checked cast from `from` to `to` keeps every value it does not refuse: wider
/// or equal integers, integers a float holds exactly, wider floats and decimals, and text or
/// bytes in another encoding.
fn lossless(from: &DataType, to: &DataType) -> bool {
    use DataType as T;
    let small = matches!(
        from,
        T::Int8 | T::Int16 | T::Int32 | T::UInt8 | T::UInt16 | T::UInt32
    );
    let text = |kind: &T| matches!(kind, T::Utf8 | T::LargeUtf8 | T::Utf8View);
    let bytes = |kind: &T| matches!(kind, T::Binary | T::LargeBinary | T::BinaryView);
    match (from, to) {
        (from, to) if from.is_integer() && to.is_integer() => true,
        (_, T::Float64) if small || *from == T::Float32 => true,
        (from, T::Decimal128(..) | T::Decimal256(..)) if from.is_integer() => true,
        (
            T::Decimal128(_, from_scale) | T::Decimal256(_, from_scale),
            T::Decimal128(_, scale) | T::Decimal256(_, scale),
        ) => scale >= from_scale,
        (from, to) if text(from) && text(to) => true,
        (T::FixedSizeBinary(_), to) if bytes(to) => true,
        (from, to) => bytes(from) && bytes(to),
    }
}

/// Arrow's cast of `array` to `to`, failing where a value does not fit instead of nulling it.
fn checked(array: &ArrayRef, to: &DataType) -> Result<ArrayRef, ArrowError> {
    let options = CastOptions {
        safe: false,
        ..CastOptions::default()
    };
    arrow_cast::cast_with_options(array, to, &options)
}

fn inexact(from: &DataType, to: &DataType) -> ArrowError {
    ArrowError::CastError(format!(
        "no conversion from {from} to {to} keeps every value"
    ))
}

fn overflow(value: impl std::fmt::Display) -> ArrowError {
    ArrowError::CastError(format!("value {value} is beyond what its wider type holds"))
}

/// Units of `unit` in a second.
fn per_second(unit: TimeUnit) -> i64 {
    match unit {
        TimeUnit::Second => 1,
        TimeUnit::Millisecond => 1_000,
        TimeUnit::Microsecond => 1_000_000,
        TimeUnit::Nanosecond => 1_000_000_000,
    }
}

/// How many of `to` one `unit` holds, where `to` is as fine or finer; a coarser unit would round.
fn finer(
    unit: TimeUnit,
    to: TimeUnit,
    from: &DataType,
    target: &DataType,
) -> Result<i64, ArrowError> {
    let (coarse, fine) = (per_second(unit), per_second(to));
    if fine < coarse {
        return Err(inexact(from, target));
    }
    Ok(fine / coarse)
}

/// Each of `values` times `factor`, refused where the product leaves an `i64`.
fn scaled(values: Vec<Option<i64>>, factor: i64) -> Result<Vec<Option<i64>>, ArrowError> {
    values
        .into_iter()
        .map(|value| match value {
            Some(value) => value
                .checked_mul(factor)
                .map(Some)
                .ok_or_else(|| overflow(value)),
            None => Ok(None),
        })
        .collect()
}

/// The values of `array`, a temporal column, as its type stores them.
fn raw(array: &ArrayRef) -> Vec<Option<i64>> {
    fn wide<T: ArrowPrimitiveType<Native = i64>>(array: &ArrayRef) -> Vec<Option<i64>> {
        array.as_primitive::<T>().iter().collect()
    }
    fn narrow<T: ArrowPrimitiveType<Native = i32>>(array: &ArrayRef) -> Vec<Option<i64>> {
        let values = array.as_primitive::<T>().iter();
        values.map(|value| value.map(i64::from)).collect()
    }
    match array.data_type() {
        DataType::Date32 => narrow::<Date32Type>(array),
        DataType::Date64 => wide::<Date64Type>(array),
        DataType::Time32(TimeUnit::Second) => narrow::<Time32SecondType>(array),
        DataType::Time32(_) => narrow::<Time32MillisecondType>(array),
        DataType::Time64(TimeUnit::Microsecond) => wide::<Time64MicrosecondType>(array),
        DataType::Time64(_) => wide::<Time64NanosecondType>(array),
        DataType::Timestamp(TimeUnit::Second, _) => wide::<TimestampSecondType>(array),
        DataType::Timestamp(TimeUnit::Millisecond, _) => wide::<TimestampMillisecondType>(array),
        DataType::Timestamp(TimeUnit::Microsecond, _) => wide::<TimestampMicrosecondType>(array),
        DataType::Timestamp(TimeUnit::Nanosecond, _) => wide::<TimestampNanosecondType>(array),
        DataType::Duration(TimeUnit::Second) => wide::<DurationSecondType>(array),
        DataType::Duration(TimeUnit::Millisecond) => wide::<DurationMillisecondType>(array),
        DataType::Duration(TimeUnit::Microsecond) => wide::<DurationMicrosecondType>(array),
        DataType::Duration(TimeUnit::Nanosecond) => wide::<DurationNanosecondType>(array),
        _ => vec![None; array.len()],
    }
}

/// The days `array`, dates, holds; a `Date64` that is no whole day is refused.
fn days(array: &ArrayRef) -> Result<Vec<Option<i64>>, ArrowError> {
    const DAY: i64 = 86_400_000;
    let values = raw(array);
    if *array.data_type() != DataType::Date64 {
        return Ok(values);
    }
    values
        .into_iter()
        .map(|millis| match millis {
            Some(millis) if millis % DAY != 0 => Err(overflow(millis)),
            other => Ok(other.map(|millis| millis / DAY)),
        })
        .collect()
}

/// `local` wall-clock values of `unit` as the instants they name in `zone`, or as they are
/// without one.
fn placed(
    local: Vec<Option<i64>>,
    unit: TimeUnit,
    zone: Option<&str>,
) -> Result<Vec<Option<i64>>, ArrowError> {
    let Some(zone) = zone else {
        return Ok(local);
    };
    let tz: Tz = zone.parse()?;
    local
        .into_iter()
        .map(|value| {
            value
                .map(|value| instant(value, unit, zone, tz))
                .transpose()
        })
        .collect()
}

/// The instant the wall-clock `value`, of `unit`, names in `zone`.
fn instant(value: i64, unit: TimeUnit, zone: &str, tz: Tz) -> Result<i64, ArrowError> {
    let offset = if let Some(offset) = fixed_offset(zone) {
        offset
    } else {
        // A named zone's offsets are known only for the years a calendar holds.
        let local = naive(value, unit).ok_or_else(|| overflow(value))?;
        let offset = match tz.offset_from_local_datetime(&local) {
            LocalResult::Single(offset) | LocalResult::Ambiguous(offset, _) => offset,
            // Clocks skipped `local`: the offset in force a day earlier moves it forward by
            // the gap.
            LocalResult::None => {
                let before = local
                    .checked_sub_signed(TimeDelta::days(1))
                    .ok_or_else(|| overflow(value))?;
                tz.offset_from_utc_datetime(&before)
            }
        };
        i64::from(offset.fix().local_minus_utc())
    };
    offset
        .checked_mul(per_second(unit))
        .and_then(|offset| value.checked_sub(offset))
        .ok_or_else(|| overflow(value))
}

/// Seconds a zone written as `UTC` or `±HH:MM` is ahead of UTC; `None` for a named zone.
fn fixed_offset(zone: &str) -> Option<i64> {
    if zone == "UTC" || zone == "Z" {
        return Some(0);
    }
    let sign = match zone.as_bytes().first()? {
        b'+' => 1,
        b'-' => -1,
        _ => return None,
    };
    let digits = zone.get(1..)?;
    let (hours, minutes) = match digits.split_once(':') {
        Some(parts) => parts,
        None => (digits.get(..2)?, digits.get(2..)?),
    };
    Some(sign * (hours.parse::<i64>().ok()? * 3_600 + minutes.parse::<i64>().ok()? * 60))
}

/// The wall-clock time `value`, of `unit`, where a calendar can hold it.
fn naive(value: i64, unit: TimeUnit) -> Option<NaiveDateTime> {
    match unit {
        TimeUnit::Second => as_datetime::<TimestampSecondType>(value),
        TimeUnit::Millisecond => as_datetime::<TimestampMillisecondType>(value),
        TimeUnit::Microsecond => as_datetime::<TimestampMicrosecondType>(value),
        TimeUnit::Nanosecond => as_datetime::<TimestampNanosecondType>(value),
    }
}

fn timestamps(values: Vec<Option<i64>>, unit: TimeUnit, zone: Option<Arc<str>>) -> ArrayRef {
    type Of<T> = PrimitiveArray<T>;
    match unit {
        TimeUnit::Second => {
            Arc::new(Of::<TimestampSecondType>::from(values).with_timezone_opt(zone))
        }
        TimeUnit::Millisecond => {
            Arc::new(Of::<TimestampMillisecondType>::from(values).with_timezone_opt(zone))
        }
        TimeUnit::Microsecond => {
            Arc::new(Of::<TimestampMicrosecondType>::from(values).with_timezone_opt(zone))
        }
        TimeUnit::Nanosecond => {
            Arc::new(Of::<TimestampNanosecondType>::from(values).with_timezone_opt(zone))
        }
    }
}

fn durations(values: Vec<Option<i64>>, unit: TimeUnit) -> ArrayRef {
    match unit {
        TimeUnit::Second => Arc::new(PrimitiveArray::<DurationSecondType>::from(values)),
        TimeUnit::Millisecond => Arc::new(PrimitiveArray::<DurationMillisecondType>::from(values)),
        TimeUnit::Microsecond => Arc::new(PrimitiveArray::<DurationMicrosecondType>::from(values)),
        TimeUnit::Nanosecond => Arc::new(PrimitiveArray::<DurationNanosecondType>::from(values)),
    }
}

/// `values` as times of day of the type `to`, refused where its 32 bits cannot hold one.
fn times(values: Vec<Option<i64>>, to: &DataType) -> Result<ArrayRef, ArrowError> {
    let narrow = |values: Vec<Option<i64>>| {
        values
            .into_iter()
            .map(|value| match value {
                Some(value) => i32::try_from(value).map(Some).map_err(|_| overflow(value)),
                None => Ok(None),
            })
            .collect::<Result<Vec<Option<i32>>, ArrowError>>()
    };
    Ok(match to {
        DataType::Time32(TimeUnit::Second) => {
            Arc::new(PrimitiveArray::<Time32SecondType>::from(narrow(values)?))
        }
        DataType::Time32(TimeUnit::Millisecond) => Arc::new(
            PrimitiveArray::<Time32MillisecondType>::from(narrow(values)?),
        ),
        DataType::Time64(TimeUnit::Microsecond) => {
            Arc::new(PrimitiveArray::<Time64MicrosecondType>::from(values))
        }
        DataType::Time64(TimeUnit::Nanosecond) => {
            Arc::new(PrimitiveArray::<Time64NanosecondType>::from(values))
        }
        // Arrow has no other time of day: seconds and milliseconds in 32 bits, the finer in 64.
        other => return Err(inexact(other, to)),
    })
}

/// `source` as a struct of `fields`: each field its column of that name, converted, or nulls.
fn structs(source: &StructArray, fields: &Fields) -> Result<ArrayRef, ArrowError> {
    let columns = fields
        .iter()
        .map(|field| match source.column_by_name(field.name()) {
            Some(column) => retyped(column, field.data_type()),
            None => Ok(new_null_array(field.data_type(), source.len())),
        })
        .collect::<Result<Vec<_>, _>>()?;
    let converted = StructArray::try_new(fields.clone(), columns, source.nulls().cloned())?;
    Ok(Arc::new(converted))
}

/// `source` as a list of `item`: its items converted, its lists as they are.
fn lists(source: &ListArray, item: &FieldRef) -> Result<ArrayRef, ArrowError> {
    let values = retyped(source.values(), item.data_type())?;
    let converted = ListArray::try_new(
        Arc::clone(item),
        source.offsets().clone(),
        values,
        source.nulls().cloned(),
    )?;
    Ok(Arc::new(converted))
}
