//! The exact-cast guard for the Arrow path: refuse any cast that would
//! silently ALTER a value, at every nesting depth arrow's own `cast`
//! recurses through.

use arrow::{
    array::{
        Array as _, Decimal128Array, Int64Array, StructArray, Time64NanosecondArray,
        TimestampNanosecondArray,
    },
    datatypes::{DataType, TimeUnit},
};

use super::infer::int64_fits_in_f64;

/// Refuse any cast `source` → `target` that would silently ALTER a value:
/// arrow's `cast` recurses through struct fields, list elements, and
/// dictionary values, so the exactness check recurses with it — a top-level
/// check alone lets the same silent roundings through one nesting level
/// down.
///
/// Six leaf shapes are lossy and refused — three that ROUND and three
/// that leave the target's RANGE; everything else the registry can
/// produce casts exactly (representation differences and in-range
/// widenings; `Float64 ⊔ Decimal = Utf8` escalates in the lattice before
/// a cast is ever built):
///
/// - **Int64 → Float64**: an integer beyond ±2^53 has no exact f64;
///   arrow rounds. The JSON path escalates the same value to text.
/// - **Nanosecond → microsecond** (timestamp or time): arrow's integer
///   division truncates TOWARD ZERO — pre-epoch values round the wrong
///   way. Nanosecond is arrow's default unit for many producers; the
///   engine's canonical unit is microsecond.
/// - **Date64 → Date32** with a pre-epoch intra-day value: truncation
///   toward zero is not day-floor, mis-dating by one day.
/// - **Date64 → Date32** with a day count outside i32: arrow truncates
///   the day count to i32 — a silently WRONG date.
/// - **Timestamp unit upscale** (s/ms → µs): arrow multiplies in i64 and
///   its safe mode turns an overflowing product into NULL.
/// - **Decimal → Decimal** with a value whose rescaled magnitude exceeds
///   the target precision: arrow validates no decoded value against its
///   declared precision, and nulls or carries the out-of-precision result.
///
/// The seat that runs this walk also compares null counts across the
/// cast (its belt): the walk makes a refusal PRECISE, the belt makes it
/// COMPLETE, so an unmodeled safe-mode null still refuses.
///
/// `path` names the position for the refusal message (`v` at the top
/// level, `v.f`/`v[]` nested). Depth is bounded by the flatbuffer
/// verifier's 64-level schema cap upstream — this walk follows types
/// arrow itself will walk, never deeper.
pub(super) fn refuse_inexact_cast(
    source: &dyn arrow::array::Array,
    target: &DataType,
    path: &str,
) -> Result<(), String> {
    // A dictionary encodes another type; casting one casts its VALUES,
    // so the walk descends to them — through the any-dictionary view,
    // which covers every key width arrow admits (a fixed downcast list
    // would panic on the widths it missed).
    if let Some(dict) = arrow::array::AsArray::as_any_dictionary_opt(source) {
        return refuse_inexact_cast(dict.values().as_ref(), target, path);
    }
    let target = match target {
        DataType::Dictionary(_, value) => value.as_ref(),
        _ => target,
    };
    match (source.data_type(), target) {
        (DataType::Struct(fields), DataType::Struct(target_fields)) => {
            let struct_array = source
                .as_any()
                .downcast_ref::<StructArray>()
                .expect("arrow types and arrays agree");
            // Pair by NAME (the registry's join is a name-union); a
            // target field the source lacks is null-filled by the seat
            // before the cast, and a source field the target lacks never
            // reaches a cast.
            for target_field in target_fields {
                if let Some((index, _)) = fields.find(target_field.name()) {
                    refuse_inexact_cast(
                        struct_array.column(index),
                        target_field.data_type(),
                        &format!("{path}.{}", target_field.name()),
                    )?;
                }
            }
            Ok(())
        }
        (
            DataType::List(_) | DataType::LargeList(_) | DataType::FixedSizeList(_, _),
            DataType::List(target_item)
            | DataType::LargeList(target_item)
            | DataType::FixedSizeList(target_item, _),
        ) => {
            let values = source
                .as_any()
                .downcast_ref::<arrow::array::GenericListArray<i32>>()
                .map(|list| list.values())
                .or_else(|| {
                    source
                        .as_any()
                        .downcast_ref::<arrow::array::GenericListArray<i64>>()
                        .map(|list| list.values())
                })
                .or_else(|| {
                    source
                        .as_any()
                        .downcast_ref::<arrow::array::FixedSizeListArray>()
                        .map(|list| list.values())
                })
                .expect("the match arm admits exactly these three list arrays");
            refuse_inexact_cast(
                values.as_ref(),
                target_item.data_type(),
                &format!("{path}[]"),
            )
        }
        (DataType::Int64, DataType::Float64) => {
            let ints = source
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("arrow types and arrays agree");
            (0..ints.len())
                .any(|i| !ints.is_null(i) && !int64_fits_in_f64(ints.value(i)))
                .then(|| {
                    format!(
                        "column `{path}`: widening Int64 to Float64 would silently round a value \
                     beyond ±2^53 (losslessness is the column's contract, and the JSONL path \
                     escalates the same value to text) — declare the column as text, or keep \
                     the source integral"
                    )
                })
                .map_or(Ok(()), Err)
        }
        (
            DataType::Timestamp(TimeUnit::Nanosecond, _),
            DataType::Timestamp(TimeUnit::Microsecond, _),
        ) => {
            let nanos = source
                .as_any()
                .downcast_ref::<TimestampNanosecondArray>()
                .expect("arrow types and arrays agree");
            let inexact =
                (0..nanos.len()).any(|i| !nanos.is_null(i) && nanos.value(i) % 1_000 != 0);
            refuse_sub_microsecond(inexact, path, "timestamps")
        }
        // A coarser source unit multiplies up: refuse where the product
        // leaves i64 — checked on the extreme present values, since the
        // multiplication is monotone.
        (DataType::Timestamp(from, _), DataType::Timestamp(to, _))
            if unit_multiple(*from) < unit_multiple(*to) =>
        {
            let factor = unit_multiple(*to) / unit_multiple(*from);
            let overflowing = timestamp_bounds(source)
                .into_iter()
                .flat_map(|(min, max)| [min, max])
                .find(|extreme| extreme.checked_mul(factor).is_none());
            overflowing
                .map(|value| {
                    format!(
                        "column `{path}`: casting {} to {target} would overflow for the value \
                         {value} (arrow multiplies the unit up in 64 bits and nulls the \
                         overflow) — deliver values within ±{} in the source unit, or declare \
                         the column as text",
                        source.data_type(),
                        i64::MAX / factor
                    )
                })
                .map_or(Ok(()), Err)
        }
        (DataType::Time64(TimeUnit::Nanosecond), DataType::Time64(TimeUnit::Microsecond)) => {
            let nanos = source
                .as_any()
                .downcast_ref::<Time64NanosecondArray>()
                .expect("arrow types and arrays agree");
            let inexact =
                (0..nanos.len()).any(|i| !nanos.is_null(i) && nanos.value(i) % 1_000 != 0);
            refuse_sub_microsecond(inexact, path, "times")
        }
        (DataType::Date64, DataType::Date32) => {
            let days = source
                .as_any()
                .downcast_ref::<arrow::array::Date64Array>()
                .expect("arrow types and arrays agree");
            const MS_PER_DAY: i64 = 86_400_000;
            // Arrow's Date64→Date32 cast truncates the day count to i32:
            // a day count outside that range wraps to a wrong date.
            if let Some(value) = (0..days.len())
                .filter(|&i| !days.is_null(i))
                .map(|i| days.value(i))
                .find(|value| i32::try_from(value / MS_PER_DAY).is_err())
            {
                return Err(format!(
                    "column `{path}`: casting Date64 to Date32 would wrap the value {value} ms \
                     (day {}) outside the 32-bit day range ±{} (arrow truncates the day count \
                     to i32) — deliver dates within that range, or declare the column as text",
                    value / MS_PER_DAY,
                    i32::MAX
                ));
            }
            (0..days.len())
                .any(|i| !days.is_null(i) && days.value(i) < 0 && days.value(i) % MS_PER_DAY != 0)
                .then(|| {
                    format!(
                        "column `{path}`: casting Date64 to Date32 would mis-date a pre-epoch \
                         intra-day value by one day (arrow truncates toward zero, not to the \
                         day floor) — deliver whole-day values, or declare the column as text"
                    )
                })
                .map_or(Ok(()), Err)
        }
        (DataType::Decimal128(_, source_scale), DataType::Decimal128(precision, scale)) => {
            let decimals = source
                .as_any()
                .downcast_ref::<Decimal128Array>()
                .expect("arrow types and arrays agree");
            // Arrow validates no decoded value against its declared
            // precision, so the source may carry more digits than it
            // claims; refuse any value whose rescaled magnitude leaves
            // the target's precision, or whose scale reduction would drop
            // digits — arrow nulls or carries the former and rounds the
            // latter.
            let limit = 10i128.pow(u32::from(*precision));
            let inexact = (0..decimals.len())
                .filter(|&i| !decimals.is_null(i))
                .map(|i| decimals.value(i))
                .find(|value| {
                    rescale_decimal(*value, *source_scale, *scale)
                        .is_none_or(|scaled| scaled.unsigned_abs() >= limit.unsigned_abs())
                });
            inexact
                .map(|value| {
                    format!(
                        "column `{path}`: casting {} to {target} would lose the value {value} \
                         (scale {source_scale}) — it does not fit precision {precision} at \
                         scale {scale} (arrow's decimal cast validates no value against its \
                         declared precision) — declare the column as text, or keep values \
                         within the declared precision",
                        source.data_type()
                    )
                })
                .map_or(Ok(()), Err)
        }
        _ => Ok(()),
    }
}

/// Ticks per second of a timestamp unit — the ratio of two units is the
/// cast's multiplier or divisor.
fn unit_multiple(unit: TimeUnit) -> i64 {
    match unit {
        TimeUnit::Second => 1,
        TimeUnit::Millisecond => 1_000,
        TimeUnit::Microsecond => 1_000_000,
        TimeUnit::Nanosecond => 1_000_000_000,
    }
}

/// The smallest and largest non-null values of a timestamp array (every
/// unit is an i64 count, so the array reinterprets as Int64 without a
/// copy); `None` when every value is null.
fn timestamp_bounds(source: &dyn arrow::array::Array) -> Option<(i64, i64)> {
    let ints =
        arrow::compute::cast(source, &DataType::Int64).expect("a timestamp reinterprets as Int64");
    let ints = ints
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("arrow types and arrays agree");
    Some((arrow::compute::min(ints)?, arrow::compute::max(ints)?))
}

/// A decimal value carried from `from` scale to `to` scale, exactly:
/// `None` when the scale-up overflows i128 or the scale-down would drop
/// non-zero digits.
fn rescale_decimal(value: i128, from: i8, to: i8) -> Option<i128> {
    let delta = u32::try_from(i32::from(to) - i32::from(from));
    match delta {
        Ok(up) => value.checked_mul(10i128.checked_pow(up)?),
        Err(_) => {
            let divisor =
                10i128.checked_pow(u32::try_from(i32::from(from) - i32::from(to)).ok()?)?;
            (value % divisor == 0).then(|| value / divisor)
        }
    }
}

/// The nanosecond→microsecond leaf, shared by the timestamp and time
/// arms (both wrap i64 nanoseconds).
fn refuse_sub_microsecond(inexact: bool, path: &str, kind: &str) -> Result<(), String> {
    inexact
        .then(|| {
            format!(
                "column `{path}`: casting nanosecond {kind} to the canonical microsecond unit \
                 would silently truncate a value not divisible by 1,000 (arrow divides toward \
                 zero, so pre-epoch values even round the wrong way) — deliver unit-consistent \
                 batches, or declare the column as text"
            )
        })
        .map_or(Ok(()), Err)
}
