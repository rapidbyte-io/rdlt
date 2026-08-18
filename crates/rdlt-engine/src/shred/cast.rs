//! The exact-cast guard for the Arrow path: refuse any cast that would
//! silently ALTER a value, at every nesting depth arrow's own `cast`
//! recurses through.

use arrow::{
    array::{Array as _, Int64Array, StructArray, Time64NanosecondArray, TimestampNanosecondArray},
    datatypes::{DataType, TimeUnit},
};

use super::infer::int64_fits_in_f64;

/// Refuse any cast `source` → `target` that would silently ALTER a value:
/// arrow's `cast` recurses through struct fields, list elements, and
/// dictionary values, so the exactness check recurses with it — a top-level
/// check alone lets the same silent roundings through one nesting level
/// down.
///
/// Three leaf shapes are lossy and refused; everything else the
/// registry can produce casts exactly (representation differences and
/// in-range widenings; `Float64 ⊔ Decimal = Utf8` escalates in the
/// lattice before a cast is ever built):
///
/// - **Int64 → Float64**: an integer beyond ±2^53 has no exact f64;
///   arrow rounds. The JSON path escalates the same value to text.
/// - **Nanosecond → microsecond** (timestamp or time): arrow's integer
///   division truncates TOWARD ZERO — pre-epoch values round the wrong
///   way. Nanosecond is arrow's default unit for many producers; the
///   engine's canonical unit is microsecond.
/// - **Date64 → Date32** with a pre-epoch intra-day value: truncation
///   toward zero is not day-floor, mis-dating by one day.
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
            // target field the source lacks is null-filled elsewhere,
            // and a source field the target lacks never reaches a cast.
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
        _ => Ok(()),
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
