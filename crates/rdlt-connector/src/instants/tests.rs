use std::sync::Arc;

use arrow_array::types::{Date32Type, Int32Type, Int64Type, TimestampSecondType};
use arrow_array::{
    Array, ArrayRef, Date32Array, Date64Array, PrimitiveArray, Time32SecondArray,
    TimestampMillisecondArray, TimestampNanosecondArray, TimestampSecondArray,
};
use arrow_buffer::NullBuffer;
use arrow_schema::{DataType, TimeUnit};

use super::{DAY, nanos, stored, widened, widens};

#[test]
fn a_date_denotes_its_midnight_in_utc_whatever_zone_shows_it() {
    let dates: ArrayRef = Arc::new(Date32Array::from(vec![Some(18_262), None, Some(-1)]));
    for zone in [None, Some("UTC"), Some("America/New_York"), Some("+05:30")] {
        let to = DataType::Timestamp(TimeUnit::Second, zone.map(Arc::from));
        let placed = widened(&dates, &to).unwrap();
        let placed = placed
            .as_any()
            .downcast_ref::<PrimitiveArray<TimestampSecondType>>()
            .unwrap();
        assert_eq!(
            placed.iter().collect::<Vec<_>>(),
            [Some(18_262 * 86_400), None, Some(-86_400)],
            "{zone:?}"
        );
    }
}

#[test]
fn a_date64_is_the_day_it_is_within() {
    let dates: ArrayRef = Arc::new(Date64Array::from(vec![-1, 86_400_000 + 1]));
    let days = widened(&dates, &DataType::Date32).unwrap();
    let days = days
        .as_any()
        .downcast_ref::<PrimitiveArray<Date32Type>>()
        .unwrap();
    assert_eq!(days.values().to_vec(), [-1, 1]);
    assert_eq!(nanos(&DataType::Date64, -1), Some(-DAY));
}

#[test]
fn a_widening_never_moves_an_instant_and_refuses_what_it_would_round_or_drop() {
    let zoned: ArrayRef = Arc::new(TimestampSecondArray::from(vec![7]).with_timezone("+01:00"));
    let utc = DataType::Timestamp(TimeUnit::Millisecond, Some("UTC".into()));
    let moved = widened(&zoned, &utc).unwrap();
    assert_eq!(
        moved
            .as_any()
            .downcast_ref::<TimestampMillisecondArray>()
            .unwrap()
            .value(0),
        7_000
    );
    // A zoned instant is no wall-clock time; a coarser unit would round.
    assert!(widened(&zoned, &DataType::Timestamp(TimeUnit::Second, None)).is_err());
    let fine: ArrayRef = Arc::new(TimestampMillisecondArray::from(vec![1]));
    assert!(widened(&fine, &DataType::Timestamp(TimeUnit::Second, None)).is_err());
    // A value the finer unit cannot hold is refused, never wrapped.
    let far: ArrayRef = Arc::new(TimestampSecondArray::from(vec![i64::MAX]));
    assert!(widened(&far, &DataType::Timestamp(TimeUnit::Nanosecond, None)).is_err());
    let late: ArrayRef = Arc::new(Time32SecondArray::from(vec![i32::MAX]));
    assert!(widened(&late, &DataType::Time32(TimeUnit::Millisecond)).is_err());
    assert!(widened(&late, &DataType::Date32).is_err());
}

#[test]
fn a_timestamp_that_only_takes_a_zone_keeps_its_buffer() {
    let naive: ArrayRef = Arc::new(TimestampSecondArray::from(vec![Some(7), None]));
    let zoned = DataType::Timestamp(TimeUnit::Second, Some("UTC".into()));
    let widened = widened(&naive, &zoned).unwrap();
    assert_eq!(widened.data_type(), &zoned);
    let buffer = |array: &ArrayRef| array.to_data().buffers()[0].as_ptr();
    assert_eq!(buffer(&widened), buffer(&naive));
}

/// Every temporal type a widening starts or ends at.
fn temporal_types() -> Vec<DataType> {
    let units = [
        TimeUnit::Second,
        TimeUnit::Millisecond,
        TimeUnit::Microsecond,
        TimeUnit::Nanosecond,
    ];
    let mut types = vec![
        DataType::Date32,
        DataType::Date64,
        DataType::Time32(TimeUnit::Second),
        DataType::Time32(TimeUnit::Millisecond),
        DataType::Time64(TimeUnit::Microsecond),
        DataType::Time64(TimeUnit::Nanosecond),
    ];
    for unit in units {
        types.push(DataType::Timestamp(unit, None));
        types.push(DataType::Timestamp(unit, Some("UTC".into())));
        types.push(DataType::Duration(unit));
    }
    types
}

/// `values`, stored as `data_type` stores them, a null last over `hidden`.
fn stored_as(data_type: &DataType, values: &[i64], hidden: i64) -> ArrayRef {
    let nulls = NullBuffer::from_iter((0..=values.len()).map(|row| row < values.len()));
    let all = values.iter().copied().chain([hidden]);
    let plain: ArrayRef = match data_type {
        DataType::Date32 | DataType::Time32(_) => {
            let narrow = all.map(|value| i32::try_from(value).unwrap_or(i32::MAX));
            Arc::new(PrimitiveArray::<Int32Type>::new(
                narrow.collect(),
                Some(nulls),
            ))
        }
        _ => Arc::new(PrimitiveArray::<Int64Type>::new(all.collect(), Some(nulls))),
    };
    let data = plain.to_data().into_builder().data_type(data_type.clone());
    arrow_array::make_array(data.build().unwrap())
}

#[test]
fn every_widening_keeps_each_value_s_instant() {
    let mut widenings = 0;
    for from in temporal_types() {
        for to in temporal_types().into_iter().filter(|to| widens(&from, to)) {
            // Within what every finer type holds: 86,401 days are nanoseconds an `i64` holds,
            // and as a `Date64`'s milliseconds part of the day before the epoch's.
            let values = [0, 1, -1, 59, 1_000, -86_401];
            let array = stored_as(&from, &values, 0);
            let widened = widened(&array, &to).unwrap();
            assert_eq!(widened.data_type(), &to, "{from} to {to}");
            assert!(widened.is_null(values.len()), "{from} to {to}");
            for row in 0..values.len() {
                let value = stored(array.as_ref(), row).unwrap();
                let denoted = nanos(&from, i128::from(value)).unwrap();
                let held = i128::from(stored(widened.as_ref(), row).unwrap());
                assert_eq!(nanos(&to, held), Some(denoted), "{from} {value} to {to}");
            }
            widenings += 1;
        }
    }
    assert!(widenings >= 60, "{widenings}");
}

#[test]
fn a_null_slot_hiding_a_value_beyond_the_target_is_never_widened() {
    let shapes = [
        // 64-bit in, 64-bit out.
        (
            DataType::Timestamp(TimeUnit::Second, None),
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            i64::MAX,
        ),
        // 32-bit in, 64-bit out.
        (
            DataType::Date32,
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            i64::from(i32::MAX),
        ),
        // 32-bit in, 32-bit out.
        (
            DataType::Time32(TimeUnit::Second),
            DataType::Time32(TimeUnit::Millisecond),
            i64::from(i32::MAX),
        ),
        // 64-bit in, 32-bit out.
        (DataType::Date64, DataType::Date32, i64::MAX),
    ];
    for (from, to, hidden) in shapes {
        let array = stored_as(&from, &[1], hidden);
        let widened =
            widened(&array, &to).unwrap_or_else(|error| panic!("{from} to {to}: {error}"));
        assert!(widened.is_null(1), "{from} to {to}");
        assert!(widened.is_valid(0), "{from} to {to}");
    }
}

#[test]
fn a_value_beyond_its_target_is_refused_naming_it_and_the_target() {
    let far: ArrayRef = Arc::new(TimestampSecondArray::from(vec![7, i64::MAX]));
    let to = DataType::Timestamp(TimeUnit::Nanosecond, None);
    let error = widened(&far, &to).unwrap_err();
    assert_eq!(
        error.to_string(),
        format!("Cast error: value {} is beyond what a {to} holds", i64::MAX)
    );
    let late: ArrayRef = Arc::new(Time32SecondArray::from(vec![i32::MAX]));
    let to = DataType::Time32(TimeUnit::Millisecond);
    assert_eq!(
        widened(&late, &to).unwrap_err().to_string(),
        format!("Cast error: value {} is beyond what a {to} holds", i32::MAX)
    );
}

#[test]
fn a_date64_part_of_a_day_widens_to_its_midnight() {
    let dates: ArrayRef = Arc::new(Date64Array::from(vec![-1, 86_400_000 + 1, -86_400_000]));
    let to = DataType::Timestamp(TimeUnit::Millisecond, None);
    let widened = widened(&dates, &to).unwrap();
    let widened = widened
        .as_any()
        .downcast_ref::<TimestampMillisecondArray>()
        .unwrap();
    assert_eq!(
        widened.values().to_vec(),
        [-86_400_000, 86_400_000, -86_400_000]
    );
}

#[test]
fn a_slice_widens_its_own_rows() {
    let seconds: ArrayRef = Arc::new(TimestampSecondArray::from(vec![
        Some(1),
        None,
        Some(3),
        Some(4),
        None,
        Some(6),
        Some(7),
    ]));
    let to = DataType::Timestamp(TimeUnit::Nanosecond, None);
    let whole = widened(&seconds, &to).unwrap();
    for (offset, len) in [(0, 7), (1, 3), (2, 4), (4, 1), (6, 1), (3, 0)] {
        let part = widened(&seconds.slice(offset, len), &to).unwrap();
        assert_eq!(
            part.to_data(),
            whole.slice(offset, len).to_data(),
            "{offset} {len}"
        );
        let part = part
            .as_any()
            .downcast_ref::<TimestampNanosecondArray>()
            .unwrap();
        let rows: Vec<Option<i64>> = part.iter().collect();
        let expected: Vec<Option<i64>> = (offset..offset + len)
            .map(|row| {
                seconds
                    .is_valid(row)
                    .then(|| i64::try_from(row + 1).unwrap() * 1_000_000_000)
            })
            .collect();
        assert_eq!(rows, expected, "{offset} {len}");
    }
}
