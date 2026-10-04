use std::sync::Arc;

use arrow_array::types::{Date32Type, TimestampSecondType};
use arrow_array::{
    Array, ArrayRef, Date32Array, Date64Array, Time32SecondArray, TimestampMillisecondArray,
    TimestampSecondArray,
};
use arrow_schema::{DataType, TimeUnit};

use super::{DAY, nanos, widened};

#[test]
fn a_date_denotes_its_midnight_in_utc_whatever_zone_shows_it() {
    let dates: ArrayRef = Arc::new(Date32Array::from(vec![Some(18_262), None, Some(-1)]));
    for zone in [None, Some("UTC"), Some("America/New_York"), Some("+05:30")] {
        let to = DataType::Timestamp(TimeUnit::Second, zone.map(Arc::from));
        let placed = widened(&dates, &to).unwrap();
        let placed = placed
            .as_any()
            .downcast_ref::<arrow_array::PrimitiveArray<TimestampSecondType>>()
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
        .downcast_ref::<arrow_array::PrimitiveArray<Date32Type>>()
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
