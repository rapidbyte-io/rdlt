mod rendered;

use arrow_array::types::Date32Type;
use arrow_array::{Date32Array, StructArray};

use super::text::{clock, date, duration};
use super::*;

#[test]
fn dates_render_across_the_whole_range() {
    assert_eq!(date(0), "1970-01-01");
    assert_eq!(
        date(11_016),
        "2000-02-29",
        "the last day of a 400-year cycle"
    );
    assert_eq!(date(-135_081), "1600-02-29");
    assert_eq!(date(-25_508), "1900-03-01");
    assert_eq!(date(47_540), "2100-02-28");
    assert_eq!(date(-719_162), "0001-01-01");
    assert_eq!(date(-719_528), "0000-01-01");
    assert_eq!(date(-719_529), "-0001-12-31");
    assert_eq!(date(2_932_897), "+10000-01-01");
    assert_eq!(date(i128::from(i32::MAX)), "+5881580-07-11");
}

#[test]
fn durations_render_as_arrow_renders_them() {
    assert_eq!(duration(1_500_000_000), "PT1.5S");
    assert_eq!(duration(0), "PT0S");
    assert_eq!(duration(-500_000_000), "-PT0.5S");
    assert_eq!(duration(2_000_000_000), "PT2S");
    assert_eq!(
        duration(i128::from(i64::MIN) * NANOS_PER_SECOND),
        "-PT9223372036854775808S"
    );
}

fn timestamps(array: &ArrayRef) -> Vec<Option<i64>> {
    (0..array.len())
        .map(|row| {
            (!array.is_null(row)).then(|| raw_timestamp(array.as_ref(), row, unit_of(array)))
        })
        .collect()
}

fn unit_of(array: &ArrayRef) -> TimeUnit {
    match array.data_type() {
        DataType::Timestamp(unit, _) => *unit,
        other => panic!("{other} is not a timestamp"),
    }
}

fn dates(days: Vec<Option<i32>>) -> ArrayRef {
    Arc::new(Date32Array::from(days))
}

#[test]
fn dates_become_their_midnight_in_a_fixed_zone() {
    let zone: Arc<str> = Arc::from("+05:30");
    let placed = midnights(
        &dates(vec![Some(0), Some(1), None]),
        TimeUnit::Millisecond,
        Some(&zone),
    )
    .unwrap();
    assert_eq!(
        timestamps(&placed),
        [Some(-19_800_000), Some(66_600_000), None]
    );
    let utc = midnights(&dates(vec![Some(-1)]), TimeUnit::Second, None).unwrap();
    assert_eq!(timestamps(&utc), [Some(-86_400)]);
    assert_eq!(
        placed.data_type(),
        &DataType::Timestamp(TimeUnit::Millisecond, Some(zone))
    );
}

#[test]
fn a_midnight_a_named_zone_skips_or_repeats_is_placed_by_the_offset_in_force_or_the_earlier() {
    let skipped: Arc<str> = Arc::from("America/Sao_Paulo");
    let placed = midnights(&dates(vec![Some(17_839)]), TimeUnit::Second, Some(&skipped)).unwrap();
    assert_eq!(
        timestamps(&placed),
        [Some(1_541_300_400)],
        "03:00 UTC, at -03:00"
    );
    let repeated: Arc<str> = Arc::from("America/Havana");
    let placed = midnights(
        &dates(vec![Some(18_203)]),
        TimeUnit::Second,
        Some(&repeated),
    )
    .unwrap();
    assert_eq!(
        timestamps(&placed),
        [Some(1_572_753_600)],
        "the earlier midnight, at -04:00"
    );
}

#[test]
fn a_date_beyond_its_units_range_is_refused_and_a_named_zone_beyond_its_years_too() {
    assert!(midnights(&dates(vec![Some(200_000)]), TimeUnit::Nanosecond, None).is_err());
    let named: Arc<str> = Arc::from("Asia/Kolkata");
    assert!(midnights(&dates(vec![Some(i32::MAX)]), TimeUnit::Second, Some(&named)).is_err());
    let fixed: Arc<str> = Arc::from("-03:30");
    let far = midnights(&dates(vec![Some(i32::MAX)]), TimeUnit::Second, Some(&fixed)).unwrap();
    assert_eq!(
        timestamps(&far),
        [Some(i64::from(i32::MAX) * 86_400 + 12_600)]
    );
}

#[test]
fn wall_clock_times_become_the_instants_they_name_in_a_zone() {
    let naive: ArrayRef = Arc::new(TimestampMillisecondArray::from(vec![Some(1_000), None]));
    let zone: Arc<str> = Arc::from("+05:00");
    let placed = localized(&naive, TimeUnit::Second, &zone).unwrap();
    assert_eq!(timestamps(&placed), [Some(1 - 18_000), None]);
    let huge: ArrayRef = Arc::new(TimestampSecondArray::from(vec![i64::MAX / 2]));
    assert!(
        localized(&huge, TimeUnit::Nanosecond, &zone).is_err(),
        "no unit wraps"
    );
}

#[test]
fn fixed_offsets_read_from_their_names() {
    assert_eq!(fixed_offset("UTC"), Some(0));
    assert_eq!(fixed_offset("+05:30"), Some(19_800));
    assert_eq!(fixed_offset("-03:30"), Some(-12_600));
    assert_eq!(fixed_offset("+0530"), Some(19_800));
    assert_eq!(fixed_offset("Asia/Kolkata"), None);
}

#[test]
fn instants_beyond_the_years_arrow_renders_are_rendered_in_utc() {
    let far: ArrayRef =
        Arc::new(TimestampSecondArray::from(vec![100_000_000_000_000]).with_timezone("+05:00"));
    let rendered = text(&far).unwrap();
    assert_eq!(
        rendered.as_string::<i32>().value(0),
        "+3170843-11-07T09:46:40Z"
    );
    let naive: ArrayRef = Arc::new(TimestampSecondArray::from(vec![100_000_000_000_000]));
    assert_eq!(
        text(&naive).unwrap().as_string::<i32>().value(0),
        "+3170843-11-07T09:46:40"
    );
}

#[test]
fn clocks_render_fractions_in_three_six_or_nine_digits() {
    assert_eq!(clock(3_661 * NANOS_PER_SECOND), "01:01:01");
    assert_eq!(clock(1_500_000_000), "00:00:01.500");
    assert_eq!(clock(1_000_001_000), "00:00:01.000001");
    assert_eq!(clock(1_000_000_001), "00:00:01.000000001");
}

#[test]
fn times_outside_a_day_render_as_signed_clocks() {
    let times: ArrayRef = Arc::new(Time32MillisecondArray::from(vec![-1_500, 90_000_000]));
    let rendered = text(&times).unwrap();
    let rendered = rendered.as_string::<i32>();
    assert_eq!(rendered.value(0), "-00:00:01.500");
    assert_eq!(rendered.value(1), "25:00:00");
}

#[test]
fn a_time_a_zone_east_of_utc_skips_moves_forward_by_the_gap() {
    // Berlin skipped from 02:00 to 03:00 on 2024-03-31: 02:30 is 03:30 at +02:00, 01:30 UTC.
    let naive: ArrayRef = Arc::new(TimestampSecondArray::from(vec![1_711_852_200]));
    let berlin: Arc<str> = Arc::from("Europe/Berlin");
    let placed = localized(&naive, TimeUnit::Second, &berlin).unwrap();
    assert_eq!(timestamps(&placed), [Some(1_711_848_600)]);
    // Tehran skipped midnight on 2020-03-21: it is 01:00 at +04:30, 20:30 UTC the day before.
    let tehran: Arc<str> = Arc::from("Asia/Tehran");
    let placed = midnights(&dates(vec![Some(18_342)]), TimeUnit::Second, Some(&tehran)).unwrap();
    assert_eq!(timestamps(&placed), [Some(1_584_736_200)]);
}

#[test]
fn an_instant_beyond_the_years_arrow_renders_at_midnight_shows_an_unsigned_clock() {
    let midnight: ArrayRef = Arc::new(TimestampSecondArray::from(vec![1_000_000_000 * 86_400]));
    let rendered = text(&midnight).unwrap();
    assert_eq!(
        rendered.as_string::<i32>().value(0),
        "+2739877-01-03T00:00:00"
    );
}

#[test]
fn a_date64_holding_part_of_a_day_is_the_day_it_is_within_everywhere() {
    use rdlt_connector::{Field, LogicalType, TimeUnit as Unit};
    let day = 86_400_000_i64;
    let millis = vec![-1_i64, -day - 1, day - 1, -day, 0];
    let within = [-1_i64, -2, 0, -1, 0];
    let date64: ArrayRef = Arc::new(arrow_array::Date64Array::from(millis.clone()));
    let seconds: Vec<i64> = within.iter().map(|days| days * 86_400).collect();
    let placed = midnights(&date64, TimeUnit::Second, None).unwrap();
    assert_eq!(
        timestamps(&placed),
        seconds.iter().copied().map(Some).collect::<Vec<_>>()
    );
    let convert = crate::table::convert::convert;
    let as_timestamp = convert(
        &date64,
        &LogicalType::Date,
        &LogicalType::Timestamp(Unit::Second, None),
    );
    assert_eq!(timestamps(&as_timestamp.unwrap()), placed_values(&seconds));
    let as_date = convert(&date64, &LogicalType::Date, &LogicalType::Date).unwrap();
    let days: Vec<i64> = as_date
        .as_primitive::<Date32Type>()
        .values()
        .iter()
        .map(|days| i64::from(*days))
        .collect();
    assert_eq!(days, within);
    let rendered = text(&date64).unwrap();
    let texts: Vec<&str> = rendered.as_string::<i32>().iter().flatten().collect();
    assert_eq!(
        texts,
        [
            "1969-12-31",
            "1969-12-30",
            "1970-01-01",
            "1969-12-31",
            "1970-01-01"
        ]
    );
    // A struct holding a `Date64` holds the day too, whichever way it converts.
    let field = Field::new("d", LogicalType::Date, true);
    let logical = LogicalType::Struct(rdlt_connector::Fields::new(vec![field]).unwrap());
    let fields =
        arrow_schema::Fields::from(vec![arrow_schema::Field::new("d", DataType::Date64, true)]);
    let nested: ArrayRef =
        Arc::new(StructArray::try_new(fields, vec![Arc::clone(&date64)], None).unwrap());
    let held = convert(&nested, &logical, &logical).unwrap();
    let held = held.as_struct().column(0).as_primitive::<Date32Type>();
    let held: Vec<i64> = held.values().iter().map(|days| i64::from(*days)).collect();
    assert_eq!(held, within);
}

fn placed_values(seconds: &[i64]) -> Vec<Option<i64>> {
    seconds.iter().copied().map(Some).collect()
}

#[test]
fn a_date64_beyond_a_date32_s_days_is_refused_as_a_date() {
    use rdlt_connector::LogicalType;
    let far: ArrayRef = Arc::new(arrow_array::Date64Array::from(vec![i64::MAX, i64::MIN]));
    for row in 0..2 {
        let one = far.slice(row, 1);
        let refused = crate::table::convert::convert(&one, &LogicalType::Date, &LogicalType::Date);
        assert!(refused.is_err(), "{row}");
    }
}
