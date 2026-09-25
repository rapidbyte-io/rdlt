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
    Arc::new(arrow_array::Date32Array::from(days))
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
    let times: ArrayRef = Arc::new(arrow_array::Time32MillisecondArray::from(vec![
        -1_500, 90_000_000,
    ]));
    let rendered = text(&times).unwrap();
    let rendered = rendered.as_string::<i32>();
    assert_eq!(rendered.value(0), "-00:00:01.500");
    assert_eq!(rendered.value(1), "25:00:00");
}
