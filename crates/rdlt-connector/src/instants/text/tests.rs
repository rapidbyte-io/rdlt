use super::{NANOS_PER_SECOND, clock, date, duration};

/// What `render` appends for `value` to an empty string.
fn appended(render: fn(i128, &mut String), value: i128) -> String {
    let mut out = String::new();
    render(value, &mut out);
    out
}

#[test]
fn dates_render_across_the_whole_range() {
    assert_eq!(appended(date, 0), "1970-01-01");
    assert_eq!(
        appended(date, 11_016),
        "2000-02-29",
        "the last day of a 400-year cycle"
    );
    assert_eq!(appended(date, -135_081), "1600-02-29");
    assert_eq!(appended(date, -25_508), "1900-03-01");
    assert_eq!(appended(date, 47_540), "2100-02-28");
    assert_eq!(appended(date, -719_162), "0001-01-01");
    assert_eq!(appended(date, -719_528), "0000-01-01");
    assert_eq!(appended(date, -719_529), "-0001-12-31");
    assert_eq!(appended(date, 2_932_897), "+10000-01-01");
    assert_eq!(appended(date, i128::from(i32::MAX)), "+5881580-07-11");
}

#[test]
fn durations_render_as_iso_8601_seconds() {
    assert_eq!(appended(duration, 1_500_000_000), "PT1.5S");
    assert_eq!(appended(duration, 0), "PT0S");
    assert_eq!(appended(duration, -500_000_000), "-PT0.5S");
    assert_eq!(appended(duration, 2_000_000_000), "PT2S");
    assert_eq!(
        appended(duration, i128::from(i64::MIN) * NANOS_PER_SECOND),
        "-PT9223372036854775808S"
    );
}

#[test]
fn clocks_render_fractions_in_three_six_or_nine_digits() {
    assert_eq!(appended(clock, 3_661 * NANOS_PER_SECOND), "01:01:01");
    assert_eq!(appended(clock, 1_500_000_000), "00:00:01.500");
    assert_eq!(appended(clock, 1_000_001_000), "00:00:01.000001");
    assert_eq!(appended(clock, 1_000_000_001), "00:00:01.000000001");
}
