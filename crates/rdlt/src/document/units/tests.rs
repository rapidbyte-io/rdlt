use std::time::Duration;

use proptest::prelude::*;

use super::{size, sized, span, spanned};

#[test]
fn sizes_and_spans_read_their_units_and_refuse_anything_else() {
    for (text, bytes) in [
        ("512MiB", 512 << 20),
        ("1", 1),
        ("64KiB", 65_536),
        ("0B", 0),
        ("3GiB", 3 << 30),
        ("2TiB", 2 << 40),
    ] {
        assert_eq!(size(text), Some(bytes), "{text}");
    }
    for text in [
        "",
        "MiB",
        "+5",
        "5 MiB",
        "5mib",
        "18446744073709551616",
        "17TiB0",
        "1TiBB",
        "-1",
        "1.5GiB",
        "16777216TiB",
    ] {
        assert_eq!(size(text), None, "{text}");
    }
    for (text, duration) in [
        ("10s", Duration::from_secs(10)),
        ("500ms", Duration::from_millis(500)),
        ("250us", Duration::from_micros(250)),
        ("5m", Duration::from_secs(300)),
        ("2h", Duration::from_secs(7_200)),
        ("7ns", Duration::from_nanos(7)),
    ] {
        assert_eq!(span(text), Some(duration), "{text}");
    }
    for text in [
        "10",
        "s",
        "1.5s",
        "-1s",
        "10 s",
        "10S",
        "+1s",
        "99999999999999999999999h",
    ] {
        assert_eq!(span(text), None, "{text}");
    }
}

#[test]
fn a_size_and_a_span_are_written_in_the_largest_unit_that_holds_them() {
    assert_eq!(sized(0), "0B");
    assert_eq!(sized(512 << 20), "512MiB");
    assert_eq!(sized((1 << 30) + 1), "1073741825B");
    assert_eq!(sized(3 << 40), "3TiB");
    assert_eq!(spanned(Duration::ZERO), "0s");
    assert_eq!(spanned(Duration::from_secs(600)), "10m");
    assert_eq!(spanned(Duration::from_millis(1_500)), "1500ms");
    assert_eq!(spanned(Duration::from_secs(7_200)), "2h");
}

proptest! {
    #[test]
    fn a_size_reads_back_as_it_was_written(bytes in any::<u64>()) {
        prop_assert_eq!(size(&sized(bytes)), Some(bytes));
    }

    #[test]
    fn a_span_reads_back_as_it_was_written(
        secs in 0..=u64::MAX / 4,
        nanos in 0..1_000_000_000_u32,
    ) {
        let duration = Duration::new(secs, nanos);
        prop_assert_eq!(span(&spanned(duration)), Some(duration));
    }
}
