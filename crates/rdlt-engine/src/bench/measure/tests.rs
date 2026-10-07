use proptest::prelude::*;

use super::{Paired, interval};

#[test]
#[expect(clippy::float_cmp, reason = "the median of these ratios is exact")]
fn the_median_is_the_middle_ratio_or_the_mean_of_the_middle_two() {
    for (ratios, median) in [
        (vec![1.2], 1.2),
        (vec![3.0, 1.0, 2.0], 2.0),
        (vec![4.0, 1.0, 3.0, 2.0], 2.5),
        (vec![1.5, 1.5, 0.5, 9.0, 1.0], 1.5),
    ] {
        assert_eq!(Paired::of(&ratios).unwrap().median, median, "{ratios:?}");
    }
}

#[test]
fn no_ratios_have_no_median() {
    assert_eq!(Paired::of(&[]), None);
}

#[test]
fn equal_ratios_have_an_interval_of_no_width() {
    let paired = Paired::of(&[1.25; 30]).unwrap();
    assert_eq!(
        paired,
        Paired {
            median: 1.25,
            low: 1.25,
            high: 1.25,
            samples: 30,
        }
    );
}

#[test]
fn the_same_ratios_give_the_same_interval_on_every_run() {
    let ratios: Vec<f64> = (0..30).map(|step| 1.0 + f64::from(step) / 100.0).collect();
    let paired = Paired::of(&ratios).unwrap();
    assert_eq!(Paired::of(&ratios), Some(paired));
    assert_eq!(
        (paired.median, paired.low, paired.high),
        (1.145, 1.095_000_000_000_000_2, 1.194_999_999_999_999_8)
    );
}

#[test]
fn the_interval_leaves_a_fortieth_of_the_values_out_at_each_end() {
    for (values, ends) in [
        (
            (0..10_000).rev().map(f64::from).collect::<Vec<_>>(),
            (250.0, 9749.0),
        ),
        ((0..80).map(f64::from).collect(), (2.0, 77.0)),
        (vec![3.0, 1.0, 2.0], (1.0, 3.0)),
    ] {
        assert_eq!(interval(values), ends);
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(rdlt_testkit::cases(32)))]
    #[test]
    fn the_interval_holds_the_median_and_lies_within_the_ratios(
        ratios in prop::collection::vec(0.5f64..2.0, 1..32),
    ) {
        let paired = Paired::of(&ratios).unwrap();
        let least = ratios.iter().copied().fold(f64::INFINITY, f64::min);
        let most = ratios.iter().copied().fold(f64::NEG_INFINITY, f64::max);
        prop_assert!(least <= paired.low);
        prop_assert!(paired.low <= paired.median);
        prop_assert!(paired.median <= paired.high);
        prop_assert!(paired.high <= most);
        prop_assert_eq!(paired.samples, ratios.len());
    }
}

#[test]
fn a_paired_ratio_renders_its_median_interval_and_samples() {
    let paired = Paired {
        median: 1.2913,
        low: 1.27,
        high: 1.31,
        samples: 30,
    };
    assert_eq!(
        paired.to_string(),
        "1.291 (95 % interval 1.270 to 1.310; samples: 30)"
    );
}
