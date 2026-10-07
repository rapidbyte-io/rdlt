use std::num::NonZeroU64;
use std::sync::Arc;

use arrow_array::builder::Int64Builder;
use arrow_array::{ArrayRef, BooleanArray, Int64Array, RecordBatch, StringArray};
use proptest::prelude::*;
use stats_alloc::{INSTRUMENTED_SYSTEM, Stats};

use super::{Allocations, Paired, counted, interval, logical_bytes};

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

#[test]
fn a_runs_allocations_spread_over_the_units_it_moved() {
    let stats = Stats {
        allocations: 40,
        reallocations: 8,
        bytes_allocated: 4000,
        ..Stats::default()
    };
    let run = Allocations::from(stats);
    assert_eq!(
        run,
        Allocations {
            allocations: 40.0,
            reallocations: 8.0,
            bytes: 4000.0,
        }
    );
    assert_eq!(
        run.per(NonZeroU64::new(5).unwrap()),
        Allocations {
            allocations: 8.0,
            reallocations: 1.6,
            bytes: 800.0,
        }
    );
}

#[test]
fn counting_runs_the_run_and_counts_nothing_another_allocator_served() {
    let mut ran = false;
    let allocated = counted(&INSTRUMENTED_SYSTEM, || {
        ran = true;
        std::hint::black_box(vec![0_u8; 64]);
    });
    assert!(ran);
    assert_eq!(allocated, Allocations::default());
}

#[test]
fn logical_bytes_count_the_rows_a_batch_references_and_not_its_capacity() {
    let ints: ArrayRef = Arc::new(Int64Array::from_iter_values(0..10));
    let mut roomy = Int64Builder::with_capacity(1024);
    roomy.append_slice(&[7; 10]);
    let roomy: ArrayRef = Arc::new(roomy.finish());
    let text: ArrayRef = Arc::new(StringArray::from(vec!["ab", "c"]));
    let flags: ArrayRef = Arc::new(BooleanArray::from(vec![Some(true), None, Some(false)]));
    let int_batch = RecordBatch::try_from_iter([("i", Arc::clone(&ints))]).unwrap();
    for (batches, bytes) in [
        (vec![int_batch.clone()], 80),
        (
            vec![RecordBatch::try_from_iter([("r", roomy)]).unwrap()],
            80,
        ),
        (vec![int_batch.slice(2, 3)], 24),
        (vec![int_batch.clone(), int_batch], 160),
        (vec![RecordBatch::try_from_iter([("t", text)]).unwrap()], 15),
        (vec![RecordBatch::try_from_iter([("f", flags)]).unwrap()], 2),
    ] {
        assert_eq!(logical_bytes(&batches), bytes, "{batches:?}");
    }
}

#[test]
fn allocations_render_each_count_and_the_bytes() {
    let allocated = Allocations {
        allocations: 2.5,
        reallocations: 0.25,
        bytes: 312.4,
    };
    assert_eq!(
        allocated.to_string(),
        "2.500 allocations, 0.250 reallocations, 312 bytes"
    );
}
