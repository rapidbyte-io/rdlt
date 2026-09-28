use std::sync::Arc;

use arrow_array::{ArrayRef, Int64Array, RecordBatch, StringArray};

use super::destination::{Fault, ROWS, every_row_once};
use super::killing::Schedule;
use super::{Loaded, unproven};

fn batch(columns: Vec<(&str, ArrayRef)>) -> RecordBatch {
    RecordBatch::try_from_iter(columns).expect("a valid batch")
}

/// Every id a load generates.
fn generated() -> Vec<i64> {
    (0..i64::try_from(ROWS).expect("few rows")).collect()
}

fn ids(values: &[i64]) -> RecordBatch {
    batch(vec![("id", Arc::new(Int64Array::from(values.to_vec())))])
}

#[test]
fn every_seed_schedules_kills_early_in_a_load() {
    for seed in (0..100_000_u64).map(|seed| seed.wrapping_mul(0x9e37_79b9_7f4a_7c15)) {
        let schedule = Schedule::seeded(seed, true);
        assert!((1..=2).contains(&schedule.settled), "{schedule:?}");
        assert!((2..=4).contains(&schedule.commit), "{schedule:?}");
        assert!(
            schedule
                .answer
                .is_some_and(|answer| (1..=3).contains(&answer)),
            "{schedule:?}"
        );
        assert_eq!(Schedule::seeded(seed, false).answer, None);
    }
}

#[test]
fn a_seed_draws_each_point_from_bits_of_its_own() {
    let drawn = |settled, commit, answer| Schedule {
        settled,
        commit,
        answer,
    };
    assert_eq!(Schedule::seeded(1, true), drawn(2, 2, Some(1)));
    assert_eq!(Schedule::seeded(515, true), drawn(2, 4, Some(1)));
    assert_eq!(Schedule::seeded(0x0002_0100, true), drawn(1, 2, Some(3)));
}

#[test]
fn seeds_draw_every_point_of_each_range() {
    let schedules: Vec<Schedule> = (0..4096)
        .map(|seed| Schedule::seeded(seed * 257, true))
        .collect();
    for settled in 1..=2 {
        assert!(schedules.iter().any(|schedule| schedule.settled == settled));
    }
    for commit in 2..=4 {
        assert!(schedules.iter().any(|schedule| schedule.commit == commit));
    }
    for answer in 1..=3 {
        assert!(
            schedules
                .iter()
                .any(|schedule| schedule.answer == Some(answer))
        );
    }
}

#[test]
fn every_generated_row_once_is_exactly_once() {
    let all = generated();
    every_row_once(&[ids(&all[..100]), ids(&all[100..])]).expect("each row once");
}

#[test]
fn a_repeated_missing_or_stray_row_is_not_exactly_once() {
    let mut repeated = generated();
    repeated.push(17);
    assert_eq!(every_row_once(&[ids(&repeated)]), Err(Fault::Repeated(17)));
    let missing: Vec<i64> = generated().into_iter().filter(|id| *id != 30).collect();
    assert_eq!(every_row_once(&[ids(&missing)]), Err(Fault::Missing(30)));
    let mut stray = generated();
    stray.push(-1);
    assert_eq!(every_row_once(&[ids(&stray)]), Err(Fault::Stray(-1)));
}

#[test]
fn ids_are_found_in_any_case_and_any_integer_rendering() {
    let rendered: Vec<String> = generated().iter().map(ToString::to_string).collect();
    let as_text = batch(vec![("ID", Arc::new(StringArray::from(rendered)))]);
    every_row_once(&[as_text]).expect("text ids cast");
}

#[test]
fn a_table_without_integer_ids_is_not_exactly_once() {
    let unnamed = batch(vec![("key", Arc::new(Int64Array::from(vec![0])))]);
    assert_eq!(every_row_once(&[unnamed]), Err(Fault::NoIds));
    let words = batch(vec![("id", Arc::new(StringArray::from(vec!["zero"])))]);
    assert!(matches!(
        every_row_once(&[words]),
        Err(Fault::NotIntegers(_))
    ));
    let nulls = batch(vec![(
        "id",
        Arc::new(Int64Array::from(vec![Some(0), None])),
    )]);
    assert_eq!(every_row_once(&[nulls]), Err(Fault::Nulls));
}

#[test]
fn each_fault_reads_apart_and_names_its_row() {
    let faults = [
        Fault::NoIds,
        Fault::NotIntegers("words".to_owned()),
        Fault::Nulls,
        Fault::Repeated(17),
        Fault::Missing(17),
        Fault::Stray(17),
    ];
    let read: std::collections::BTreeSet<String> = faults.iter().map(ToString::to_string).collect();
    assert_eq!(read.len(), faults.len(), "{read:?}");
    for fault in &faults[3..] {
        assert!(fault.to_string().contains("17"), "{fault}");
    }
    assert!(faults[1].to_string().contains("words"), "{}", faults[1]);
}

#[test]
fn a_load_proves_something_only_once_a_kill_interrupted_it() {
    let unkilled = rdlt_host::Kills::new();
    let killed = rdlt_host::Kills::new();
    killed.kill();
    for (kills, interrupted) in [(&unkilled, false), (&unkilled, true), (&killed, false)] {
        assert!(
            matches!(unproven(kills, interrupted, 9), Some(Loaded::Inapplicable(reason)) if reason.contains("kill seed 9")),
            "{} kills, interrupted: {interrupted}",
            kills.count()
        );
    }
    assert!(unproven(&killed, true, 9).is_none());
}
