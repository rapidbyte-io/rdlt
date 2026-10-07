use std::collections::BTreeMap;
use std::num::NonZeroUsize;
use std::path::Path;
use std::time::{Duration, Instant};

use parking_lot::{Condvar, Mutex};
use serde_json::{Map, Value, json};

use super::{
    CORES_VAR, Failed, Recorded, SEED_VAR, SEEDS_FROM_VAR, SEEDS_VAR, Seed, SeedRange, Weight,
    cores, drive, select, side_by_side, sweep, timings,
};

fn seeds(range: SeedRange) -> Vec<u64> {
    range.seeds().map(Seed::value).collect()
}

#[test]
fn seed_selection_follows_the_variables() {
    type Case<'a> = (Option<&'a str>, Option<&'a str>, Option<&'a str>, &'a [u64]);
    let cases: &[Case<'_>] = &[
        (None, None, None, &[0, 1, 2]),
        (None, Some("2"), None, &[0, 1]),
        (None, Some(""), None, &[0, 1, 2]),
        (Some("42"), Some("9"), None, &[42]),
        (Some(" 7 "), None, None, &[7]),
        (Some(""), Some("1"), None, &[0]),
        (Some("18446744073709551615"), None, None, &[u64::MAX]),
        (None, Some("0"), None, &[]),
        // A shard of many seeds starts where the shard before it ended.
        (None, Some("2"), Some("100"), &[100, 101]),
        (None, None, Some(" 5 "), &[5, 6, 7]),
        (None, Some("2"), Some(""), &[0, 1]),
        (Some("42"), None, Some("100"), &[42]),
        (
            None,
            Some("2"),
            Some("18446744073709551615"),
            &[u64::MAX, 0],
        ),
    ];
    for (single, count, from, expected) in cases {
        let selected = select(*single, *count, *from, 3).unwrap();
        assert_eq!(
            seeds(selected),
            *expected,
            "single {single:?}, count {count:?}, from {from:?}"
        );
    }
}

#[test]
fn malformed_variables_name_the_variable() {
    let cases = [
        (Some("abc"), None, None, SEED_VAR),
        (None, Some("-1"), None, SEEDS_VAR),
        (None, None, Some("x"), SEEDS_FROM_VAR),
    ];
    for (single, count, from, variable) in cases {
        let error = select(single, count, from, 3).unwrap_err();
        assert_eq!(error.variable, variable);
    }
}

#[test]
fn a_seed_displays_as_its_value() {
    assert_eq!(Seed::new(1234).to_string(), "1234");
}

#[derive(Debug, PartialEq, Eq)]
struct Doubled(u64);

impl Recorded for Doubled {
    fn recorded(&self) -> Map<String, Value> {
        Map::from_iter([("doubled".to_owned(), json!(self.0))])
    }
}

impl Recorded for bool {}

fn threads(count: usize) -> NonZeroUsize {
    NonZeroUsize::new(count).expect("a thread or more")
}

fn listed(values: impl IntoIterator<Item = u64>) -> Vec<Seed> {
    values.into_iter().map(Seed::new).collect()
}

/// The timing file's lines: each seed's, in the order they ended, then the summary.
fn written(path: &Path) -> (Vec<Value>, Value) {
    let mut lines: Vec<Value> = std::fs::read_to_string(path)
        .expect("the timing file was written")
        .lines()
        .map(|line| serde_json::from_str(line).expect("each line is JSON"))
        .collect();
    let summary = lines.pop().expect("a summary line");
    (lines, summary)
}

#[test]
fn every_seed_runs_once_and_results_come_back_in_seed_order() {
    let directory = tempfile::tempdir().expect("a temporary directory");
    let path = directory.path().join("sweep.jsonl");
    for count in [1, 3, 8, 100] {
        let runs = Mutex::new(BTreeMap::<u64, usize>::new());
        let results = drive(&path, &listed(0..64), threads(count), |seed| {
            *runs.lock().entry(seed.value()).or_default() += 1;
            Doubled(seed.value() * 2)
        })
        .expect("no seed fails");
        let expected: Vec<Doubled> = (0..64).map(|seed| Doubled(seed * 2)).collect();
        assert_eq!(results, expected, "{count} threads");
        let runs = runs.into_inner();
        assert_eq!(runs.len(), 64, "{count} threads");
        assert!(runs.values().all(|runs| *runs == 1), "{count} threads");
    }
}

#[test]
fn seeds_run_side_by_side_on_every_thread_given() {
    let directory = tempfile::tempdir().expect("a temporary directory");
    let path = directory.path().join("sweep.jsonl");
    let (arrived, all) = (Mutex::new(0_usize), Condvar::new());
    let met = drive(&path, &listed(0..4), threads(4), |_| {
        let mut count = arrived.lock();
        *count += 1;
        all.notify_all();
        // With fewer threads than seeds the four never meet; the deadline fails the test.
        let deadline = Instant::now() + Duration::from_secs(10);
        while *count < 4 && !all.wait_until(&mut count, deadline).timed_out() {}
        *count == 4
    })
    .expect("no seed fails");
    assert_eq!(met, [true; 4]);
}

#[test]
fn every_failing_seed_is_named_once_all_have_run() {
    let directory = tempfile::tempdir().expect("a temporary directory");
    let path = directory.path().join("sweep.jsonl");
    let ran = Mutex::new(Vec::new());
    let failed = drive(&path, &listed([5, 9, 2, 7]), threads(2), |seed| {
        ran.lock().push(seed.value());
        assert!(![9, 2].contains(&seed.value()), "seed {seed} fails");
    })
    .expect_err("seeds 9 and 2 fail");
    assert_eq!(
        failed,
        Failed {
            failed: listed([9, 2]),
            ran: 4,
        }
    );
    let mut ran = ran.into_inner();
    ran.sort_unstable();
    assert_eq!(ran, [2, 5, 7, 9]);
}

#[test]
fn a_sweep_with_a_failing_seed_panics_once_all_have_run() {
    // A workspace of its own, so the sweep's timing file stays out of the real one.
    let workspace = tempfile::tempdir().expect("a temporary directory");
    std::fs::write(workspace.path().join("Cargo.lock"), "").expect("a lockfile");
    let ran = Mutex::new(0_usize);
    let swept = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        sweep(
            workspace.path(),
            "a-failing-sweep",
            listed([3, 4, 5]),
            Weight::One,
            |seed| {
                *ran.lock() += 1;
                assert_ne!(seed.value(), 4, "seed 4 fails");
            },
        )
    }));
    assert!(swept.is_err());
    assert_eq!(ran.into_inner(), 3);
    let (lines, _) = written(
        &workspace
            .path()
            .join("target/sim-timings/a-failing-sweep.jsonl"),
    );
    assert_eq!(lines.len(), 3);
}

#[test]
fn the_seeds_given_are_the_seeds_run() {
    let directory = tempfile::tempdir().expect("a temporary directory");
    let path = directory.path().join("found.jsonl");
    let results = drive(&path, &listed([19, 9_576, 21_118]), threads(2), |seed| {
        Doubled(seed.value())
    })
    .expect("no seed fails");
    assert_eq!(results, [Doubled(19), Doubled(9_576), Doubled(21_118)]);
}

#[test]
fn no_seeds_run_nothing_and_write_only_the_summary() {
    let directory = tempfile::tempdir().expect("a temporary directory");
    let path = directory.path().join("none.jsonl");
    let results = drive(&path, &[], threads(4), |_| -> bool {
        panic!("no seed runs")
    });
    assert_eq!(results, Ok(Vec::new()));
    let (lines, summary) = written(&path);
    assert!(lines.is_empty());
    assert_eq!(summary["seeds"], 0);
}

#[test]
fn each_seed_writes_one_timing_line_a_failing_seed_included_then_a_summary() {
    let directory = tempfile::tempdir().expect("a temporary directory");
    let path = directory.path().join("nested/sweep.jsonl");
    let failed = drive(&path, &listed(0..6), threads(3), |seed| {
        assert_ne!(seed.value(), 4, "seed 4 fails");
        Doubled(seed.value() * 2)
    })
    .expect_err("seed 4 fails");
    assert_eq!(failed.failed, [Seed::new(4)]);
    let (mut lines, summary) = written(&path);
    lines.sort_by_key(|line| line["seed"].as_u64());
    assert_eq!(lines.len(), 6);
    for (seed, line) in (0..6_u64).zip(&lines) {
        assert_eq!(line["seed"], seed);
        assert!(
            line["wall_ms"].as_f64().is_some_and(|wall| wall >= 0.0),
            "{line}"
        );
        if seed == 4 {
            assert_eq!(line["outcome"], "failed");
            assert!(line.get("doubled").is_none(), "{line}");
        } else {
            assert_eq!(line["outcome"], "passed");
            assert_eq!(line["doubled"], seed * 2);
        }
    }
    assert_eq!(
        (&summary["seeds"], &summary["failed"], &summary["threads"]),
        (&json!(6), &json!(1), &json!(3))
    );
    assert!(
        summary["seeds_per_s"]
            .as_f64()
            .is_some_and(|rate| rate > 0.0),
        "{summary}"
    );
}

#[test]
fn runs_go_side_by_side_as_the_cores_hold_them_and_one_at_least() {
    let cases = [
        (16, Weight::One, 16),
        (4, Weight::One, 4),
        (1, Weight::One, 1),
        (16, Weight::Threaded, 2),
        (8, Weight::Threaded, 1),
        (4, Weight::Threaded, 1),
        (3, Weight::Threaded, 1),
    ];
    for (cores, weight, expected) in cases {
        assert_eq!(
            side_by_side(cores, weight).get(),
            expected,
            "{cores} cores, {weight:?}"
        );
    }
}

#[test]
fn the_cores_shared_are_the_host_s_unless_the_variable_names_a_count() {
    let host = threads(6);
    let cases = [
        (None, 6),
        (Some(""), 6),
        (Some("2"), 2),
        (Some(" 3 "), 3),
        (Some("32"), 32),
    ];
    for (value, expected) in cases {
        assert_eq!(cores(value, host).unwrap().get(), expected, "{value:?}");
    }
    for value in ["0", "-1", "four"] {
        let error = cores(Some(value), host).unwrap_err();
        assert_eq!(error.variable, CORES_VAR, "{value:?}");
    }
}

#[test]
fn timings_go_beneath_the_workspace_a_test_runs_in() {
    let workspace = tempfile::tempdir().expect("a temporary directory");
    std::fs::write(workspace.path().join("Cargo.lock"), "").expect("a lockfile");
    let package = workspace.path().join("crates/rdlt-sim");
    std::fs::create_dir_all(&package).expect("a package directory");
    assert_eq!(
        timings(&package, "exactly_once"),
        workspace
            .path()
            .join("target/sim-timings/exactly_once.jsonl")
    );
    assert_eq!(
        timings(workspace.path(), "changes"),
        workspace.path().join("target/sim-timings/changes.jsonl")
    );
}
