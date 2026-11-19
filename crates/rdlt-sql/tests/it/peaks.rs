//! What a query holds at its peak, the copies its `CASE`s make of their rows among it, within
//! the bound it declares: nests of `CASE` of every form, over text and over numbers alone, on a
//! batch whole and cut.

use std::sync::Arc;

use arrow_array::{ArrayRef, Int64Array, RecordBatch, StringArray, StringViewArray};
use rdlt_engine::Transform;
use rdlt_sql::Sql;

use crate::HEAP;

/// Rows of the batches here, and bytes of text each holds.
const ROWS: usize = 1024;
const TEXT: usize = 1024;

/// `CASE`s each nest holds, one within another.
const DEPTH: usize = 8;

/// What `run` allocates at its peak beyond what the heap held as it began, the least over eight
/// windows: a window counts only where dropping what `run` returned leaves the heap as it began,
/// so one another thread allocated in is not counted.
fn peak<T>(mut run: impl FnMut() -> T) -> u64 {
    let mut least: Option<usize> = None;
    for _ in 0..8 {
        HEAP.reset_peak_usage();
        let before = HEAP.current_usage();
        let out = run();
        let peak = HEAP.peak_usage().saturating_sub(before);
        drop(out);
        if HEAP.current_usage() == before && least.is_none_or(|low| peak < low) {
            least = Some(peak);
        }
    }
    let peak = least.expect("a window ended with the heap holding what it began with");
    u64::try_from(peak).expect("a peak within 64 bits")
}

/// `n`, each row's index, and `t`, a kilobyte of text a row, as views of it where `view` says.
fn batch(view: bool) -> RecordBatch {
    let texts = (0..ROWS).map(|row| format!("{row:0>TEXT$}"));
    let t: ArrayRef = if view {
        Arc::new(StringViewArray::from_iter_values(texts))
    } else {
        Arc::new(StringArray::from_iter_values(texts))
    };
    let n =
        Int64Array::from_iter_values((0..ROWS).map(|row| i64::try_from(row).expect("few rows")));
    RecordBatch::try_from_iter([("t", t), ("n", Arc::new(n) as ArrayRef)])
        .expect("the columns are of one length")
}

/// `inner` within [`DEPTH`] `CASE`s, each `CASE` a branch of the `CASE` around it where `nest`
/// puts it, and each of its conditions false on a row of its own, so each copies its rows.
fn nested(nest: fn(usize, &str) -> String, inner: &str) -> String {
    (0..DEPTH)
        .rev()
        .fold(inner.to_owned(), |within, level| nest(level, &within))
}

/// The forms of `CASE` a nest is made of, by the branch the next lies in.
const NESTS: [fn(usize, &str) -> String; 6] = [
    // The rows left to test after the first `WHEN` and those parted from them for the second's
    // branch are both held while the branch runs: the most a `CASE` holds.
    |level, within| {
        let next = level + DEPTH;
        format!("CASE WHEN n = {level} THEN 0 WHEN n <> {next} THEN {within} END")
    },
    |level, within| format!("CASE WHEN n <> {level} THEN {within} END"),
    |level, within| format!("CASE WHEN n = {level} THEN 0 ELSE {within} END"),
    |level, within| {
        let next = level + DEPTH;
        format!("CASE WHEN n = {level} THEN 0 WHEN n = {next} THEN 1 ELSE {within} END")
    },
    |level, within| format!("CASE n WHEN {level} THEN 0 ELSE {within} END"),
    // A `coalesce` is planned as a `CASE`: its first argument is null but on one row.
    |level, within| {
        let one = format!("nullif(n, n + CAST(n = {level} AS BIGINT))");
        format!("coalesce({one} * 0, {within})")
    },
];

#[test]
fn what_a_nest_of_case_holds_at_its_peak_stays_within_its_bound() {
    for view in [false, true] {
        let whole = batch(view);
        for part in [whole.clone(), whole.slice(1, ROWS - 2)] {
            for nest in NESTS {
                for inner in ["length(t)", "n * 2"] {
                    let nested = nested(nest, inner);
                    let projected = format!("SELECT {nested} AS y FROM input");
                    let filtered = format!("SELECT n FROM input WHERE {nested} > 0");
                    for text in [projected, filtered] {
                        let sql = Sql::parse(&text).unwrap_or_else(|error| panic!("{error}"));
                        let bound = sql.bound(part.schema_ref()).expect("the query plans");
                        let held = peak(|| {
                            let kept = sql.keep(&part).expect("the filter runs");
                            let applied = sql.apply(part.clone()).expect("the query runs");
                            (kept, applied)
                        });
                        let limit = bound.of(&part);
                        let rows = part.num_rows();
                        assert!(
                            held <= limit,
                            "{text} on {rows} rows, views {view}: {held} > {limit}"
                        );
                    }
                }
            }
        }
    }
}
