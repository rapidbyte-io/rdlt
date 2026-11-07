//! What a SQL transform's stage allocates under a counting allocator, as `cargo xtask
//! instructions` counts its cases: a case's name and a count of iterations run its work that
//! many times and print the allocations the process made; `--list` lists the cases as a test
//! harness lists its tests, and no argument runs each case once.

#![forbid(unsafe_code)]

use std::alloc::System;
use std::hint::black_box;
use std::process::ExitCode;
use std::sync::Arc;

use arrow_array::{ArrayRef, Int64Array, RecordBatch, StringArray};
use rdlt_connector::StreamName;
use rdlt_engine::StreamPlan;
use rdlt_engine::bench::Transformed;
use rdlt_sql::Sql;
use stats_alloc::{INSTRUMENTED_SYSTEM, StatsAlloc};

#[global_allocator]
static HEAP: &StatsAlloc<System> = &INSTRUMENTED_SYSTEM;

/// Rows of the batch each case transforms.
const ROWS: i64 = 8192;

/// A case's name, and what runs it: its inputs made once, then its work as many times as asked.
type Case = (&'static str, fn(usize));

/// Every case, and what runs it.
const CASES: [Case; 2] = [("sql/projection", projected), ("sql/filter", filtered)];

#[expect(
    clippy::print_stdout,
    reason = "names and allocation counts are what is asked for"
)]
fn main() -> ExitCode {
    let args: Vec<String> = std::env::args().collect();
    if args.iter().any(|arg| arg == "--list") {
        if !args.iter().any(|arg| arg == "--ignored") {
            for (name, _) in CASES {
                println!("{name}: test");
            }
        }
        return ExitCode::SUCCESS;
    }
    let named: Vec<&str> = args
        .iter()
        .skip(1)
        .filter(|arg| !arg.starts_with('-'))
        .map(String::as_str)
        .collect();
    let times = named.get(1).map_or(Some(1), |times| times.parse().ok());
    match (named.first(), times) {
        (None, _) => {
            for (_, run) in CASES {
                run(1);
            }
        }
        (Some(case), Some(times)) => {
            let Some((_, run)) = CASES.iter().find(|(name, _)| name == case) else {
                return ExitCode::FAILURE;
            };
            run(times);
            println!("allocations {}", HEAP.stats().allocations);
        }
        (Some(_), None) => return ExitCode::FAILURE,
    }
    ExitCode::SUCCESS
}

/// [`ROWS`] people: an id, an email, an amount and a status, one in ten of them `test`.
fn people() -> RecordBatch {
    let emails = (0..ROWS).map(|row| format!("Person{row}@Example.com"));
    let statuses = (0..ROWS).map(|row| if row % 10 == 0 { "test" } else { "live" });
    RecordBatch::try_from_iter([
        (
            "id",
            Arc::new(Int64Array::from_iter_values(0..ROWS)) as ArrayRef,
        ),
        (
            "email",
            Arc::new(StringArray::from_iter_values(emails)) as ArrayRef,
        ),
        (
            "amount",
            Arc::new(Int64Array::from_iter_values(0..ROWS)) as ArrayRef,
        ),
        (
            "status",
            Arc::new(StringArray::from_iter_values(statuses)) as ArrayRef,
        ),
    ])
    .expect("the columns are of one length")
}

/// The stage of a stream transformed by `text`, run `times` times on [`people`].
fn ran(text: &str, times: usize) {
    let sql = Sql::parse(text).expect("the case's query parses");
    let plan = StreamPlan::new(StreamName::new("people").expect("a valid name")).transform(sql);
    let transformed =
        Transformed::try_new(&plan, &["id"], people()).expect("the stage takes the batch");
    for _ in 0..times {
        black_box(transformed.run().expect("the stage runs"));
    }
}

/// A projection: a bare column, a lowered one and an arithmetic one, of [`ROWS`] rows.
fn projected(times: usize) {
    ran(
        "SELECT id, lower(email) AS email, amount * 100 AS cents FROM input",
        times,
    );
}

/// A filter keeping nine rows in ten of [`ROWS`], every column passed on.
fn filtered(times: usize) {
    ran("SELECT * FROM input WHERE status <> 'test'", times);
}
