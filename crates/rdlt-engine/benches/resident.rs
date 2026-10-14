//! What a run holds resident against what its budget bounds: runs one bench workload once, in a
//! process of its own, and fails where the run raises the process's peak resident memory past the
//! limit ADR 0039's bound gives it.
//!
//! The call is `resident <workload> <cores>`, the workload `passthrough` or `normalized`. The
//! peak is reset once the inputs are made, so it counts what the run adds to the inputs' base. A
//! test runner lists the bench with `--list` and finds no test in it, and cargo's `--bench` is
//! ignored. The exit code is 1 where the run exceeds its limit, and 2 on a wrong call or on a host
//! without `/proc`.

#![forbid(unsafe_code)]

use std::num::{NonZeroU64, NonZeroUsize};
use std::process::ExitCode;

use rdlt_engine::Cores;
use rdlt_engine::bench::{Cell, Normalized, Passthrough, Resident, reset_peak, resident_kib};

/// Bytes: the budget each run is given.
const MEMORY: u64 = 256 << 20;
/// Roots the normalized run loads.
const ROOTS: u64 = 200_000;
/// Rows of a push of the normalized run.
const PER_PUSH: u64 = 50_000;

/// The workloads a call may name.
const WORKLOADS: [&str; 2] = ["passthrough", "normalized"];

#[expect(
    clippy::print_stdout,
    clippy::print_stderr,
    reason = "the line and the usage are this bench's output"
)]
fn main() -> ExitCode {
    let args: Vec<String> = std::env::args().skip(1).collect();
    if args.iter().any(|arg| arg == "--list") {
        return ExitCode::SUCCESS;
    }
    let named: Vec<&str> = args
        .iter()
        .filter(|arg| !arg.starts_with('-'))
        .map(String::as_str)
        .collect();
    let [workload, cores] = named.as_slice() else {
        eprintln!("usage: resident <{}> <cores>", WORKLOADS.join("|"));
        return ExitCode::from(2);
    };
    let Some(cores) = cores.parse().ok().and_then(NonZeroUsize::new) else {
        eprintln!("resident: cores is a count of at least one");
        return ExitCode::from(2);
    };
    if !WORKLOADS.contains(workload) {
        eprintln!("resident: no workload {workload}");
        return ExitCode::from(2);
    }
    if read_status().is_none() || reset_peak().is_err() {
        eprintln!("resident: this host has no /proc to read and reset the peak through");
        return ExitCode::from(2);
    }
    let cores = Cores::new(cores);
    let (peak_memory, base, high, push) = run(workload, cores);
    let threads = cores.runtime_workers().get() + cores.count().get() + 1;
    let threads = u64::try_from(threads).expect("a count fits in 64 bits");
    let limit = Resident {
        memory: MEMORY,
        push,
        threads,
    }
    .limit();
    let growth = high.saturating_sub(base).saturating_mul(1024);
    let tunables = if std::env::var_os("GLIBC_TUNABLES").is_some() {
        "/tunables"
    } else {
        ""
    };
    println!(
        "resident/{workload}/{}{tunables}: peak_memory {} MiB, resident growth {} MiB, limit {} MiB",
        cores.count(),
        peak_memory >> 20,
        growth >> 20,
        limit >> 20,
    );
    if growth > limit {
        ExitCode::FAILURE
    } else {
        ExitCode::SUCCESS
    }
}

/// The text of `/proc/self/status`, where the host has it.
fn read_status() -> Option<String> {
    std::fs::read_to_string("/proc/self/status").ok()
}

/// A field of `/proc/self/status`, in kibibytes.
fn field(name: &str) -> u64 {
    let status = read_status().expect("the status reads once it has");
    resident_kib(&status, name).expect("the status has the field")
}

/// Makes the workload's inputs, resets the peak, runs it once; the peak of the budget it used in
/// bytes, the resident base before the run and the peak after it in kibibytes, and its largest
/// push in bytes.
fn run(workload: &str, cores: Cores) -> (u64, u64, u64, u64) {
    let host = "the pool starts";
    if workload == "passthrough" {
        let bench = Passthrough::try_new(cores, Cell::PAIRED).expect(host);
        let push = Passthrough::bytes(Cell {
            batches: 1,
            ..Cell::PAIRED
        });
        let base = rebase();
        let report = bench.engine_report();
        (report.peak_memory, base, field("VmHWM"), push)
    } else {
        let per_push = NonZeroU64::new(PER_PUSH).expect("not zero");
        let bench = Normalized::within(cores, ROOTS, per_push, MEMORY).expect(host);
        let push = bench.largest_push();
        let base = rebase();
        let report = bench.report();
        (report.peak_memory, base, field("VmHWM"), push)
    }
}

/// Resets the peak to what the process holds now; what it holds, in kibibytes.
fn rebase() -> u64 {
    reset_peak().expect("the peak resets on Linux");
    field("VmRSS")
}
