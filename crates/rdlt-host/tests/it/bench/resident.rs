//! The served line of `just resident`: the passthrough workload's large frames, read from sixteen
//! partitions, through a host whose connectors it spawned over a socket pair, as production runs
//! them; the host's resident growth against the limit ADR 0039's bound gives it, and each
//! connector process's peak resident memory beside it.
//!
//! The call is `served --resident <cores>`. The host's peak is reset once its inputs are made, so
//! it counts what the run adds; a connector process is spawned for the run, so its peak is all it
//! held. The exit code is 1 where the host's growth passes its limit, and 2 on a wrong call or on
//! a host without `/proc`.

use std::num::{NonZeroU16, NonZeroUsize};
use std::process::ExitCode;
use std::sync::Arc;

use rdlt_engine::bench::{Cell, Passthrough, Resident, reset_peak, resident_kib};
use rdlt_engine::{Cores, EngineConfig};
use rdlt_host::Options;
use rdlt_wire::limits::CREDIT_FLOOR;

use crate::processes::Processes;
use crate::{Budget, Case, Frames, Layout, Mode, Transport, Workload, connectors};

/// The flag a call asks for the served line with, followed by the cores.
const FLAG: &str = "--resident";

/// Partitions the source reads the batches from, each an even share.
const PARTITIONS: NonZeroUsize = NonZeroUsize::new(16).expect("not zero");

/// Writers the served sink declares, each a call of its own.
const WRITERS: NonZeroU16 = NonZeroU16::MIN;

/// Whether this process was started to measure the served line.
pub(crate) fn asked() -> bool {
    std::env::args().any(|arg| arg == FLAG)
}

/// Runs the served line once and prints it: the host's peak budget, resident growth and limit,
/// and each connector process's peak.
#[expect(
    clippy::print_stdout,
    clippy::print_stderr,
    reason = "the line and the usage are this call's output"
)]
pub(crate) fn measure() -> ExitCode {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let cores = args
        .iter()
        .position(|arg| arg == FLAG)
        .and_then(|flag| args.get(flag + 1))
        .and_then(|cores| cores.parse().ok())
        .and_then(NonZeroUsize::new);
    let Some(cores) = cores else {
        eprintln!("usage: served {FLAG} <cores>, a count of at least one");
        return ExitCode::from(2);
    };
    if peak_kib("self").is_none() || reset_peak().is_err() {
        eprintln!("resident: this host has no /proc to read and reset the peak through");
        return ExitCode::from(2);
    }
    let cores = Cores::new(cores);
    let case = Case {
        layout: Layout::Processes,
        transport: Transport::Socket,
        mode: Mode::Both,
        frames: Frames::Large,
        partitions: PARTITIONS,
        writers: WRITERS,
        budget: Budget::Default,
    };
    let workload = Workload::new(cores, case.frames, case.budget);
    let measured = run(&workload, case);
    let limit = limit(cores, case);
    let growth = measured
        .host
        .saturating_sub(measured.base)
        .saturating_mul(1024);
    let tunables = if std::env::var_os("GLIBC_TUNABLES").is_some() {
        "/tunables"
    } else {
        ""
    };
    let mib = |kib: Option<u64>| {
        kib.map_or_else(|| "unmeasured".to_owned(), |kib| (kib >> 10).to_string())
    };
    println!(
        "resident/served/{}/{}/{}{tunables}: peak_memory {} MiB, resident growth {} MiB, limit {} \
         MiB; spawned source peak {} MiB, spawned destination peak {} MiB",
        case.served(),
        case.shape(),
        cores.count(),
        measured.peak_memory >> 20,
        growth >> 20,
        limit >> 20,
        mib(measured.source),
        mib(measured.destination),
    );
    if growth > limit {
        ExitCode::FAILURE
    } else {
        ExitCode::SUCCESS
    }
}

/// What one run of the served line measured.
struct Measured {
    /// Bytes: the most of its budget the engine reserved.
    peak_memory: u64,
    /// Kibibytes: the host's resident memory once its peak was reset, before the run.
    base: u64,
    /// Kibibytes: the host's peak once the run ended.
    host: u64,
    /// Kibibytes: the source process's peak, where it was read.
    source: Option<u64>,
    /// Kibibytes: the destination process's peak, where it was read.
    destination: Option<u64>,
}

/// Runs `case` once from `workload`'s batches, its connectors spawned for it, and reads every
/// process's peak before the connectors stop.
fn run(workload: &Workload, case: Case) -> Measured {
    let mut processes = Processes::new();
    let options = Options::default();
    reset_peak().expect("the peak resets on Linux");
    let base = resident_kib(&status("self"), "VmRSS").expect("the status has the field");
    let passthrough = &workload.passthrough;
    let connected = connectors(case, None, Some(&mut processes), &options);
    let (source, destination, pids) = passthrough.block_on(connected);
    // A connector in a process of its own is held until its peak is read: dropped, it stops.
    let held = (Arc::clone(&source), Arc::clone(&destination));
    let report = passthrough.run(source, destination);
    let source = pids.source.and_then(|pid| peak_kib(&pid.to_string()));
    let destination = pids.destination.and_then(|pid| peak_kib(&pid.to_string()));
    drop(held);
    Measured {
        peak_memory: report.peak_memory,
        base,
        host: peak_kib("self").expect("the status reads once it has"),
        source,
        destination,
    }
}

/// Bytes: what ADR 0039's bound lets the run raise the host's resident memory by: the budget, a
/// fifth of it and 32 MiB, a frame each partition holds before it is admitted, each open call's
/// stream window, a read's each partition and a write's each writer, and a stack a thread.
fn limit(cores: Cores, case: Case) -> u64 {
    let (_, rows) = case.frames.shape();
    let frame = Passthrough::bytes(Cell {
        rows,
        batches: 1,
        ..Cell::PAIRED
    });
    let partitions = u64::try_from(case.partitions.get()).expect("a count fits in 64 bits");
    let threads = cores.runtime_workers().get() + cores.count().get() + 1;
    Resident {
        memory: EngineConfig::default().memory().get(),
        push: frame.saturating_mul(partitions),
        transport: CREDIT_FLOOR.saturating_mul(partitions + u64::from(WRITERS.get())),
        threads: u64::try_from(threads).expect("a count fits in 64 bits"),
    }
    .limit()
}

/// The text of `/proc/<process>/status`, `self` for this one; empty where it cannot be read.
pub(crate) fn status(process: &str) -> String {
    std::fs::read_to_string(format!("/proc/{process}/status")).unwrap_or_default()
}

/// Kibibytes: the peak resident memory of `process`, where its status reads.
pub(crate) fn peak_kib(process: &str) -> Option<u64> {
    resident_kib(&status(process), "VmHWM")
}
