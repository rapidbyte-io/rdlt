//! The passthrough bench's many-table cells with both connectors spawned over the socket.
//!
//! As production runs them: 16, 64 and 192 followed streams and a snapshot of 256 tables, each run
//! as that bench runs it in process, through a writer a core. A served source reads each followed
//! stream over a call of its one connection, which carries the engine 199, so a run follows at most
//! 197 streams of it and 256 cannot be served.

use std::sync::Arc;
use std::time::Duration;

use criterion::{BenchmarkId, Criterion, SamplingMode};
use rdlt_engine::bench::{
    Metered, Readings, Schedule, Tables, TablesCell, phases, reset_peak, resident_kib, spread,
};
use rdlt_engine::{Cores, Report};
use rdlt_host::Options;

use crate::processes::{Placed, Processes};
use crate::resident::{peak_kib, status};
use crate::{Pids, Transport, Usage};

/// The runs of a cell its readings are of: criterion's samples, its warm-up's left out.
const SAMPLES: usize = 10;

/// One run of a cell: its report with each process's CPU time at its window's bounds, and
/// resident memory in KiB, where the platform tells it: the bench's process's before the run and
/// at its peak, and each connector process's peak.
struct Run {
    metered: Metered<Usage>,
    host: Option<(u64, u64)>,
    source: Option<u64>,
    destination: Option<u64>,
}

/// Times each cell, its connectors spawned over the socket for each run, then prints what it is
/// and its readings over its runs.
pub(crate) fn tables(c: &mut Criterion) {
    let cores = Cores::try_from_host().expect("the host says how many cores the bench may use");
    let timed = std::env::args().any(|arg| arg == "--bench");
    let schedule = if timed {
        Schedule::TIMED
    } else {
        Schedule::TESTED
    };
    let mut group = c.benchmark_group("served");
    group
        .sample_size(SAMPLES)
        .sampling_mode(SamplingMode::Flat)
        .warm_up_time(Duration::from_millis(1));
    for cell in TablesCell::served() {
        let measured = match cell {
            // A followed run lasts its span: ten samples of one run each.
            TablesCell::Followed(_) => schedule.span * u32::try_from(SAMPLES).unwrap_or(1),
            TablesCell::Snapshot => Duration::from_secs(5),
        };
        group.measurement_time(measured);
        let (mut made, mut runs) = (None, Vec::new());
        let id = BenchmarkId::new("process/socket", format!("both/tables/{}", cell.name()));
        group.bench_function(id, |b| {
            let (tables, processes) = made.get_or_insert_with(|| {
                let tables = Tables::try_new(cores, cell, schedule).expect("the pool starts");
                (tables, Processes::new())
            });
            b.iter_custom(|iterations| {
                let mut took = Duration::ZERO;
                for _ in 0..iterations {
                    let run = run(tables, processes);
                    took += run.metered.report.elapsed;
                    runs.push(run);
                }
                took
            });
        });
        if let Some((tables, _)) = &made {
            print(tables, &runs[runs.len().saturating_sub(SAMPLES)..], cores);
        }
    }
    group.finish();
}

/// Runs `tables` once, its connectors spawned by `processes` and the bench's resident peak reset
/// first, each process's peak read before its connector stops.
fn run(tables: &Tables, processes: &mut Processes) -> Run {
    let options = Options::default();
    let (source, destination, pids) = tables.block_on(async {
        let config = tables.replay_config();
        let source = processes.source(Transport::Socket, &config, &options).await;
        let config = tables.sink_config();
        let destination = processes
            .destination(Transport::Socket, &config, &options)
            .await;
        let Placed { connector, pid } = source;
        let pids = Pids {
            source: Some(pid),
            destination: Some(destination.pid),
        };
        (connector, destination.connector, pids)
    });
    // Each connector is held until its peak is read: dropped, it stops.
    let held = (Arc::clone(&source), Arc::clone(&destination));
    let base = reset_peak()
        .ok()
        .and_then(|()| resident_kib(&status("self"), "VmRSS"));
    let metered = tables.run(source, destination, || Usage::now(pids));
    let peak = |pid: Option<u32>| pid.and_then(|pid| peak_kib(&pid.to_string()));
    let run = Run {
        metered,
        host: base.zip(peak_kib("self")),
        source: peak(pids.source),
        destination: peak(pids.destination),
    };
    drop(held);
    run
}

/// Prints what `tables` is and its readings over `runs`.
#[expect(
    clippy::print_stdout,
    reason = "criterion reports the runs' times; the readings are printed beside them"
)]
#[expect(
    clippy::cast_precision_loss,
    reason = "kibibytes of a process far below 2^52"
)]
fn print(tables: &Tables, runs: &[Run], cores: Cores) {
    let Some(last) = runs.last() else {
        return;
    };
    let cell = tables.cell();
    println!(
        "served/process/socket/both/tables/{}: {tables}; {} runs",
        cell.name(),
        runs.len()
    );
    let reports: Vec<&Report> = runs.iter().map(|run| &run.metered.report).collect();
    println!("  commit phases, ms a commit: {}", phases(&reports));
    let busy = |side: fn(&Usage) -> Option<Duration>| {
        spread(runs.iter().map(|run| {
            let [before, after] = run.metered.readings;
            let mut taken = Usage::default();
            after.add_since(before, &mut taken);
            let window = run.metered.window.as_secs_f64();
            side(&taken).map(|time| time.as_secs_f64() / window)
        }))
    };
    let window = match cell {
        TablesCell::Followed(_) => "while its streams are quiet",
        TablesCell::Snapshot => "over the run",
    };
    println!(
        "  CPU seconds a wall second {window}: the bench's process {}; source {}; destination {}",
        busy(|taken| Some(taken.host)),
        busy(|taken| taken.source),
        busy(|taken| taken.destination),
    );
    let kept = reports.iter().map(|report| report.counters.kept_peak).max();
    let mebibytes = |kib: Option<u64>| kib.map(|kib| kib as f64 / 1024.0);
    let resident =
        |side: fn(&Run) -> Option<u64>| spread(runs.iter().map(|run| mebibytes(side(run))));
    println!(
        "  reads kept at most {} KiB of their {} KiB share; resident peak, MiB: the bench's \
         process {}, above the run's base {}; spawned source {}; spawned destination {}",
        kept.unwrap_or(0) >> 10,
        tables.kept_share() >> 10,
        resident(|run| run.host.map(|(_, peak)| peak)),
        resident(|run| run.host.map(|(base, peak)| peak.saturating_sub(base))),
        resident(|run| run.source),
        resident(|run| run.destination),
    );
    let report = &last.metered.report;
    let pushes = u64::try_from(cell.pushes()).expect("pushes fit in 64 bits");
    let readings = Readings::of(report.elapsed, &report.counters, pushes, cores.count());
    println!("  readings: {readings}");
}
