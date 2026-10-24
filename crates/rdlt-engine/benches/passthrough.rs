//! Arrow passthrough: the engine moving the same rows in batches of three sizes, from one to
//! sixteen partitions of one or four streams, committed at the end or every sixteenth of the
//! rows, the small batches also checkpointed every 6 and every 97 pushes, each cell's readings
//! and its sink's writes a run printed after it; and its paired cell, within 1 GiB and through
//! one writer as its bound is stated, against a bare loop writing the same batches to the same
//! destination, timed in blocks that run each side first as often as last, so drift between runs
//! cancels out of their ratio.
//!
//! The chosen cells run unless an argument names `full`, which runs every cell. As a test, each
//! cell runs once in two small batches for each partition of each stream.
//!
//! The logged cells move a pipeline of two streams, the second of which its source cannot read
//! again, so that each load logs both streams' batches ahead, on tmpfs and on the disk beside the
//! build; beside them, the same pipeline with both streams readable again logs nothing.
//!
//! The JSON cells push nested rows and rows of 200 columns, sixteen pushes of about 8 MiB a run, a
//! checkpoint after each, from one partition and from sixteen, into the null sink, each printing
//! its readings and the passes shredding took. As a test, each pushes two small pushes a
//! partition.
//!
//! The many-table cells follow 16, 64, 192 and 256 streams, each an unbounded partition pushing
//! five batches of a thousand rows a second apart with a checkpoint after each, then nothing,
//! committed every two seconds and followed for ten, reading a partition more than the streams
//! within the default budget where it admits them and within the least that does otherwise; and
//! read a snapshot of 256 tables of four such pushes each at the engine's defaults. Each writes
//! through a writer a core and prints, over its runs, each commit phase's time a commit, the
//! process's CPU seconds a wall second while the streams are quiet, from seven seconds to ten, or
//! over the snapshot's run, the most its reads kept, its resident peak, and its last run's
//! readings. As a test, each follows for two seconds, its pushes 20 ms apart.

#![forbid(unsafe_code)]

use std::hint::black_box;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

use criterion::measurement::WallTime;
use criterion::{
    BenchmarkGroup, BenchmarkId, Criterion, SamplingMode, Throughput, criterion_group,
    criterion_main,
};
use rdlt_engine::bench::{
    Cell, JsonCell, JsonPassthrough, Metered, Mixed, Paired, Passthrough, Readings, Schedule,
    Tables, TablesCell, phases, reset_peak, resident_kib, spread, written,
};
use rdlt_engine::{Cores, LocalWal, Report, WalStore};

/// Samples the paired group takes, each the ratio of the blocks it ran.
const SAMPLES: usize = 30;

/// One side of a pair.
#[derive(Clone, Copy)]
enum Side {
    Bare,
    Engine,
}

/// The runs of one block: each side runs first once and last once.
const BLOCK: [Side; 4] = [Side::Bare, Side::Engine, Side::Engine, Side::Bare];

/// Whether criterion times the benchmarks, rather than running each once as a test.
fn timed() -> bool {
    std::env::args().any(|arg| arg == "--bench")
}

/// `cell` at the size this run moves it: whole when timed, small as a test.
fn sized(cell: Cell) -> Cell {
    if timed() { cell } else { cell.tested() }
}

#[expect(
    clippy::print_stdout,
    reason = "criterion reports the engine's time; the ratio is printed beside it"
)]
#[expect(
    clippy::disallowed_methods,
    reason = "a benchmark times its runs on the real clock"
)]
fn passthrough(c: &mut Criterion) {
    let cores = Cores::try_from_host().expect("the host says how many cores the bench may use");
    let (workers, threads) = (cores.runtime_workers(), cores.count());
    let layout = format!("{workers}+{threads}");
    let mut group = c.benchmark_group("passthrough");
    let paired = sized(Cell::PAIRED);
    let mut bench = None;
    let mut ratios = Vec::new();
    group.sample_size(SAMPLES).sampling_mode(SamplingMode::Flat);
    group.throughput(Throughput::Bytes(Passthrough::bytes(paired)));
    group.bench_function(BenchmarkId::new("paired", &layout), |b| {
        let bench = bench
            .get_or_insert_with(|| Passthrough::paired(cores, paired).expect("the pool starts"));
        b.iter_custom(|blocks| {
            if blocks == 0 {
                return Duration::ZERO;
            }
            let mut took = [Duration::ZERO; 2];
            for _ in 0..blocks {
                for side in BLOCK {
                    let started = Instant::now();
                    black_box(match side {
                        Side::Bare => bench.bare_loop(),
                        Side::Engine => bench.engine_run(),
                    });
                    took[side as usize] += started.elapsed();
                }
            }
            let [bare, engine] = took;
            ratios.push(engine.as_secs_f64() / bare.as_secs_f64());
            engine / 2
        });
    });
    drop(bench);
    // Only a run that measured has ratios, so listing the benchmarks prints nothing.
    if let Some(paired) = Paired::of(&ratios[ratios.len().saturating_sub(SAMPLES)..]) {
        println!(
            "passthrough/paired/{layout}: {workers} runtime workers, {threads} compute threads"
        );
        println!("  engine over bare loop: {paired}");
    }
    matrix(&mut group, cores);
    group.finish();
}

/// The matrix's cells asked for, each timed in `group` and its readings and sink writes printed
/// after it.
#[expect(
    clippy::print_stdout,
    reason = "criterion reports the engine's time; the readings are printed beside it"
)]
fn matrix(group: &mut BenchmarkGroup<'_, WallTime>, cores: Cores) {
    let args: Vec<String> = std::env::args().collect();
    let (kind, cells) = Cell::asked(&args);
    group.sample_size(10);
    for cell in cells {
        let moved = sized(cell);
        group.throughput(Throughput::Bytes(Passthrough::bytes(moved)));
        let (mut bench, mut last) = (None, None);
        // Named by the whole cell, so a listing and a timed run name it alike.
        group.bench_function(BenchmarkId::new(kind, cell.name()), |b| {
            let bench = bench.get_or_insert_with(|| {
                Passthrough::try_new(cores, moved).expect("the pool starts")
            });
            b.iter(|| {
                let before = written();
                let report = bench.engine_report();
                last = Some((report, written() - before));
            });
        });
        if let Some((report, writes)) = &last {
            let batches = u64::from(moved.batches);
            let readings = Readings::of(report.elapsed, &report.counters, batches, cores.count());
            println!(
                "passthrough/{kind}/{}: {readings}, sink writes {writes}",
                cell.name()
            );
        }
    }
}

/// The roots logs are kept under: tmpfs where the host has one, and the disk beside the build.
#[expect(clippy::print_stderr, reason = "a skipped cell is said once")]
fn roots() -> Vec<(&'static str, PathBuf)> {
    let mut roots = Vec::new();
    let shm = Path::new("/dev/shm");
    if shm.is_dir() {
        roots.push(("tmpfs", shm.to_path_buf()));
    } else {
        eprintln!("passthrough: no /dev/shm, so no tmpfs cell");
    }
    roots.push(("disk", PathBuf::from(env!("CARGO_TARGET_TMPDIR"))));
    roots
}

/// The mixed cell logged under each root, then unlogged, each made when it first runs and its
/// log counters printed after it.
#[expect(
    clippy::print_stdout,
    reason = "the log counters are printed beside criterion's times"
)]
fn logged(c: &mut Criterion) {
    let cores = Cores::try_from_host().expect("the host says how many cores the bench may use");
    let cell = sized(Mixed::CELL);
    let mut group = c.benchmark_group("passthrough/logged");
    group.sample_size(10).sampling_mode(SamplingMode::Flat);
    group.throughput(Throughput::Bytes(Passthrough::bytes(cell)));
    let mut cases: Vec<(&str, Option<PathBuf>)> = roots()
        .into_iter()
        .map(|(root, path)| (root, Some(path)))
        .collect();
    cases.push(("unlogged", None));
    for (name, root) in cases {
        let (mut bench, mut last) = (None, None);
        // Named by the whole cell, so a listing and a timed run name it alike.
        group.bench_function(BenchmarkId::new(name, Mixed::CELL.name()), |b| {
            let (_, mixed) = bench.get_or_insert_with(|| {
                let Some(root) = &root else {
                    return (None, Mixed::unlogged(cores, cell).expect("the pool starts"));
                };
                let dir = tempfile::tempdir_in(root).expect("a directory of the bench's own");
                let store: Arc<dyn WalStore> = Arc::new(LocalWal::new(dir.path()));
                let mixed = Mixed::logged(cores, cell, store).expect("the pool starts");
                (Some(dir), mixed)
            });
            b.iter(|| last = Some(mixed.run()));
        });
        if let (Some((_, mixed)), Some(report)) = (&bench, &last) {
            println!(
                "passthrough/logged/{name}/{}: {}",
                Mixed::CELL.name(),
                counted(report, mixed.frame_bytes())
            );
        }
    }
    group.finish();
}

/// What a run of a mixed cell logged: its bytes a commit, the share of them that the batch
/// frames of the stream its source can read again take, and the log phase a commit; `frames`
/// that stream's batch frames' bytes and every stream's.
#[expect(
    clippy::cast_precision_loss,
    reason = "ratios and means printed to three places"
)]
fn counted(report: &Report, (replayable, frames): (u64, u64)) -> String {
    let commits = report.commits.max(1);
    let log = &report.counters.log;
    let phase = report.counters.phases.log.as_secs_f64() * 1e3 / commits as f64;
    let share = if log.appended == 0 {
        0.0
    } else {
        replayable as f64 / log.appended as f64
    };
    format!(
        "commits {}, logged {:.3} MiB a commit, {:.3} of the frames, of which the stream that \
         reads again {share:.3}, log phase {phase:.3} ms a commit",
        report.commits,
        log.appended as f64 / f64::from(1 << 20) / commits as f64,
        log.appended as f64 / frames as f64,
    )
}

/// Each JSON cell, made when it first runs, its readings and the passes shredding took printed
/// after it.
#[expect(
    clippy::print_stdout,
    reason = "criterion reports the engine's time; the readings are printed beside it"
)]
fn json(c: &mut Criterion) {
    let cores = Cores::try_from_host().expect("the host says how many cores the bench may use");
    let mut group = c.benchmark_group("passthrough/json");
    group.sample_size(10).sampling_mode(SamplingMode::Flat);
    for cell in JsonCell::all() {
        let (mut bench, mut last) = (None, None);
        let made = || {
            let pushes = if timed() {
                cell.pushes()
            } else {
                cell.tested()
            };
            JsonPassthrough::try_new(cores, cell, pushes).expect("the pool starts")
        };
        group.bench_function(cell.name(), |b| {
            let bench = bench.get_or_insert_with(made);
            b.iter(|| last = Some(bench.run()));
        });
        if let (Some(bench), Some(report)) = (&bench, &last) {
            let pushes = u64::try_from(JsonCell::PUSHES).expect("pushes fit in 64 bits");
            let readings = Readings::of(report.elapsed, &report.counters, pushes, cores.count());
            let shred = &report.streams["events"].shred;
            println!(
                "passthrough/json/{}: {} rows of {} bytes a run; {readings}; shred passes \
                 parsed {}, tripped {}, observed {}, rebuilt {}, exact {}",
                cell.name(),
                bench.rows(),
                bench.bytes(),
                shred.parsed,
                shred.tripped,
                shred.observed,
                shred.rebuilt,
                shred.exact,
            );
        }
    }
    group.finish();
}

/// The runs of a many-table cell its readings are of: criterion's samples, its warm-up's left out.
const TABLE_SAMPLES: usize = 10;

/// One run of a many-table cell: its report with the process's CPU time at its window's bounds,
/// and its resident memory, in KiB, before the run and at its peak, where the platform tells them.
struct TableRun {
    metered: Metered<Option<Duration>>,
    resident: Option<(u64, u64)>,
}

/// Each many-table cell, made when it first runs, its readings over its runs printed after it.
fn tables(c: &mut Criterion) {
    let cores = Cores::try_from_host().expect("the host says how many cores the bench may use");
    let schedule = if timed() {
        Schedule::TIMED
    } else {
        Schedule::TESTED
    };
    let mut group = c.benchmark_group("passthrough/tables");
    group
        .sample_size(TABLE_SAMPLES)
        .sampling_mode(SamplingMode::Flat)
        .warm_up_time(Duration::from_millis(1));
    for cell in TablesCell::ALL {
        let measured = match cell {
            // A followed run lasts its span: ten samples of one run each.
            TablesCell::Followed(_) => schedule.span * u32::try_from(TABLE_SAMPLES).unwrap_or(1),
            TablesCell::Snapshot => Duration::from_secs(5),
        };
        group.measurement_time(measured);
        let (mut made, mut runs) = (None, Vec::new());
        group.bench_function(cell.name(), |b| {
            let tables = made.get_or_insert_with(|| {
                Tables::try_new(cores, cell, schedule).expect("the pool starts")
            });
            b.iter_custom(|iterations| {
                let mut took = Duration::ZERO;
                for _ in 0..iterations {
                    let run = table_run(tables);
                    took += run.metered.report.elapsed;
                    runs.push(run);
                }
                took
            });
        });
        if let Some(tables) = &made {
            let runs = &runs[runs.len().saturating_sub(TABLE_SAMPLES)..];
            print_tables(tables, runs, cores);
        }
    }
    group.finish();
}

/// Runs `tables` once, its connectors made and the process's resident peak reset first.
fn table_run(tables: &Tables) -> TableRun {
    let (source, destination) = tables.block_on(tables.connectors());
    let base = reset_peak().ok().and_then(|()| status_kib("VmRSS"));
    let metered = tables.run(source, destination, process_cpu);
    TableRun {
        metered,
        resident: base.zip(status_kib("VmHWM")),
    }
}

/// Kibibytes: the field `field` of this process's status, where the platform keeps one.
fn status_kib(field: &str) -> Option<u64> {
    let status = std::fs::read_to_string("/proc/self/status").ok()?;
    resident_kib(&status, field)
}

/// The CPU time this process has taken so far, every thread's.
#[cfg(unix)]
fn process_cpu() -> Option<Duration> {
    let time = rustix::time::clock_gettime(rustix::time::ClockId::ProcessCPUTime);
    Duration::try_from(time).ok()
}

/// Unmeasured: only a Unix process reads its own CPU clock without `unsafe`.
#[cfg(not(unix))]
fn process_cpu() -> Option<Duration> {
    None
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
fn print_tables(tables: &Tables, runs: &[TableRun], cores: Cores) {
    let Some(last) = runs.last() else {
        return;
    };
    let cell = tables.cell();
    println!(
        "passthrough/tables/{}: {tables}; {} runs",
        cell.name(),
        runs.len()
    );
    let reports: Vec<&Report> = runs.iter().map(|run| &run.metered.report).collect();
    println!("  commit phases, ms a commit: {}", phases(&reports));
    let busy = runs.iter().map(|run| {
        let [before, after] = run.metered.readings;
        let window = run.metered.window.as_secs_f64();
        before
            .zip(after)
            .map(|(before, after)| after.saturating_sub(before).as_secs_f64() / window)
    });
    let window = match cell {
        TablesCell::Followed(_) => "while its streams are quiet",
        TablesCell::Snapshot => "over the run",
    };
    println!("  CPU seconds a wall second {window}: {}", spread(busy));
    let kept = reports.iter().map(|report| report.counters.kept_peak).max();
    let mebibytes = |kib: u64| kib as f64 / 1024.0;
    let peaks = runs
        .iter()
        .map(|run| run.resident.map(|(_, peak)| mebibytes(peak)));
    let grown = runs.iter().map(|run| {
        run.resident
            .map(|(base, peak)| mebibytes(peak.saturating_sub(base)))
    });
    println!(
        "  reads kept at most {} KiB of their {} KiB share; resident peak, MiB: {}, above the \
         run's base: {}",
        kept.unwrap_or(0) >> 10,
        tables.kept_share() >> 10,
        spread(peaks),
        spread(grown),
    );
    let report = &last.metered.report;
    let pushes = u64::try_from(cell.pushes()).expect("pushes fit in 64 bits");
    let readings = Readings::of(report.elapsed, &report.counters, pushes, cores.count());
    println!("  readings: {readings}");
}

criterion_group!(benches, passthrough, logged, json, tables);
criterion_main!(benches);
