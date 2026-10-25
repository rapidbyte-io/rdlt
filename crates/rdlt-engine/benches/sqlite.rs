//! SQLite: rows loaded through the engine into the reference SQLite destination, on disk beside
//! the build: appends at one commit and at a commit every 10k rows, a million rows at a commit
//! every 10k from a source that cannot read them again, each told its cursor once its commit
//! lands, and a million rows merged by key into a million, a tenth of them updates.
//!
//! Each append prints the time its last run's commits took, phase by phase. The database's files
//! are `loaded.db` and SQLite's beside it, so a run's syncs tell apart by path.

#![forbid(unsafe_code)]

use std::num::NonZeroU64;
use std::path::Path;
use std::time::{Duration, Instant};

use criterion::{
    BenchmarkId, Criterion, SamplingMode, Throughput, criterion_group, criterion_main,
};
use rdlt_connector::destination_factory;
use rdlt_connector_reference::SqliteDestination;
use rdlt_engine::bench::{Load, Loading};
use rdlt_engine::{Cores, Report};
use serde_json::json;
use tempfile::TempDir;

/// Rows of each append's batches.
const APPEND_BATCH: NonZeroU64 = NonZeroU64::new(10_000).expect("not zero");
/// Rows of each merge's batches.
const MERGE_BATCH: NonZeroU64 = NonZeroU64::new(50_000).expect("not zero");
/// Rows between commits of the appends that commit as they go.
const COMMIT_ROWS: NonZeroU64 = NonZeroU64::new(10_000).expect("not zero");
/// Rows of the merge, and of the table it merges into.
const MERGED: u64 = 1_000_000;

/// Whether an append's source can read its rows again, or cannot.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Source {
    Replayable,
    Unreplayable,
}

/// The appends: rows, the rows between commits or none for one commit at the end, and whether
/// their source can read them again.
const APPENDS: [(u64, Option<NonZeroU64>, Source); 5] = [
    (1_000_000, None, Source::Replayable),
    (200_000, Some(COMMIT_ROWS), Source::Replayable),
    (500_000, Some(COMMIT_ROWS), Source::Replayable),
    (1_000_000, Some(COMMIT_ROWS), Source::Replayable),
    (1_000_000, Some(COMMIT_ROWS), Source::Unreplayable),
];

/// A database of its own in a directory of its own beside the build, which the destination
/// requires its user alone to write.
fn database() -> (TempDir, std::path::PathBuf) {
    let dir =
        tempfile::tempdir_in(env!("CARGO_TARGET_TMPDIR")).expect("a directory beside the build");
    let path = dir.path().join("loaded.db");
    (dir, path)
}

/// Loads `loading` into the database at `path`; how long the run took, and its report.
#[expect(
    clippy::disallowed_methods,
    reason = "a benchmark times its runs on the real clock"
)]
fn load(loading: &Loading, path: &Path) -> (Duration, Report) {
    let factory = destination_factory::<SqliteDestination>();
    let destination = loading.connect(factory.as_ref(), json!({ "path": path }));
    let started = Instant::now();
    let report = loading.run(destination);
    (started.elapsed(), report)
}

/// The loading of `rows` appended rows committed every `commit` rows or once, from `source`.
fn appends(cores: Cores, rows: u64, commit: Option<NonZeroU64>, source: Source) -> Loading {
    let made = match source {
        Source::Replayable => Loading::try_new(cores, Load::Append, rows, APPEND_BATCH, commit),
        Source::Unreplayable => Loading::unreplayable(cores, rows, APPEND_BATCH, commit),
    };
    made.expect("the pool starts")
}

/// The time `report`'s commits took a commit, phase by phase.
#[expect(clippy::cast_precision_loss, reason = "commits are far below 2^52")]
fn phases(report: &Report) -> String {
    let phases = &report.counters.phases;
    let commits = report.commits.max(1) as f64;
    let each = |phase: Duration| phase.as_secs_f64() * 1e3 / commits;
    format!(
        "commits {}; a commit's phases: flush {:.3} ms, commit {:.3} ms, ack {:.3} ms",
        report.commits,
        each(phases.flush),
        each(phases.commit),
        each(phases.ack),
    )
}

/// Asserts that the table at `path` holds `rows` rows and its staging none.
fn holds(path: &Path, rows: u64) {
    let connection = rusqlite::Connection::open(path).expect("the database opens");
    let count = |table: &str| -> i64 {
        connection
            .query_row(&format!("SELECT COUNT(*) FROM \"{table}\""), [], |row| {
                row.get(0)
            })
            .expect("the table counts")
    };
    assert_eq!(u64::try_from(count("events")), Ok(rows));
    assert_eq!(
        count("_rdlt_staging__events"),
        0,
        "every staged row was published"
    );
}

#[expect(
    clippy::print_stdout,
    reason = "criterion reports times; each append's commit phases are printed beside them"
)]
fn appending(c: &mut Criterion) {
    let cores = Cores::try_from_host().expect("the host says how many cores the bench may use");
    let mut group = c.benchmark_group("sqlite");
    group.sample_size(10).sampling_mode(SamplingMode::Flat);
    for (rows, commit, source) in APPENDS {
        let every = commit.map_or_else(|| "once".to_owned(), |rows| rows.to_string());
        let name = match source {
            Source::Replayable => format!("{rows}/{every}"),
            Source::Unreplayable => format!("{rows}/{every}/unreplayable"),
        };
        let (mut loading, mut reported) = (None, None);
        group.throughput(Throughput::Elements(rows));
        group.bench_function(BenchmarkId::new("append", &name), |b| {
            let loading = loading.get_or_insert_with(|| appends(cores, rows, commit, source));
            b.iter_custom(|runs| {
                let mut took = Duration::ZERO;
                for _ in 0..runs {
                    let (_dir, path) = database();
                    let (elapsed, report) = load(loading, &path);
                    took += elapsed;
                    let least = commit.map_or(1, |every| rows.div_ceil(every.get()));
                    let made = report.commits;
                    assert!(made >= least, "{made} commits of {rows} rows");
                    holds(&path, rows);
                    reported = Some(report);
                }
                took
            });
        });
        if let Some(report) = &reported {
            println!("sqlite/append/{name}: {}", phases(report));
        }
    }
    group.finish();
}

/// A table of a million rows merged by key, written once, and the merge of a million more over
/// it, which each run applies to a copy of the table.
struct Merging {
    table: TempDir,
    update: Loading,
}

impl Merging {
    fn new(cores: Cores) -> Self {
        let (table, path) = database();
        let merge = Loading::try_new(cores, Load::Merge, MERGED, MERGE_BATCH, None)
            .expect("the pool starts");
        load(&merge, &path);
        holds(&path, MERGED);
        // The copies each run takes are of the database file alone, its log folded into it.
        let connection = rusqlite::Connection::open(&path).expect("the database opens");
        connection
            .query_row("PRAGMA wal_checkpoint(TRUNCATE)", [], |_| Ok(()))
            .expect("the log folds into the database");
        let update = Loading::try_new(cores, Load::Update, MERGED, MERGE_BATCH, None)
            .expect("the pool starts");
        Self { table, update }
    }
}

fn merging(c: &mut Criterion) {
    let cores = Cores::try_from_host().expect("the host says how many cores the bench may use");
    let mut group = c.benchmark_group("sqlite");
    group.sample_size(10).sampling_mode(SamplingMode::Flat);
    group.throughput(Throughput::Elements(MERGED));
    let mut merging = None;
    group.bench_function(BenchmarkId::new("merge", MERGED), |b| {
        let merging = merging.get_or_insert_with(|| Merging::new(cores));
        b.iter_custom(|runs| {
            let mut took = Duration::ZERO;
            for _ in 0..runs {
                let (_dir, path) = database();
                std::fs::copy(merging.table.path().join("loaded.db"), &path)
                    .expect("the table copies");
                let (elapsed, _) = load(&merging.update, &path);
                took += elapsed;
                // A tenth of the rows merged replaced rows the table held.
                holds(&path, MERGED + MERGED - MERGED / 10);
            }
            took
        });
    });
    group.finish();
}

criterion_group!(benches, appending, merging);
criterion_main!(benches);
