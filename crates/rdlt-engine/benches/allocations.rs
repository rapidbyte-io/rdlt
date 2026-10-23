//! What the engine's hot paths allocate under a counting allocator: what one run of each bench
//! workload allocates, a row and a batch, and each case the instruction counts run.
//!
//! The workloads are the shred bench's single-core groups, both sides of the passthrough pairs,
//! each batch the lowering bench prepares, each run of the normalized and wide benches, and the
//! wal bench's load logged into memory; the timed benches keep the system allocator, since
//! counting every call slows the runs that allocate most. An argument, the name of one of those
//! benches, counts only its workloads. The workloads run at the benches' full sizes only when
//! `--bench` is among the arguments, as `just bench` passes it; otherwise, as a test run, they
//! run on small inputs that still check each workload's shape and counting.
//!
//! A case's name, then a count of iterations, as `cargo xtask instructions` runs it under
//! callgrind, makes the case's inputs, runs its work that many times, once without a count, and
//! prints the allocations the process made. `--list` lists the benches and the cases as a test
//! harness lists its tests, so a test runner runs each in a process of its own; a case's name holds
//! a `/` and a bench's does not, which is how `cargo xtask instructions` tells the cases apart.

#![forbid(unsafe_code)]

use std::alloc::System;
use std::hint::black_box;
use std::num::{NonZeroU64, NonZeroUsize};
use std::process::ExitCode;
use std::sync::Arc;

use arrow_array::RecordBatch;
use rdlt_connector::{ConnectContext, destination_factory};
use rdlt_connector_reference::MemoryDestination;
use rdlt_engine::Cores;
use rdlt_engine::bench::{
    CHUNK_BYTES, CORPUS_BYTES, Cell, Corpus, Form, Logged, Lowering, Normalized, Passthrough,
    Replayed, Wide, check_json, counted, json_column, memory_wal, normalize, null_sink, pruned,
    replay, rows_pushed, sample_log, scan_log, shred,
};
use rdlt_wire::{Decoder, Encoder, Limits};
use stats_alloc::{INSTRUMENTED_SYSTEM, StatsAlloc};

#[global_allocator]
static HEAP: &StatsAlloc<System> = &INSTRUMENTED_SYSTEM;

/// The benches whose workloads this counts, each with what counts them.
const COUNTED: [(&str, fn()); 6] = [
    ("shred", shredding),
    ("passthrough", passing_through),
    ("lowering", lowering),
    ("normalized", normalizing),
    ("wide", widening),
    ("wal", logging),
];

/// Bytes of JSON each shredding and normalizing case reads.
const COUNTED_BYTES: usize = 1 << 20;
/// Bytes of JSON each cliff corpus has as a test: several chunks, so a push still crosses one.
const TESTED_CLIFF_BYTES: usize = 4 << 20;
/// Rows of the stream the normalized cases read as a test.
const TESTED_ROOTS: u64 = 20_000;
/// Batches each wide case pushes as a test.
const TESTED_PUSHES: u32 = 1;
/// Rows of each lowering batch as a test.
const TESTED_LOWERING_ROWS: u32 = 8_192;
/// Batches the passthrough case moves each time.
const BATCHES: u32 = 8;
/// Rows of each batch the passthrough, log, codec and lowering cases move.
const ROWS: u32 = 8192;
/// Two cores, whatever the host has: a case counts the same on every machine only on a count it
/// fixes, which is a measuring fixture and no deployment's.
const LAYOUT: Cores = Cores::new(NonZeroUsize::new(2).expect("two is not zero"));
/// Partitions whose seals the commit case's one commit holds.
const COMMITTED: NonZeroUsize = NonZeroUsize::new(1_024).expect("not zero");

/// A case's name, and what runs it: its inputs made once, then its work as many times as asked.
type Case = (&'static str, fn(usize));

/// Every case the instruction counts run.
const CASES: [Case; 29] = [
    ("passthrough/null_sink", passed_through),
    ("commit/1024", committed),
    ("shred/nested", |times| shredded(Corpus::Nested, times)),
    ("shred/rows", shredded_rows),
    ("shred/with_arrays", |times| {
        shredded(Corpus::WithArrays, times);
    }),
    ("shred/flat_narrow", |times| {
        shredded(Corpus::FlatNarrow, times);
    }),
    ("shred/wide_200", |times| {
        shredded(Corpus::Wide(200), times);
    }),
    ("shred/string_heavy", |times| {
        shredded(Corpus::StringHeavy, times);
    }),
    ("shred/array_form", |times| {
        shredded(Corpus::ArrayForm, times);
    }),
    ("shred/drift_json", |times| {
        shredded(Corpus::DriftJson, times);
    }),
    ("shred/drift_scalar", |times| {
        shredded(Corpus::DriftScalar, times);
    }),
    ("shred/big_numbers", |times| {
        shredded(Corpus::BigNumbers, times);
    }),
    ("normalize/keyless/nested", |times| {
        normalized(Corpus::Nested, &[], times);
    }),
    ("normalize/keyless/with_arrays", |times| {
        normalized(Corpus::WithArrays, &[], times);
    }),
    ("normalize/keyed/with_arrays", |times| {
        normalized(Corpus::WithArrays, &["id"], times);
    }),
    ("normalize/dropped/with_arrays", |times| {
        dropped(Corpus::WithArrays, times);
    }),
    ("wal/encode", log_encoded),
    ("wal/scan", log_scanned),
    ("ipc/roundtrip", round_tripped),
    ("json/check", json_checked),
    ("lowering/history", |times| lowered("history", times)),
    ("lowering/changes", |times| lowered("changes", times)),
    ("lowering/one_bad_value", |times| {
        lowered("one_bad_value", times);
    }),
    ("lowering/one_dropped_row", |times| {
        lowered("one_dropped_row", times);
    }),
    ("lowering/temporal_widening", |times| {
        lowered("temporal_widening", times);
    }),
    ("lowering/append_text", |times| {
        lowered("append_text", times);
    }),
    ("lowering/merge_unique", |times| {
        lowered("merge_unique", times);
    }),
    ("lowering/merge_duplicates", |times| {
        lowered("merge_duplicates", times);
    }),
    ("lowering/split_json", |times| lowered("split_json", times)),
];

#[expect(
    clippy::print_stdout,
    reason = "names and allocation counts are what is asked for"
)]
fn main() -> ExitCode {
    let benches = COUNTED.map(|(name, _)| name);
    let cases = CASES.map(|(name, _)| name);
    let args: Vec<String> = std::env::args().collect();
    if args.iter().any(|arg| arg == "--list") {
        // None of them is ignored. The instruction counts take the names holding a `/` as the
        // cases, so a bench's name holds none.
        if !args.iter().any(|arg| arg == "--ignored") {
            for name in benches.iter().chain(&cases) {
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
    match named.as_slice() {
        [] => {
            for (_, count) in COUNTED {
                count();
            }
            for (_, run) in CASES {
                run(1);
            }
        }
        [bench] if benches.contains(bench) => {
            COUNTED
                .iter()
                .filter(|(name, _)| name == bench)
                .for_each(|(_, count)| count());
        }
        [case] | [case, _] if cases.contains(case) => {
            let times = match named.get(1) {
                None => 1,
                Some(times) => match times.parse() {
                    Ok(times) => times,
                    Err(_) => return usage(),
                },
            };
            CASES
                .iter()
                .filter(|(name, _)| name == case)
                .for_each(|(_, run)| run(times));
            println!("allocations {}", HEAP.stats().allocations);
        }
        _ => return usage(),
    }
    ExitCode::SUCCESS
}

#[expect(
    clippy::print_stderr,
    reason = "the usage is the error a wrong call gets"
)]
fn usage() -> ExitCode {
    eprintln!(
        "usage: allocations [<bench> | <case> [<iterations>] | --list]; benches {:?}, \
         cases {:?}",
        COUNTED.map(|(name, _)| name),
        CASES.map(|(name, _)| name)
    );
    ExitCode::FAILURE
}

/// Whether `--bench` asks for the benches' full sizes, rather than a test run's small ones.
fn timed() -> bool {
    std::env::args().any(|arg| arg == "--bench")
}

/// The single-core shred and normalize groups' workloads.
fn shredding() {
    let (shredded_bytes, cliff_bytes) = if timed() {
        (CORPUS_BYTES, CORPUS_BYTES)
    } else {
        (COUNTED_BYTES, TESTED_CLIFF_BYTES)
    };
    for corpus in Corpus::SHREDDED {
        let pushes = corpus.pushes(shredded_bytes);
        report(&format!("shred/{}", corpus.name()), || {
            moved(&shred(&pushes, CHUNK_BYTES).expect("the corpus shreds"))
        });
    }
    for corpus in Corpus::CLIFFS {
        let pushes = corpus.pushes(cliff_bytes);
        report(&format!("shred/{}", corpus.name()), || {
            moved(&shred(&pushes, CHUNK_BYTES).expect("the corpus shreds"))
        });
    }
    let pushes = Corpus::WithArrays.pushes(shredded_bytes);
    for (name, key) in [
        ("shred_only", None),
        ("keyed", Some(&["id"][..])),
        ("keyless", Some(&[][..])),
    ] {
        report(&format!("normalize/{name}"), || {
            let shredded = shred(&pushes, CHUNK_BYTES).expect("the corpus shreds");
            if let Some(key) = key {
                for batch in &shredded {
                    black_box(normalize(batch, 8, key).expect("the batch normalizes"));
                }
            }
            moved(&shredded)
        });
    }
}

/// Both sides of the passthrough bench's pairs.
fn passing_through() {
    let cores = Cores::try_from_host().expect("the host says how many cores the bench may use");
    let passthrough = Passthrough::paired(cores, Cell::PAIRED).expect("the pool starts");
    let batches = moved(passthrough.batches());
    report("passthrough bare loop", || {
        black_box(passthrough.bare_loop());
        batches
    });
    report("passthrough engine", || {
        black_box(passthrough.engine_run());
        batches
    });
}

/// Each batch the lowering bench prepares, prepared once after the prepare that makes it.
fn lowering() {
    let rows = if timed() {
        Lowering::ROWS
    } else {
        TESTED_LOWERING_ROWS
    };
    for lowering in Lowering::all(rows) {
        black_box(lowering.prepare());
        report(&format!("lowering/{}", lowering.name()), || {
            black_box(lowering.prepare());
            (
                usize::try_from(lowering.rows()).expect("rows fit in usize"),
                1,
            )
        });
    }
}

/// Each case of the normalized bench, run once: its rows those of the three tables it loads,
/// its batches the flushes.
fn normalizing() {
    let cores = Cores::try_from_host().expect("the host says how many cores the bench may use");
    let roots = if timed() {
        Normalized::ROOTS
    } else {
        TESTED_ROOTS
    };
    for per_push in Normalized::PUSH_ROWS {
        let bench = Normalized::try_new(cores, roots, per_push).expect("the pool starts");
        let flushes = roots.div_ceil(per_push.get());
        report(&format!("normalized/keyless/{per_push}"), || {
            let rows = bench.run();
            (
                usize::try_from(rows).expect("rows fit in usize"),
                usize::try_from(flushes).expect("flushes fit in usize"),
            )
        });
    }
}

/// Each case of the wide bench, run once: its batches the pushes.
fn widening() {
    let cores = Cores::try_from_host().expect("the host says how many cores the bench may use");
    let pushes = if timed() { Wide::PUSHES } else { TESTED_PUSHES };
    for (form, name) in [(Form::Arrow, "arrow"), (Form::Json, "json")] {
        for columns in Wide::COLUMNS {
            let bench = Wide::try_new(cores, form, columns, pushes).expect("the pool starts");
            report(&format!("wide/{name}/{columns}"), || {
                let rows = bench.run();
                let pushes = usize::try_from(pushes).expect("pushes fit in usize");
                (usize::try_from(rows).expect("rows fit in usize"), pushes)
            });
        }
    }
}

/// The wal bench's load logged ahead, its log in memory and a commit each batch: what one run
/// allocates, a row and a batch of those it loads.
fn logging() {
    let every = NonZeroU64::new(1).expect("one is not zero");
    let cores = Cores::try_from_host().expect("the host says how many cores the bench may use");
    let logged =
        Logged::try_new(cores, memory_wal(), BATCHES, ROWS, Some(every)).expect("the pool starts");
    let rows = usize::try_from(BATCHES * ROWS).expect("rows fit in usize");
    report("wal/engine/memory/each", || {
        black_box(logged.run());
        (
            rows,
            usize::try_from(BATCHES).expect("batches fit in usize"),
        )
    });
}

/// The rows and the count of `batches`.
fn moved(batches: &[RecordBatch]) -> (usize, usize) {
    (
        batches.iter().map(RecordBatch::num_rows).sum(),
        batches.len(),
    )
}

/// Prints what one run of `run` allocated, a row and a batch of the rows and batches it moved.
#[expect(clippy::print_stdout, reason = "the counts are this bench's output")]
fn report(id: &str, run: impl FnOnce() -> (usize, usize)) {
    let mut moving = (0, 0);
    let allocated = counted(HEAP, || moving = run());
    assert!(
        allocated.allocations > 0.0,
        "the counting allocator serves this process"
    );
    let units = |count: usize| {
        NonZeroU64::new(u64::try_from(count).expect("a count fits in 64 bits"))
            .expect("a run moves rows in batches")
    };
    let (rows, batches) = moving;
    println!(
        "{id}: {} a row; {} a batch",
        allocated.per(units(rows)),
        allocated.per(units(batches))
    );
}

/// The engine moving [`BATCHES`] batches of [`ROWS`] rows from a replaying source to the null
/// sink, on [`LAYOUT`].
fn passed_through(times: usize) {
    let cell = Cell {
        rows: ROWS,
        batches: BATCHES,
        ..Cell::PAIRED
    };
    let passthrough = Passthrough::try_new(LAYOUT, cell).expect("the pool starts");
    for _ in 0..times {
        let replayed = Replayed::Batches(passthrough.batches().to_vec());
        let source = passthrough.block_on(replay("passthrough", replayed));
        black_box(passthrough.run(source, passthrough.block_on(null_sink())));
    }
}

/// One commit of [`COMMITTED`] partitions' seals: a batch of one row from each partition of a
/// replaying source, committed once, at the end, into the in-memory destination, a store of its
/// own each time, on [`LAYOUT`].
fn committed(times: usize) {
    let cell = Cell {
        rows: 1,
        batches: u32::try_from(COMMITTED.get()).expect("the partitions fit in 32 bits"),
        partitions: COMMITTED,
        ..Cell::PAIRED
    };
    let passthrough = Passthrough::try_new(LAYOUT, cell).expect("the pool starts");
    let factory = destination_factory::<MemoryDestination>();
    for time in 0..times {
        let source = passthrough.block_on(passthrough.source());
        let config = serde_json::json!({ "store": format!("commit-{time}") });
        let connected = passthrough.block_on(factory.connect(config, ConnectContext::new()));
        let destination = Arc::from(connected.expect("the destination connects"));
        let report = black_box(passthrough.run(source, destination));
        assert_eq!(report.commits, 1, "one commit holds every partition's seal");
    }
}

/// Shredding [`COUNTED_BYTES`] of `corpus` on the calling thread.
fn shredded(corpus: Corpus, times: usize) {
    let pushes = corpus.pushes(COUNTED_BYTES);
    for _ in 0..times {
        black_box(shred(&pushes, CHUNK_BYTES).expect("the corpus shreds"));
    }
}

/// Shredding [`COUNTED_BYTES`] of the nested corpus on the calling thread, pushed as a source's
/// one call of `Emitter::rows` with its records pushes them.
fn shredded_rows(times: usize) {
    let pushes = rows_pushed(Corpus::Nested.pushes(COUNTED_BYTES));
    for _ in 0..times {
        black_box(shred(&pushes, CHUNK_BYTES).expect("the rows shred"));
    }
}

/// Normalizing, to depth 8 and with `key`, the batches [`COUNTED_BYTES`] of `corpus` shreds into.
fn normalized(corpus: Corpus, key: &[&str], times: usize) {
    let batches = shred(&corpus.pushes(COUNTED_BYTES), CHUNK_BYTES).expect("the corpus shreds");
    for _ in 0..times {
        for batch in &batches {
            black_box(normalize(batch, 8, key).expect("the batch normalizes"));
        }
    }
}

/// Normalizing `corpus`'s batches keyless with one root row in ten dropped, and their children
/// with them.
fn dropped(corpus: Corpus, times: usize) {
    let batches = shred(&corpus.pushes(COUNTED_BYTES), CHUNK_BYTES).expect("the corpus shreds");
    let every = NonZeroUsize::new(10).expect("ten is not zero");
    for _ in 0..times {
        for batch in &batches {
            black_box(pruned(batch, 8, every).expect("the batch normalizes"));
        }
    }
}

/// Preparing the lowering bench's batch `name`, of [`ROWS`] rows, on the calling thread; the
/// first prepare makes its plan.
fn lowered(name: &str, times: usize) {
    let lowering = Lowering::all(ROWS)
        .into_iter()
        .find(|lowering| lowering.name() == name)
        .expect("the lowering bench prepares a batch of each name a case gives");
    for _ in 0..times {
        black_box(lowering.prepare());
    }
}

/// A log's frames for one batch of [`ROWS`] rows.
fn log_encoded(times: usize) {
    let batch = Passthrough::batch(0, ROWS);
    for _ in 0..times {
        black_box(sample_log(batch.clone()));
    }
}

/// The scan of that log.
fn log_scanned(times: usize) {
    let log = sample_log(Passthrough::batch(0, ROWS));
    for _ in 0..times {
        black_box(scan_log(&log).expect("the log reads back"));
    }
}

/// Checking a column of [`ROWS`] escaped JSON values.
fn json_checked(times: usize) {
    let batch = json_column(ROWS as usize, true);
    for _ in 0..times {
        check_json(&batch).expect("the column is JSON");
    }
}

/// The wire codec over one batch of [`ROWS`] rows.
fn round_tripped(times: usize) {
    let batch = Passthrough::batch(0, ROWS);
    for _ in 0..times {
        black_box(roundtrip(&batch));
    }
}

/// `batch` encoded as one sender encodes it, then decoded as its receiver decodes it.
fn roundtrip(batch: &RecordBatch) -> RecordBatch {
    let mut encoder = Encoder::default();
    let schema = encoder.schema(&batch.schema()).expect("the schema encodes");
    let frames = encoder.batch(batch).expect("the batch encodes");
    let mut decoder = Decoder::new(Limits::default());
    decoder.schema(&schema).expect("the schema decodes");
    let mut received = None;
    // A batch's dictionaries come before it, so the frames are decoded in the order sent.
    for frame in &frames {
        received = decoder
            .frame(frame)
            .expect("the frame decodes")
            .or(received);
    }
    received.expect("the last frame is the batch")
}
