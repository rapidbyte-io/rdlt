//! What the engine's hot paths allocate under a counting allocator: what one run of each bench
//! workload allocates, a row and a batch, and each case the instruction counts run.
//!
//! The workloads are the shred bench's single-core groups, both sides of the passthrough pairs,
//! each batch the lowering bench prepares, and each run of the normalized and wide benches; the
//! timed benches keep the system allocator, since counting every call slows the runs that
//! allocate most. An argument, the name of one of those benches, counts only its workloads.
//!
//! A case's name, then a count of iterations, as `cargo xtask instructions` runs it under
//! callgrind, makes the case's inputs, runs its work that many times, once without a count, and
//! prints the allocations the process made; `--cases` names the cases. `--list` lists the benches
//! and the cases as a test harness lists its tests, so a test runner runs each in a process of its
//! own.

#![forbid(unsafe_code)]

use std::alloc::System;
use std::hint::black_box;
use std::num::{NonZeroU64, NonZeroUsize};
use std::process::ExitCode;

use arrow_array::RecordBatch;
use rdlt_engine::Cores;
use rdlt_engine::bench::{
    CHUNK_BYTES, CORPUS_BYTES, Corpus, Form, Lowering, Normalized, Passthrough, Replayed, Wide,
    counted, normalize, null_sink, replay, sample_log, scan_log, shred,
};
use rdlt_wire::{Decoder, Encoder, Limits};
use stats_alloc::{INSTRUMENTED_SYSTEM, StatsAlloc};

#[global_allocator]
static HEAP: &StatsAlloc<System> = &INSTRUMENTED_SYSTEM;

/// The benches whose workloads this counts, each with what counts them.
const COUNTED: [(&str, fn()); 5] = [
    ("shred", shredding),
    ("passthrough", passing_through),
    ("lowering", lowering),
    ("normalized", normalizing),
    ("wide", widening),
];

/// Bytes of JSON each shredding and normalizing case reads.
const COUNTED_BYTES: usize = 1 << 20;
/// Batches the passthrough case moves each time.
const BATCHES: u32 = 8;
/// Rows of each batch the passthrough, log and codec cases move.
const ROWS: u32 = 8192;
/// One runtime worker and one compute thread, whatever the host has: a case counts the same on
/// every machine only on a layout it fixes, which is a measuring fixture and no deployment's.
const LAYOUT: Cores = Cores::new(
    NonZeroUsize::new(2).expect("two is not zero"),
    NonZeroUsize::MIN,
);

/// A case's name, and what runs it: its inputs made once, then its work as many times as asked.
type Case = (&'static str, fn(usize));

/// Every case the instruction counts run.
const CASES: [Case; 12] = [
    ("passthrough/null_sink", passed_through),
    ("shred/nested", |times| shredded(Corpus::Nested, times)),
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
    ("normalize/keyless/nested", |times| {
        normalized(Corpus::Nested, &[], times);
    }),
    ("normalize/keyless/with_arrays", |times| {
        normalized(Corpus::WithArrays, &[], times);
    }),
    ("normalize/keyed/with_arrays", |times| {
        normalized(Corpus::WithArrays, &["id"], times);
    }),
    ("wal/encode", log_encoded),
    ("wal/scan", log_scanned),
    ("ipc/roundtrip", round_tripped),
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
        // None of them is ignored.
        if !args.iter().any(|arg| arg == "--ignored") {
            for name in benches.iter().chain(&cases) {
                println!("{name}: test");
            }
        }
        return ExitCode::SUCCESS;
    }
    if args.iter().any(|arg| arg == "--cases") {
        for name in cases {
            println!("{name}");
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
        "usage: allocations [<bench> | <case> [<iterations>] | --cases | --list]; benches {:?}, \
         cases {:?}",
        COUNTED.map(|(name, _)| name),
        CASES.map(|(name, _)| name)
    );
    ExitCode::FAILURE
}

/// The single-core shred and normalize groups' workloads.
fn shredding() {
    for corpus in Corpus::SHREDDED {
        let pushes = corpus.pushes(CORPUS_BYTES);
        report(&format!("shred/{}", corpus.name()), || {
            moved(&shred(&pushes, CHUNK_BYTES).expect("the corpus shreds"))
        });
    }
    let pushes = Corpus::WithArrays.pushes(CORPUS_BYTES);
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
    let passthrough = Passthrough::try_new(cores, Passthrough::BATCHES, Passthrough::ROWS)
        .expect("the pool starts");
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
    for lowering in Lowering::all(Lowering::ROWS) {
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
    for per_push in Normalized::PUSH_ROWS {
        let bench =
            Normalized::try_new(cores, Normalized::ROOTS, per_push).expect("the pool starts");
        let flushes = Normalized::ROOTS.div_ceil(per_push.get());
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
    for (form, name) in [(Form::Arrow, "arrow"), (Form::Json, "json")] {
        for columns in Wide::COLUMNS {
            let bench = Wide::try_new(cores, form, columns, Wide::PUSHES).expect("the pool starts");
            report(&format!("wide/{name}/{columns}"), || {
                let rows = bench.run();
                let pushes = usize::try_from(Wide::PUSHES).expect("pushes fit in usize");
                (usize::try_from(rows).expect("rows fit in usize"), pushes)
            });
        }
    }
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
    let passthrough = Passthrough::try_new(LAYOUT, BATCHES, ROWS).expect("the pool starts");
    for _ in 0..times {
        let replayed = Replayed::Batches(passthrough.batches().to_vec());
        let source = passthrough.block_on(replay("passthrough", replayed));
        black_box(passthrough.run(source, passthrough.block_on(null_sink())));
    }
}

/// Shredding [`COUNTED_BYTES`] of `corpus` on the calling thread.
fn shredded(corpus: Corpus, times: usize) {
    let pushes = corpus.pushes(COUNTED_BYTES);
    for _ in 0..times {
        black_box(shred(&pushes, CHUNK_BYTES).expect("the corpus shreds"));
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
