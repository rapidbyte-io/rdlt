//! What one run of each bench workload allocates, a row and a batch, under a counting allocator:
//! the shred bench's single-core groups, both sides of the passthrough pairs, each batch the
//! lowering bench prepares and each run of the normalized bench.
//!
//! The timed benches keep the system allocator, since counting every call slows the runs that
//! allocate most.
//!
//! An argument, the name of one of those benches, counts only its workloads.

#![forbid(unsafe_code)]

use std::alloc::System;
use std::hint::black_box;
use std::num::NonZeroU64;

use arrow_array::RecordBatch;
use rdlt_engine::Cores;
use rdlt_engine::bench::{
    CHUNK_BYTES, CORPUS_BYTES, Corpus, Lowering, Normalized, Passthrough, counted, normalize, shred,
};
use stats_alloc::{INSTRUMENTED_SYSTEM, StatsAlloc};

#[global_allocator]
static HEAP: &StatsAlloc<System> = &INSTRUMENTED_SYSTEM;

/// The benches whose workloads this counts, each with what counts them.
const COUNTED: [(&str, fn()); 4] = [
    ("shred", shredding),
    ("passthrough", passing_through),
    ("lowering", lowering),
    ("normalized", normalizing),
];

fn main() {
    let only = std::env::args().skip(1).find(|arg| !arg.starts_with('-'));
    let names = COUNTED.map(|(name, _)| name);
    assert!(
        only.as_deref().is_none_or(|bench| names.contains(&bench)),
        "only the workloads of {names:?} are counted, not {only:?}"
    );
    for (name, count) in COUNTED {
        if only.as_deref().is_none_or(|only| only == name) {
            count();
        }
    }
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

/// Each batch the lowering bench prepares, prepared once.
fn lowering() {
    for lowering in Lowering::all(Lowering::ROWS) {
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
