//! Shred-stage-only measurement.
//!
//! Reads a JSONL file, shreds it slab-by-slab through the full shred path
//! (parse → shape observation → schema resolution → Arrow build) with NO
//! destination and NO I/O beyond the initial read — the mirror of dlt's
//! `normalize` stage in `benches/baseline/normalize_only.py`.
//!
//! Usage: `shred_only <rows.jsonl>` — emits JSON on stdout.

fn main() {
    let path = std::env::args()
        .nth(1)
        .expect("usage: shred_only <rows.jsonl>");
    let data = std::fs::read(&path).expect("read input");
    let line_count = data.iter().filter(|b| **b == b'\n').count() as u64;

    // 8 MB slabs on line boundaries — the same unit the file source pushes.
    const SLAB: usize = 8 << 20;
    let mut slabs: Vec<&[u8]> = Vec::new();
    let mut start = 0usize;
    while start < data.len() {
        let tentative = (start + SLAB).min(data.len());
        let end = match data[..tentative].iter().rposition(|b| *b == b'\n') {
            Some(nl) if nl >= start => nl + 1,
            _ => tentative,
        };
        slabs.push(&data[start..end]);
        start = end;
    }

    let started = std::time::Instant::now();
    let mut rows = 0u64;
    for slab in &slabs {
        rows += rdlt_engine::fuzzing::bench_shred_bytes(slab);
    }
    let elapsed = started.elapsed().as_secs_f64();

    assert!(rows >= line_count, "row conservation sanity");
    println!(
        r#"{{"rows":{line_count},"emitted_rows":{rows},"seconds":{elapsed:.4},"rows_per_s":{:.0}}}"#,
        line_count as f64 / elapsed
    );
}
