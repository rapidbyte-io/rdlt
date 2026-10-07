# JSON shredding throughput

The spec gates JSON shredding at four times the old engine on one core, and at 0.7·N times one
core for one stream over N ≤ 8 cores (§21.1). Both are ratios measured on one machine, so this
record keeps the method with the numbers.

## Method

- **Corpus.** 200 000 nested rows of about 181 bytes (36.2 MB), written by the script below; the
  criterion bench's `nested` corpus follows the same shape.
- **Old engine.** `rdlt.old` at `0249668f`, through its production shred path
  `rdlt_engine::fuzzing::bench_shred_bytes`, over 8 MiB pushes cut at line ends, built with fat
  LTO and one codegen unit, best of five runs. The harness is below.
- **This engine.** `RDLT_SHRED_CORPUS=<corpus> just bench shred 0 '^shred/file$'`: 8 MiB pushes,
  1 MiB chunks, built with the release profile's fat LTO and one codegen unit, criterion's mean.
- **Cores.** `shred_cores/N` shreds the `nested` corpus within N cores, for one core, each power
  of two below the cores the bench may run on, and all of them, run on cores of one type, since
  the development machine mixes them: `just bench shred 0-3 '^shred_cores/4$'` on the performance
  cores and `just bench shred 4-11 '^shred_cores/8$'` on the efficient ones. The bench's runtime
  has one worker, which only waits, and the compute pool the N − 1 threads it leaves, one at
  N = 1; the bench prints each pool's threads.
- **Allocations.** The `allocations` bench, whose global allocator is `stats_alloc`'s counting
  system allocator, runs each single-core group's workload once and prints its allocations,
  reallocations and bytes requested, a row and a batch of what it shredded. The timed bench keeps
  the system allocator. Throughput is the corpus's JSON bytes.
- **Profiles.** `just profiling shred` builds the bench with line tables and frame pointers for
  `perf record --call-graph fp`.

```python
"""Nested ~170-byte JSON lines rows, deterministic: gen.py ROWS OUT."""
import json, random, sys
random.seed(7)
n, out = int(sys.argv[1]), sys.argv[2]
cities = ["Warsaw", "Krakow", "Gdansk", "Wroclaw", "Poznan"]
with open(out, "w") as f:
    for i in range(n):
        row = {"id": i, "name": f"user-{i:07d}", "score": round(random.random() * 1000, 2),
               "active": i % 3 == 0, "created_at": f"2026-09-{1 + i % 28:02d}T12:{i % 60:02d}:00Z",
               "profile": {"city": random.choice(cities), "zip": f"{random.randint(10000, 99999)}",
                           "geo": {"lat": round(random.uniform(49, 55), 5), "lon": round(random.uniform(14, 24), 5)}}}
        f.write(json.dumps(row, separators=(",", ":")) + "\n")
```

The old engine's harness is a binary crate with `rdlt-engine = { path = "<rdlt.old>/crates/rdlt-engine" }`
and `[profile.release] lto = "fat"`, `codegen-units = 1`:

```rust
use std::time::Instant;

fn main() {
    let path = std::env::args().nth(1).expect("corpus path");
    let bytes = std::fs::read(&path).expect("corpus");
    let mut pushes = Vec::new();
    let mut start = 0;
    while start < bytes.len() {
        let mut end = (start + 8 * 1024 * 1024).min(bytes.len());
        while end < bytes.len() && bytes[end - 1] != b'\n' {
            end += 1;
        }
        pushes.push(&bytes[start..end]);
        start = end;
    }
    let mut best = f64::MAX;
    for _ in 0..5 {
        let began = Instant::now();
        for push in &pushes {
            rdlt_engine::fuzzing::bench_shred_bytes(push);
        }
        best = best.min(began.elapsed().as_secs_f64());
    }
    let mb = bytes.len() as f64 / 1e6;
    println!("old engine: {:.1} MB/s", mb / best);
}
```

## Results

Intel Core Ultra X7 358H (4 performance, 8 efficient and 4 low-power cores), on mains power,
2026-09-24; the old engine was measured right after this engine. On battery in power-saving
mode every figure falls to about a third, so compare only runs taken back to back.

| Measure | Old engine | This engine | Ratio | Gate |
|---|---|---|---|---|
| Nested corpus, one core | 120.3 MB/s (137.5 MB/s at best on other runs) | 703.6 MB/s | 5.85× (5.1× against 137.5) | ≥ 4× |

| Corpus (one core) | Throughput |
|---|---|
| `nested` | 692 MiB/s |
| `sparse` (`nested`, one row in ten thousand with an optional key after its name) | 697 MiB/s, 3 % below `nested` measured with it (718 MiB/s); rebuilding every chunk whose shape differs, as the first draft did, ran it at 407 MiB/s |
| `flat_narrow` | 490 MiB/s |
| `flat_wide` (200 columns) | 506 MiB/s |
| `string_heavy` | 557 MiB/s |

| Cores | Layout | One core | N cores | Scaling | Spec's bound |
|---|---|---|---|---|---|
| 8 efficient cores (`taskset -c 4-11`) | 1 worker, 7 compute threads | 446.8 MiB/s | 2094 MiB/s | 4.69× | ≥ 5.6× |
| 4 performance cores (`taskset -c 0-3`) | 1 worker, 3 compute threads | 598.4 MiB/s | 1507 MiB/s | 2.52× | ≥ 2.8× |

Measured 2026-10-06, `main` at `b46a4574` with the change that last edited this record, load
average 0.80–0.99, governor `powersave`; the median of five runs.

Neither count meets the spec's bound. Before any chunk parses, one job walks every record of every
push to cut the chunks, and the pool's other threads wait for it; each chunk then finds its records
again. Cutting the chunks in parallel with the parse is the planned change; the bound stays per
core.

## Build profile

The figures here assume the release profile. Cargo's defaults, which an embedder gets without
setting its own profile, shred more slowly, one core (`taskset -c 0`), the median of five
interleaved runs, 2026-10-06, load average 0.78–0.99:

| Build | `nested` | `flat_wide` | `string_heavy` | `normalize/keyless` | Instructions per `nested` pass |
|---|---|---|---|---|---|
| Release profile (`lto = "fat"`, `codegen-units = 1`) | 578 MiB/s | 482 MiB/s | 590 MiB/s | 177 MiB/s | 1.305 × 10⁹ |
| Cargo's release defaults | 426 MiB/s | 376 MiB/s | 484 MiB/s | 149 MiB/s | 1.583 × 10⁹ |

Instructions are callgrind's inclusive count of `rdlt_engine::bench::shred` over one pass in
criterion's test mode (`--test '^shred/nested$'`): the shredder's own work, without the pool's and
criterion's threads, which a whole-process count includes and which vary by a few percent from
run to run.

## Metering (ADR 0040)

Since ADR 0040 the shredder charges every builder what it is made with and grows by, and each
column its fixed parts a chunk, and a chunk that would pass twice its text is read again
observing before its batch is built. Dense data stays within its allowance and parses once; the
charges cost a few percent of throughput. Measured on the same machine against `main`
(`8ed5317d`), one core (`taskset -c 0`), the two benches' runs interleaved five times, criterion's
mean of each, the median of five; 2026-10-03, load average 3 to 9:

| Corpus | `main` | ADR 0040 | Ratio |
|---|---|---|---|
| `nested` | 506 MiB/s | 466 MiB/s | 0.92 |
| `sparse` | 499 MiB/s | 464 MiB/s | 0.93 |
| `flat_narrow` | 391 MiB/s | 378 MiB/s | 0.97 |
| `flat_wide` | 453 MiB/s | 432 MiB/s | 0.95 |
| `string_heavy` | 536 MiB/s | 522 MiB/s | 0.97 |
| normalize `shred_only` | 412 MiB/s | 377 MiB/s | 0.92 |
| normalize `keyed` | 328 MiB/s | 304 MiB/s | 0.93 |
| normalize `keyless` | 208 MiB/s | 197 MiB/s | 0.95 |

The figures are lower than those above, measured another day, on mains power: only the ratios
compare. `nested` stays above five times the old engine (5.85 × 0.92), past the gate of four. The
cost is spread through the parse: a builder's append returns whether its meter had room, text
counts what it writes, a row's end may charge its record's growth, and each chunk keeps its
shape for the reckoning. Two choices keep it this small: a growing builder is charged what it
grows by, not what it holds while it copies (charging the copy made `string_heavy` chunks trip
and parse three times, at 40 % of `main`), and identity writes a float's canonical text in place
rather than allocating it (allocating cost `keyless` a quarter).

## The `arrow-json` fast path

The spec lets flat JSON of a known schema go through `arrow-json`'s decoder where that is faster
(§7.4). `just bench shred 0 '^fast_path/'` measures both on the flat corpora, the decoder given
the schema:

| Corpus | Shredder | `arrow-json` |
|---|---|---|
| `flat_narrow` | 513 MiB/s | 429 MiB/s |
| `flat_wide` (200 columns) | 527 MiB/s | 279 MiB/s |

The shredder is faster on both, and needs no schema, so the engine has no fast path (ADR 0008).

## Normalizing

A normalized stream's batches are split into child tables after shredding, and each row gets its
lineage (spec §8.7). `just bench shred 0 '^normalize/'` measures it on one core, on the
`with_arrays` corpus: `nested`'s rows with up to three orders of a few tags each, so rows reach
two child tables. `shred_only` shreds it; `keyed` shreds and normalizes rows identified by their
`id`; `keyless` shreds and normalizes rows identified by their whole content, whose canonical
encoding is the extra cost.

| Group | Time | Throughput | Against `shred_only` |
|---|---|---|---|
| `shred_only` | 55.8–56.0 ms | 571–573 MiB/s | 1 |
| `keyed` | 72.3–75.1 ms | 426–443 MiB/s | 1.29–1.35× |
| `keyless` | 112.0–113.2 ms | 283–286 MiB/s | 2.0× |

Intel Core Ultra X7 358H, 4 performance cores (`taskset -c 0-3`), on mains power, 2026-09-25, two
runs back to back. A first draft of the encoding, which built each row's encoding a column at a
time with a buffer per field, ran `keyless` at 5.8× on battery; it now encodes a row at a time
into one buffer, with lengths in LEB128. Formatting floats as their shortest round-trip text, which
the spec asks of every number, is most of what remains.

## Allocations

One run of each single-core group, from `just bench shred 0 '^(shred|normalize)/'` on 2026-10-07
at commit `d482d4ecb150`; the counts are the same on every run and do not depend on the
machine's load.

| Group | Allocations a row | Reallocations a row | Bytes a row | Allocations a batch | Bytes a batch |
|---|---|---|---|---|---|
| `shred/nested` | 0.024 | 0.001 | 115 | 140.594 | 664719 |
| `shred/sparse` | 0.026 | 0.001 | 121 | 148.500 | 702084 |
| `shred/flat_narrow` | 0.001 | 0.000 | 16 | 27.781 | 334835 |
| `shred/flat_wide` | 3.538 | 0.077 | 2382 | 1386.969 | 933766 |
| `shred/string_heavy` | 1.007 | 4.901 | 616 | 4772.531 | 2918951 |
| `normalize/shred_only` | 2.562 | 0.003 | 298 | 10775.844 | 1254479 |
| `normalize/keyed` | 2.615 | 0.036 | 1012 | 11000.844 | 4256876 |
| `normalize/keyless` | 5.623 | 0.036 | 1109 | 23650.781 | 4664724 |
