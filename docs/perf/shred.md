# JSON shredding throughput

JSON shredding is held to four times the old engine on one core, on nested rows of about 180
bytes, and, for one stream within N ≤ 8 cores, to 0.7·N times one core (the spec's bounds). Both
are ratios measured on one machine, so this record keeps the method with the figures.

## Method

- **Build.** The release profile: fat LTO and one codegen unit, which benches inherit. Cargo
  applies only the root package's profiles, so an embedder sets the same in its own
  `[profile.release]`; [Build profile](#build-profile) states what Cargo's defaults give.
- **Corpora.** The bench generates each corpus from a fixed seed, 32 MiB cut into 8 MiB pushes
  at line ends, and shreds it in 1 MiB chunks; throughput counts the corpus's JSON bytes:
  - `nested`: rows of about 180 bytes holding an object two levels deep;
  - `sparse`: `nested`, with one row in ten thousand carrying an optional key after its name;
  - `flat_narrow`: three narrow columns;
  - `wide_200`: 200 columns, integers and short strings by turns;
  - `string_heavy`: long strings with escapes;
  - `with_arrays`: `nested` with up to three orders of a few tags each, for normalizing.
- **Running it.** `just bench shred <cores> <filter>`, which pins the cores with `taskset` and
  records the commit, the load average, each core's governor and energy preference, the
  allocations, and `perf stat`'s instructions a cycle of each benchmark, ten seconds of it.
  Criterion takes 10 samples a benchmark; each figure is its point estimate.
  One-core groups run on core 0, on the calling thread.
- **Cores.** `shred_cores/N` shreds `nested` within N cores, pinned to N cores of one type, since
  this machine mixes them. The bench builds each compute pool through `SystemEnv::try_new` beside
  a runtime of one worker, which only waits: the pool has N − 1 threads, one at N = 1. The bound
  compares N cores, the waiting worker included, with one.
- **Allocations.** The `allocations` bench, whose global allocator is `stats_alloc`'s counting
  system allocator, runs each one-core workload once and prints its allocations, reallocations
  and bytes requested, a row and a batch of what it shredded. The timed bench keeps the system
  allocator.
- **Instructions.** Callgrind's inclusive count of `rdlt_engine::bench::shred` over one `nested`
  pass in criterion's test mode (`--test '^shred/nested$'`): the shredder's own work, without
  the corpus generation and criterion's threads.
- **Profiles.** `just profiling shred` builds the bench with line tables and frame pointers;
  [profiling.md](profiling.md) holds the recipes.
- **Old engine.** `rdlt.old` at `0249668f`, through its production shred path
  `rdlt_engine::fuzzing::bench_shred_bytes`, over 8 MiB pushes cut at line ends, built with fat
  LTO and one codegen unit by its own toolchain, 1.96.0. Its corpus is 200 000 `nested` rows
  (36.2 MB) written by the script below, which this engine shreds as the `file` corpus
  (`RDLT_SHRED_CORPUS`); the two run alternately on core 0. The old engine's figure is the best of
  five passes in MB/s; this engine's is criterion's point estimate over its samples, converted
  from MiB/s.

```python
"""Nested JSON lines rows of about 180 bytes, deterministic: gen.py ROWS OUT."""
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

The old engine's harness is a binary crate with `rdlt-engine = { path = "<rdlt.old>/crates/rdlt-engine" }`,
`rdlt.old`'s `Cargo.lock`, a `rust-toolchain.toml` naming 1.96.0 and `[profile.release] lto = "fat"`,
`codegen-units = 1`:

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

Commit `864cb1f2`, 2026-10-10. Intel Core Ultra X7 358H (4 performance, 8 efficient and 4 low-power
cores) on mains power, platform profile `performance`, governor `powersave`, energy preference
`performance`, turbo on. Two rounds, interleaved with the passthrough bench in opposite orders; each
invocation started at a one-minute load average of at most 1.0 with its cores idle, the load average
0.21 to 1.00 at their starts. Each figure is the median of the two rounds, their range in brackets,
and each round of every engine figure lies within 3 % of the median. A third round ran later under
other load on the machine and is left out (the owner's ruling of 2026-10-10).

### Against the old engine

| Corpus, one core | Old engine | This engine | Ratio | Spec's bound |
|---|---|---|---|---|
| 200 000 `nested` rows | 128 MB/s | 618 MB/s | 4.83× [4.75–4.91] | ≥ 4×, met |

### One core

| Corpus | Throughput | Instructions a cycle |
|---|---|---|
| `nested` | 603 MiB/s [601–604] | 5.93 |
| `sparse` | 593 MiB/s [592–594] | 5.95 |
| `flat_narrow` | 441 MiB/s [441–442] | 5.20 |
| `wide_200` | 486 MiB/s [484–487] | 5.56 |
| `string_heavy` | 584 MiB/s [582–585] | 5.20 |

### More cores

| Cores | N | tokio workers + compute threads | Throughput | Against N = 1 | Spec's bound |
|---|---|---|---|---|---|
| performance (`0`) | 1 | 1 + 1 | 612 MiB/s | 1 | |
| performance (`0-3`) | 4 | 1 + 3 | 1528 MiB/s | 2.50× [2.48–2.51] | ≥ 2.8×, not met |
| efficient (`4`) | 1 | 1 + 1 | 453 MiB/s | 1 | |
| efficient (`4-7`) | 4 | 1 + 3 | 1185 MiB/s | 2.62× [2.61–2.62] | ≥ 2.8×, not met |
| efficient (`4-11`) | 8 | 1 + 7 | 2122 MiB/s | 4.69× [4.68–4.69] | ≥ 5.6×, not met |

### Metering

The shredder charges every builder what it is made with and grows by, and each column its fixed
parts a chunk; a chunk that would pass twice its text is read again, observing, before its batch is
built (ADR 0040). In a frame-pointer profile of `nested` on core 0 (`just profiling shred`, 10 526
samples of user cycles), 1.1 % of the samples hold a frame of `shred::meter` in their stack. Dense
data stays within its allowance and parses once.

### The `arrow-json` fast path

The `fast_path` group measures `arrow-json`'s decoder, given the schema, against the shredder on
the flat corpora, on one core. The `arrow-json` column times arrow-json's own decoder as a
comparison: it is not an acceptance figure (the owner's ruling of 2026-10-10), and gives the
measured median with its range across the rounds.

| Corpus | Shredder | `arrow-json`, a comparison |
|---|---|---|
| `flat_narrow` | 449 MiB/s [447–450] | 430 MiB/s [427–434] |
| `wide_200` | 486 MiB/s [484–488] | 260 MiB/s [251–269] |

The shredder is faster on both, and needs no schema, so the engine has no fast path (ADR 0008).

### Normalizing

A normalized stream's batches are split into child tables after shredding, and each row gets
its lineage. The `normalize` group measures it on one core, on `with_arrays`: `shred_only`
shreds it; `keyed` shreds and normalizes rows identified by their `id`; `keyless` shreds and
normalizes rows identified by their whole content, whose canonical encoding is the extra cost.

| Group | Time | Throughput | Against `shred_only` |
|---|---|---|---|
| `shred_only` | 67.4 ms | 475 MiB/s [471–479] | 1 |
| `keyed` | 117.3 ms | 273 MiB/s [271–275] | 1.74× |
| `keyless` | 181.2 ms | 177 MiB/s [176–178] | 2.69× |

### Allocations

One run of each one-core group; the counts were the same in both rounds.

| Group | Allocations a row | Reallocations a row | Bytes a row | Allocations a batch | Bytes a batch |
|---|---|---|---|---|---|
| `shred/nested` | 0.024 | 0.001 | 115 | 140.656 | 665521 |
| `shred/sparse` | 0.026 | 0.001 | 121 | 148.500 | 702870 |
| `shred/flat_narrow` | 0.001 | 0.000 | 16 | 27.781 | 335176 |
| `shred/wide_200` | 3.538 | 0.077 | 2390 | 1386.969 | 936829 |
| `shred/string_heavy` | 1.007 | 4.901 | 616 | 4772.531 | 2919291 |
| `normalize/shred_only` | 2.562 | 0.003 | 298 | 10775.844 | 1255532 |
| `normalize/keyed` | 2.615 | 0.036 | 1012 | 11000.844 | 4257929 |
| `normalize/keyless` | 5.623 | 0.036 | 1109 | 23650.781 | 4665777 |

## Build profile

The figures here assume the release profile. Cargo's defaults (thin local LTO, sixteen codegen
units), which an embedder gets without a profile of its own, built in a target directory of their
own and measured in the same rounds on core 0, shred `nested` 23.5 % slower:

| Build | `nested` | `wide_200` | `string_heavy` | `normalize/keyless` | Instructions a `nested` pass |
|---|---|---|---|---|---|
| Release profile (`lto = "fat"`, `codegen-units = 1`) | 603 MiB/s | 486 MiB/s | 584 MiB/s | 177 MiB/s | 1 302 867 923 |
| Cargo's release defaults | 461 MiB/s | 397 MiB/s | 506 MiB/s | 156 MiB/s | 1 555 860 022 |
