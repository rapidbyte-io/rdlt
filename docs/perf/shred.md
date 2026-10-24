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
  - The cliff corpora of the `shred_cliffs` group, 32 MiB cut into 8 MiB pushes, one core:
    `array_form` (`nested` rows as the elements of one JSON array a push); `drift_json` (a column
    of integers that is an object holding a float in one row of four thousand); `drift_scalar`
    (the same column, a string in one row of four thousand); `int_arrays` (an id and sixty-four
    one-digit integers a row); `big_numbers` (integers beyond 64 bits, beyond a decimal's 38
    digits and floats beyond 2⁶³); `pretty` (`nested` rows printed over several lines each, as
    the elements of one JSON array a push). The bench prints the passes each makes: chunks
    parsed, tripped (past twice their text), observed, rebuilt and read exactly.
- **Running it.** `just bench shred <cores> <filter>`, which pins the cores with `taskset` and
  records the commit, the load average, each core's governor and energy preference, the
  allocations, and `perf stat`'s instructions a cycle of each benchmark, ten seconds of it.
  Criterion takes 10 samples a benchmark; each figure is its point estimate.
  One-core groups run on core 0, on the calling thread.
- **Cores.** `shred_cores/N` shreds `nested` with a pool for N cores, each 8 MiB push a call,
  shredded alone as a partition shreds one. The bench runs every N its cores allow in one process,
  pinned to the cores `just bench` was given, of one type, since this machine mixes them.
  The bench builds each compute pool through `SystemEnv::try_new` beside a runtime of one worker,
  which only waits: the pool has N − 1 threads, one at N = 1. The bound compares N cores, the
  waiting worker included, with one.
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

The One core, Columns of JSON and Normalizing tables come from the change on `3c0260b2`,
2026-10-12, on core 0: four rounds of `shred/nested`, `shred/wide_200`, `shred/string_heavy` and
`normalize/keyless`, and two of every other benchmark, the first two of the four. The rounds started
at one-minute load averages of 2.81, 2.52, 2.92 and 6.21. Each figure is the median of every run of
its benchmark, with the range of the runs in brackets.

The `normalize/*` rows of the Allocations table come from commit `a40d359e`, 2026-10-11: five
rounds interleaved with the merge-base in alternating orders, each started at a one-minute load
average of 0.38 to 0.97, the median with its range in brackets.

### Against the old engine

| Corpus, one core | Old engine | This engine | Ratio | Spec's bound |
|---|---|---|---|---|
| 200 000 `nested` rows | 128 MB/s | 618 MB/s | 4.83× [4.75–4.91] | ≥ 4×, met |

### One core

This change on `3c0260b2`, 2026-10-12, rounds as above.

| Corpus | Throughput | Instructions a cycle |
|---|---|---|
| `nested` | 591 MiB/s [589–606] | 6.05 |
| `sparse` | 575 MiB/s [570–580] | 6.04 |
| `flat_narrow` | 454 MiB/s [452–457] | 5.24 |
| `wide_200` | 461 MiB/s [436–471] | 5.41 |
| `string_heavy` | 530 MiB/s [508–543] | 4.86 |

### More cores

The record scan of this change alone on `3c0260b2`, 2026-10-12, cores `0-3` (performance), three
rounds interleaved with the merge-base in alternating orders, started at one-minute load averages of
2.65, 3.96 and 1.79 (the merge-base's 2.95, 2.78 and 1.22); the median, with the range in brackets.
Each 8 MiB push is a call. The process was pinned to cores `0-3` for all three rows, so the pools of
N = 1 and 2 had cores to spare. Efficient cores were not run in this record.

| Cores | N | tokio workers + compute threads | Throughput | Against N = 1 | Spec's bound |
|---|---|---|---|---|---|
| performance (`0-3`) | 1 | 1 + 1 | 597 MiB/s [534–600] | 1 | |
| performance (`0-3`) | 2 | 1 + 1 | 583 MiB/s [520–595] | 0.98× | ≥ 1.4×, not met |
| performance (`0-3`) | 4 | 1 + 3 | 1402 MiB/s [1337–1406] | 2.35× [2.35–2.50] | ≥ 2.8×, not met |

The bound of 0.7·N at N = 4 is 2.8×, which this change does not reach.

### Cliffs

Where the shredder parses a chunk again, observes it, or reads its numbers exactly, on one core
(`0`), `shred_cliffs` with this change on `0ab7edde`, 2026-10-13: two rounds interleaved with
`0ab7edde` in alternating orders, started at one-minute load averages of 1.28 and 0.43 (`0ab7edde`'s
1.04 and 0.92), with `int_arrays` in two more pairs. Each figure is the median of the rounds, the
range in brackets; instructions a cycle come from one `just bench shred 0 shred_cliffs` after them.
The pass counts are the same in every round, over 32 MiB in 1 MiB chunks.

| Corpus | Throughput | `0ab7edde` | Instructions a cycle | Parsed | Tripped | Observed | Rebuilt | Exact |
|---|---|---|---|---|---|---|---|---|
| `array_form` | 397 MiB/s [391–404] | 390 MiB/s | 5.87 | 32 | 0 | 0 | 0 | 0 |
| `drift_json` | 214 MiB/s [211–217] | 178 MiB/s | 5.70 | 32 | 0 | 0 | 32 | 32 |
| `drift_scalar` | 590 MiB/s [586–593] | 300 MiB/s | 6.02 | 32 | 0 | 0 | 0 | 0 |
| `int_arrays` | 70 MiB/s [67–73] | 71 MiB/s | 5.94 | 32 | 32 | 32 | 32 | 0 |
| `big_numbers` | 240 MiB/s [240–241] | 200 MiB/s | 5.05 | 64 | 0 | 0 | 0 | 32 |
| `pretty` | 392 MiB/s [392–392] | 388 MiB/s | 5.48 | 32 | 0 | 0 | 0 | 0 |

`shred/nested` ran in the same rounds at 639 MiB/s [622–656], `0ab7edde` 620 MiB/s. `int_arrays`
moved within its rounds' spread (67–73 MiB/s on both sides), and its instructions under `perf
stat` differ by 0.02 %.

What each row of this change does, and the rule it was held to:

- **The exact parse walks its containers in place.** It walked each object and array through a
  vector of its entries and copied each number's text into a `String`; it now walks sonic-rs's
  iterators and borrows the text. Callgrind (`just instructions`): `shred/drift_json` 8.8 % fewer
  instructions and 57 % fewer allocations, `shred/big_numbers` 10.2 % and 54 %, no case more.
  Wall time in three rounds of this row alone: `drift_json` 1.19× (178 to 212 MiB/s),
  `big_numbers` 1.16× (199 to 231 MiB/s); the whole change gives the table's 214 and 240. The
  roadmap's 1.5× on `drift_json` is not met.
- **The large tier is not built.** It was to be prototyped if `drift_json` stayed below half of
  `shred/nested`. This row alone left it at 0.34 of it (212 against 620 MiB/s), and the tier is
  closed under the complexity rule.
- **A column of JSON renders into one buffer.** A value took a `String`, each string and key one
  more and each object a set of keys; the parse now lends one buffer, escapes into it, and keeps
  an object's keys as ranges of it, sorted at the object's end to find a repeat. `shred/drift_json`
  allocates 33 % less, `shred/drift_scalar` 98 % less, and no case gains 2 % of instructions.
- **Scalars render into a column of JSON as they were read.** A column of scalars meeting another
  kind stopped building, and its chunk was parsed again; a column of scalars but floats now
  renders what it took and goes on as JSON, and a chunk whose scalars the join made JSON renders
  them without its text, reckoned at most the chunk's own bytes. `drift_scalar` rebuilds no chunk
  (32 before), with 48 % fewer instructions, and runs at 1.97× (300 to 590 MiB/s).
- **Rebuilt text is not presized from each chunk's bytes.** The reservation was its only gain, and
  only chunks drifting to floats, objects or arrays are rebuilt now. A count of each column's text
  would thread through every observation, and a rebuilt column of JSON, whose builder doubles as
  it grows, has no bound of once its source; the safe part, exact counts for strings, saves about
  a quarter of `drift_json`'s chunk of reservation. Not built.
- **Re-observation after a wait is kept.** Nested orders at 4 cores under a 64 MiB budget, 200 000
  rows in pushes of 20 000 (a push of 50 000 is past that budget's push limit), waited 6 times for
  38 to 59 µs together and observed no chunk: re-observation is 0 % of shredding, below the 5 % that
  would build it.
- **The visitor trees share their rules.** One record seed refuses a record that is not an
  object for the building and the observing parse, and the depth check and an integer's digits
  are one function each; no case moves by more than 0.35 % of instructions.

### Rows pushed as lines

`Emitter::rows` pushes a row a line, which the engine cuts at its line ends as it cuts any JSON
lines, where it pushed one JSON array, cut by a walk of its structure. This change against its
merge base `017f65a4`, 2026-10-24, each row held to a rule written before its run:

- **`shred/rows` takes 38.3 % fewer instructions.** Under `just instructions` it takes 38,657,657
  instructions an iteration against the merge base's 62,699,079, as many as `shred/nested`
  (38,684,356), since the push is now the nested corpus's own lines byte for byte; it allocates
  190 times, as before. The rule was at least 25 % fewer, with no other case more than 2 % above
  its base; the largest rise was `passthrough/null_sink`'s 0.30 %, a case whose count moves
  between runs of one tree.
- **A string's end is not found with `memchr2`.** Jumping to a string's next quote or backslash
  with `memchr::memchr2`, as the engine's JSON reader does, instead of the structure scan's byte
  loop, was held to `shred_cliffs/array_form` at least 1.10× its base, with `pretty` not slower
  than its base's range. Two rounds of `just bench shred 0 shred_cliffs` of this change with
  `memchr2` added, interleaved with the merge base in alternating orders, started at one-minute
  load averages of 1.66 and 1.24 (the merge base's 4.52 and 1.86), give each round's figure, the
  range in brackets, below. Its instructions on `shred/array_form` fell 0.66 %, while the
  instructions a cycle fell more: the call costs more cycles on the corpora's short strings than
  the bytes it skips save. Both halves of the rule fail, and the byte loop stays.

| Corpus | Merge base | With `memchr2` | Against the base | Instructions a cycle, base and with |
|---|---|---|---|---|
| `array_form` | 301 MiB/s [275–320], 372 MiB/s [371–373] | 301 MiB/s [291–314], 316 MiB/s [304–325] | 1.00×, 0.85× | 5.86, 5.80; 4.90, 5.02 |
| `pretty` | 359 MiB/s [357–361], 365 MiB/s [364–365] | 302 MiB/s [289–317], 335 MiB/s [333–336] | 0.84×, 0.92× | 5.42, 5.39; 5.06, 5.06 |

### Columns of JSON

`json_check` checks a column of 100 000 JSON values as an Arrow push's is checked, on one core,
at the same commit and in the same rounds as Cliffs. `plain` holds values without escapes and `escaped`
the string-heavy corpus's. The `json/check` case of the `allocations` bench, 8,192 escaped values,
allocates twice a call.

| Column | Throughput |
|---|---|
| `json_check/plain` | 635 MiB/s [631–639] |
| `json_check/escaped` | 957 MiB/s [942–971] |

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
This change on `3c0260b2`, 2026-10-12, rounds as above.

| Group | Time | Throughput | Against `shred_only` |
|---|---|---|---|
| `shred_only` | 67.6 ms | 473 MiB/s [471–476] | 1 |
| `keyed` | 115.1 ms | 278 MiB/s [278–278] | 1.70× |
| `keyless` | 174.5 ms | 184 MiB/s [179–190] | 2.58× |

### Allocations

One run of each one-core group; the counts were the same in every round. This change on
`0ab7edde` leaves every row the same. Callgrind's counts over one pass of the cliff corpora fall:
`shred/drift_json` from 74 940 allocations to 21 563, `shred/drift_scalar` from 11 225 to 209 and
`shred/big_numbers` from 81 404 to 37 046.

| Group | Allocations a row | Reallocations a row | Bytes a row | Allocations a batch | Bytes a batch |
|---|---|---|---|---|---|
| `shred/nested` | 0.024 | 0.001 | 115 | 140.656 | 665521 |
| `shred/sparse` | 0.026 | 0.001 | 121 | 148.500 | 702870 |
| `shred/flat_narrow` | 0.001 | 0.000 | 16 | 27.781 | 335176 |
| `shred/wide_200` | 3.538 | 0.077 | 2390 | 1386.969 | 936829 |
| `shred/string_heavy` | 1.007 | 4.901 | 616 | 4772.531 | 2919291 |
| `normalize/shred_only` | 2.562 | 0.003 | 298 | 10775.844 | 1255532 |
| `normalize/keyed` | 2.604 | 0.006 | 654 | 10951.844 | 2749029 |
| `normalize/keyless` | 2.611 | 0.006 | 655 | 10982.844 | 2753071 |

## Build profile

The figures here assume the release profile. Cargo's defaults (thin local LTO, sixteen codegen
units), which an embedder gets without a profile of its own, built in a target directory of their
own and measured in the same rounds on core 0, shred `nested` 23.5 % slower:

| Build | `nested` | `wide_200` | `string_heavy` | `normalize/keyless` | Instructions a `nested` pass |
|---|---|---|---|---|---|
| Release profile (`lto = "fat"`, `codegen-units = 1`) | 603 MiB/s | 486 MiB/s | 584 MiB/s | 177 MiB/s | 1 302 867 923 |
| Cargo's release defaults | 461 MiB/s | 397 MiB/s | 506 MiB/s | 156 MiB/s | 1 555 860 022 |

### Stack check

The shredder checks the remaining stack at every level of nesting. `stacker::maybe_grow` and
`visit::nest` are inlined, so the check's share is that of `stacker::remaining_stack` plus
`rust_psm_stack_pointer`: 0.46 % of the instructions of `shred/nested`, 0.52 % of
`shred/with_arrays` and 0.22 % of `shred/array_form`. `shred_cliffs/pretty` has no instruction case.

### Build target

The build target is the default, x86-64. A build for x86-64-v3 (`-C target-cpu=x86-64-v3`) of this
change on `3c0260b2`, run against the default build on core 0 in the first two rounds above, is
faster on 15 of the 16 benchmarks, by 3 % (`shred/nested`) to 13 % (`shred/string_heavy`), and
slower on `shred_cliffs/int_arrays` in both rounds: 534 ms against 507 ms, 5.3 % slower. It would
also exclude CPUs older than Haswell, so the default stays.
