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
  which only waits: the pool has a thread a core, N threads. The bound compares N cores with one.
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

The Columns of JSON and Normalizing tables come from the change on `3c0260b2`,
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

`main` at `6365cc13` with the served bench's readings (`d2fc0f95`; the engine's code is
`main`'s), 2026-10-25 and -26, core `0`: three rounds, interleaved with the other benches' rounds
([campaign.md](campaign.md)), each started once no other build ran; the median, with the range in
brackets. Instructions a cycle are not recorded on this tree.

| Corpus | Throughput |
|---|---|
| `nested` | 596 MiB/s [589–605] |
| `sparse` | 587 MiB/s [572–587] |
| `flat_narrow` | 469 MiB/s [461–469] |
| `wide_200` | 454 MiB/s [448–460] |
| `string_heavy` | 548 MiB/s [547–558] |

### More cores

`shred_cores` in the same three rounds and tree: on the performance cores (`0-3`) and on the
efficient ones (`4-11`), a compute thread a core, so the pool for N has N threads beside the one
runtime worker, which only waits. The median, with the range in brackets.

| Cores | N | tokio workers + compute threads | Throughput | Against N = 1 | Spec's bound |
|---|---|---|---|---|---|
| performance (`0-3`) | 1 | 1 + 1 | 593 MiB/s [565–597] | 1 | |
| performance (`0-3`) | 2 | 1 + 2 | 1 068 MiB/s [1 058–1 069] | 1.80× | ≥ 1.4×, met |
| performance (`0-3`) | 4 | 1 + 4 | 1 699 MiB/s [1 495–1 733] | 2.87× | ≥ 2.8×, met |
| efficient (`4-11`) | 1 | 1 + 1 | 461 MiB/s [456–463] | 1 | |
| efficient (`4-11`) | 2 | 1 + 2 | 898 MiB/s [891–903] | 1.95× | ≥ 1.4×, met |
| efficient (`4-11`) | 4 | 1 + 4 | 1 605 MiB/s [1 605–1 609] | 3.48× | ≥ 2.8×, met |
| efficient (`4-11`) | 8 | 1 + 8 | 2 120 MiB/s [2 066–2 145] | 4.60× | ≥ 5.6×, not met |

The bound of 0.7·N holds up to four cores of either kind; at eight efficient cores it is 5.6×,
which the pool does not reach.

### Jobs sized by rows, cores and a limit

A flush's fixed 1 MiB chunks are both its unit of parallelism and the batches it makes. A rule
sizing each job from the flush was built and held to rules written before its sweep ran; no
candidate held them, so nothing landed and the fixed 1 MiB job stays.

**The rule.** `chunk = min(limit, max(ROWS × r, ⌈text ÷ 2T⌉))`, computed in the cut's pool job:
`r` the bytes a record takes: the flush's first 64 KiB over the records that end in them (line
ends, or an array's top-level commas), rounded up, and the limit where none ends there; `text` the
bytes of the flush's pushes together; `T` the engine's cores and `limit`
`BatchPolicy::chunk_bytes`. The fallback drops the cores term:
`chunk = min(limit, max(ROWS × r, 1 MiB))`.

**The rules of the sweep.** Six candidates, `ROWS` in {1 024, 4 096} by limit in {2, 4, 8} MiB,
the smallest limit first and then the smallest `ROWS`; the first to hold every rule lands:

- (a) `wide/json/1000` and `wide/json/5000` ([wide.md](wide.md)) at least 1.5 times the base's
  rows a second, with at most half its allocations a row.
- (b) `shred_cores/8` on the efficient cores (`4-11`) at least 1.3 times the base.
- (c) The JSON cells ([passthrough.md](passthrough.md), "JSON through the engine") on `4-11`: one
  `nested` partition at least 1.15 times the base; sixteen partitions within its range or above,
  on `0-3` too; one `wide_200` partition within 3 % on both.
- (d) `shred_cores` at N ≤ 4 on `0-3` within the base's range.
- (e) `just instructions` within 2 %, `just resident` within its bound, the heap-peak tests green
  and the paired passthrough cell within its 19 %.

If none holds them all, the fallback runs on (a), on (c) without its 1.15 and on (d) and (e); if
that fails too, nothing lands.

**Method.** `main` at `257004c8` against a build of each candidate's two constants, release
profile. Each round held the measurement lock once, started once no build ran and the one-minute
load was under 3.0 (2.45, 1.93 and 1.64 at the three starts, 2026-10-31 01:36, 02:27 and 04:22),
and ran each bench's criterion pass under `taskset` (`--bench --noplot` and the filter the rule
reads): `wide` on `0-3`, `shred_cores` on `0-3` and `4-11`, the JSON cells on both, the base and
the candidates interleaved bench by bench, the base first in the first and third rounds and last
in the second. A figure is criterion's point estimate, and a speedup the base's time over the
candidate's in the same round. Allocations a row come from the `allocations` bench at full size on
`0-3`, alike on every run. A candidate's rounds stopped once a rule it failed was settled. Rules
that read cuts the candidates share were read once a cut: wide JSON's job is the limit whatever
`ROWS` and with or without the cores term, since a thousand of its 14 or 74 KB records pass every
limit; `shred_cores/8` cuts its 8 MiB pushes alike at every limit, 16 jobs at `ROWS` 1 024 and 12
at 4 096; and `wide_200`'s records, about 2.7 KB, make 2.7 MB jobs at `ROWS` 1 024 under a 4 MiB
limit with or without the cores term.

**Wide JSON on `0-3`** (rule (a); the base's allocations a row are 225.2 and 6 528.6):

| Limit | Builds | `json/1000` | `json/5000` | Allocations a row |
|---|---|---|---|---|
| 2 MiB | (1 024, 2 MiB) | 163.1 → 114.5 ms, 1.43× | 696.0 → 490.1 ms, 1.42× | 120.6 and 3 648: 0.54 and 0.56 |
| 4 MiB | (4 096, 4 MiB); fallback (1 024, 4 MiB) | 1.63×; 1.58× | 2.81×; 2.74× | 68.3 and 1 809: 0.30 and 0.28 |
| 8 MiB | (4 096, 8 MiB); fallback (1 024, 8 MiB) | 1.17×; 1.11× | 2.93×; 2.99× | 42.2 and 1 115: 0.19 and 0.17 |

**`shred_cores/8` on `4-11`** (rule (b); the base cuts each 8 MiB push into eight jobs):

| `ROWS` | Jobs a push | Rounds |
|---|---|---|
| 1 024 | 16 | 15.64 → 13.24 ms, 1.18×; 15.38 → 13.58 ms, 1.13× |
| 4 096 | 12 | 15.38 → 13.48 ms, 1.14×; 15.79 → 13.71 ms, 1.15× |

**One partition of the JSON cells** (rule (c)):

| Build | `wide_200` jobs | `wide_200` on `4-11` | `wide_200` on `0-3` | `nested` on `4-11` |
|---|---|---|---|---|
| (1 024, 2 MiB) | 2 MiB | 1.00× | 1.09× | 1.08× |
| (1 024, 4 MiB) | 2.7 MB | 0.87× | 1.16× | 1.09× |
| Fallback (1 024, 4 MiB) | 2.7 MB | 0.90× | 1.14× | 0.98×, the base's cut |
| (4 096, 4 MiB) | 4 MiB | 0.68×; 0.69× | 0.96× | 1.08×; 1.02× |
| (4 096, 8 MiB) | 8 MiB | 0.38× | 0.56× | 1.03× |

Sixteen `wide_200` partitions ran 1.11–1.19 times the base on both kinds of core with jobs of up to
4 MiB, and 0.79 times on `4-11` at 8 MiB; sixteen `nested` partitions 0.95–1.01 times.
`shred_cores` at N ≤ 4 on `0-3` read 0.95–1.03 times in every build (rule (d)).

**Verdicts.** Every candidate failed one of (a) to (d), and every fallback build (a) or (c)
without its 1.15, so rule (e) was not read.

| Candidate | Fails |
|---|---|
| (1 024, 2 MiB) | (a): allocations 0.54 and 0.56, time 1.42–1.43×; (b): 1.18×, 1.13×; (c): `nested` 1.08× |
| (4 096, 2 MiB) | (a), the same cut; (b): 1.14×, 1.15× |
| (1 024, 4 MiB) | (b), the same cut as at 2 MiB; (c): `wide_200` 0.87× on `4-11`, `nested` 1.09× |
| (4 096, 4 MiB) | (b): 1.14×, 1.15×; (c): `wide_200` 0.68× and 0.69× on `4-11`, 0.96× on `0-3` |
| (1 024, 8 MiB), (4 096, 8 MiB) | (a): `json/1000` 1.17×, 1.11× |
| Fallback at 2 MiB | (a), the same cut |
| Fallback (1 024, 4 MiB) | (c): `wide_200` 0.90× on `4-11`, 0.87× on the same cut a round before |
| Fallback (4 096, 4 MiB) | (c): `wide_200` 0.68× and 0.69× on `4-11`, on the same cut |
| Fallback at 8 MiB | (a), the same cut |

**What the runs show.** Wide JSON wants larger jobs: at 4 MiB, `json/5000` loads 2.7–2.8 times
the rows a second with 0.28 of the allocations a row, each 8 MiB push making two batches where it
made eight. The same jobs leave one partition of 200-column records on eight cores two jobs a
flush where it had eight, and it loads 10–13 % slower; 2 MiB jobs keep it whole, but cut wide
JSON's allocations only to 0.54–0.56. Two jobs a core of a `nested` flush ran `shred_cores/8`
1.13–1.18 times, short of 1.3. Smaller jobs also cost what is done once a job: a merge compacts
each job's batch alone, so the merged bench's 2 000-row flush cut into two jobs at two cores staged
1 295 root rows of 1 192 keys, against one a key in one job; and each batch of a sparse flush holds
a null for every column the others hold, 21.3 MB beyond the text of 1 500 records of a key each
in two jobs against 20.3 MB in one.

### Cliffs

`shred_cliffs` on core `0` in the same three rounds and tree, the median with the range in
brackets. The pass counts are the same in every round, over 32 MiB in 1 MiB chunks.

| Corpus | Throughput | Parsed | Tripped | Observed | Rebuilt | Exact |
|---|---|---|---|---|---|---|
| `array_form` | 353 MiB/s [351–367] | 32 | 0 | 0 | 0 | 0 |
| `drift_json` | 194 MiB/s [188–206] | 32 | 0 | 0 | 32 | 32 |
| `drift_scalar` | 548 MiB/s [533–553] | 32 | 0 | 0 | 0 | 0 |
| `int_arrays` | 60 MiB/s [59–62] | 32 | 32 | 32 | 32 | 0 |
| `big_numbers` | 232 MiB/s [229–234] | 64 | 0 | 0 | 0 | 32 |
| `pretty` | 366 MiB/s [353–373] | 32 | 0 | 0 | 0 | 0 |

The exact parse's changes on `0ab7edde`, 2026-10-13, each held to its rule:

- **The exact parse walks its containers in place.** It walked each object and array through a
  vector of its entries and copied each number's text into a `String`; it now walks sonic-rs's
  iterators and borrows the text. Callgrind (`just instructions`): `shred/drift_json` 8.8 % fewer
  instructions and 57 % fewer allocations, `shred/big_numbers` 10.2 % and 54 %, no case more.
  Wall time in three rounds of this row alone: `drift_json` 1.19× (178 to 212 MiB/s),
  `big_numbers` 1.16× (199 to 231 MiB/s); the whole change gave 214 and 240 MiB/s on its tree. The
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

### Where one core's time goes

A per-function profile of five corpora on the same tree: `just profiling shred`,
`perf record -e cpu_core/cycles/u -F 999` over ten seconds of `--profile-time` on core `0`, about
11 000 samples each; each function's own samples, grouped. The fast parse is the shredder's visit
over sonic-rs's deserializer and sonic-rs's own parser; the exact parse is `json::Reader` and the
token walk over it.

| Corpus | Fast parse | Exact parse | Building columns | libc: copies, compares, allocation | UTF-8 checks and scans | Big numbers | Other |
|---|---|---|---|---|---|---|---|
| `nested` | 58.6 % | 0 | 9.5 % | 7.6 % | 9.3 % | 0 | 6.1 % |
| `wide_200` | 54.4 % | 0 | 11.2 % | 16.9 % | 3.6 % | 0 | 5.2 % |
| `string_heavy` | 46.6 % | 0 | 1.7 % | 23.4 % | 9.9 % | 0 | 10.4 % |
| `drift_json` | 20.9 % | 38.9 % | 6.0 % | 8.6 % | 6.9 % | 0 | 11.5 % |
| `big_numbers` | 1.7 % | 60.7 % | 1.9 % | 4.3 % | 3.3 % | 12.9 % | 6.5 % |

Functions under 0.5 % of a profile's samples are left out, so a row sums to under 100 %. In
`string_heavy`, sonic-rs's escaped-string parse takes 35.3 % alone; in `drift_json`, every chunk
is parsed fast first and again exactly; `big_numbers` parses exactly throughout, Arrow's
big-integer parse 12.9 % of it.

### One tokenizer

The shredder reads records with two parsers: sonic-rs's, fast, which reads an integer beyond 64
bits as the float nearest it and keeps no float's text, and the engine's own reader, exact, which
keeps every number as it is written. Scans of each record's text decide where a chunk is parsed
again exactly, and a float under a column of JSON has its chunk built again exactly. Two arms with
one parser each were built and held to a rule written before their rounds ran; neither held it,
so both parsers stay.

**The arms.**

- A: `main` at `05c69b59`, today's build.
- B, exact only: every chunk parsed through the engine's reader from its first parse.
- C, jiter: B with the engine's reader swapped for jiter 0.17.0, its newest release at least two
  weeks old, under the same serde walk, `known_number_bytes` handing each number's text to the
  exact number path; the walk holds the nesting limit and the exponent's 18 digits itself.

**The rule.** An arm qualifies if it runs at least 0.97 times A on `nested`, `sparse`,
`flat_narrow`, `wide_200` and `string_heavy`, and no `shred_cliffs` corpus runs slower. jiter is
preferred if it qualifies and meets four requirements: a nesting bound the caller holds, with no
recursion it does not control; typed refusals that name the record; a number's written bytes; and
no allocation a record outside the meter, its string scratch reused across a chunk's records. B
wins only if it qualifies and is at least 1.05 times C, or if jiter fails a requirement. If
neither qualifies, nothing changes.

**Method.** A against B and C, each built from its own commit with the release profile, B and C
being the two commits of the branch `refactor/one-tokenizer-closed`. Each round held the
measurement lock once, started once no build ran, and ran each arm's `shred` bench on core `0`
(`--bench --noplot`, filtered to the five corpora and `shred_cliffs`), A first in the first round
and last in the second, 2026-10-31 09:41 and 10:44; the one-minute load was 6.18 at the first
round's start, falling to 1.37 by its third arm, and 1.05 at the second's. A figure is criterion's
throughput point estimate, and a ratio an arm's figure over A's in the same round. The rounds
stopped at two, which settle the rule for both arms. Each arm's passes were alike in both rounds.

**Figures**, each round's in order, MiB/s:

| Corpus | A | B | C | B against A | C against A |
|---|---|---|---|---|---|
| `nested` | 559, 623 | 298, 298 | 389, 397 | 0.53, 0.48 | 0.70, 0.64 |
| `sparse` | 562, 615 | 297, 297 | 388, 380 | 0.53, 0.48 | 0.69, 0.62 |
| `flat_narrow` | 461, 497 | 258, 261 | 345, 326 | 0.56, 0.53 | 0.75, 0.66 |
| `wide_200` | 466, 489 | 242, 255 | 333, 315 | 0.52, 0.52 | 0.72, 0.64 |
| `string_heavy` | 579, 595 | 393, 421 | 465, 459 | 0.68, 0.71 | 0.80, 0.77 |
| `array_form` | 388, 401 | 246, 257 | 274, 279 | 0.63, 0.64 | 0.71, 0.69 |
| `drift_json` | 214, 214 | 164, 169 | 190, 191 | 0.77, 0.79 | 0.89, 0.89 |
| `drift_scalar` | 582, 593 | 319, 329 | 368, 374 | 0.55, 0.55 | 0.63, 0.63 |
| `int_arrays` | 67, 68 | 43, 44 | 46, 46 | 0.64, 0.64 | 0.69, 0.68 |
| `big_numbers` | 266, 269 | 267, 271 | 261, 270 | 1.00, 1.01 | 0.98, 1.00 |
| `pretty` | 399, 401 | 306, 311 | 321, 322 | 0.77, 0.77 | 0.80, 0.80 |

B and C parse each chunk once, exactly: `big_numbers` 32 parses where A makes 64. `drift_json`
still builds all 32 chunks again, since its column turns to JSON whatever the parser, parsing
exactly both times; `int_arrays`' three passes a chunk are all exact. Under the same walk, jiter
runs 1.03 to 1.38 times the engine's reader, round against round, on every corpus but
`big_numbers` (0.98 and 0.99).

**jiter's requirements**, read in its 0.17.0 source, paths under its `src/`:

- *A nesting bound the caller holds, with no recursion it does not control*: met. The iterator
  keeps no containers of its own (`parse.rs:61-65`), and each call the walk makes reads one token
  without recursing (`jiter.rs:82-85`, `:181-190`, `:202-210`, `:311-319`, `:335-362`;
  `parse.rs:81-177`); only `known_value` and `known_skip` recurse, to a depth of 200
  (`jiter.rs:239-274`, `errors.rs:177`), and the walk calls neither.
- *Typed refusals that name the record*: met. An error is a kind and a byte offset
  (`errors.rs:207-228`), the kinds an enum (`errors.rs:6-76`); the shredder numbers the record.
- *A number's written bytes*: met. `known_number_bytes` hands over the number's text once its
  grammar is checked (`jiter.rs:181-190`, `number_decoder.rs:490-582`). Its limits differ from the
  reader's: an integer of more than 4 300 digits is refused (`number_decoder.rs:557-558`), and an
  exponent may have any number of digits (`number_decoder.rs:584-609`), so the walk keeps the
  18-digit bound.
- *No allocation a record outside the meter, its string scratch reused across a chunk's
  records*: not met. Each `Jiter` decodes escaped strings and keys onto a buffer of its own,
  begun empty (`jiter.rs:39-47`, and `:20-30` for a clone; `string_decoder.rs:130-131`,
  `:137-149`), which no call takes from the caller (`string_decoder.rs:7`, private to the crate at
  `lib.rs:164`). A `Jiter` a record therefore grows that buffer, outside the meter, for each record
  that holds an escape. One `Jiter` over a chunk's records would keep it, stepping lines with
  `peek` and an array's elements with `array_step` (`parse.rs:81-126`), but would walk a record
  that does not end on its line into the next before it could tell, so a refusal's kind would
  depend on where the chunk was cut. The calls that hand over a string's raw bytes take no buffer
  (`jiter.rs:222-227`, `:358-362`; `string_decoder.rs:283-325`), but check no UTF-8
  (`string_decoder.rs:282`) and leave the escapes to the caller, which would keep the engine's
  own string reading beside jiter. A value tree's parse keeps its buffer between documents
  (`value.rs:170-182`); the iterator's does not.

jiter passes `cargo deny` as the workspace configures it (advisories, bans, licences and sources),
adding `bitvec` and its four crates to the lock file.

**Verdict.** Neither arm qualifies. On the five corpora B runs 0.48 to 0.71 times A and C 0.62 to
0.80 times, against 0.97; on the cliffs both are slower in both rounds on every corpus but
`big_numbers`. jiter does not meet its fourth requirement either. Nothing changes: sonic-rs, the
scans that confirm its numbers and the exact parses after them stay, and `json::Reader` stays the
engine's reader of JSON text.

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
shreds it; `keyed` shreds and normalizes a merge's rows, whose roots hash their `id`; `keyless`
shreds and normalizes rows that carry their positions alone, from which lowering builds their
ids, outside this group. Ids by role against its base `51f61a9c`, 2026-10-24: three rounds on
core 0, base and change interleaved, under the measurement lock, the one-minute load average
falling from 8.7 at the first round's start, as other work ended, to 1.3 by the last; each
round's criterion median.

| Group | Base | Change | Change against `shred_only` |
|---|---|---|---|
| `shred_only` | 72.5, 66.6, 65.9 ms | 68.2, 66.8, 66.7 ms | 1 |
| `keyed` | 117.9, 113.5, 112.2 ms | 83.7, 82.0, 81.6 ms | 1.22–1.23× |
| `keyless` | 180.5, 175.3, 174.3 ms | 70.8, 70.0, 69.8 ms | 1.04–1.05× |

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
| `normalize/shred_only` | 0.061 | 0.004 | 178 | 255.312 | 750372 |
| `normalize/keyed` | 0.098 | 0.007 | 263 | 410.312 | 1106206 |
| `normalize/keyless` | 0.095 | 0.007 | 227 | 401.312 | 953463 |

The `normalize` rows are of ids by role, at full size; its base, `51f61a9c`, made 533 bytes a
row keyed and 534 keyless, with 0.103 and 0.110 allocations a row.

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
