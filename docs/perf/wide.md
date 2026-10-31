# Wide tables

A table of thousands of columns costs the engine work for every column of every flush: its
schema checked and converted, and each column looked up among those converted. This record keeps
what loading tables of 1 000 and 5 000 columns costs, a table kept to 100 of 5 000 columns, and a
sparse one whose rows each hold 100 of 5 000.

## Method

- **Rows.** Pushes of about 8 MiB each, the default coalescing target, a checkpoint after each:
  - `arrow/<columns>`: eight batches of the passthrough bench's ten mixed kinds of column, again
    and again to the width, 849 rows a batch at 1 000 columns and 166 at 5 000, 61 MB a run.
  - `json/<columns>`: eight pushes of the shred bench's `wide_<columns>` corpus, flat rows of
    integers and short strings by turns, 606 rows a push at 1 000 columns and 114 at 5 000, 67 MB a
    run.
  - `arrow/100`: nine batches of 166 rows of 100 such columns.
  - `arrow/5000_keep_100`: nine batches of 166 rows of 5 000 columns, the first holding the first
    100 columns alone, so the table exists with those; the stream keeps them by discarding every
    other column's values (`SchemaPolicy::DiscardValue`), which it counts.
  - `sparse/arrow` and `sparse/json`: 100 pushes of 166 rows of a table of 5 000 columns whose rows
    each hold 100, every row of a push the same window of 100 columns, the pushes taking the 50
    windows in turn, twice; as Arrow batches and as JSON lines.
- **Engine.** `Engine::run` from the replay source into a sink that counts rows and drops them,
  within the four cores (2 runtime workers and a compute thread a core), a budget of 1 GiB, one
  commit. Every run checks every row arrives and every discarded value is counted.
- **Throughput.** Rows a second; the bench prints the bytes a run pushes, Arrow's logical bytes or
  JSON's text.
- **Allocations.** The `allocations` bench runs each case once under its counting allocator, on
  the same tree and cores.
- **Running.** `just bench wide 0-3`, five rounds, each started under the measurement lock;
  release profile. `arrow/5000` also runs with both connectors spawned
  (`served/process/socket/both/wide/arrow/5000`, [served.md](served.md)).

## Results

`main` at `6365cc13` with the served bench's readings (`d2fc0f95`; the engine's code is `main`'s),
2026-10-25 and -26: five rounds interleaved with the other benches' rounds
([campaign.md](campaign.md)), each started once no other build ran, the one-minute load average
1.86 to 2.38 at their starts. The fourth round ran slower in every case, its load rising to 4.25 by
its end though no build ran; it is kept. The median of the five rounds and their range.

| Case | Rows a run | Time a run | Rows a second | MB a second | Allocations a row |
|---|---|---|---|---|---|
| `arrow/1000` | 6 792 | 18.16 ms (17.94–20.73) | 374 k | 3 362 | 24.3 |
| `arrow/5000` | 1 328 | 83.46 ms (81.82–96.30) | 15.9 k | 730 | 617 |
| `json/1000` | 4 848 | 186.14 ms (183.03–273.45) | 26.0 k | 360 | 252 |
| `json/5000` | 912 | 852.32 ms (848.33–1 169.20) | 1 070 | 79 | 8 033 |
| `arrow/100` | 1 494 | 2.20 ms (2.19–2.23) | 679 k | 597 | 12.7 |
| `arrow/5000_keep_100` | 1 494 | 55.01 ms (54.07–58.44) | 27.2 k | 1 110 | 334 |
| `sparse/arrow` | 16 600 | 560.22 ms (553.30–606.42) | 29.6 k | 27 | 406 |
| `sparse/json` | 16 600 | 646.89 ms (639.24–693.64) | 25.7 k | 38 | 426 |

- The same bytes take 4.6 times as long at 5 000 columns as at 1 000, pushed as Arrow or as JSON:
  the engine's work grows with the columns a flush holds, not with its bytes. A row of 5 000
  columns makes 617 allocations pushed as Arrow, 0.12 a column, and 8 033 pushed as JSON, 1.6 a
  column.
- Kept to its first 100 columns, the 5 000-column table loads 27.2 k rows a second, 4.0 % of a
  100-column table's 679 k, discarding 6.5 million values a run.
- Served, `arrow/5000` loads 8.58 k rows a second (8.27–8.67 k), 54 % of its rate in process,
  at 5.25 CPU seconds a GB: the host's process 2.71, the source's 1.60, the destination's 0.93.

## Where a wide run's time goes

A ten-second profile of four of the eight cases, below, on four cores (`just profiling wide`, `perf
record -e cycles:u -F 999 --call-graph fp`, 10 000–18 000 samples each), each sample counted once,
in the first stage its stack holds, in this order: the per-batch work that has the schema's shape
(the incoming schema taken for a flush, `Schemas::of`; the plan looked up for it, `Tables::plan` on
a hit; a batch measured into pieces, `Measure::piece`; the check that lowering copies nothing,
`copies_nothing`; and a null array built for each column a batch lacks); then the table fitted to a
schema a plan misses, which creates columns; the allocation sets that count what each batch keeps
alive; shredding; the rest of lowering; the lanes and the sink.

| Case | `Schemas::of` | `Tables::plan`, a hit | `Measure::piece` | `copies_nothing` | Null fill | Fitting on a miss | Allocation sets | Shredding | Lowering, the rest | Lanes and sink | The rest |
|---|---|---|---|---|---|---|---|---|---|---|---|
| `arrow/5000` | 0.7 % | 0.0 % | 7.5 % | 0.4 % | 0.0 % | 24.4 % | 24.9 % | – | 7.0 % | 6.4 % | 28.6 % |
| `json/5000` | 0.3 % | 0.0 % | 1.4 % | 0.2 % | 0.0 % | 1.6 % | 9.8 % | 65.4 % | 2.6 % | 3.5 % | 15.1 % |
| `sparse/arrow` | 0.1 % | 0.0 % | 0.2 % | 0.0 % | 18.0 % | 40.6 % | 15.3 % | – | 5.8 % | 10.3 % | 9.7 % |
| `sparse/json` | 0.1 % | 0.0 % | 0.2 % | 0.0 % | 16.7 % | 34.3 % | 14.5 % | 9.1 % | 5.0 % | 8.7 % | 11.4 % |

- The schema-shaped work takes 8.6 % of `arrow/5000`'s samples, most of it measuring pieces; and
  18.3 % and 17.0 % of the sparse table's, nearly all of it the null fill.
- A plan's miss is the largest stage of each Arrow case profiled: the first flush of each
  `arrow/5000` run creates the table's 5 000 columns, and each of the sparse table's windows is a
  schema of its own.
- The allocation sets take 9.8–24.9 %.
- `json/5000` is shredding: 65.4 % of its samples.

**Allocations.** One run of each case on four cores under heaptrack, on the profiling build, its
setup told apart by the frames that make the pushes; the calls it counts for a run agree with the
`allocations` bench's within 7 %. The schema-shaped work makes 6.6 % of `arrow/5000`'s allocation
calls, all of them `Schemas::of`'s, 4.3 % of `json/5000`'s, and 34.3 % of each sparse case's, 33.7
points of it the null fill. Other stages are not split: heaptrack names inlined frames by their
own short names and files, which repeat across crates.

## Schema-shaped work done once a schema

Four changes took the work that has the schema's shape off each batch: a unit's table schema and
column paths are shared, not copied, and its plan is found by pointer; a batch that fits its piece
is measured once, not in doubling stretches; the columns of one type a batch lacks share one array
of nulls, charged once; and a normalized stream keeps the schemas of the parts its batches split
into for the batches of one Arrow schema. Each was kept only where its own pair, the change against
the tree before it, showed at least 2 % of its case.

- **Trees.** `main` at `ea57c46d` (the base), then each change's commit on it in turn. The
  changes land on a later `main`, whose newer commits charge what cutting a unit keeps and change
  no figure's case here but the allocations, given again on it below.
- **Rounds.** Each tree's `wide` and `lowering` benches built beforehand; their criterion pass
  run as `just bench <bench> 0-3` runs it, under `taskset -c 0-3`. Five rounds of the wide bench,
  each one hold of the measurement lock: the base and the branch on every case, and between them
  each change's case on the trees before and after it, the order reversed between rounds; the
  one-minute load average 1.60 to 2.98 at their starts, and the first round's end disturbed (7.64
  as it ended). A figure is the median of the rounds' point estimates; a change is also given as
  the median of its rounds' own ratios.
- **Allocations.** The `allocations` bench, one run of each case on each tree.
- **Normalizing.** The `normalize/*` cases under callgrind as `just instructions` counts them, at
  one and three iterations, halved.

**Allocations a row.**

| Case | Base | Schemas shared | Nulls shared | Change |
|---|---:|---:|---:|---:|
| `arrow/1000` | 21.686 | 20.505 | 20.505 | −5.4 % |
| `arrow/5000` | 550.989 | 520.849 | 520.855 | −5.5 % |
| `json/1000` | 225.243 | 212.005 | 212.006 | −5.9 % |
| `json/5000` | 7 317.319 | 6 965.375 | 6 966.168 | −4.8 % |
| `arrow/100` | 11.254 | 10.637 | 10.641 | −5.4 % |
| `arrow/5000_keep_100` | 280.864 | 253.747 | 253.770 | −9.6 % |
| `sparse/arrow` | 402.673 | 402.003 | 257.662 | −36.0 % |
| `sparse/json` | 420.775 | 420.099 | 268.842 | −36.1 % |

Measuring a fitting batch once allocates nothing either way, and the part schemas a normalized
stream keeps touch none of these cases. On the `main` the changes land on (`05c69b59`) the counts
are the same to within a few hundredths, `json/5000` aside, which that `main` already makes fewer:
6 528.054 a row there, 6 177.758 with the changes (−5.4 %); `sparse/arrow` 402.620 to 257.662 and
`sparse/json` 420.633 to 268.841 (−36.0 % and −36.1 %).

**Time a run.** The base against the tree holding the first three changes, five rounds; the
fourth, which only normalized streams take, ran beside them in the last two rounds:
`arrow/5000` −8.9 %, `json/5000` −3.8 % and `sparse/arrow` −32.6 % against the base there.

| Case | Base | Range | Branch | Range | Change | Ratio of the rounds |
|---|---:|---|---:|---|---:|---|
| `arrow/1000` | 15.35 ms | 15.16–18.54 | 14.05 ms | 13.14–16.27 | −8.4 % | −11.2 % (−14.4 to −6.7) |
| `arrow/5000` | 73.10 ms | 71.12–88.05 | 66.21 ms | 63.25–89.79 | −9.4 % | −9.4 % (−11.1 to +2.0) |
| `json/1000` | 172.22 ms | 170.45–224.99 | 167.88 ms | 158.33–282.37 | −2.5 % | −3.0 % (−8.1 to +25.5) |
| `json/5000` | 793.69 ms | 776.61–1 087 | 781.70 ms | 727.12–1 048 | −1.5 % | −4.2 % (−6.4 to −1.5) |
| `arrow/100` | 1.88 ms | 1.87–1.97 | 1.70 ms | 1.67–1.81 | −9.6 % | −9.4 % (−10.9 to −5.6) |
| `arrow/5000_keep_100` | 46.10 ms | 45.37–59.22 | 41.10 ms | 39.35–42.76 | −10.8 % | −13.3 % (−27.8 to −9.9) |
| `sparse/arrow` | 526.28 ms | 517.48–681.79 | 353.70 ms | 338.97–432.63 | −32.8 % | −32.6 % (−36.5 to −31.9) |
| `sparse/json` | 606.70 ms | 603.50–713.97 | 431.41 ms | 421.57–494.66 | −28.9 % | −29.7 % (−30.7 to −28.5) |

**Each change against the tree before it**, on its case, five rounds:

| Change | Case | Before | After | Change | Kept |
|---|---|---:|---:|---|---|
| Schemas shared, plans found by pointer | `arrow/5000` | 73.10 ms, 550.989 a row | 70.88 ms, 520.849 a row | −3.0 % time, −5.5 % allocations | Yes |
| A fitting batch measured once | `arrow/5000` | 70.88 ms | 65.54 ms | −7.5 % (ratio −7.4 %, −8.8 to −3.9) | Yes |
| One array of nulls a type | `sparse/arrow` | 520.37 ms, 400.944 a row | 353.70 ms, 257.662 a row | −32.0 % time, −35.7 % allocations | Yes |
| One array of nulls a type | `sparse/json` | 608.37 ms, 420.132 a row | 431.41 ms, 268.842 a row | −29.1 % time, −36.0 % allocations | Yes |

**Part schemas kept a normalized stream's batch schema**, its rule fewer instructions or
allocations by at least 5 % in `normalize/*`, each case's batches normalized as a partition keeps
their schemas:

| Case | Instructions before | After | Allocations before | After |
|---|---:|---:|---:|---:|
| `normalize/keyless/nested` | 29 974 | 20 068 (−33.0 %) | 74 | 27 (−63.5 %) |
| `normalize/keyless/with_arrays` | 690 267 | 675 047 (−2.2 %) | 146 | 77 (−47.3 %) |
| `normalize/keyed/with_arrays` | 5 676 978 | 5 661 343 (−0.3 %) | 155 | 84 (−45.8 %) |
| `normalize/dropped/with_arrays` | 1 864 031 | 1 847 502 (−0.9 %) | 201 | 132 (−34.3 %) |

It is kept. The `normalized` bench allocates about 9 % less a batch with it: 883.3 to 801.0 at
2 000 rows a push, 1 777.6 to 1 618.0 at 10 000 and 7 798.0 to 7 084.0 at 50 000, on `05c69b59`.

**Lowering.** `just bench lowering`'s cases, the base against the branch: their allocations are the
same in every case. Whole rounds of the bench, one tree after the other, put the tree run first
up to 17 % behind, so the verdict reads two rounds that run each case on the base, the branch,
the branch and the base in turn (04:14 and 06:06 starts, load 1.92 and 2.84), each case's change
the ratio of the two pairs' geometric means:

| Case | First round | Second | Case | First round | Second |
|---|---:|---:|---|---:|---:|
| `append_native` | +3.1 % | +4.4 % | `merge_unique` | +0.6 % | +0.7 % |
| `append_text` | −2.3 % | −3.0 % | `one_bad_value` | +1.8 % | +0.1 % |
| `changes` | +1.9 % | −0.9 % | `one_dropped_row` | +1.1 % | +1.0 % |
| `history` | −1.2 % | −0.7 % | `split_json` | +0.1 % | +2.1 % |
| `merge_duplicates` | +13.0 % | +14.1 % | `temporal_widening` | +2.7 % | +0.9 % |

`merge_duplicates` does the same work on both: 10 974 445 and 10 976 174 instructions an
iteration under callgrind, the same branch misses, and 6 % more first-level data misses in its
cache simulation. Its large buffers go where glibc's moving mmap threshold puts them, which what
was allocated before them decides; with the threshold fixed (`glibc.malloc.mmap_threshold=65536`)
the branch is 1.3 % faster than the base. `append_native` measured equal in a further round in
the same order: 1.342 and 1.347 µs on the base, 1.344 and 1.348 on the branch.

- The sparse table gains most: each of its batches built an array of nulls for each of the 4 900
  columns it lacks, and now builds one for each of their six types.
- `arrow/5000` is 9.4 % faster and makes 5.5 % fewer allocations a row, short of the 10 % and
  15 % its own clause asks; the sparse table's 36 % fewer allocations a row pass the rule's third.
  The JSON cases are no slower.
