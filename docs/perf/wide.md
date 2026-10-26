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
