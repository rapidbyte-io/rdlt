# Wide tables

A table of thousands of columns costs the engine work for every column of every flush: its
schema checked and converted, and each column looked up among those converted. This record keeps
what loading tables of 1 000 and 5 000 columns costs.

## Method

- **Rows.** Eight pushes of about 8 MiB each, the default coalescing target, a checkpoint after
  each:
  - `arrow`: batches of the passthrough bench's ten mixed kinds of column, again and again to the
    width, 849 rows a batch at 1 000 columns and 166 at 5 000, 61 MB a run.
  - `json`: the shred bench's `wide_<columns>` corpus, flat rows of integers and short strings by
    turns, 606 rows a push at 1 000 columns and 114 at 5 000, 67 MB a run.
- **Engine.** `Engine::run` from the replay source into a sink that counts rows and drops them,
  within the four cores (2 runtime workers and 2 compute threads), a budget of 1 GiB, one commit.
  Every run checks every row arrives.
- **Throughput.** Rows a second; the bench prints the bytes a run pushes.
- **Running.** `just bench wide 0-3`, five rounds, each started at a one-minute load average under
  1.0 under the measurement lock; release profile.

## Results

Intel Core Ultra X7 358H, mains power, 2026-10-07, commit `28726685db15`, governor `powersave`
with energy preference `performance`, one-minute load average 0.91–0.99 before the runs; the
median of five rounds and their range.

| Case | Time a run | Rounds within ±3 % | Rows a second | MB a second | CPUs busy | Allocations a row |
|---|---|---|---|---|---|---|
| `arrow/1000` | 26.45 ms (26.01–28.66 ms) | 4 of 5 | 257 k | 2 309 | 1.14 | 28.6 |
| `arrow/5000` | 102.69 ms (100.19–104.63 ms) | 5 of 5 | 12.9 k | 593 | 1.04 | 728 |
| `json/1000` | 234.93 ms (229.54–271.30 ms) | 3 of 5 | 20.6 k | 285 | 1.45 | 283 |
| `json/5000` | 1.02 s (1.01–1.19 s) | 3 of 5 | 890 | 66 | 1.58 | 8 214 |

The same bytes take nearly four times as long at 5 000 columns as at 1 000 pushed as Arrow, and
four and a third times as long pushed as JSON: the engine's work grows with the columns a flush
holds, not with its bytes. A row of 5 000 columns makes 728 allocations pushed as Arrow, 0.15 a
column, and 8 214 pushed as JSON, 1.6 a column.
