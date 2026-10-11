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

Intel Core Ultra X7 358H, mains power, 2026-10-11, commit `f64df77f787a`, governor `powersave`
with energy preference `performance`, one-minute load average 0.94–0.99 before the runs; five
rounds, interleaved with rounds of the merge-base and with the passthrough, shredding and served
benches, in alternating orders; the median of the five rounds and their range. The fifth round
ran under other load on the machine, which raised the one-minute load average to 2.79 by its end,
and the first ran slower than the other three too; both are kept, the slowest two of each case.

| Case | Time a run | Rounds within ±3 % | Rows a second | MB a second | CPUs busy | Allocations a row |
|---|---|---|---|---|---|---|
| `arrow/1000` | 21.22 ms (21.05–25.44 ms) | 3 of 5 | 320 k | 2 879 | 1.07 | 25.8 |
| `arrow/5000` | 83.08 ms (82.10–96.05 ms) | 3 of 5 | 16.0 k | 733 | 1.03 | 657 |
| `json/1000` | 210.39 ms (209.12–253.80 ms) | 3 of 5 | 23.0 k | 318 | 1.48 | 255 |
| `json/5000` | 928.58 ms (917.04 ms–1.18 s) | 4 of 5 | 982 | 72 | 1.62 | 7 304 |

The same bytes take nearly four times as long at 5 000 columns as at 1 000 pushed as Arrow, and
4.4 times as long pushed as JSON: the engine's work grows with the columns a flush holds, not with
its bytes. A row of 5 000 columns makes 657 allocations pushed as Arrow, 0.13 a column, and 7 304
pushed as JSON, 1.5 a column.
