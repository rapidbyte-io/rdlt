# Normalized writes

A normalized stream's flush is shredded into a unit a chunk of JSON, and each unit split into its
table and child tables before it is lowered. A flush of several units is judged across all of
them before any is written. This record keeps the write path's cost for flushes of one unit and of
several.

## Method

- **Rows.** The first 100 000 rows of the `orders` corpus: keyless nested JSON, an object and as
  many items as the row's id modulo four, each item with two tags, 18.7 MB in all. Normalized,
  they load 550 000 rows into three tables, a number that follows from the roots alone, so every
  run checks it.
- **Pushes.** 2 000, 10 000 or 50 000 rows a push, a checkpoint after each, so each push is one
  flush: 2 000 rows fit one chunk of 1 MiB, one unit a flush; 10 000 rows make two units a flush
  and 50 000 rows nine. The bench prints the units it counts with the shredder's own cut.
- **Engine.** `Engine::run` from the replay source into a sink that counts rows and drops them,
  within the cores the bench may use (2 runtime workers and 2 compute threads on four cores), a
  budget of 1 GiB and one commit at the end, as the review's ablation ran.
- **Throughput.** Rows a second of the three tables.
- **Running.** `just bench normalized 0-3`, five rounds on the performance cores, each started at
  a one-minute load average under 1.0 under the measurement lock; release profile.

## Results

Intel Core Ultra X7 358H, mains power, 2026-10-07, commit `28726685db15`, governor `powersave`
with energy preference `performance`, one-minute load average 0.88–0.95 before the runs; the
median of five rounds and their range.

| Rows a push | Units a flush | Time a run | Rounds within ±3 % | Rows a second | JSON | CPUs busy |
|---|---|---|---|---|---|---|
| 2 000 | 1 | 183.9 ms (181.5–184.5 ms) | 5 of 5 | 2.99 M | 101 MB/s | 1.09 |
| 10 000 | 2 | 239.8 ms (238.9–243.9 ms) | 5 of 5 | 2.29 M | 77.8 MB/s | 1.08 |
| 50 000 | 9 | 221.6 ms (215.6–232.4 ms) | 3 of 5 | 2.48 M | 84.2 MB/s | 1.07 |

| Rows a push | Allocations a row | Bytes a row | Allocations a flush |
|---|---|---|---|
| 2 000 | 0.997 | 246 | 10 966 |
| 10 000 | 1.410 | 468 | 77 552 |
| 50 000 | 1.405 | 480 | 386 311 |

A flush of two or more units takes 20–30 % longer than flushes of one unit moving the same rows,
and allocates 40 % more a row and nearly twice the bytes: the difference the review's ablation
found judging a flush across its units to make. A run keeps barely more than one CPU busy, at
3.7–3.8 instructions a cycle in every case.
