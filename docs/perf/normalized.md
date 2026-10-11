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

Intel Core Ultra X7 358H, mains power, 2026-10-11, commit `a40d359e0741`, governor `powersave`
with energy preference `performance`, one-minute load average 0.37–0.97 before the runs; the
median of five rounds and their range.

| Rows a push | Units a flush | Time a run | Rounds within ±3 % | Rows a second | JSON | CPUs busy |
|---|---|---|---|---|---|---|
| 2 000 | 1 | 165.7 ms (163.8–168.2 ms) | 5 of 5 | 3.32 M | 113 MB/s | 1.10 |
| 10 000 | 2 | 126.4 ms (125.6–127.9 ms) | 5 of 5 | 4.35 M | 148 MB/s | 1.18 |
| 50 000 | 9 | 111.8 ms (111.0–112.8 ms) | 5 of 5 | 4.92 M | 167 MB/s | 1.17 |

| Rows a push | Allocations a row | Bytes a row | Allocations a flush |
|---|---|---|---|
| 2 000 | 0.541 | 155 | 5 953 |
| 10 000 | 0.490 | 150 | 26 962 |
| 50 000 | 0.486 | 150 | 133 662 |

The runs whose flushes hold two or nine units take 24–33 % less time than the run whose flushes
hold one unit, moving the same rows, and allocate 9–10 % fewer times a row for 3 % fewer bytes. A
run keeps 1.10–1.18 CPUs busy.
