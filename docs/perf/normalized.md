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

## Ids by role

A row that a merge does not identify by its key carries its position, from which lowering builds
its id, where every root and item was hashed with BLAKE3 before. Against its base `51f61a9c`,
2026-10-24, same machine, `normalized/keyless/50000` alone, on core 0 (one runtime worker and one
compute thread), three rounds, base and change interleaved with the `normalize` group's, under
the measurement lock, the one-minute load average 3.5, 1.7 and 1.4 at the base's starts; each
round's criterion median:

| Tree | Rounds | Allocations a row | Bytes a row |
|---|---|---|---|
| `51f61a9c` | 139.0, 139.6, 137.1 ms | 0.033 | 128 |
| Ids by role | 56.0, 55.4, 55.4 ms | 0.032 | 76 |

60 % less time a run (59.6–60.3 %), each round's change below every round of its base. Bytes a
row fall 41 % at 50 000 rows a push, and from 133 and 129 to 74 at 2 000 and 10 000.
`just resident`'s normalized lines grow less than its base's: 13, 13 and 14 MiB against 20, 20
and 23 MiB at 2, 4 and 16 cores, and 15, 16 and 19 MiB against 23, 26 and 34 MiB with glibc's
tunables, each peak 144 MiB.

## Ids hashed across the compute pool: closed

Hashing a normalized split's ids in chunks of 4 096 rows, each chunk a job joined on the compute
pool, was built and measured against commit `450ca8493630` (a compute thread for every core), and
not taken. The rule that called for it, one compute thread holding more than about 40 % of a
normalized run's samples, no longer holds on that commit: in a 15-second profile of
`normalized/keyless/50000` on four cores (2 runtime workers, 4 compute threads, 9 units a flush)
the four compute threads hold 34.5 %, 24.4 %, 22.7 % and 16.9 % of the samples and the runtime's
workers 1.3 %; BLAKE3's compression is 31 % of them.

Measured 2026-10-15, three interleaved rounds a side under the measurement lock, one-minute load
average 1.2–1.9 at their starts; each round's criterion median, ms:

| Cores | `450ca8493630` | Ids hashed across the pool | Change |
|---|---|---|---|
| 4 (`0-3`) | 109.2, 112.0, 110.1 | 94.4, 96.1, 95.7 | −13 % |
| 1 (`0`) | 123.7, 127.0, 126.4 | 127.0, 129.6, 130.0 | +2 to +3 % |

Each round's four-core figures lie outside the other side's range. The change kept about 1.79
cores busy in its profile against 1.25, about 25 % more CPU a run for 13 % less time. Called from
a thread outside the pool, as `benches/shred.rs` normalizes, hashing on four threads took longer
than in order: 115.4 ms against 103.0 keyed and 173.7 ms against 160.9 keyless, a join waking
idle pool threads and blocking its caller.

It closes: it misses the 25 % it was to gain at four cores, costs a single core 2–3 % and every
run a quarter more CPU, slows callers outside the pool, and adds a pool method and a pool argument
through normalizing for it.
