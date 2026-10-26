# Normalized writes

A normalized stream's flush is shredded into a unit a chunk of JSON, and each unit split into its
table and child tables before it is lowered. A flush of several units is judged across all of
them before any is written. This record keeps the write path's cost for flushes of one unit and of
several, from one partition and from many, and of a merge of hot keys.

## Method

- **Rows.** The first 100 000 rows of the `orders` corpus: keyless nested JSON, an object and as
  many items as the row's id modulo four, each item with two tags, 18.7 MB in all. Normalized,
  they load 550 000 rows into three tables, a number that follows from the roots alone, so every
  run checks it.
- **Pushes.** 2 000, 10 000 or 50 000 rows a push, a checkpoint after each, so each push is one
  flush: 2 000 rows fit one chunk of 1 MiB, one unit a flush; 10 000 rows make two units a flush
  and 50 000 rows nine. The bench prints the units it counts with the shredder's own cut.
- **Engine.** `Engine::run` from the replay source into a sink that counts rows and drops them,
  within the cores the bench may use (2 runtime workers and a compute thread a core on four
  cores; one of each on one), a budget of 1 GiB and one commit at the end, as the review's
  ablation ran.
- **Throughput.** Rows a second of the three tables.
- **Running.** `just bench normalized 0-3`, five rounds on the performance cores, and
  `just bench normalized 0 keyless` on one, each under the measurement lock; release profile.

## Results

`main` at `6365cc13` with the served bench's readings (`d2fc0f95`; the engine's code is `main`'s),
2026-10-25 and -26: five rounds on the performance cores, interleaved with the other benches'
rounds ([campaign.md](campaign.md)), each started once no other build ran, the one-minute load
average 1.15 to 2.33 at their starts; then the same cases on core `0` alone (one runtime worker
and one compute thread) in the same holds. The median of the five rounds and their range.

| Rows a push | Units a flush | Four cores (`0-3`) | Rows a second | JSON | One core (`0`) | Four cores against one |
|---|---|---|---|---|---|---|
| 2 000 | 1 | 71.0 ms (70.7–73.2) | 7.74 M | 263 MB/s | 58.8 ms (58.4–60.5) | 0.83× |
| 10 000 | 2 | 38.5 ms (38.2–39.7) | 14.3 M | 485 MB/s | 54.8 ms (54.6–55.8) | 1.42× |
| 50 000 | 9 | 26.2 ms (25.9–28.6) | 21.0 M | 713 MB/s | 53.4 ms (53.3–54.3) | 2.04× |

The more units a flush holds, the more four cores gain: none at one unit, where the run on four
is slower than on one, and twice one core's rate at nine.

**Where four cores' time goes.** A 15-second profile of `normalized/keyless/50000` on four cores
(`just profiling normalized`, `perf record -e cycles:u -F 999 --call-graph fp`, 30 229 samples):
the four compute threads hold 28.1 %, 24.1 %, 23.7 % and 20.0 % of the samples and the runtime's
workers 3.7 %. The split jobs, each unit's concatenation, merge-key check and normalizing, hold
3.7 % of the samples; lowering the parts holds most of the rest.

### From many partitions

200 000 orders in pushes of 10 000 rows read from 4 or 16 partitions, an even share each, within
256 MiB and within 1 GiB, in the same five rounds: the median and range, and the last run's
readings' medians, the waits summed over the run's tasks.

| Partitions | Budget | Time a run | Rows a second | Intake waits | Work waits | Pressure flushes | Peak memory |
|---|---|---|---|---|---|---|---|
| 4 | 256 MiB | 68.0 ms (66.5–74.9) | 16.2 M | 154 % | 153 % | 110 | 147 MiB |
| 4 | 1 GiB | 45.0 ms (43.5–48.9) | 24.5 M | 94 % | 155 % | 111 | 590 MiB |
| 16 | 256 MiB | 49.8 ms (48.5–51.5) | 22.1 M | 504 % | 465 % | 111 | 147 MiB |
| 16 | 1 GiB | 45.3 ms (44.5–52.2) | 24.3 M | 378 % | 435 % | 111 | 590 MiB |

Within the default budget four partitions take 51 % longer than within 1 GiB, and sixteen 9.9 %
longer.

### A merge of hot keys

`normalized/merge/zipf`: 200 000 orders merged by `id`, their ids drawn from 100 000 keys by a Zipf
distribution of θ = 0.99, YCSB's default constant, 50 000 rows a push, a checkpoint after each,
into the IPC sink, one commit at the end; in process on the performance cores, and served with
both connectors spawned (`served/process/socket/both/normalized/merge/zipf`, [served.md](served.md)),
in the production slice's three rounds.

| Placement | Time a run | Rows a second | CPU s a GB of JSON |
|---|---|---|---|
| In process | 111.7 ms (111.4–112.1) | 1.79 M | |
| Spawned | 129.1 ms (125.0–130.2) | 1.55 M | 8.86 (8.77–8.99): host 6.68, source 0.29, destination 1.84 |

Each run's commit stages 98 776 root rows and 900 000 child rows for 39 099 distinct keys, the same
in every round: the root rows keep each key's last row within a push, and every row's children
are staged.

The `allocations` bench's counts of one run of each, on the same tree and cores:

| Rows a push | Allocations a row | Bytes a row | Allocations a flush |
|---|---|---|---|
| 2 000 | 0.089 | 74 | 978 |
| 10 000 | 0.036 | 74 | 1 973 |
| 50 000 | 0.032 | 76 | 8 687 |

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
