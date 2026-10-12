# Write-ahead log

What the write-ahead log's own bookkeeping costs beside the frames it writes.

## The room a log keeps for ending its chunks

After every publish and receipt the writer notes the room it keeps for its own frames: what
ending the chunk staged writes, and what staging and ending one more after it writes (a preamble
and header, a closing frame, and an end frame naming every chunk and commit the log then holds).
Those lengths are counted, not encoded: an end frame's from the digits of its numbers, a header's
by writing its JSON to a counter that keeps only how many bytes it was given.

- **Signal.** The heap's peak while `Log::ending()` measures a fresh log, as `peak_alloc` counts
  it in the test's own process: exact, whatever the machine or its load.
- **Test.** `measuring_the_room_a_log_keeps_allocates_nothing`
  (`crates/rdlt-engine/src/wal/writer/tests/sized.rs`), which asserts the figure.
- **Lengths.** `a_header_frame_s_length_is_what_it_encodes_to` and the property test
  `an_end_frame_s_length_is_what_it_encodes_to` (`crates/rdlt-engine/src/wal/frame/tests.rs`)
  hold each counted length to the frame's encoding, at every digit boundary of its numbers.
- **Build.** `cargo nextest` of rdlt-engine's library tests, dev profile, Rust 1.98.1, at commit
  `f18394480449`.

| Measurement | Heap peak |
|---|---|
| `Log::ending()` on a fresh log | 0 bytes |

## Logging and replay

What the engine writes to its log while it moves batches, and what a replay reads back.

### Method

- **Cases.** `wal/encode/{1MiB,7MiB}` encodes one batch into a log in memory, of 12,000 rows and
  about 1 MiB or of 80,000 rows and about 7 MiB. `wal/engine/<root>/<cadence>` runs the engine
  over 256 batches of 12,000 rows, about 1 MiB each, of the passthrough workload's columns, into
  the IPC sink, logging each ahead of the sink, the log kept under `root` and committing at
  `cadence`. The roots are `tmpfs` (`/dev/shm`) and `disk` (the build's target
  directory). The cadences are `each` (a commit each batch, 257 commits), `16` (a commit every
  sixteen batches, 17 commits) and `once` (one commit). `wal/replay/<root>` scans a log of the
  same 256 batches, left by an `once` run. The engine runs move their batches into the IPC sink,
  whose encoding is part of each case's time.
- **Running it.** `just bench wal 0-3`, which pins the cases to cores 0 to 3 and records the
  commit, the load average, the governor, `perf stat`'s instructions a cycle and the CPUs kept
  busy, the syscalls of one run, and the allocations. Criterion takes 10 samples a case (100 for
  the encode cases); each figure is its point estimate. A case's log is made when it first runs,
  and its counters are printed after it.
- **Throughput.** The bytes of the batch frames: one batch's frame times 256 batches. The encode
  cases count the batch's logical bytes. Replay's throughput counts the same frame bytes, and
  `read ÷ logged` beside it is the bytes the scan read over the bytes the log holds.
- **Counters.** The engine's report gives the log phase (milliseconds a commit), the bytes the
  log appended and carried (rewrote into a later chunk) over the frames' bytes, and the publish
  and remove requests a commit.
- **Fsyncs.** A commit's fsyncs are the `fsync` and `fdatasync` calls of the case's `strace -c`
  count, divided by that run's commits. The count comes from the case's run in test mode: 16
  batches, so 17 commits at `each`, 2 at `16` and 1 at `once`, and the calls of its setup are in
  it. It is one run, so the figure is the same every round.
- **Replay's source.** `wal/replay/disk` reads a log it has just written, so it reads from the
  page cache.
- **Allocations.** `wal/engine/memory/each` is a case of the `allocations` bench, not of the
  timed run: 8 batches of 8,192 rows, a commit each batch, the log kept in memory. Its counts are
  not comparable with the timed cases' 256 batches of 12,000 rows.

### Results

This change on `7d3604e0`, 2026-10-12. Intel Core Ultra X7 358H, cores 0 to 3 (performance), on
mains power, governor `powersave`, energy preference `performance`. Three rounds, each started at a
one-minute load average of 1.32, 3.25 and 3.16 (the rounds follow one another, so the second and
third started while the machine's load average still held the round before). Each figure is the
median of the three rounds, their range in brackets. Rust 1.98.1.

The three rounds did not all lie within 3 % of the median: the ranges below show which did not.

| Case | Throughput |
|---|---|
| `wal/encode/1MiB` | 860 MiB/s [859–879] |
| `wal/encode/7MiB` | 1067 MiB/s [961–1081] |

| Case | Throughput | Log phase a commit | Appended ÷ frames | Carried ÷ frames | Publishes a commit | Removes a commit | Fsyncs a commit, setup included |
|---|---|---|---|---|---|---|---|
| `wal/engine/tmpfs/each` | 930 MiB/s [908–967] | 1.026 ms [0.927–1.083] | 1.642 [1.638–1.706] | 0.614 [0.610–0.678] | 1.00 | 1.00 | 3.6 |
| `wal/engine/tmpfs/16` | 2001 MiB/s [1963–2017] | 1.161 ms [1.076–1.195] | 1.027 [1.027–1.031] | 0.000 [0.000–0.004] | 1.06 | 1.00 | 8.0 |
| `wal/engine/tmpfs/once` | 1784 MiB/s [1693–1822] | 2.493 ms [2.162–2.633] | 1.027 | 0.000 | 2.00 | 1.00 | 13.0 |
| `wal/engine/disk/each` | 244 MiB/s [242–250] | 3.733 ms [3.731–3.918] | 1.915 [1.907–1.931] | 0.887 [0.879–0.903] | 1.00 | 1.00 | 3.6 |
| `wal/engine/disk/16` | 918 MiB/s [902–950] | 13.562 ms [13.303–14.219] | 1.244 [1.216–1.260] | 0.217 [0.189–0.233] | 1.06 | 1.00 | 8.0 |
| `wal/engine/disk/once` | 1407 MiB/s [1368–1413] | 21.815 ms [17.861–22.403] | 1.027 | 0.000 | 2.00 | 1.00 | 13.0 |

`reliefs` is 0 in every round. On disk, `carried` varies between rounds: 0.879 to 0.903 at
`each` and 0.189 to 0.233 at `16`. On tmpfs it is 0.610 to 0.678 at `each` and 0.000 to 0.004 at
`16`; at `once` it is 0 on both roots.

| Case | Throughput | Read ÷ logged | Reads |
|---|---|---|---|
| `wal/replay/tmpfs` | 475 MiB/s [462–499] | 2.000 | 5387 |
| `wal/replay/disk` | 463 MiB/s [453–465] | 2.000 | 5387 |

| Case, 8 batches of 8,192 rows | Allocations a row | Reallocations a row | Bytes a row | Allocations a batch | Reallocations a batch | Bytes a batch |
|---|---|---|---|---|---|---|
| `wal/engine/memory/each` | 0.102 | 0.012 | 611 | 834.000 | 99.375 | 5008268 |
