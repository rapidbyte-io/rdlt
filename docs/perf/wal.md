# Write-ahead log

What the write-ahead log's own bookkeeping costs beside the frames it writes.

## The room a log keeps for ending its chunks

After every publish the writer notes the room it keeps for its own frames: what ending the chunk
staged writes, and what staging and ending one more after it writes (a preamble and header, a
closing frame, and an end frame naming every chunk and commit the log then holds). Those lengths
are counted, not encoded: an end frame's from the digits of its numbers, a header's by writing its
JSON to a counter that keeps only how many bytes it was given.

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
  sixteen batches, 17 commits) and `once` (one commit). `wal/engine/tmpfs/tight` commits each
  batch into a log whose limit is four batch frames, about 4.2 MB, so that a batch finds the log
  full and a relief makes room in every run. `wal/replay/<root>/one` reads back a log of the
  same 256 batches, left in one chunk by an `once` run, as a replay does: it fences the log,
  scans it and reads each batch frame again, then deletes its fence so the next sample reads the
  same log. `wal/replay/<root>/chunks` does the same with a log whose bound is twice its frames'
  bytes, which cuts it into five chunks. Results before the section on a fenced log's last chunk
  were taken by `wal/replay/<root>`, a scan that did not fence the log. The engine runs move
  their batches into the IPC sink, whose encoding is part of each case's time.
- **Running it.** `just bench wal 0-3`, which pins the cases to cores 0 to 3 and records the
  commit, the load average, the governor, `perf stat`'s instructions a cycle and the CPUs kept
  busy, the syscalls of one run, and the allocations. Criterion takes 10 samples a case (100 for
  the encode cases); each figure is its point estimate. A case's log is made when it first runs,
  and its counters are printed after it.
- **Throughput.** The bytes of the batch frames: one batch's frame times 256 batches. The encode
  cases count the batch's logical bytes. Replay's throughput counts the same frame bytes, and
  `read ÷ logged` beside it is the bytes the scan read over the bytes the log holds.
- **Counters.** The engine's report gives the log phase (milliseconds a commit), the bytes the
  log appended and carried (rewrote into a later chunk) over the frames' bytes, and the reliefs
  a run.
- **Fsyncs.** A commit's fsyncs are the `fsync` and `fdatasync` calls of the case's `strace -c`
  count, divided by that run's commits. The count comes from the case's run in test mode: 16
  batches, so 17 commits at `each` and `tight`, 2 at `16` and 1 at `once`, and the calls of its
  setup are in it. It is one run, so the figure is the same every round, but at `tight`, whose
  run fires none to five reliefs, each a publish of its own.
- **Replay's source.** `wal/replay/disk/*` reads a log it has just written, so it reads from the
  page cache.
- **Allocations.** `wal/engine/memory/each` is a case of the `allocations` bench, not of the
  timed run: 8 batches of 8,192 rows, a commit each batch, the log kept in memory. Its counts are
  not comparable with the timed cases' 256 batches of 12,000 rows.

### Results

This change on `b951816a`, 2026-10-12. Intel Core Ultra X7 358H, cores 0 to 3 (performance), on
mains power, governor `powersave`, energy preference `performance`. Three rounds, interleaved with
main in alternating order; a fourth, which another build disturbed, was dropped. This change's
runs started at a one-minute load average of 3.48, 1.27 and 3.58. Each figure is the median of the
three rounds, their range in brackets. Rust 1.98.1.

| Case | Throughput |
|---|---|
| `wal/encode/1MiB` | 1228 MiB/s [1208–1299] |
| `wal/encode/7MiB` | 1140 MiB/s [1055–1140] |

| Case | Throughput | Log phase a commit | Appended ÷ frames | Carried ÷ frames | Reliefs a run | Fsyncs a commit, setup included |
|---|---|---|---|---|---|---|
| `wal/engine/tmpfs/each` | 1905 MiB/s [1672–2158] | 0.506 ms [0.390–0.536] | 1.029 | 0.000 | 0 | 3.6 |
| `wal/engine/tmpfs/16` | 2335 MiB/s [2020–2435] | 0.604 ms [0.582–0.827] | 1.027 | 0.000 | 0 | 8.0 |
| `wal/engine/tmpfs/once` | 1958 MiB/s [1833–2036] | 2.326 ms [2.241–2.433] | 1.027 | 0.000 | 0 | 13.0 |
| `wal/engine/tmpfs/tight` | 1972 MiB/s [1438–2015] | 0.305 ms [0.301–0.399] | 1.033 [1.029–1.033] | 0.004 [0.000–0.004] | 35 [29–42] | 4.8 [4.8–5.0] |
| `wal/engine/disk/each` | 395 MiB/s [366–410] | 2.306 ms [2.224–2.535] | 1.028 | 0.000 | 0 | 3.6 |
| `wal/engine/disk/16` | 1161 MiB/s [994–1202] | 10.189 ms [9.994–11.320] | 1.027 | 0.000 | 0 | 8.0 |
| `wal/engine/disk/once` | 1362 MiB/s [1293–1523] | 22.856 ms [20.188–32.477] | 1.027 | 0.000 | 0 | 13.0 |

| Case | Throughput | Read ÷ logged | Reads |
|---|---|---|---|
| `wal/replay/tmpfs` | 467 MiB/s [396–475] | 2.000 | 5387 |
| `wal/replay/disk` | 428 MiB/s [405–454] | 2.000 | 5387 |

| Case, 8 batches of 8,192 rows | Allocations a batch | Bytes a batch |
|---|---|---|
| `wal/engine/memory/each` | 829.625 | 3533804 |

A batch frame's bytes are copied twice in user space: the wire's encoder writes the batch into its
IPC messages (`arrow::encode`, `crates/rdlt-engine/src/wal/frame/arrow.rs`), and `Framing` writes
the frame's head, its header and those messages into one buffer (`Framing::batch`,
`crates/rdlt-engine/src/wal/frame.rs`), which the store writes as it is.

### Against main

Carrying only in a relief, chunks chosen by what they free, against main at `b951816a`, whose
receipts carried open segments, in the same rounds. The change was to move
`wal/engine/{tmpfs,disk}/each` above main with the ranges apart, with no case slower beyond noise,
in throughput or log phase, and a case where reliefs fire.

| Case | Main | This change | Change |
|---|---|---|---|
| `wal/engine/tmpfs/each` | 848 MiB/s [706–876] | 1905 MiB/s [1672–2158] | +125 %, ranges apart |
| `wal/engine/disk/each` | 233 MiB/s [233–238] | 395 MiB/s [366–410] | +70 %, ranges apart |
| `wal/engine/tmpfs/each`, log phase a commit | 1.147 ms [1.095–1.273] | 0.506 ms [0.390–0.536] | −56 %, ranges apart |
| `wal/engine/disk/each`, log phase a commit | 3.984 ms [3.810–3.989] | 2.306 ms [2.224–2.535] | −42 %, ranges apart |
| `wal/engine/tmpfs/16` | 1907 MiB/s [1883–2187] | 2335 MiB/s [2020–2435] | +22 %, ranges overlap |
| `wal/engine/disk/16` | 772 MiB/s [753–908] | 1161 MiB/s [994–1202] | +50 %, ranges apart |
| `wal/engine/tmpfs/once` | 1818 MiB/s [1759–2064] | 1958 MiB/s [1833–2036] | +8 %, ranges overlap |
| `wal/engine/disk/once` | 1390 MiB/s [1313–1532] | 1362 MiB/s [1293–1523] | −2 %, ranges overlap |
| `wal/engine/tmpfs/tight` | 1527 MiB/s [1430–1612] | 1972 MiB/s [1438–2015] | +29 %, ranges overlap |
| `wal/engine/tmpfs/tight`, log phase a commit | 0.479 ms [0.451–0.487] | 0.305 ms [0.301–0.399] | −36 %, ranges apart |
| `wal/engine/tmpfs/tight`, reliefs a run | 42 [30–52] | 35 [29–42] | |
| `wal/replay/tmpfs` | 428 MiB/s [384–462] | 467 MiB/s [396–475] | +9 %, ranges overlap |
| `wal/replay/disk` | 413 MiB/s [349–437] | 428 MiB/s [405–454] | +4 %, ranges overlap |
| `wal/encode/1MiB` | 1024 MiB/s [935–1317] | 1228 MiB/s [1208–1299] | +20 %, ranges overlap |
| `wal/encode/7MiB` | 1030 MiB/s [866–1075] | 1140 MiB/s [1055–1140] | +11 %, ranges overlap |

The rule is met: both commit-each cases move up with the ranges apart; no case is lower with the
ranges apart, the lowest median being `disk/once` at −2 %; and reliefs fire in every run of
`tight`. The encode cases run the same code in both, so their spread is this machine's noise.
Main carries 0.738 [0.718–0.750] of the frames at `tmpfs/each` and 0.903 [0.903–0.915] at
`disk/each`, appending 1.766 and 1.931 of them; this change carries none there. Fsyncs a commit
are the same in both on every case but `tight`, where they vary with the test-mode run's reliefs
in both. `wal/engine/memory/each` allocates 3533804 bytes a batch, main 3481226 to 3533840.

`just instructions b951816a` finds no case above 2 %: `passthrough/null_sink` +0.05 %,
`shred/drift_json` −0.01 %, every other case unchanged.

### Commits answered before their chunks are deleted

A commit answered before the writer deletes the chunks it frees, against frames built in one pass
before deletion was deferred, in rounds on `3c0260b2`. The change
was to lower the log phase a commit at `wal/engine/{tmpfs,disk}/each` with the ranges apart, with
the fsyncs a commit unchanged and the throughput not lower beyond noise.

| Case | Before | After | Change | Rule |
|---|---|---|---|---|
| `wal/engine/tmpfs/each`, log phase a commit | 1.136 ms [1.129–1.199] | 1.082 ms [1.073–1.129] | −5 %, ranges overlap | missed |
| `wal/engine/disk/each`, log phase a commit | 3.991 ms [3.742–3.998] | 3.935 ms [3.623–4.183] | −1 %, ranges overlap | missed |
| `wal/engine/{tmpfs,disk}/each`, fsyncs a commit | 3.6 | 3.6 | none | met |
| `wal/engine/tmpfs/each` | 813 MiB/s [771–828] | 894 MiB/s [834–894] | +10 %, ranges apart | met |
| `wal/engine/disk/each` | 242 MiB/s [234–250] | 251 MiB/s [245–252] | +4 %, ranges overlap | met |

### A frame's representation

A batch frame is written into one buffer: its head, its header, then the batch's IPC messages
copied in after it. Against it, a piece list was measured on a prototype that is not kept: the
frame as its head and header in one small buffer and the wire encoder's own buffers, uncopied,
appended as a list, the local log writing them with `write_vectored`. Both were built on frames
built in one pass, before deletion was deferred, and run by the method above in three interleaved
rounds on 2026-10-12.

The piece list would replace the single buffer only where it moved `wal/engine/tmpfs/once` and
`wal/engine/disk/once` at least 10 % more MiB/s, the ranges apart; `wal/engine/*/each` was not
lower beyond noise; and a batch frame allocated no more bytes.

| Case | One buffer | Pieces | Change |
|---|---|---|---|
| `wal/engine/tmpfs/once` | 2026 MiB/s [1842–2131] | 2267 MiB/s [1968–2382] | +12 %, ranges overlap |
| `wal/engine/disk/once` | 1539 MiB/s [1473–1573] | 1557 MiB/s [1541–1652] | +1 %, ranges overlap |
| `wal/engine/tmpfs/each` | 813 MiB/s [771–828] | 799 MiB/s [792–801] | −2 %, ranges overlap |
| `wal/engine/disk/each` | 242 MiB/s [234–250] | 240 MiB/s [238–241] | −1 %, ranges overlap |
| `wal/encode/1MiB` | 1197 MiB/s [1109–1304] | 9852 MiB/s [9322–9883] | 8.2 times |
| `wal/encode/7MiB` | 1293 MiB/s [1012–1780] | 7999 MiB/s [7720–8089] | 6.2 times |
| Bytes a batch frame of 12,000 rows allocates, over its bytes | 2.51 | 1.51 | |

The frame stays one buffer. The piece list allocates less and encodes a frame six to eight times
faster, but the engine's runs do not show it: `once` moved 12 % on tmpfs and 1 % on disk, the
ranges overlapping on both. Writing frames to the store as they are encoded is not built either,
since it needs the piece list.

### Carrying open segments

Frames of a segment still open are carried only in a relief (ADR 0041, ADR 0045). A settled chunk
that an open segment shares stays until the segment settles or a batch finds the log full; its
relief then copies the open frames into the chunk it publishes, taking the old chunks by what
deleting each frees beside its copy, the most first, and deletes them. So the log fills toward its
limit before it carries anything, and at a commit each batch with room to spare it carries
nothing; main, whose receipts carried open segments, rewrote most of what the log wrote a second
time.

| Case | Main, carried ÷ frames | Main, appended ÷ frames | This change, carried ÷ frames | This change, appended ÷ frames |
|---|---|---|---|---|
| `wal/engine/tmpfs/each` | 0.738 [0.718–0.750] | 1.766 [1.746–1.779] | 0.000 | 1.029 |
| `wal/engine/disk/each` | 0.903 [0.903–0.915] | 1.931 [1.931–1.943] | 0.000 | 1.028 |
| `wal/engine/tmpfs/tight` | 0.004 [0.000–0.004] | 1.033 [1.029–1.033] | 0.004 [0.000–0.004] | 1.033 [1.029–1.033] |

Appended counts the carried bytes too, so the figure that says how much a carry rewrites is
carried ÷ frames.

At `tight` both carry 0.4 % of the frames over 29 to 52 reliefs a run: a relief there mostly
publishes what commits freed, and seldom finds open frames to carry, so the case shows that reliefs
run within a small log, not how much the choice by freed bytes copies. The log there held at most
3,241,665 bytes, about 77 % of its limit, by a directory poll every 2 ms through 20 s of runs, which
can miss a shorter peak; main's peak was the same within 24 bytes. In single exploratory runs at
limits of 16 to 32 frames, the reliefs of both designs fired or not with timing and load, and
either could copy more than the frames under pressure; no timed case covers those limits.

### Re-encoding a batch's schema

Each batch frame holds the schema message as well as the batch. The signal is the share of
`rdlt_wire::Encoder::schema` (inclusive) in the instructions of encoding one batch frame, for a
batch of 1,000 rows of 1,000 columns. The count comes from callgrind at 1 and 3 iterations,
differenced, on the release profile with fat LTO, with this change.

`Encoder::schema` takes 4,081,717 of the 27,987,387 instructions of a frame, which is 14.6 %:

| Part of `Encoder::schema` | Instructions a frame |
|---|---|
| Writing the schema message | 1.60 M |
| Verifying the flatbuffer | 1.56 M |
| Reading it back into the columns the encoder keeps | 0.84 M |
| Dropping them | 0.38 M |

The rule: a cache of the schema message is built only if the share is at least 3 % of the frame's
encoding. The share is above it, and the cache is not built. It needs an encoder that starts a
fresh dictionary epoch without re-encoding the schema, which is a new `rdlt-wire` API, and a
message kept for each table version under the budget. Only a batch of 1,000 columns was measured.

### A fenced log's last chunk, read once

A replay fences a log by reading its highest chunk and publishing the next. Its scan took that
reading only on this change: on `main` it read the chunk again, then each batch frame a third
time as it replayed. Each case below reads a log of 256 batches of about 1 MiB (256.4 MiB of
batch frames) by the method above. Three rounds on 2026-10-15, interleaved with `main` at
`223b7f6a` in alternating order, each holding the timed lock, at one-minute load averages of
1.3 to 3.6. Each time is the median of the rounds' point estimates for one replay of the whole
log, their range in brackets.

| Case | Main | This change | Change |
|---|---|---|---|
| `wal/replay/tmpfs/one` | 889 ms [877–909] | 542 ms [502–546] | −39 %, ranges apart |
| `wal/replay/disk/one` | 1022 ms [1004–1036] | 608 ms [602–621] | −41 %, ranges apart |
| `wal/replay/tmpfs/chunks` | 493 ms [489–518] | 496 ms [483–522] | +1 %, ranges overlap |
| `wal/replay/disk/chunks` | 598 ms [582–606] | 585 ms [575–598] | −2 %, ranges overlap |

| Case | Main, read ÷ logged | Main, reads | This change, read ÷ logged | This change, reads |
|---|---|---|---|---|
| `wal/replay/*/one` | 3.000 | 10523 | 2.000 | 5392 |
| `wal/replay/*/chunks` | 2.008 | 5845 | 2.000 | 5300–5319 |

A log in one chunk is read twice instead of three times, in half the reads, and replays in about
40 % less time. A log of several chunks saves the share of it its last chunk holds: in
`chunks`, whose last chunk holds the batches after the last relief, 0.8 % of the log, which no
timing shows. A chunk holds about an eighth of the log's bound at most, so a log past its first
chunk saves at most that much of a read. Per batch, nothing changes: the log is read only by a
replay.

The deterministic count is `a_replay_reads_each_chunk_once_and_its_batch_frames_once_more`
(`crates/rdlt-engine/src/wal/scan/tests.rs`): every chunk of a replayed log is read as many bytes
as one scan of it takes, and each of its batch frames once more.
