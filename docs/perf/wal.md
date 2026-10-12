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

This change on `3c0260b2`, 2026-10-12. Intel Core Ultra X7 358H, cores 0 to 3 (performance), on
mains power, governor `powersave`, energy preference `performance`. Three rounds, interleaved with
the other states and prototypes this page compares, five runs a round in alternating order; this
change's runs started at a one-minute load average of 3.75, 3.16 and 3.33. Each figure is the
median of the three rounds, their range in brackets. Rust 1.98.1.

| Case | Throughput |
|---|---|
| `wal/encode/1MiB` | 1307 MiB/s [1160–1366] |
| `wal/encode/7MiB` | 1371 MiB/s [1193–1506] |

| Case | Throughput | Log phase a commit | Appended ÷ frames | Carried ÷ frames | Publishes a commit | Removes a commit | Fsyncs a commit, setup included |
|---|---|---|---|---|---|---|---|
| `wal/engine/tmpfs/each` | 894 MiB/s [834–894] | 1.082 ms [1.073–1.129] | 1.770 [1.730–1.803] | 0.742 [0.702–0.774] | 1.00 | 1.00 | 3.6 |
| `wal/engine/tmpfs/16` | 2327 MiB/s [2187–2545] | 0.626 ms [0.599–0.631] | 1.027 [1.027–1.039] | 0.000 [0.000–0.012] | 1.06 | 1.00 | 8.0 |
| `wal/engine/tmpfs/once` | 2154 MiB/s [1932–2216] | 2.588 ms [2.050–2.688] | 1.027 | 0.000 | 2.00 | 1.00 | 13.0 |
| `wal/engine/disk/each` | 251 MiB/s [245–252] | 3.935 ms [3.623–4.183] | 1.919 [1.919–1.923] | 0.891 [0.891–0.895] | 1.00 | 1.00 | 3.6 |
| `wal/engine/disk/16` | 940 MiB/s [898–948] | 15.678 ms [12.793–18.080] | 1.240 [1.220–1.264] | 0.213 [0.193–0.237] | 1.06 | 1.00 | 8.0 |
| `wal/engine/disk/once` | 1443 MiB/s [1432–1597] | 23.813 ms [23.410–24.237] | 1.027 | 0.000 | 2.00 | 1.00 | 13.0 |

`reliefs` is 0 in every round.

| Case | Throughput | Read ÷ logged | Reads |
|---|---|---|---|
| `wal/replay/tmpfs` | 489 MiB/s [438–514] | 2.000 | 5387 |
| `wal/replay/disk` | 456 MiB/s [391–456] | 2.000 | 5387 |

| Case, 8 batches of 8,192 rows | Allocations a row | Reallocations a row | Bytes a row | Allocations a batch | Reallocations a batch | Bytes a batch |
|---|---|---|---|---|---|---|
| `wal/engine/memory/each` | 0.101 | 0.013 | 431 | 830.625 | 109.750 | 3532168 |

A batch frame's bytes are copied twice in user space: the wire's encoder writes the batch into its
IPC messages (`arrow::encode`, `crates/rdlt-engine/src/wal/frame/arrow.rs`), and `Framing` writes
the frame's head, its header and those messages into one buffer (`Framing::batch`,
`crates/rdlt-engine/src/wal/frame.rs`), which the store writes as it is.

### Against main

Frames built in one pass, each site's into one buffer, against main at `3c0260b2`, in the same
rounds. The change was to move `wal/engine/{tmpfs,disk}/each` above main with the ranges apart,
`wal/engine/tmpfs/once` at least 10 % and `wal/encode/7MiB` beyond noise; to allocate at most 2.2
times a batch frame's bytes for the frame; and to add no more than 2 % to any case of
`just instructions`.

| Case | Main | One pass | Change | Rule |
|---|---|---|---|---|
| `wal/encode/7MiB` | 773 MiB/s [678–793] | 1293 MiB/s [1012–1780] | +67 %, ranges apart | met |
| `wal/engine/tmpfs/once` | 1667 MiB/s [1514–1721] | 2026 MiB/s [1842–2131] | +22 %, ranges apart | met |
| `wal/engine/tmpfs/each` | 891 MiB/s [834–940] | 813 MiB/s [771–828] | −9 %, ranges apart | missed |
| `wal/engine/disk/each` | 238 MiB/s [233–245] | 242 MiB/s [234–250] | +2 %, ranges overlap | missed |
| Bytes a batch frame of 12,000 rows allocates, over its bytes | | 2.51 | | missed, at most 2.2 |

`just instructions 3c0260b2` finds no case above 2 %: `wal/encode` takes 29.6 % fewer
instructions (4801702 to 3380703) and 8 % fewer allocations (151 to 139), and every other case is
within 0.1 %. The case writes its batch frame into one buffer with the log's other frames, so its
count includes copying that buffer to its exact length; a load encodes each batch frame alone,
with no copy. `wal/engine/memory/each` allocates 3532096 bytes a batch, main 5008268.

At `each` on tmpfs, frames built in one pass carry 0.718 [0.706–0.750] of the frames' bytes into
later chunks, main 0.690 [0.670–0.698].

### Commits answered before their chunks are deleted

A commit answered before the writer deletes the chunks it frees, against frames built in one pass
before deletion was deferred, in the same rounds. The change was to lower the log phase a commit
at `wal/engine/{tmpfs,disk}/each` with the ranges apart, with the fsyncs a commit unchanged and
the throughput not lower beyond noise.

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

At a commit each batch, each receipt carries the frames of segments still open out of the chunks
it leaves holding little else, rewriting them into the chunk staged (ADR 0041). With this change
that copies most of what the log writes twice:

| Case | Carried ÷ frames | Appended ÷ frames |
|---|---|---|
| `wal/engine/tmpfs/each` | 0.742 [0.702–0.774] | 1.770 [1.730–1.803] |
| `wal/engine/disk/each` | 0.891 [0.891–0.895] | 1.919 [1.919–1.923] |

Appended counts the carried bytes too, so carried ÷ appended stays below 0.5 unless carried is at
least the frames: the figure that says how much a carry rewrites is carried ÷ frames.

A prototype that carries only in a relief, the receipt's carry deleted, was measured against this
change in the same rounds. The rule: carrying only in a relief replaces eager carry where carried
is at least half of the frames at a commit each batch, and the prototype moves at least 10 % more
MiB/s there, the ranges apart. It fired on both roots:

| Case | Eager carry | Relief-only carry | Change |
|---|---|---|---|
| `wal/engine/tmpfs/each` | 894 MiB/s [834–894] | 1933 MiB/s [1892–2102] | +116 %, ranges apart |
| `wal/engine/disk/each` | 251 MiB/s [245–252] | 388 MiB/s [364–402] | +55 %, ranges apart |
| `wal/engine/tmpfs/each`, log phase a commit | 1.082 ms [1.073–1.129] | 0.444 ms [0.380–0.469] | −59 % |
| `wal/engine/disk/each`, log phase a commit | 3.935 ms [3.623–4.183] | 2.312 ms [2.280–2.331] | −41 % |

Carrying only in a relief appends 1.03 of the frames and carries nothing in these runs, since no
relief comes. It gives up ADR 0041's bound that a copy costs at most what it frees and an open
segment keeps at most as much of others' data as of its own, so the log holds more on disk between
reliefs, within ADR 0045's limit. Eager carry still runs in this change.

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
