# rdlt performance analysis

**Date**: 2026-07-25 · **Commit**: `270c903` · **Toolchain**: rustc 1.96.0
**Machine**: 32 cores, 62 GB RAM, Fedora Atomic host, work performed inside a
fedora-toolbox container · **Fixtures**: the feature-018 bench fixtures,
byte-identically seeded

Everything below is measured. No claim in this document rests on reading the
code alone; where a mechanism is inferred rather than measured it says so
explicitly, and names the experiment that would settle it. The negative results
in §5 are as much a part of the review as the positive ones — four
plausible-sounding optimizations were killed by direct measurement and ten more
by adversarial review against the code.

---

## TL;DR

rdlt is genuinely fast. The recorded matrix reproduces on this machine to within
3%, and against dlt on the same hardware it wins by 5–60× on four of five cells
while using 5–6× less memory. But the benchmark contains one defect that
manufactures its only recorded loss, and the engine leaves a large amount of
performance on the table in ways that are measurable and fixable without a
single line of `unsafe`.

| # | Finding | Measured effect | Effort |
|---|---|---|---|
| **F1** | The `pg-to-pg-dedup-1m` cell moves **3M rows, not 1M** — the source discovers every table in the schema on top of the declared query stream | **14.7 s → 5.07 s** (2.9×). Turns a recorded **0.85× loss** into a **2.5× win** vs dlt | S (cell spec) |
| **F2** | The WAL encodes every batch as **Parquet** — dictionary encoding, RLE, interning — then deletes it | **−18% / −21% wall**, **−28% CPU**, **−20% RSS** on the 1M cells, measured with a working Arrow-IPC swap | S |
| **F3** | Every pipeline runs at **under one core** on a 32-core box | 8 concurrent pipelines reach **4.2× the throughput** of one. Single-pipeline headroom ≈ **3.5×** | L |
| **F4** | Every row is written to Postgres **twice** — COPY into stage, then `INSERT…SELECT` into the target | Publish costs **565 ms of a 2.0 s cell**; a swap costs **21 ms**. Net win after the logged-stage penalty ≈ **340 ms (17%)** | M |
| **F5** | The COPY encoder heap-allocates **one `Box` per cell** (~12M per run) | Direct Arrow→wire encoding is **2.3×** faster, byte-identical output | M |
| **F6** | ~**25%** of the flagship JSONL cell is BLAKE3 row-identity hashing | Declaring a primary key already cuts **9% of CPU** today | M |
| **F7** | Parquet `WriterProperties` are hard-coded to defaults in three places and **not configurable at all** | Dictionary encoding costs **5.6×** no-dictionary here; and rdlt writes **210 MB where dlt writes 74 MB** for the same 1M rows | S |
| **F8** | `[profile.release]` is absent — no LTO, `codegen-units=16` | LTO+CGU1: **−14% CPU** on pg-to-pg (wall unchanged, see F3) | S |

**The most important structural insight** is in F3: rdlt has two independent
performance problems, and they need different fixes. CPU work that sits *on the
serial critical path* (F2) converts almost 1:1 into wall-clock. CPU work that
does not (F8, and partly F5) buys CPU headroom but moves the wall clock barely
at all — because the pipeline spends 30–70% of its life blocked. Until F3 is
addressed, **most micro-optimization will not show up in the benchmark.**

---

## 1. Where rdlt stands today

Baseline, release build as shipped, destination reset before every run, driven
directly (not through `rdlt-bench`) so `perf` could wrap the process:

| cell | wall (measured here) | wall (recorded 018) | CPU-s | %CPU of 32 cores | peak RSS |
|---|---|---|---|---|---|
| pg-to-pg-1m | 2.02 s | 2.05 s | 1.61 | **70%** | 150 MB |
| pg-to-s3parquet-1m | 1.63 s | 1.61 s | 1.13 | **70%** | 158 MB |
| s3jsonl-to-pg-200k | 1.14 s | 1.13 s | 0.97 | **89%** | 198 MB |
| s3jsonl-to-s3parquet-200k | 0.96 s | 0.956 s | 0.87 | **90%** | 219 MB |
| pg-to-pg-dedup-1m | 14.7 s | 14.81 s | 4.7 | **32%** | 290 MB |

The harness reproduces. That matters: every delta below is measured against
this table on the same machine in the same session.

dlt 1.29.0 (connectorx) was re-measured **on this machine, this session**, so
the comparisons below are not quoted from the record:

| cell | dlt here | dlt recorded | dlt peak RSS |
|---|---|---|---|
| pg-to-pg-dedup-1m | 12.20 / 12.55 / 12.73 s | 12.48 s | 785–819 MB |
| pg-to-s3parquet-1m | 1.68 s | 1.67 s | 510 MB |

The competitor baseline reproduces too. rdlt's memory advantage is large and
consistent — 152 MB vs ~800 MB on the dedup cell, 158 MB vs 510 MB on the
parquet cell.

**The column the benchmark does not report is the interesting one.** Every
pipeline runs at under one core. See F3.

---

## 2. Method

### Instruments

- **Wall/CPU/RSS**: `/usr/bin/time -v` around the release CLI. A/B experiments
  are **interleaved** (A, B, A, B, …) so machine drift hits both arms equally,
  and report medians of 5 pairs unless stated.
- **CPU profile**: `perf record -F 1999 --call-graph fp` against a binary built
  with `-C force-frame-pointers=yes -C debuginfo=1`. Frame pointers were
  necessary — DWARF unwinding failed to produce usable stacks through the async
  frames, and the first flat profile was ambiguous as a result (see §2.1).
- **Server-side attribution**: `log_min_duration_statement = 0` on the fixture,
  reading per-statement durations out of the container log. Restored to `-1`
  afterwards.
- **Isolated component costs**: standalone Rust microbenchmarks built against
  the same pinned crate versions (`arrow`/`parquet` 58.3, `postgres-types` 0.2),
  operating on the exact batch shape the bench produces (57,813 rows).
- **End-to-end validation of proposed changes**: for F2 the change was actually
  implemented in a throwaway build, measured, verified to produce correct
  output, and then reverted. The repository is unmodified apart from this file.
- **Adversarial review**: every candidate optimization was then re-derived
  independently against the code by a second pass whose explicit brief was to
  *refute* it — checking that cited APIs exist in the pinned dependency
  versions, that claimed magnitudes are consistent with the profile, and that
  no stated invariant is broken. The second table in §5 is its output: ten
  plausible optimizations that do not survive contact with the code. Where its
  estimates disagreed with a direct measurement, the measurement wins and the
  text says so (F2, F8, and the `work_mem` item in §4 are all cases where a
  first-principles estimate was wrong in one direction or the other).

### Limitations, stated up front

- `kernel.perf_event_paranoid = 2` on this host and the sysctl is not writable
  from the container, so **profiles are user-space only**. Kernel time —
  syscalls, socket I/O, page faults — is unattributed. This is why every
  profile-derived claim is cross-checked with a wall-clock A/B, which measures
  what the sampler cannot see.
- The fixture Postgres runs in a container on the same host as rdlt, so
  round-trip latency is ~0.05 ms. Real deployments see 0.5–5 ms; §3.8 flags the
  one place where that changes the ranking.
- Every measurement was taken with LLM analysis agents running concurrently on
  the same machine. Absolute numbers are therefore slightly inflated versus a
  fully quiet machine; all comparisons are interleaved A/B, so ratios hold.
  Where an absolute number matters it is cross-checked against the recorded
  018 session, which it matches.

### 2.1 A methodological note

The first version of the WAL experiment appeared to *disprove* the WAL
hypothesis: removing `workdir:` from the pipeline spec changed nothing. The
control run then showed `Wal::record` still consuming 23% of CPU. The cause is
`crates/rdlt/src/pipeline_spec.rs:261` — **an absent `workdir` defaults to
`.rdlt`**, so the "no-WAL" arm still had a WAL. The real A/B needed a
one-line experimental patch to `EngineConfig`.

Two consequences. First, the finding in §3.2 survived only because the control
was run. Second, this is a real product observation in its own right: **there is
currently no way to disable the WAL from a pipeline spec**, though
`EngineConfig::workdir` is an `Option` and the library API can.

---

## 3. Findings

### F1 — The dedup cell measures rdlt doing three times the work

**This is the highest-value finding in the review, and it is a benchmark defect,
not an engine defect.**

`crates/rdlt-connector-postgres/src/source/config.rs:34` documents the `tables`
field as:

> `/// Absent ⇒ discover ALL tables in `schema`.`

`benches/cells/pipelines/pg-to-pg-dedup.yaml` (and its LOAD 1 sibling) declares
only `queries:` — no `tables:`. So the source streams the declared query stream
**plus every table in `public`**, which for the dedup fixture is `events` (1M)
and `events_v2` (1M).

Server-side statement log from one `pg-to-pg-dedup` run:

```
LOAD 1  COPY (…FROM "public"."events")           1583 ms
        COPY (…FROM "public"."events_v2")        1649 ms
        COPY (…FROM (SELECT * FROM events) q)    1510 ms
        INSERT INTO "events"        …SELECT      2317 ms
        INSERT INTO "events_v2"     …SELECT      2225 ms
        INSERT INTO "events_merged" …SELECT      2297 ms
LOAD 2  COPY (…FROM "public"."events")           1456 ms
        COPY (…FROM "public"."events_v2")        1531 ms
        COPY (…FROM (SELECT * FROM events_v2) q) 1590 ms
        INSERT INTO "events"        …SELECT      4081 ms   ← not wanted
        INSERT INTO "events_v2"     …SELECT      3795 ms   ← not wanted
        INSERT INTO "events_merged" …SELECT      3768 ms   ← the cell's claim
```

The destination confirms it:

```
bench.events        = 1000000 rows
bench.events_merged = 1000000 rows   ← the only table cell.verify checks
bench.events_v2     = 1000000 rows
```

`[cell.verify]` checks `events_merged` only, so the two unintended 1M-row
upserts are invisible to the harness. dlt's script (`pipeline_pg_pg_dedup.py`)
moves exactly one table. **The arms are not comparable.**

Measured with the source scoped so only the declared query stream is
discovered — identical pipeline, identical merge strategy, `events_merged`
still 1,000,000 rows and now the only table written:

| variant | wall | CPU-s | peak RSS |
|---|---|---|---|
| cell as specified today | 14.7 s | 4.7 | 290 MB |
| **only the intended stream** | **5.07 s** | 1.47 | 152 MB |
| only the intended stream + F2 | **4.78 s** | 1.16 | 126 MB |
| dlt 1.29.0 connectorx (same machine) | 12.55 s | — | ~800 MB |

**0.85× loss → 2.6× win, and 6.4× less memory.** The recorded policy-log entry
calling the merge path "an optimization target" is chasing a phantom.

Two actions, and they are different in kind:

1. **Fix the cell.** Scope the dedup pipelines' source so only the query stream
   is delivered, re-run the session, and correct the RESULTS.md policy log.
   Until then `pg-to-pg-dedup-1m` is not a valid three-way comparison.
2. **Reconsider the default.** "Omit `tables` ⇒ replicate the entire schema" is
   a defensible default for a replication tool and a surprising one when the
   user has explicitly enumerated `queries`. Note also that `tables: []` is
   *rejected* (`config.rs:569`, "omit it to discover all"), so **there is no way
   to express "queries only, discover nothing"**. That expressiveness hole is
   what let the defect hide. At minimum, a pipeline that declares `queries` and
   omits `tables` deserves a warning naming the tables it is about to
   replicate.

*What would confirm this*: re-run the full three-way session with the corrected
cell. The rowcount check above already proves the output is identical.

---

### F2 — The WAL writes Parquet, and Parquet is the wrong format for a scratch buffer

`crates/rdlt-engine/src/wal/mod.rs:214` encodes every batch as a Parquet file
with default `WriterProperties` (dictionary encoding on, RLE, interning),
synchronously with blocking `std::fs` on the async loader task, fsyncs it at
commit, and then `remove_file`s it. Its own module doc calls the WAL "a
replayable buffer, never the source of truth", and it carries an explicit
`WAL_FORMAT_VERSION`.

Frame-pointer profile of pg-to-pg-1m:

```
23.89%  (inclusive)  rdlt_engine::wal::Wal::record
  └ parquet::arrow::arrow_writer::ArrowWriter<W>::write
      └ ArrowColumnWriter::write_internal
          ├ write_primitive → ColumnValueEncoderImpl::write_slice → Interner::intern
          └ ByteArrayEncoder::write_gather                        → Interner::intern
```

Nearly a quarter of the CPU of a Postgres-to-Postgres pipeline goes into a
Parquet dictionary encoder for data that is deleted seconds later. The bench
source has ~1M-cardinality text columns (`name = 'user-<i>'`, uuid-as-text),
which is the pathological input for dictionary encoding.

Isolated cost of encoding one real batch (57,813 rows × 13 columns):

| segment format | ms/batch | bytes |
|---|---|---|
| **parquet, default props (shipping today)** | **44.8** | 7.82 MB |
| parquet, dictionary + encodings off | 8.0 | 8.32 MB |
| arrow IPC stream | 5.9 | 8.89 MB |
| **arrow IPC file** | **5.8** | 8.89 MB |

**7.7× cheaper for 13% more bytes on a file that never outlives the run.**

I implemented the swap for real — `write_segment` → `arrow::ipc::writer::
FileWriter`, `open_segment` → `arrow::ipc::reader::FileReader` (both are
drop-in: the reader is the same `Iterator<Item = Result<RecordBatch, _>>`
shape) — built it, measured it, verified the output, and reverted it. Five
interleaved pairs, medians:

| cell | parquet WAL | **IPC WAL** | Δ wall | CPU-s | Δ CPU | RSS |
|---|---|---|---|---|---|---|
| pg-to-pg-1m | 2.02 s | **1.65 s** | **−18%** | 1.61 → 1.18 | **−27%** | 150 → 121 MB |
| pg-to-s3parquet-1m | 1.63 s | **1.29 s** | **−21%** | 1.13 → 0.81 | **−28%** | 158 → 143 MB |
| s3jsonl-to-s3parquet-200k | 0.96 s | **0.91 s** | −5% | 0.87 → 0.82 | −6% | 219 → 214 MB |
| s3jsonl-to-pg-200k | 1.14 s | 1.10 s | −3% (noisy) | ~0 | ~0 | — |

Output verified: `events = 1000000`; and for the JSONL cell
`events = 200000, events__tags = 400000`.

An upper-bound control (WAL disabled entirely) gives 1.55 s and 1.24 s on the
two 1M cells — so **the IPC swap recovers ~90% of the theoretical maximum. The
cost was the encoding, not the I/O.**

Why this one converts to wall-clock when others do not: `Loader::process`
(`load/mod.rs:119-133`) records to the WAL *before* handing the batch to the
destination. It is squarely on the serial critical path.

Recommended, in order — note that (0) and (1) are **alternatives**, not
increments, and (0) is strictly cheaper to land:

0. **Cheapest possible version: keep Parquet, turn off what the WAL never
   uses.** `WriterProperties::builder().set_dictionary_enabled(false)
   .set_encoding(PLAIN)` is a one-line change to `wal/mod.rs:216` with **no
   format-version bump, no reader change, no replay-compatibility question** —
   old and new segments are both valid Parquet. The microbench above puts it at
   8.0 ms/batch versus 44.8, i.e. it captures roughly **85% of the IPC win for
   ~5% of the risk**. Do this first; it is reversible in a line.
1. **Then, if you want the rest: switch the segment format to Arrow IPC** and
   bump `WAL_FORMAT_VERSION`. This is the variant measured above (−18%/−21%).
   Contained to two functions; rename the segment extension off `.parquet`
   while you are there. The crash-sweep suite (`TARGET=sweep make test`) and
   the WAL replay tests are the gate.
2. **Move the blocking write off the async task** (`spawn_blocking`), or
   overlap it with the destination write — they are independent until commit.
   Not measured; the sampler cannot see the blocked time, and the ceiling is
   necessarily *below* the full-removal A/B (overlapping cannot beat removing).
   *Experiment*: `tokio-console`, or a span timer around `wal.record`.
3. **Skip the WAL entirely when every stream in the run is Replace.** A Replace
   span is provably discardable — recovery re-extracts from the source and
   overwrites. This is the only option that reaches the full −21%/−23% ceiling,
   and it needs no new user-facing surface because the engine already knows the
   write mode of every stream. It does nothing for the dedup cell (merge), which
   at 14.6 s is the largest single number in the matrix.
4. **Allow opting out explicitly.** `workdir` currently hard-defaults to
   `.rdlt` (`pipeline_spec.rs:261`), so there is no door at all. Defaulting
   *on* is the right direction; there should still be a way out — and it should
   not be spelled `workdir: null`, which reads like "no working directory"
   rather than "no crash recovery".

---

### F3 — The engine runs at under one core, and the headroom is ~3.5×

This is the largest opportunity in the codebase and the hardest to take.

The topology in `crates/rdlt-engine/src/runtime/run.rs` is strictly serial:
one source task per stream → one `ShredOwner` that ping-pongs a *single*
`spawn_blocking` call at a time (`run.rs:375-433`, the owner is consumed and
handed back per slab) → one byte-bounded channel → **one** loader task owning
**one** `LoadSession` → **one** Postgres connection doing `COPY IN`.

The decisive experiment: run N independent pg-to-pg pipelines concurrently into
N destination schemas. If the server, the network or the disk were the limit,
throughput would not scale. All arms verified at 1,000,000 rows each.

| N pipelines | wall | rows moved | rows/s | vs N=1 | %CPU |
|---|---|---|---|---|---|
| 1 | 2.76 s | 1M | 362,319 | 1.0× | 58% |
| 2 | 2.28 s | 2M | 877,193 | **2.4×** | 151% |
| 4 | 3.14 s | 4M | 1,273,885 | **3.5×** | 244% |
| 6 | 4.15 s | 6M | 1,445,783 | **4.0×** | 291% |
| 8 | 5.25 s | 8M | 1,523,810 | **4.2×** | 324% |

Throughput saturates around **1.5M rows/s at ~3.2 cores** — that is where the
Postgres fixture becomes the limit. A single rdlt pipeline delivers 362k rows/s.
**One pipeline is leaving ~4× on the table, and ~3.5× of it is reachable before
the server becomes the constraint.**

Supporting signal: pg-to-pg-1m performs **113,552 voluntary context switches**
versus 888 for s3jsonl-to-s3parquet. `BinaryCopyInWriter::write(&refs).await`
is awaited **once per row** (`dest/commit.rs:371`).

Where the parallelism could come from, roughly in ascending risk:

1. **Overlap WAL-record with destination-write.** They are independent until
   commit, so a depth-1 hand-off (record batch *n+1* while the destination
   writes *n*) is the cheapest structural change in this list. Largely
   subsumed by F2 if the WAL gets cheap enough — take F2 first and re-measure.
2. **Parallel staging connections.** Staging COPY runs on `self.client`
   *outside* the publish transaction (`dest/commit.rs:344`) and is therefore
   already auto-committed, so the transactional boundary does not forbid
   several connections COPYing into the same stage table. This attacks the
   ~700 ms of serialized stage COPY measured in F4. **But it is not as clean as
   it looks**: `__rdlt_arrival` is a `BIGSERIAL` whose ordering is what makes
   merge dedup "last wins", and its meaning across concurrent connections needs
   a real answer before this is safe. It is also arguably the *engine's*
   problem rather than the connector's — the loader task is the bottleneck, and
   fixing it inside one destination pushes the same work onto every other one.
3. **A shredder pool per stream.** Harder: `TableBuffer` observation state is
   stateful and order-sensitive, and schema-delta ordering is a correctness
   invariant. Needs per-slab parsing with a serialized observe/drain step, i.e.
   an owning-arena redesign — and it only helps the two JSONL cells, since the
   pg cells take the passthrough path. Do not build it before a profile splits
   parse from observe.
4. **Concurrent per-table writes** in the loader. **Blocked at the SPI**:
   `LoadSession::write` takes `&mut self` (`rdlt-connector/src/lib.rs:123`), so
   this cannot be done without changing the connector trait that every
   destination implements. Real, but a much bigger commitment than it appears.

Every one of these touches an invariant the project deliberately protects.
None of them requires `unsafe`. I would sequence F2 and F4 first — they are
cheap and they shrink the serial path that F3 then parallelises.

Three plausible explanations for the idle time were checked and **do not hold**:
the tokio runtime is not starved (it spawns 32 workers for ~4 tasks), the
64 MiB `byte_budget` is not the constraint (§5), and the checkpoint→commit
cadence is irrelevant on the two 1M cells because cursor-less snapshot streams
never checkpoint (`EVIDENCE` E9 — they commit exactly once).

*What would confirm the ceiling*: repeat the N-scaling table against a
Postgres instance with more headroom (or a non-Postgres destination) to
separate rdlt's ceiling from the fixture's.

---

### F4 — Every row is written to Postgres twice

Server-side decomposition of one pg-to-pg-1m run (1.97 s wall):

| phase | server time |
|---|---|
| `COPY (SELECT … FROM events) TO STDOUT BINARY` (source) | 342 ms |
| 18 × `COPY <stage> FROM STDIN BINARY` (one per batch, ~40 ms each) | ~700 ms |
| **`INSERT INTO events (…) SELECT … FROM <stage>` (publish)** | **710 ms** |
| `COMMIT` | 27 ms |
| DDL, probes, receipts | ~100 ms |

The source costs 342 ms. The destination costs ~1,440 ms — and **710 ms of it
is a pure server-side re-copy** of data that already reached the server four
seconds of CPU ago.

Isolated, in one transaction on the same fixture:

```
A: TRUNCATE target; INSERT INTO target SELECT * FROM stage; COMMIT   579 ms
B: DROP TABLE target; ALTER TABLE stage RENAME TO target; COMMIT      21 ms
```

**27× cheaper.** But the honest accounting has to include the catch: the stage
is `UNLOGGED` (`dest/commit.rs:202`), and a swapped-in table must be logged
(`ALTER TABLE … SET LOGGED` rewrites the table, giving back everything saved).
So the stage would have to be logged from the start. Priced, order-controlled:

```
write 1M rows into an UNLOGGED table (today's stage)   255 ms / 289 ms  → ~272 ms
write 1M rows into a LOGGED table (swap-able stage)    502 ms / 454 ms  → ~478 ms
```

**Net: 579 − 21 − 206 ≈ 350 ms saved, ~17% of the cell.** Not the 28% a naive
reading of the swap number suggests.

Two shapes are worth considering, and they land in the same place:

- **Replace via table swap** — stage logged, then `DROP` + `RENAME` in the
  publish transaction (DDL is transactional in Postgres). Loses the target's
  OID, so dependent views/FKs break; indexes, constraints, grants and comments
  must be reproduced on the stage. Only valid for Replace in a **single commit
  unit**.
- **Replace by COPYing straight into the target** inside the publish
  transaction (`TRUNCATE target; COPY INTO target; COMMIT`). Same arithmetic
  (~478 ms of logged write instead of ~272 + 565), and semantically simpler —
  no swap, no OID change. The cost is holding the publish transaction open
  across the whole load, which is already what "one commit unit" means.

I would evaluate the second first. Both are Replace-only; merge genuinely needs
the stage.

---

### F5 — The COPY encoder allocates a `Box` per cell

`PgSession::write` (`dest/commit.rs:355-372`) builds, **per row**, a
`Vec<Box<dyn ToSql + Sync + Send>>` and a `Vec<&dyn ToSql>`; `encode::cell_value`
(`dest/encode.rs:91`) heap-allocates every individual value, and text columns
additionally `String::to_owned()` the bytes out of the Arrow buffer. For
pg-to-pg-1m that is roughly **12M boxes and 2M Vecs per run**.

The profile agrees. Across two pg-to-pg-1m runs, **35.4% of all cycles are in
`libc.so.6`**, dominated by allocator traffic (`_int_free_chunk` alone samples
at 10.9% and 18.4% in the two runs — the spread is sampling variance, the
ranking is stable):

```
10.88%  _int_free_chunk
 3.82%  __memmove_avx512_unaligned_erms
 3.75%  malloc
 3.23%  __libc_malloc2
 1.85%  __memcmp_evex_movbe
 1.59%  cfree
```

`tokio_postgres::Client::copy_in` is generic over `U: Buf` and returns
`CopyInSink<U>` (verified in tokio-postgres 0.7.17, `client.rs:630`) — so the
sink will accept **pre-encoded `Bytes` directly**. `BinaryCopyInWriter` and the
whole `ToSql` round-trip is a convenience, not a requirement. And rdlt already
hand-rolls the only non-trivial wire encoders it needs (`NumericWire`,
`JsonbWire`, `UuidWire` in `encode.rs`). The remaining format is trivial:
`int16` field count, then per field `int32` length (or `-1`) followed by bytes.

Measured on the same batch shape, byte-identical output (9,649,476 bytes both):

| encoder | ms/batch | rows/s |
|---|---|---|
| `Vec<Box<dyn ToSql>>` per row (shipping today) | 13.9 | 4.15 M |
| **direct from Arrow buffers into `BytesMut`** | **6.1** | **9.45 M** |

**2.3× faster.** For pg-to-pg-1m that is ~140 ms of the ~2.0 s wall (**~7%**),
plus the removal of ~12M allocations, which is global relief for a process that
spends a quarter of its cycles in the allocator. Three smaller costs ride along
in the same loop and disappear with it: `cell_value`'s `cast!` macro
(`encode.rs:97-104`) downcasts the array **per cell** rather than per column
(12M downcasts per run), `array.is_null(row)` (`encode.rs:105`) is a virtual
call per cell where the column's null buffer could be hoisted once, and
`numeric_wire_bytes` (`encode.rs:195`) allocates a `String` and two `Vec`s per
decimal value.

**The framing granularity is a second, independent problem in the same call.**
`BinaryCopyInWriter::write_raw` flushes at a hard-coded **4096 bytes** into an
`mpsc::channel(1)`, so ~150 MB of binary-COPY payload becomes tens of thousands
of task hand-offs and socket writes. That is the mechanism behind the measured
**113,552 voluntary context switches**. Feeding `CopyInSink<Bytes>` ~64 KiB
chunks directly collapses it. (64 KiB rather than something larger keeps the
buffer under glibc's 128 KiB mmap threshold so it is reused rather than
re-mapped.)

I could not isolate the framing half without implementing it. *Experiment*:
implement the direct encoder emitting ~64 KiB `Bytes` chunks and re-measure
wall, CPU, **and `%w` from `/usr/bin/time`** — the context-switch count is the
clean discriminator between the two halves of this finding.

Effort is M, not S: the golden-SQL and dest-conformance suites are the safety
net, and the source's own binary-COPY decoder is already the round-trip oracle
(`encode.rs` tests already do exactly this).

---

### F6 — A quarter of the flagship JSONL cell is identity hashing

s3jsonl-to-s3parquet-200k, inclusive cost:

```
16.73% (self)  _blake3_compress_in_place_avx512
15.54%         rdlt_engine::shred::table::row_identity
 9.87%         rdlt_engine::shred::canon::canonical_json_bytes
 9.78%         rdlt_core::identity::child_row_id
 5.48% (self)  __memcmp_evex_movbe          ← object-key sort in canonicalisation
 4.87% (self)  realloc
```

Every keyless root row and every child row gets a full canonical-JSON
materialization (`canon.rs:44`, recursive, sorts object keys, serialises numbers
through serde_json) followed by a BLAKE3 hash. For 200k roots + 400k children
that is 600k canonicalisations and 600k hashes.

This is the cost of a real feature — `_rdlt_id` is what makes subtree merge see
changed children — and this cell is already 40–62× faster than dlt. So the
framing is headroom, not defect. Note also that the 25% is the *cost of the
feature*, not the size of any single available win: no individual change below
recovers more than a few percent, and they must be judged on that basis.

Three levers on the identity path, in ascending invasiveness:

1. **Declare a primary key** (already supported: `connector-file/src/source/
   config.rs:43`). Roots then take the cheap keyed path — hash the key fields
   only, no canonicalisation. Measured on this cell, 4 interleaved pairs:
   CPU **0.885 → 0.805 s (−9%)**, wall 0.98 → 0.96 s. Free today; it is a
   documentation and defaults question, not an engine change. Note it changes
   `_rdlt_id` values (different domain separator), so it is a deliberate opt-in.
2. **Stop materialising the canonical form.** `content_hash_with`
   (`table.rs:134`) builds the whole canonical byte string into a scratch Vec,
   then feeds it to the hasher. Hashing incrementally during the canonical walk
   would remove the `realloc` traffic (4.87%) and one full pass over the bytes,
   **without changing a single output byte**. This is the best
   effort/risk/reward item in F6.
3. **A cheaper hash.** BLAKE3 over ~150 MB of canonical JSON is ~150 ms here.
   A short-input-optimised 128-bit hash would be several times faster — but
   `_rdlt_id` is persisted, so this is a data-format change requiring a version
   gate and a migration story. Mentioned for completeness; I would not do it.

*What would confirm (2)*: implement the streaming hash and assert the existing
identity property tests (`tests/identity_props.rs`) still pass byte-for-byte,
then re-measure the cell.

**And the shred path has a comparable item that is not hashing.** `build_batch`
(`shred/build.rs:67`) is structured **column-major**:

```rust
for column in &schema.columns {
    …
    for row in rows { … }
}
```

so for every column it walks every row and probes that row's JSON object for
the field. In my profile `build_batch` is **12.41% inclusive** of this cell,
with `DrainRow::get_top` — the per-(row × column) field probe — at 2.38% self
and `build_scalar` at 3.11%. A single pass over rows filling all column
builders would touch each row once instead of once per column. Not all of the
12.4% is recoverable (the Arrow builder appends are real work), but the probing
overhead is, and it is the same order as the identity levers above.

Two more in the same path, both cheaper to do:

- **~20 allocations per document** — the per-push `Arena` (`tape.rs:98`), the
  `content_hash` scratch `Vec` (`table.rs:128`, allocated fresh per root while
  the child path already threads a reusable one), the `child_lists` boxes, and
  the `key.to_owned()` at `tape.rs:177`. Worth **4–6%**, and the
  `content_hash` scratch alone is a three-line change.
- **Memoize the child-table index** per (parent table, source key).
  `child_table_idx` (`tape.rs:231`) does a linear scan over `self.tables`
  comparing `TableName`s for every child list of every row. Worth **~2%**.

None of these touch persisted formats.

---

### F7 — Parquet writer properties are hard-coded and unreachable

Three call sites pass `None`/`default()`:

- `crates/rdlt-connector-file/src/dest/session.rs:51` (the parquet destination)
- `crates/rdlt-engine/src/wal/mod.rs:216` (the WAL — addressed by F2)
- `crates/rdlt-connector-iceberg/src/dest/writer.rs:64`

So every parquet file rdlt writes uses dictionary encoding, uncompressed pages,
and default row-group sizing, and **a user cannot change any of it**. On
pg-to-s3parquet-1m the dictionary path is the dominant cost:

```
17.9%  hashbrown::raw::RawTable::reserve_rehash   (4 monomorphisations)
17.6%  parquet::util::interner::Interner::intern  (4 monomorphisations)
 5.8%  ahash::random_state::RandomState::hash_one
 5.2%  __memcmp_evex_movbe
```

Dictionary encoding is a *good* default for low-cardinality columns and a
pathological one for high-cardinality columns, which is what this fixture has.
Measured, same batch: dictionary on 44.8 ms vs off 8.0 ms — **5.6×** — for 6%
larger output.

**And it is also a benchmark-comparability problem.** rdlt writes
`Compression::UNCOMPRESSED` because that is the parquet-rs default and nothing
overrides it. Measured output for the same 1,000,000 rows on the same bucket,
same session:

| product | objects | bytes written |
|---|---|---|
| rdlt | 26 | **210.0 MB** |
| dlt | 5 | **73.7 MB** |

**rdlt writes 2.85× the bytes** — and still finishes in 1.63 s against dlt's
1.68 s. That cuts both ways. It makes rdlt's "parity" result *more* impressive
on a like-for-like basis, and it means the two arms **do not produce equivalent
artifacts**: a user gets three times the storage bill and materially different
downstream read performance. The cell reports a wall-clock tie between two
different pieces of work.

The recommendation is **not** "turn dictionaries off" — that would hurt real
workloads with repetitive columns, and file size and read speed are part of the
product. It is:

- **Expose `WriterProperties` as destination config** — at minimum
  `compression`, `dictionary_enabled`, `max_row_group_size`, `data_page_size`.
  Today a user with 1M-cardinality string columns has no escape hatch, and a
  user who wants compressed output has no way to ask for it at all.
- **The cheapest partial fix for the CPU half** is
  `set_dictionary_page_size_limit` — parquet-rs abandons the dictionary once it
  exceeds the limit, so a low cap makes high-cardinality columns bail out early
  instead of interning a million distinct strings. This gets most of the
  encoder win without changing behaviour for low-cardinality columns, which is
  exactly the asymmetry you want.
- **Match compression across arms in the benchmark**, or state the difference
  in the cell note. Right now `pg-to-s3parquet-1m` compares uncompressed
  against compressed and calls it parity.
- One parquet *file* is written per batch (`session.rs:47`). Consolidating to
  one writer per commit window was considered and rejected: the file
  destination's staged-part publish protocol names and fsyncs whole parts, and
  a batch-spanning writer would break the replay/dedup path.

Also visible on the S3 cells: **6.72% in `ring` SHA-256** — SigV4 hashing every
request body. Worth checking whether `object_store` can be configured for
unsigned or streaming payloads over an endpoint you trust.

---

### F8 — Build profile and allocator: real CPU wins, little wall

**The workspace `Cargo.toml` has no `[profile.release]` section at all** —
`lto = false`, `codegen-units = 16`, `panic = "unwind"`.

`lto = "fat"` + `codegen-units = 1`, interleaved, 5 pairs, medians:

| cell | stock wall | LTO wall | stock CPU-s | LTO CPU-s | Δ CPU |
|---|---|---|---|---|---|
| pg-to-pg-1m | 1.96 s | 1.95 s | 1.53 | 1.32 | **−14%** |
| pg-to-s3parquet-1m | 1.61 s | 1.59 s | 1.12 | 1.10 | −2% |
| s3jsonl-to-s3parquet-200k | 0.98 s | 0.96 s | 0.88 | 0.85 | −3% |

**This is the clearest illustration of the F3 distinction**: LTO buys a real
14% of CPU on pg-to-pg and moves the wall clock by nothing, because that CPU was
not on the critical path — the process was blocked anyway. It is still worth
taking: embedders and CPU-constrained deployments get it directly, and it
compounds once F3 removes the blocking.

Its two plausible downsides both measured *favourably*, so there is no tension
with the embeddability constraints (20 hyperfine runs each, the recorded
cold-start protocol):

| binary | cold start (median) | size |
|---|---|---|
| stock release | 25.6 ms | 94 MB |
| **lto=fat, codegen-units=1** | **24.7 ms** | **79 MB (−16%)** |
| ipc-wal (F2) | 24.5 ms | — |

Cold start *improves* and the binary *shrinks*; the ≤40 ms gate is untouched.
The only real cost is build time (~2 min for the CLI here, versus ~50 s).

`panic = "abort"` should **not** go in `[profile.release]`. It changes the
semantics every embedder of the library inherits, and `cargo test` cannot use
an abort profile. Put it in a separate `[profile.dist]` used only for the
shipped CLI, where it is worth ~10 MB of unwind tables (plus ~20 MB more from
stripping symbols) off a 94 MB binary with no cold-start cost. Confirm first
that nothing in the CLI's documented exit-code taxonomy depends on unwinding.

`lto = "thin"` is the more defensible default than `"fat"` if build time is a
concern — most of the win with a fraction of the link cost. I measured `"fat"`;
`"thin"` should be re-measured rather than assumed.

**The allocator tuning is not free.** `crates/rdlt-cli/src/main.rs:38-42`
sets `M_ARENA_MAX = 2` and `M_TRIM_THRESHOLD = 128 KB`, with a comment stating
"no measured wall-time cost". Interleaved A/B against glibc defaults:

| cell | arena=2 wall | default wall | arena=2 RSS | default RSS |
|---|---|---|---|---|
| pg-to-pg-1m | 1.96 s | 1.90 s (−3%) | 150 MB | 176 MB (+17%) |
| s3jsonl-to-s3parquet-200k | 1.00 s | **0.91 s (−9%)** | 219 MB | 265 MB (+21%) |

So it costs up to **9% of wall** on the JSONL cell to save ~21% of RSS. That is
a defensible trade for an embeddable engine — but the comment claiming no
wall-time cost is now contradicted and should be updated with these numbers.
Note the `sys` time drops sharply with defaults (0.21 → 0.12 s), which points at
`M_TRIM_THRESHOLD` (aggressive return-to-OS, then re-faulting) rather than
`M_ARENA_MAX` as the wall-clock culprit. **The two knobs are separable and were
never measured separately** — worth one experiment before concluding.

An alternative allocator (mimalloc/jemalloc) is the obvious next question given
26% of cycles are in glibc malloc/free, but it adds a dependency to an
explicitly small engine and would need its own RSS measurement.

---

## 4. Smaller items, each worth doing

**A per-row allocation in the source decoder.**
`CopyDecoder::try_consume_tuple` (`source/copy_decode.rs:407`) allocates a
`Vec<Option<(usize, usize)>>` **per row** to hold field ranges:

```rust
let mut ranges: Vec<Option<(usize, usize)>> = Vec::with_capacity(self.plans.len());
```

That is 1M heap allocations per pg-to-pg-1m run, on the tokio I/O thread.
Hoisting it to a reusable field on the decoder (`self.ranges.clear()`) is a
handful of lines and removes them all. `CopyDecoder::feed` is 5.9% self-time and
this is a slice of it — worth ~3–5% of CPU together with the sibling per-row
copies in the same function, ~0% of wall today. S-effort, no correctness
surface, and it feeds the same allocator pressure as F5.

**`SET LOCAL work_mem` in the publish transaction.** The merge arms sort the
stage through a `DISTINCT ON` dedup subquery (`sqlcore/plan/arms.rs:95`). At
Postgres's default `work_mem = 4 MB` a 1M-row sort spills to disk. Measured on
the corrected dedup cell (3 runs each):

```
work_mem = 4MB    (default)   4.96 / 5.24 / 5.11 s
work_mem = 256MB              4.92 / 4.85 / 4.96 s
```

**−4%** (~200 ms). Smaller than a first-principles estimate suggests, but it is
one `SET LOCAL` inside the publish transaction, it is scoped to that
transaction so it cannot affect the rest of the server, and it will matter more
on wider rows and larger merges. Worth taking; do not oversell it.

---

## 5. Negative results

These sounded plausible and **measurement killed them**. They are recorded so
nobody spends time on them twice.

Measured directly:

| hypothesis | measured | verdict |
|---|---|---|
| Larger source batches amortise per-batch overhead | `batch_target_bytes` 8 / 32 / 64 MiB → 2.09 / 2.05 / 1.97 s (3 runs each) | **No effect.** Within noise. Don't tune it. |
| Opening a `COPY IN` per batch is expensive | 18 COPY statements, ~40 ms each = 1.37M rows/s server-side ingest | **Near the server's own limit.** Statement setup is not the problem. |
| The WAL's cost is its disk I/O | IPC swap recovers ~90% of the no-WAL upper bound | **It is the encoding, not the I/O.** |
| Removing `workdir:` disables the WAL | `Wal::record` still 23% of CPU in the control | **False** — `pipeline_spec.rs:261` defaults it to `.rdlt`. |

Killed by code inspection against the measured profile, before anyone builds
them:

| hypothesis | why it fails |
|---|---|
| The 64 MiB `byte_budget` throttles pipelining | Not the ceiling. The channel is not the constraint; the loader task is. |
| tokio is starved — 32 workers for ~4 tasks | The runtime is `multi_thread` with `worker_threads = available_parallelism` (32). Nothing is starved; the work is serial by construction. |
| Commit cadence publishes too often on the 1M cells | Cursor-less pg snapshot streams **never checkpoint**, so both 1M cells commit exactly once. |
| Concurrent per-table destination writes | `LoadSession::write` takes `&mut self` (`rdlt-connector/src/lib.rs:123`). Not reachable without an SPI change across every destination. |
| Postgres 15 `MERGE`, or splitting upsert into UPDATE-then-INSERT | Neither is expected to beat `INSERT … ON CONFLICT DO UPDATE` for this shape, and both break the golden-SQL pins for nothing. |
| Build Arrow string buffers directly to validate UTF-8 once per column | arrow-rs already does exactly this — one `str::from_utf8` over the whole values buffer. Already optimal. |
| Drop `__rdlt_arrival` from stage tables that are not merge targets | The stage schema is created before the write mode is known at that call site. |
| `ANALYZE` the stage after COPY so merge arms plan better | Postgres already handles this for the shapes in play. |
| `fillfactor` on upsert targets to enable HOT updates | The arithmetic does not pay: the merge rewrites most rows anyway. |
| `target-cpu=native` / `x86-64-v3` for the shipped binary | Wrong for a distributed binary, and the hot loops are already vectorised (`__memmove_avx512`, `blake3 avx512`, `sha256_hw`). |
| Consolidate parquet files to one writer per commit window | Breaks the file destination's staged-part publish and replay-dedup protocol. |
| uuid as `FixedSizeBinary(16)` instead of 36-char text | The Postgres wire form is 16 bytes either way; the saving would be in Arrow buffers only, and it changes the schema contract. |

---

## 6. Recommended sequence

Ordered by (measured value) ÷ (risk × effort). The first two are cheap and
mostly independent; the rest get easier once the serial path is shorter.

1. **F1 — fix the dedup cell and re-run the session.** Not a performance change
   at all: it corrects the public record. The engine already wins this cell
   2.6×. *(S)*
2. **F2 — make the WAL stop encoding Parquet.** Land the one-line
   `set_dictionary_enabled(false)` version first (no format change, ~85% of the
   win); then Arrow IPC with `WAL_FORMAT_VERSION` bumped if you want the rest.
   Measured −18%/−21% wall and −28% CPU on the 1M cells, output verified.
   Contained to two functions; the crash-sweep suite is the gate. Consider
   skipping the WAL entirely for all-Replace runs, which reaches the full
   ceiling. *(S)*
3. **F8 — add `[profile.release]`** with `lto = "fat"`, `codegen-units = 1`.
   −14% CPU on pg-to-pg, no wall change *yet*, and it improves cold start
   (25.6 → 24.7 ms) and shrinks the binary 16%. Separately, **re-measure
   `M_ARENA_MAX` and `M_TRIM_THRESHOLD` independently** and correct the
   "no measured wall-time cost" comment. *(S)*
4. **§4 + the shred allocation work — the free removals.** Hoist the per-row
   `ranges` Vec in the decoder; reuse the `content_hash` scratch buffer;
   memoize the child-table index; `SET LOCAL work_mem` in the publish
   transaction. Every one is byte-identical output, no format risk, and
   together they are worth several percent of CPU on the cells they touch.
   Then **transpose `build_batch` to row-major** — it is 12.4% inclusive of the
   JSONL cell and a good part of that is re-probing. *(S–M)*
5. **F5 — direct Arrow → binary-COPY encoding.** 2.3× on the encoder, ~7% of
   pg-to-pg, and it removes ~12M allocations per run. *(M)*
6. **F4 — stop writing Replace-mode rows twice.** ~350 ms, ~17% of pg-to-pg.
   Prefer COPY-into-target over table swap. Replace-only. *(M)*
7. **F7 — expose `WriterProperties`**, starting with
   `dictionary_page_size_limit` (cheap CPU win, no behaviour change for
   low-cardinality columns) and `compression` (currently impossible to set, and
   the reason rdlt writes 2.85× dlt's bytes). Also worth taking here:
   `UNSIGNED-PAYLOAD` for S3 bodies where the operator opts in, which the
   profile prices at ~3–4% of the parquet cell. *(S)*
8. **F3 — attack the serial path.** Start with parallel staging connections
   (the transactional boundary already permits it) and WAL/destination overlap.
   This is where the remaining ~3.5× lives, and it is the only item that
   requires real design work. *(L)*

Items 2–6 are largely additive on CPU. I deliberately do **not** offer a stacked
wall-clock estimate: F3 and F8 show that CPU reductions off the critical path
do not add up in wall-clock terms, and I have not measured the stack. The one
stack I did measure — F1 + F2 on the dedup cell — gave 14.7 s → 4.78 s.

---

## 7. What I could not measure, and what I'd do next

- **Blocked time attribution.** `perf_event_paranoid=2` is not writable in this
  container, so kernel/off-CPU time is invisible. The 30% idle in pg-to-pg is
  inferred from CPU-vs-wall and confirmed structurally by the N-scaling test,
  but I could not attribute it to specific await points. *Next*: an off-CPU
  profile (`perf record -e sched:sched_switch`, needs paranoid ≤ 1) or
  `tokio-console`.
- **rdlt's own ceiling vs the fixture's.** The N-scaling curve saturates at
  ~1.5M rows/s, but that may be the single Postgres container, not rdlt.
  *Next*: repeat against a tuned server or a non-Postgres destination.
- **Network-latency sensitivity.** Every commit runs a serial preamble — two
  `count(*)` probes plus one `EXISTS` per full-feed stage
  (`dest/commit.rs:396-441`) — before the publish steps. At the ~0.05 ms
  loopback RTT here that is invisible. At a realistic 1–5 ms RTT, a pipeline
  committing often (the JSONL cell commits 5×) would pay tens to hundreds of
  milliseconds. *Next*: re-run with `tc netem` adding 2 ms, which would also
  re-rank F3 and F5.
- **The merge SQL shape itself.** Once F1 is fixed, the dedup cell's real cost
  is one ~3.8 s server-side `INSERT … SELECT … ON CONFLICT DO UPDATE` over 1M
  staged rows — still ~75% of that cell. A dedicated pass over
  `sqlcore/plan/arms.rs` concluded that neither Postgres 15's `MERGE` nor a
  split UPDATE-then-INSERT should beat the current shape, and both would break
  the golden-SQL pins for nothing; `SET LOCAL work_mem` (§4) is the one thing
  that measurably helps, at −4%. So this is *mostly* closed — but it remains
  the single largest unexplained number in the matrix and deserves an
  `EXPLAIN (ANALYZE, BUFFERS)` before it is written off.
- **Redundant dedup-subquery evaluation.** The scd2 arm interpolates the
  `deduped(…)` subquery into three separate statements and the hard-delete
  upsert arm into two, so Postgres re-executes the sort each time. Zero effect
  on all five measured cells (none uses scd2 or hard-delete), but it is a real
  multiplier for anyone who does. A CTE or a materialised temp table would fix
  it.

---

## Appendix — reproducing this

Fixtures were brought up exactly as `benches/bench-setup.sh` does (postgres:16
on 5439 seeded from `benches/fixtures/seed_pg.sql`, RUSTFS on 19110 seeded with
200k nested JSONL). Fixture identity matched the recorded session:
`events` md5 `e840f51738a6b4b15f9f085ea85e3df8`, `events_v2`
`7e208273f4d5333658fff2fa1c9839d9`.

Pipelines were the committed `benches/cells/pipelines/*.yaml` with the harness's
template variables substituted, run through `target/release/rdlt run`, with
rdlt's destination schemas dropped and recreated before every single run
(mirroring `fixtures.toml`'s `@reset_dest_schemas`).

Experimental binaries were built from one-line patches, measured, and the
patches reverted immediately; `git status` is clean of them. **This file is the
only change to the repository.** Two pre-existing working-tree deletions
(`BENCH_REFINMENT.md`, `REFACTORING.md`) were present before this work started
and were left untouched.

The fixture's `log_min_duration_statement` was set to 0 for the server-side
decomposition and restored to `-1` afterwards.
