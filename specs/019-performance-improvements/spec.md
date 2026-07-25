# Feature Specification: Performance Improvements — Measured Wins and the Serial-Path Ceiling

**Feature Branch**: `019-performance-improvements`

**Created**: 2026-07-25

**Status**: Draft

**Input**: User description: "performance improvements as per PERF_ANALYSIS.md (this is greenfield take so better option should stay and old one cleaned up)"

## Source Document

The authoritative evidence base is `PERF_ANALYSIS.md` at the repo root, produced
2026-07-25 against commit `270c903` on the recorded 018 bench fixtures. Every
number this specification cites comes from that document, which is itself
measurement-first: profiles taken with a frame-pointer build, wall/CPU/RSS from
interleaved A/B pairs, server-side attribution from per-statement logging,
isolated component costs from microbenchmarks pinned to the workspace's own
crate versions, and — for the write-ahead log — a fully implemented, executed,
output-verified change that was then reverted.

This specification defines the outcomes and how completion is judged. The
analysis holds the item-level detail, the file:line anchors, and the reasoning
behind each magnitude. Its findings are referenced here as **F1**–**F8**.

Two properties of the source document carry into this feature as obligations:

- **Its negative results are binding.** `PERF_ANALYSIS.md` §5 records fourteen
  rejected hypotheses — four killed by direct measurement, ten by code
  inspection against the measured profile. They are adopted here as recorded
  non-goals (see *Non-Goals*) so they are not re-litigated.
- **Its uncertainties are named.** Where the analysis says a magnitude is
  inferred rather than measured, this feature inherits the obligation to run
  the confirming experiment before acting on it, not to assume the estimate.

### Baseline of record

All deltas in this specification are relative to this baseline, measured on the
recorded fixtures. Re-establishing it on the implementation machine is the
first obligation of the feature.

| cell | wall | CPU-s | %CPU (32 cores) | peak RSS |
|---|---|---|---|---|
| pg-to-pg-1m | 2.02 s | 1.61 | 70% | 150 MB |
| pg-to-s3parquet-1m | 1.63 s | 1.13 | 70% | 158 MB |
| s3jsonl-to-pg-200k | 1.14 s | 0.97 | 89% | 198 MB |
| s3jsonl-to-s3parquet-200k | 0.96 s | 0.87 | 90% | 219 MB |
| pg-to-pg-dedup-1m | 14.7 s | 4.7 | 32% | 290 MB |

Competitor baseline re-measured in the same session: dlt 1.29.0 (connectorx) at
12.55 s / ~800 MB on the dedup cell and 1.68 s / 510 MB on the parquet cell.

### Decisions adopted at specification time

These were open questions in the analysis. They are settled here and are not
re-opened during planning.

- **D1 — Greenfield replacement.** Where a measured-better option exists it
  REPLACES the current one. No compatibility shims, aliases, dual code paths,
  or feature flags that keep a superseded path alive. The superseded code is
  deleted in the same change that introduces its replacement.
- **D2 — Write-ahead logging stays on for every run.** The segment encoding
  changes; the guarantee does not. Skipping the log entirely for all-Replace
  runs was considered and **rejected**: it buys only a further ~4–6% over the
  encoding change (1.65 s → 1.55 s on pg-to-pg-1m) while making crash recovery
  a full source re-extraction, which is cheap against a local database and
  expensive against a rate-limited or paid-per-request source.
- **D3 — Segment encoding abandons the columnar analytics format.** The
  replacement is a streaming record-batch container that performs no dictionary
  construction, no run-length encoding, and no page-statistics computation. The
  displaced writer and reader are deleted, the format version is bumped, and the
  segment file extension is renamed to match. No fallback reader is retained.
- **D4 — Default parquet compression becomes `snappy`** for the file and
  Iceberg destinations, replacing the uncompressed default. This is the parquet
  ecosystem's conventional default and the one the reference implementation
  writes, which makes the parquet benchmark cell a like-for-like artifact
  comparison for the first time.
- **D5 — The parallelism work (F3) is fully in scope**, including a breaking
  change to the destination session write interface if the design requires one.
  This **opens the recorded 0.2 → 0.3 semver window** that features 014 and 017
  named but deliberately left closed. Opening it is an explicit, recorded act of
  this feature.
- **D6 — Row identity values remain byte-identical.** `_rdlt_id` is persisted.
  Replacing the identity hash function is rejected regardless of measured gain;
  only changes that leave every emitted identity byte-for-byte unchanged are
  admissible on that path.

## User Scenarios & Testing *(mandatory)*

Each story is an independently mergeable increment: it can be developed,
measured, and merged on its own with the full gate green, and it delivers a
recorded improvement (or a corrected record) by itself.

### User Story 1 - The benchmark measures what it claims (Priority: P1)

A maintainer reads the published matrix and every cell compares equivalent work
across the three products. Today one does not: the keep-in-sync cell delivers
three source streams where it claims one, because the source discovers every
table in the schema in addition to the declared query stream. The competitor
arm moves a third of the rows. The cell's row-count check only inspects the one
table it intends, so the extra work is invisible to the harness. Correcting the
cell turns the matrix's only recorded loss into a win, and the configuration
hole that allowed the mistake is closed so no future cell can repeat it.

**Why this priority**: it is the single largest recorded number in the matrix,
it corrects a public claim rather than merely improving one, and it requires no
engine optimization at all. Every later story's measurements are compared
against a matrix that must first be telling the truth.

**Independent Test**: the corrected cell is run three-way on a quiet machine;
the destination contains exactly the tables the cell declares and no others;
the recorded result changes from a loss to a win; the published results page and
the enforcement bars are updated with a governance entry explaining why.

**Acceptance Scenarios**:

1. **Given** the corrected keep-in-sync pipeline, **When** a load runs,
   **Then** the destination holds exactly one populated table — the one the
   cell verifies — with the expected row count, and no undeclared table exists.
2. **Given** any cell in the matrix, **When** the harness validates it before
   running, **Then** a cell whose delivered stream set does not match its
   declared stream set is rejected with a message naming the surplus streams.
3. **Given** a source configuration that declares query streams and no tables,
   **When** the author intends no table discovery, **Then** the configuration
   vocabulary can express that intent directly, and the resulting run delivers
   only the declared queries.
4. **Given** the re-run three-way session, **When** the results page is
   regenerated, **Then** the keep-in-sync cell reports a win against the
   reference implementation, and a governance entry records the correction, the
   prior recorded value, and why the earlier number was not comparable.
5. **Given** the corrected matrix, **When** enforcement bars are reviewed,
   **Then** any bar affected by the correction is re-derived from the new
   recorded session floor, or its absence is recorded in the policy log.

---

### User Story 2 - Crash safety stops costing a quarter of the engine (Priority: P1)

An operator runs a 1M-row pipeline and it completes noticeably faster, with
lower peak memory, while losing none of its crash-recovery guarantees. Today
nearly a quarter of the engine's processor time is spent encoding the
write-ahead log's staged batches into a columnar analytics format — building
dictionaries and computing statistics for files that are deleted seconds later,
on the critical path between receiving a batch and handing it to the
destination. The log keeps doing its job; it stops paying for capabilities a
scratch buffer never uses.

**Why this priority**: the largest measured engine win in the analysis
(−18% and −21% wall, −28% processor time, −20% peak memory on the two 1M-row
cells), already implemented and output-verified end to end, contained to two
functions, and it shortens the serial path that Story 9 later parallelises.

**Independent Test**: the two 1M-row cells are re-measured before and after on
the same machine; the improvement meets its target; the crash-point sweep suite
passes at every write, commit, and receipt-visible point; a log written by the
new code replays correctly and a log written by the old code is rejected or
discarded loudly rather than misread.

**Acceptance Scenarios**:

1. **Given** the 1M-row relational copy, **When** it runs to completion, **Then**
   wall time falls at least 15% and peak memory at least 15% against the
   baseline of record, with the destination row count unchanged.
1b. **Given** the 1M-row relational-to-lake extract, **When** it runs to
   completion, **Then** wall time falls at least 15% and peak memory at least
   **8%** against the baseline of record.

   *Corrected at plan time.* The two cells do not move together and a single
   memory figure was wrong for one of them: measured, the relational copy goes
   150 → 121 MB (−19%) while the lake extract goes 158 → 143 MB (−9.5%). The
   original single criterion (≥ 12% on "a 1M-row pipeline") was unmeetable on
   the second cell by this increment alone.
2. **Given** a crash injected at each recorded crash point, **When** the run is
   recovered, **Then** the recovered load publishes exactly once with no
   duplicates, as the sweep suite already requires.
3. **Given** a recovery log left by a build that predates this change,
   **When** recovery reads it, **Then** it is refused as an unsupported format
   version and recovery degrades to source re-extraction with the reason logged
   — never silently misread.
4. **Given** the shipped code after the change, **When** the tree is searched
   for the displaced segment writer and reader, **Then** neither exists: there
   is one segment format and no fallback path.
5. **Given** any run, **When** the write mode of its streams is Replace,
   **Then** write-ahead logging is still performed — the guarantee is uniform
   across write modes.

---

### User Story 3 - Shipped builds are actually optimized (Priority: P2)

A user installing the CLI, and an application embedding the library, get a
binary built with the optimization settings the project intends rather than the
toolchain's untuned defaults. The project has never declared a release build
profile at all, so link-time optimization is off and code generation is split
into sixteen units. Turning both around is measured to cut processor time
materially, shrink the binary, and — contrary to the usual expectation —
*improve* cold start.

**Why this priority**: the cheapest item in the feature by a wide margin, it
benefits every downstream consumer immediately, and it compounds with every
other story. It is scheduled early precisely because it is nearly free.

**Independent Test**: build with the declared profile, re-measure the cells,
the cold-start guard, and the binary size; all three move in the intended
direction or are recorded as unchanged.

**Acceptance Scenarios**:

1. **Given** the declared release profile, **When** the 1M-row Postgres cell is
   measured, **Then** processor time falls at least 10% against the baseline of
   record.
2. **Given** the same build, **When** the cold-start guard runs, **Then** the
   measured median is at or below the existing bound and no worse than the
   baseline of record.
3. **Given** the same build, **When** the shipped binary is measured, **Then**
   its size is smaller than the baseline of record and the value is recorded.
4. **Given** an application embedding the library, **When** it builds against
   the workspace, **Then** it does not inherit a panic strategy chosen for the
   distributed command-line binary; any such setting lives in a profile used
   only for distribution.
5. **Given** the allocator tuning in the command-line binary, **When** its two
   knobs are measured independently, **Then** the memory-versus-wall-time trade
   each one makes is recorded, the shipped setting is chosen from that evidence,
   and the code comment describing it matches what was measured.

---

### User Story 4 - The hot loops stop allocating once per value (Priority: P2)

A pipeline moving relational data spends a quarter of its processor time inside
the memory allocator rather than moving bytes. The destination's wire encoder
builds two vectors per row and heap-allocates every individual value — roughly
twelve million allocations for a 1M-row load — and copies every text value out
of its source buffer before sending it. The source decoder allocates a scratch
vector once per row. Replacing the encoder with one that writes wire bytes
directly from the columnar buffers is measured at 2.3× the throughput with
byte-identical output.

**Why this priority**: a large, well-understood processor-time reduction on the
most common pipeline shape, with an existing round-trip oracle (the source's own
decoder) to prove byte-identity. It is sequenced after Story 2 because both sit
on the same serial path and their measurements would otherwise confound.

**Independent Test**: the new encoder's output is compared byte-for-byte against
the displaced one for a batch covering every supported column type including
nulls and boundary values; the cell is re-measured; the blocking-wait count is
re-counted.

**Acceptance Scenarios**:

1. **Given** a batch covering every supported column type, null and non-null,
   **When** both encoders run over it, **Then** their output is byte-identical.
2. **Given** a 1M-row load, **When** it completes, **Then** the destination's
   contents are unchanged and processor time falls at least 15% against the
   baseline for that stage.
3. **Given** the same load, **When** blocking waits are counted, **Then** the
   count falls by at least an order of magnitude against the recorded 113,552.
4. **Given** the shipped code, **When** the tree is searched for the displaced
   per-value boxing path, **Then** it does not exist.
5. **Given** a batch containing a value the encoder cannot represent,
   **When** the load runs, **Then** it fails with a typed error naming the
   column — never with a corrupted or truncated wire stream.

---

### User Story 5 - Full-refresh loads stop writing every row twice (Priority: P2)

A full-refresh load into a relational destination currently sends every row to
the server once, then makes the server copy all of them again from the staging
area into the target inside the publish transaction. On the 1M-row cell that
second copy alone is over a third of the elapsed time. Publishing without the
re-copy removes it, while keeping the publish atomic: readers see either the
whole previous contents or the whole new contents, never a partial state.

**Why this priority**: a large, purely server-side saving on the flagship
relational cell, isolated to one write mode. It is deliberately scheduled after
the client-side stories so its server-side effect is measured against a client
that is no longer the bottleneck.

**Independent Test**: the full-refresh cell is re-measured; a concurrent reader
observes only complete states throughout the publish; the crash sweep confirms
the publish is still all-or-nothing; merge-mode behaviour is untouched.

**Acceptance Scenarios**:

1. **Given** a full-refresh load, **When** it publishes, **Then** the elapsed
   server-side publish cost falls by at least 40% against the baseline for that
   stage, and total cell wall time falls by at least 10%.
2. **Given** a reader querying the target continuously during a full-refresh
   publish, **When** the publish commits, **Then** the reader observes the prior
   contents in full or the new contents in full, and never a mixture or an empty
   intermediate state.
3. **Given** a crash injected at each publish crash point, **When** the load is
   recovered, **Then** it publishes exactly once, and no rows from a rolled-back
   attempt survive.
4. **Given** a merge-mode load, **When** it publishes, **Then** its behaviour
   and emitted statements are unchanged by this story.
5. **Given** a target carrying indexes, constraints, grants, or dependent
   objects, **When** a full-refresh load publishes, **Then** all of them survive
   the publish intact.

---

### User Story 6 - Semi-structured ingestion gets faster without changing a single identity (Priority: P2)

The nested-document pipeline — the engine's widest lead over the reference
implementation — spends about a quarter of its processor time computing row
identities and about an eighth assembling output batches by walking every row
once per column. Both can be reduced: scratch buffers can be reused instead of
reallocated per row, per-document allocations can be collapsed, repeated lookups
can be memoized, and batch assembly can resolve each row's fields in one pass
instead of one pass per column. Every emitted identity stays byte-for-byte what
it is today — which, as plan-time analysis established, also bounds the prize:
the hash input is length-prefixed, so the canonical rendering cannot be
eliminated while identities are frozen, only stop being reallocated (FR-029).

**Why this priority**: real processor-time reduction on the flagship
differentiating pipeline with no format risk whatsoever, but a smaller
wall-clock effect than the earlier stories because that pipeline is already
running near one full core.

**Independent Test**: the existing identity property tests pass unchanged; a
corpus of documents produces byte-identical identity values before and after;
the cell is re-measured.

**Acceptance Scenarios**:

1. **Given** a corpus spanning nested objects, arrays, scalar lists, nulls,
   absent fields, and duplicate keys, **When** identities are computed before
   and after, **Then** every emitted identity value is byte-identical.
2. **Given** the nested-document cell, **When** it runs, **Then** processor
   time falls at least 10% against the baseline of record with output unchanged.
3. **Given** the identity property tests, **When** they run, **Then** they pass
   without modification to their assertions.
4. **Given** a stream that declares a primary key, **When** it is ingested,
   **Then** the cheaper keyed identity path is used, and the documentation
   states plainly what declaring a key costs and saves.

---

### User Story 7 - Users can choose what their output files look like (Priority: P3)

A user writing to a lake today gets uncompressed files with dictionary encoding
always on, and cannot change either. The consequence is measurable in two ways:
the engine writes 2.85× the bytes the reference implementation writes for the
same rows, and on high-cardinality columns the dictionary builder is the single
largest processor cost in the pipeline. Output-format properties become
configurable with sensible defaults, and the benchmark's parquet cell starts
comparing equivalent artifacts.

**Why this priority**: it closes a genuine product gap — there is currently no
way to ask for compressed output at all — and it fixes a benchmark
comparability problem. It is a smaller wall-clock story than the earlier ones,
and the default change alters what users' existing pipelines write, so it
benefits from landing after the engine work is settled.

**Independent Test**: a pipeline writes output with non-default settings and the
resulting files carry them; the default output is compressed; the parquet cell
is re-measured and its artifact size recorded alongside the competitor's.

**Acceptance Scenarios**:

1. **Given** a destination with no output-format settings specified, **When** it
   writes, **Then** the files are compressed with the adopted default.
2. **Given** a destination configured with explicit output-format settings,
   **When** it writes, **Then** the files carry exactly those settings.
3. **Given** a source with high-cardinality text columns, **When** it is written
   with the shipped defaults, **Then** processor time attributable to output
   encoding falls at least 25% against the baseline of record.
4. **Given** the parquet benchmark cell, **When** it is re-run, **Then** both
   arms produce comparably-encoded artifacts, and the cell records the bytes
   each product wrote alongside its elapsed time.
5. **Given** an unsupported combination of output-format settings, **When** a
   pipeline is configured with it, **Then** it is rejected at configuration time
   with a typed error naming the offending setting.

---

### User Story 8 - The small, safe wins are taken (Priority: P3)

A collection of individually minor costs, each with no correctness surface and
no format risk: a per-row scratch allocation in the source decoder, a
server-side sort that spills to disk because it runs under a default working
memory limit, and merge strategies that make the server evaluate the same
deduplication subquery two or three times per publish because it is interpolated
into each emitted statement separately.

**Why this priority**: individually below the threshold that would justify their
own story, collectively worth taking, and each is small enough to review in one
sitting. The last of them affects no benchmarked cell but is a real multiplier
for the strategies that use it.

**Independent Test**: each item is measured on the cell or workload it affects;
those affecting no benchmarked cell are measured on a purpose-built workload and
the number recorded.

**Acceptance Scenarios**:

1. **Given** a 1M-row relational read, **When** it runs, **Then** the decoder
   performs a constant number of scratch allocations rather than one per row.
2. **Given** a large merge publish, **When** it runs, **Then** its sort operates
   within the transaction's own working-memory setting, and the setting does not
   leak beyond that transaction.
3. **Given** a merge strategy that emits more than one statement over the
   deduplicated staging set, **When** it publishes, **Then** the deduplication
   is evaluated once per publish rather than once per statement, and the
   published result is unchanged.
4. **Given** any change to emitted statements, **When** the golden statement
   pins run, **Then** they have been deliberately re-pinned and the change is
   visible in the diff.

---

### User Story 9 - One pipeline uses the machine the way four do (Priority: P3)

An operator runs one pipeline on a large machine and it uses less than a single
core. Running eight of the same pipeline side by side moves 4.2× the rows,
which proves the limit is inside the engine rather than in the server, the
network, or the disk. The engine's stages are serial by construction: one
source task feeds one shredder that processes one slab at a time, feeding one
loader that owns one destination session writing over one connection. Making a
single pipeline overlap those stages is the largest remaining opportunity in
the engine.

**Why this priority**: the largest potential gain in the analysis and the only
item that is not yet designed. It is scheduled last deliberately: the earlier
stories shorten the serial path it must parallelise, so its design should be
derived from the post-improvement baseline rather than today's. Its measured
ceiling is also the least certain number in the analysis — the saturation point
may belong to the benchmark's server rather than to the engine — so it opens
with a confirming measurement.

**Independent Test**: after the design measurement establishes the real ceiling,
a single pipeline's throughput is compared against the baseline of record and
against the concurrent-pipeline curve; ordering, exactly-once, and backpressure
properties are re-verified under concurrency.

**Acceptance Scenarios**:

1. **Given** the post-improvement baseline, **When** the ceiling is
   re-measured against a destination that does not itself saturate, **Then** the
   attainable single-pipeline throughput is recorded, and the design targets
   that number rather than the one measured against the benchmark fixture.
2. **Given** a single **merge-mode** pipeline on a multi-core machine, **When**
   it runs, **Then** its throughput improves by at least 50% against the
   baseline of record and its processor utilisation exceeds one core. Whether
   full-refresh loads can reach the same is answered by the ceiling measurement,
   not assumed (see SC-005).
3. **Given** a load whose stages now overlap, **When** it publishes, **Then**
   per-table row ordering is preserved exactly as before.
4. **Given** a load whose staging is performed over more than one connection,
   **When** a merge deduplicates the staged rows, **Then** the last-writer-wins
   outcome is identical to the single-connection result, and the rule that makes
   it so is stated in the code.
5. **Given** a crash injected at each recorded crash point under concurrency,
   **When** the load is recovered, **Then** it publishes exactly once.
6. **Given** memory pressure, **When** a downstream stage falls behind,
   **Then** backpressure still bounds peak memory, and peak memory does not
   exceed the baseline of record by more than 25%.
7. **Given** a change to the destination session interface, **When** the version
   window is reviewed, **Then** the break is recorded against the previously
   named version transition, and every bundled destination is updated in the
   same change.

---

### Edge Cases

- **A recovery log written by an older build** is encountered after upgrade. It
  must be refused loudly by version and recovery must degrade to source
  re-extraction, never be misread as the new format.
- **A crash between the recovery log becoming durable and the destination
  acknowledging the commit** — the canonical redelivery window — must still
  replay to exactly one published copy, under every story that touches the
  commit protocol.
- **A full-refresh publish that spans more than one commit unit** cannot use a
  single-shot publish shape; the behaviour when a load exceeds one commit unit
  must be defined rather than assumed.
- **A full-refresh target that other database objects depend on** must survive
  the publish with those dependencies intact.
- **A value that the wire encoder cannot represent** (out-of-range timestamp,
  malformed identifier, oversized decimal) must produce a typed error naming the
  column rather than a desynchronised wire stream.
- **A source whose schema drifts mid-load** must continue to behave exactly as
  today under every rewritten hot path.
- **An empty batch or an empty stream** must still produce a well-formed
  segment, a well-formed wire stream, and a materialised destination table with
  correct types.
- **A pipeline configured to discover no tables and no queries** must be
  rejected at configuration time with a message saying so, not run as a silent
  no-op.
- **Concurrent staging combined with schema evolution** — a schema delta
  arriving while more than one staging write is in flight — must preserve the
  rule that a delta is applied before any batch at the new version.
- **A machine with a single core**, or a container with a restrictive processor
  quota, must not regress: added concurrency must not cost throughput where
  there is no parallelism to exploit.

## Requirements *(mandatory)*

### Functional Requirements

**Evidence and governance (cross-cutting)**

- **FR-001**: Every increment MUST land with a before/after measurement on the
  affected benchmark cells, recorded as harness evidence. An assertion of
  improvement without a recorded measurement MUST NOT be accepted as done.
- **FR-002**: The baseline of record MUST be re-established on the
  implementation machine before the first increment, and any deviation from the
  figures in this specification MUST be recorded rather than silently adopted.
- **FR-003**: Any increment that changes emitted database statements MUST
  re-pin the golden statement fixtures, and the re-pinning MUST be visible as a
  reviewable diff.
- **FR-004**: Every increment that touches recovery, the commit protocol, or
  publish atomicity MUST pass the crash-point sweep suite with duplicate-free
  verification.
- **FR-005**: The cold-start bound MUST remain satisfied at every increment.
- **FR-006**: Where a measured-better implementation replaces an existing one,
  the superseded implementation MUST be deleted in the same change. Compatibility
  shims, aliases, dual paths, and flags that preserve a superseded path MUST NOT
  be introduced.
- **FR-007**: No increment may require `unsafe` code. A candidate that cannot be
  expressed safely MUST be rejected regardless of its measured gain.
- **FR-008**: Each increment MUST leave the full workspace gate green when
  merged.

**Benchmark integrity**

- **FR-009**: The keep-in-sync benchmark cell MUST deliver exactly the streams
  it declares, and its destination MUST contain no table the cell does not
  declare.
- **FR-010**: The benchmark harness MUST reject any cell whose delivered stream
  set does not match its declared stream set, naming the surplus streams.
- **FR-011**: The source configuration vocabulary MUST be able to express
  "deliver the declared query streams and discover no tables". A configuration
  that declares neither tables nor queries MUST be rejected at configuration
  time.
- **FR-012**: After the corrected cell is re-measured three-way, the published
  results page and the enforcement bars MUST be updated, and a governance entry
  MUST record the correction, the superseded value, and why it was not
  comparable.

**Recovery log**

- **FR-013**: Recovery-log segment encoding MUST NOT construct dictionaries,
  compute page statistics, or apply run-length encoding. Its cost MUST be
  proportional to the bytes staged, not to the cardinality of their values.
- **FR-014**: The recovery-log format version MUST be incremented, and a log
  written by an unsupported version MUST be refused with the reason logged,
  degrading to source re-extraction.
- **FR-015**: Write-ahead logging MUST be performed for every run regardless of
  write mode. No configuration or write mode may disable it.
- **FR-016**: Recovery-log segment writing MUST NOT block the asynchronous
  runtime thread that drives the load.
- **FR-017**: Exactly one recovery-log segment format MUST exist in the shipped
  code. No reader for the displaced format may remain.

**Relational destination**

- **FR-018**: The destination wire encoder MUST produce byte-identical output to
  the encoder it replaces for every supported column type, including nulls and
  representable boundary values, and this MUST be pinned by a test.
- **FR-019**: The wire encoder MUST NOT allocate per value or per row on the
  common path.
- **FR-020**: Encoded data MUST be handed to the transport in units large enough
  that the number of blocking waits per load falls by at least an order of
  magnitude.
- **FR-021**: A value the encoder cannot represent MUST produce a typed error
  naming the column, and MUST NOT emit a partial or desynchronised wire stream.
- **FR-022**: A full-refresh publish MUST NOT copy staged rows into the target a
  second time.
- **FR-023**: A full-refresh publish MUST remain atomic to concurrent readers:
  the prior contents in full, or the new contents in full, never a mixture and
  never an empty intermediate state.
- **FR-024**: A full-refresh publish MUST preserve the target's indexes,
  constraints, grants, and dependent objects.
- **FR-025**: Merge-mode publish behaviour and emitted statements MUST be
  unchanged by the full-refresh work.
- **FR-026**: A merge publish's sort MUST operate under a working-memory setting
  scoped to that transaction, which MUST NOT affect any other session.
- **FR-027**: A merge strategy that emits more than one statement over the
  deduplicated staging set MUST cause the deduplication to be evaluated once per
  publish rather than once per statement, with the published result unchanged.

**Semi-structured ingestion**

- **FR-028**: Every emitted row identity MUST be byte-identical to the value the
  current implementation emits, for roots and for children at every depth.
- **FR-029**: Row identity computation MUST NOT allocate a fresh buffer per
  row; the canonical rendering buffer MUST be reused across rows and children.

  *Corrected at plan time.* This requirement originally read "MUST NOT
  materialise a full canonical rendering of the row as an intermediate step".
  That is **unsatisfiable** jointly with FR-028 and decision D6:
  `RowIdBuilder::update_lp` (`crates/rdlt-core/src/identity.rs:61-64`) feeds
  the rendering's **length before its bytes**, so the total length must be
  known before any byte is hashed — a streaming walk cannot reproduce the same
  hash input without first producing the whole rendering. Since D6 freezes the
  emitted identity, the rendering stays and only its *allocation* is
  recoverable. See `research.md`.
- **FR-030**: Per-document scratch buffers and repeated lookups on the shred
  path MUST be reused or memoized rather than reallocated or re-derived per
  document.
- **FR-031**: Output batch assembly MUST traverse each row a constant number of
  times, independent of the column count.

**Output format**

- **FR-032**: Compression, dictionary encoding, dictionary size limit, row-group
  size, and page size MUST be configurable on the file and Iceberg destinations.
- **FR-033**: The default output compression MUST be `snappy`, and the default
  dictionary size limit MUST be low enough that high-cardinality columns abandon
  dictionary encoding rather than interning every distinct value. Uncompressed
  output and parquet's own dictionary sizing MUST remain expressible by explicit
  configuration.

  *Extended at plan time.* Adding compression alone makes encoder processor time
  strictly **rise**, which contradicts this story's own acceptance criterion; the
  measured 5.6× encoder win comes from the dictionary bail-out, not from the
  codec. The asymmetry is deliberate: a lower cap only affects columns whose
  dictionary would exceed it, so low-cardinality columns stay fully
  dictionary-encoded.
- **FR-034**: An unsupported or contradictory combination of output-format
  settings MUST be rejected at configuration time with a typed error naming the
  offending setting.
- **FR-035**: The parquet benchmark cell MUST either compare comparably-encoded
  artifacts across arms or state the difference in the cell's own note, and MUST
  record the bytes written by each product alongside its elapsed time.

**Build and runtime policy**

- **FR-036**: The workspace MUST declare an explicit release build profile.
- **FR-037**: A panic strategy chosen for the distributed command-line binary
  MUST NOT be inherited by applications embedding the library.
- **FR-038**: The two allocator tuning knobs MUST be measured independently,
  the memory-versus-wall-time trade of each recorded, the shipped setting chosen
  from that evidence, and the explanatory comment corrected to match what was
  measured.

**Parallelism**

- **FR-039**: Before the parallelism design is fixed, the attainable throughput
  ceiling MUST be re-measured against the post-improvement baseline and against
  a destination that does not itself saturate; the design MUST target the
  measured number.
- **FR-040**: A single pipeline MUST exceed one core of processor utilisation on
  a multi-core machine.
- **FR-041**: Per-table row ordering MUST be preserved under concurrency.
- **FR-042**: Where staging is performed over more than one connection, the
  deduplication outcome MUST be identical to the single-connection result, and
  the rule that guarantees it MUST be stated in the code.
- **FR-043**: Byte-bounded backpressure MUST continue to bound peak memory under
  concurrency.
- **FR-044**: Added concurrency MUST NOT reduce throughput on a single-core
  machine or under a restrictive processor quota.
- **FR-045**: If the destination session interface changes incompatibly, the
  break MUST be recorded against the previously named version transition, and
  every bundled destination MUST be updated in the same change.

### Key Entities

- **Benchmark cell**: one end-to-end pipeline comparison across products.
  Carries a declared stream set, a verification claim, and a note. Its declared
  stream set must match what runs.
- **Enforcement bar**: a threshold attached to exactly one cell, derived from a
  recorded session floor and justified by a governance entry.
- **Baseline of record**: the measured figures this feature improves against,
  and the reference for every claimed delta.
- **Recovery-log segment**: a durable, replayable copy of a staged batch, valid
  only until its commit is acknowledged. Carries a format version; never a
  source of truth.
- **Row identity**: the persisted, content-derived key that makes nested-subtree
  merge detect changed children. Its emitted value is frozen.
- **Golden statement pin**: the byte-exact record of the statements a strategy
  emits; the tripwire for unintended statement changes.
- **Output-format settings**: the per-destination description of how written
  files are encoded, newly user-controllable.

## Success Criteria *(mandatory)*

### Measurable Outcomes

- **SC-001**: The keep-in-sync benchmark cell reports a win against the
  reference implementation rather than a loss, with its destination containing
  only the tables the cell declares.
- **SC-002**: A 1M-row relational copy completes at least 25% faster than the
  baseline of record.
- **SC-003**: A 1M-row relational-to-lake extract completes at least 25% faster
  than the baseline of record.
- **SC-004**: Peak memory falls at least 15% against the baseline of record for
  the 1M-row relational copy, and at least 8% for the 1M-row relational-to-lake
  extract. *(Corrected at plan time from a single 15% figure covering both: the
  measured evidence puts the recovery-log change alone at −19% and −9.5%
  respectively, and no increment can claim 15% on the second cell by itself.)*
- **SC-005**: A single **merge-mode** pipeline sustains more than one core of
  processor utilisation and at least 50% more rows per second than the baseline
  of record.

  *Re-targeted at plan time, with a recorded consequence.* Story 5 removes the
  staging table for full-refresh loads (FR-022/FR-024 force it), and Story 9's
  parallelism lever depends on staging sitting outside the publish transaction.
  Full-refresh single-pipeline throughput therefore remains bounded by one
  bulk-load connection, and the "one pipeline uses the machine" outcome applies
  to merge workloads. Restoring it for atomic full refresh would need
  distributed commit across connections, which is out of scope here.
- **SC-006**: The nested-document pipeline uses at least 10% less processor time
  than the baseline of record while emitting byte-identical row identities.
- **SC-007**: Files written to a lake by default are compressed, and the volume
  written for the benchmark's 1M-row extract is within 25% of the reference
  implementation's for comparable settings.
- **SC-008**: Cold start remains at or below its existing bound on every merged
  increment.
- **SC-009**: The engine's advantage over the reference implementation improves
  or holds on every benchmarked cell; no cell regresses against its recorded
  value.
- **SC-010**: Every claimed improvement in the close-out is traceable to a
  recorded harness measurement; the verification matrix contains zero uncited
  claims.
- **SC-011**: Crash recovery publishes exactly once at every recorded crash
  point, under every increment, including under concurrency.
- **SC-012**: A search of the shipped tree finds no surviving copy of any
  implementation this feature replaces.

## Non-Goals

Recorded so they are not re-litigated. The first three were killed by direct
measurement, the rest by code inspection against the measured profile; all are
documented with their reasoning in `PERF_ANALYSIS.md` §5.

- Tuning source batch size. Measured across an 8× range with no effect.
- Reducing the number of staging transfer statements per load. Measured at the
  server's own ingest limit already.
- Treating the recovery log's disk cost as the problem. Measured: the cost is
  the encoding, and the encoding change recovers ~90% of the available win.
- Enlarging the inter-stage memory budget. Not the constraint; the loader is.
- Increasing asynchronous runtime worker count. Nothing is starved; the work is
  serial by construction.
- Changing commit cadence for the 1M-row cells. Those streams commit exactly
  once already.
- Replacing the merge statement shape with a database-native merge statement, or
  splitting it into separate update and insert statements. Neither is expected
  to win, and both break the golden pins for nothing.
- Building string buffers to validate text once per column. The library already
  does exactly this.
- Removing the staging arrival-order column for non-merge targets. The staging
  schema is created before the write mode is known at that point.
- Explicitly collecting statistics on the staging table after loading it.
- Adjusting target table fill factor to enable in-page updates.
- Targeting a specific processor microarchitecture for the distributed binary.
- Consolidating written files across commit windows.
- Changing the wire representation of identifier columns.
- **Replacing the row identity hash function.** Rejected by decision D6
  regardless of measured gain: the value is persisted.

## Assumptions

- The implementation machine can reproduce the baseline of record within the
  ~3% band the analysis achieved against the recorded session; if it cannot,
  re-establishing a local baseline (FR-002) is the correction.
- The benchmark's existing quiet-machine guard remains the arbiter of whether a
  recorded measurement counts as evidence.
- Improvements are additive on processor time but **not** assumed additive on
  wall time. The analysis demonstrates that processor-time reductions off the
  serial critical path do not convert to wall-clock; no stacked wall-clock
  target is claimed for that reason, and each story's target is stated
  independently.
- The percentage targets in the acceptance scenarios are floors taken from
  measured values with margin, not predictions. A story that measures better
  than its floor is not thereby incomplete elsewhere.
- Story 9's stated 50% single-pipeline improvement target is deliberately below
  the ~3.5× the concurrency experiment suggests, because that experiment's
  ceiling may belong to the benchmark's server; FR-039 requires the real ceiling
  to be established before the design is fixed, and the target may be revised
  upward at that point with evidence.
- Story 5's publish shape is left to planning. The analysis prices two
  candidates and prefers the one that avoids reconstructing the target's
  indexes, constraints, and grants; either is acceptable if it satisfies
  FR-022 through FR-025.
- Opening the recorded version window (D5) is expected to be exercised only if
  Story 9's design requires it. If the design lands without an incompatible
  interface change, the window stays closed and that outcome is recorded.
- Output-format settings are added to the file and Iceberg destinations only.
  Extending them to other destinations is out of scope.
- No new runtime dependency is assumed. Any that planning finds necessary must
  be justified against the small-core principle with registry facts.
