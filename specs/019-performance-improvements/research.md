# Phase 0 Research: feature 019

Eight designs were produced against the tree at `270c903` and the local cargo
registry cache, then each was handed to an adversarial reviewer whose brief was
to refute it. Both halves are recorded here: what was decided, and what the
review corrected. Where the review killed a decision, the killed decision stays
visible — a design that was considered and rejected on a fact is worth more to
the next reader than a clean list.

**Headline for the owner directive**: the review's single most useful
intervention was to *reject* a proposed dependency, not to add one. Off-the-shelf
is not a licence to take a crate without measurement — see §4.4.

---

## 0. Cross-cutting outcomes

### 0.1 The three headline changes cost zero new dependencies — confirmed

| change | mechanism | verification |
|---|---|---|
| Recovery-log container | `arrow::ipc::writer::FileWriter` / `reader::FileReader` | `arrow-58.3.0/Cargo.toml` `default = ["csv","ipc","json"]`; `pub use arrow_ipc as ipc` at `arrow-58.3.0/src/lib.rs:418`; exactly one `arrow-ipc 58.3.0` in `Cargo.lock`; `crates/rdlt-engine/Cargo.toml:12` already has `arrow` with default features |
| Binary-COPY value bytes | `postgres_types::ToSql::to_sql` on concrete borrowed values | already imported at `dest/encode.rs:9` |
| `snappy` output default | parquet's `snap` feature | `parquet 58.3.0` `default = [… "snap" …]`; workspace line has no `default-features = false`; `snap 1.1.2` in `Cargo.lock` |

**And one dependency is REMOVED**: `parquet` leaves `crates/rdlt-engine/Cargo.toml:14`.
Verified exhaustively — grep for `parquet` over `crates/rdlt-engine/` returns
only that manifest line, `wal/mod.rs:{1,16,141}`, `wal/resume.rs:{8,19}`, and the
unrelated test *name* `sweep_parquet_destination` at `tests/crash_sweep.rs:154`.
A direct Principle I win.

### 0.2 Three cross-story conflicts — see §9, they need decisions

The stories were designed independently and two pairs collide. This is the most
important output of Phase 0 and is escalated rather than silently resolved.

### 0.3 Instrument facts that constrain several stories

- **No iai benchmark touches the WAL.** `benches/perf-baselines.json` holds only
  `identity_*`, `passthrough_10k`, `shred_nested_10k`, `pg_copy_decode_10k`.
  Story 2 needs no baseline re-record.
- **Every bar is `kind = "ratio_vs"` with a `min_ratio` floor**, so a speed-up
  can never trip the bench gate — only a slow-down can.
- **`compare-iai.sh` is a one-sided gate**: `:105-106` appends a failure only
  when `delta > TOLERANCE`, so an improvement passes silently and leaves the
  baseline stale.
- **CI's perf-gate job already runs 39m19s** (verified: run `29958026979` at
  `6ab64bb`, 21:08:31 → 21:47:50). Anything that reduces build-artifact sharing
  in that job is expensive.

---

## 1. Story 1 — benchmark integrity

**The evidence was already committed.** `benches/results/pg-to-pg-dedup-1m.json`
records:

```json
"rdlt": { "rows": 3000000, "median_ms": 14813.79 },
"verify": { "table": "events_merged", "expected_rows": 1000000, "actual_rows": 1000000 }
```

Three million rows moved, one million verified, in the same artifact — and
nothing compared the two numbers. FR-010 is, at bottom, the assertion that
`rdlt.rows` and the verified total must be reconcilable. This is why the check
belongs in the harness rather than in a reviewer's attention.

**Decision**: redefine `tables: []` to mean *deliver the declared queries and
discover no tables*, deleting the rejection at
`crates/rdlt-connector-postgres/src/source/config.rs:567-570`. Under D1
(greenfield) the spelling changes meaning outright rather than gaining a
sibling. Reject a config that selects nothing at all (no tables **and** no
queries), and reject a `cdc:` block combined with `tables: []` in
`validate_cdc` — CDC with no tables captures nothing.

**Rationale**: the field is already `Option<Vec<TableConfig>>`; only `validate()`
behaviour moves. No generated-schema change, no public API change, no semver
event.

**Alternatives**: a new `discover: bool` field — rejected, it adds vocabulary
where redefining an already-rejected spelling costs nothing and D1 forbids two
spellings for one meaning.

**Decision**: the harness gets the delivered stream set from **`RunReport.tables`**,
which it already parses (`report_totals` / `report_table_rows`,
`crates/rdlt-bench/src/runner.rs`). No new engine channel, no CLI flag, no
destination catalogue query.

**Decision**: `Cell.verify` becomes a table→rows map (one `name = count` line per
table) and the check runs in two layers — load time in `Cell::check`
(`cells.rs:110-126`, rejecting a pipeline cell with no verify map before any
container starts) and run time in `run_once_subprocess` right after the report
parses.

**Decision**: guard the Trends delta in `report.rs` — when two compared points
carry different row counts, render the row counts instead of a percentage. A
wall-clock delta between runs that moved different volumes is not a speedup, and
the corrected dedup cell is exactly that case.

**Decision**: emit one `tracing::warn!` from `PostgresSource::streams()` when
`tables` is absent while `queries` are declared, naming the discovered tables and
the remedy. Self-contained wording, no citation IDs.

**Corrections from review** (7 of 13 decisions did not survive as written):
several file:line anchors were wrong, and the artifact `format_version` bump
must be coordinated — **Story 1 and Story 7 both proposed 2 → 3 independently**.
There is one bump, taken by whichever lands first, and the second story adds its
fields under the same version.

---

## 2. Story 2 — recovery-log segment format

**Decision**: Arrow IPC **file** format (`FileWriter` / `FileReader`), not the
stream format. Unbuffered `File::create`; no `BufWriter`.

**Rationale**: the file container has a validated footer, so a block-truncated
segment is *unrepresentable* rather than silently short. `FileReaderBuilder::build`
seeks to `End(-10)` and `read_footer_length` requires the trailing magic
(`arrow-ipc-58.3.0/src/reader.rs:1213-1219`, `:932-936`). The stream reader's
`read_meta_len` returns `Ok(None)` on `UnexpectedEof` (`:1859-1872`) — i.e. a
truncated stream *replays short and succeeds*, which is a silent-data-loss path
in a replay buffer. Price: 5.8 vs 5.9 ms/batch — noise.

**Review correction to the rationale** (the decision stands, its reason was
wrong): the original argument claimed `write_all` call boundaries make torn
segments likely. That conflates `write(2)` into the page cache with asynchronous
writeback, whose granularity is filesystem blocks. Also, a *failed* write cannot
produce a replay-visible torn segment at all, because `Wal::record` appends the
manifest `Segment` line only after `write_segment` returns (`mod.rs:144` then
`:147`) and replay follows manifest lines. **The genuine window** is: segment
and its manifest lines reach the page cache, no `sync_for_commit` yet, power
loss, and the file returns block-truncated at an offset that coincides with an
IPC message boundary. Lower probability than claimed — but real, silent, and
made unrepresentable by the file container for free.

**Decision**: `WAL_FORMAT_VERSION` 1 → 2; the resume gate changes from
`found > supported` to **exact match**, with its own scan outcome
`Scan::Unsupported { found, supported }` so a version refusal is distinguishable
from corruption without substring matching (Principle V).

**Review correction**: the claim "nothing else in the engine or its tests needs
updating" is false. The bump breaks a deliberate tripwire —
`resume.rs:282-286` asserts `WAL_FORMAT_VERSION == 1` with the message *"bump
deliberately, with a migration note"*. That assert is doing its job; it is part
of the change inventory, along with `resume.rs:307` (re-point
`matches!(run(V+1), Scan::Damaged(_))` at `Scan::Unsupported`), the doc at
`resume.rs:290-292`, the mutation-closure doc at `resume.rs:263-266`, and a new
match arm at `run.rs:353-356`. The true scope of the extension claim is
narrower: *nothing keys on the segment extension* (verified — `mark_committed`
unlinks exact paths, `clear` removes the whole directory, no globbing).

**Decision**: segment extension `.parquet` → `.arrow`.

**Decision (FR-016)**: make `Wal::record` / `sync_for_commit` / `mark_committed`
async and move only the disk-touching bodies into `spawn_blocking`, awaited
inline. Do **not** pipeline the WAL write against the destination write — the
manifest's on-disk order *is* the replay order.

**Review correction — this one is load-bearing**: `crash_point!` expands to
`fail::fail_point!($name, |_| { $err })` (`crates/rdlt-core/src/failpoint.rs:24`),
whose closure form is a **`return` from the enclosing function**. Moving a crash
point inside a `spawn_blocking` closure changes what it returns from, and under
the `panic` action moves the panic onto a blocking-pool thread. But
`crash_point!("wal.manifest.fsync")` sits at `mod.rs:175-181` — *inside* the
range the design proposed to offload as one closure. **`sync_for_commit`
therefore needs two hops, not one**: hop 1 = the `pending_sync` fsync loop plus
`manifest.flush()`; the crash point stays on the async side; hop 2 =
`manifest.sync_all()` on a `try_clone`d handle. The stated cost of "one hop per
commit" was wrong.

**Decision**: no special-casing for empty batches. Parquet's `ArrowWriter::write`
short-circuits `if batch.num_rows() == 0` (`parquet-58.3.0/src/arrow/arrow_writer/mod.rs:349-352`);
`FileWriter::write` has no such branch, so the swap *removes* a live-vs-replay
asymmetry. Pin it with a test; the mechanism that makes a zero-row batch survive
the round trip is `RecordBatchOptions::new().with_row_count(...)` at
`arrow-ipc-58.3.0/src/reader.rs:556`.

**Decision**: dictionary construction is not merely disabled but *unrepresentable*
on this path — `column_type_from_arrow` rejects `DataType::Dictionary` with a
typed error at `shred/passthrough.rs:287`, including nested inside a `Struct`,
and outgoing batches are rebuilt against the logical schema, never the source's
physical type. FR-013 holds structurally, not by configuration.

**Documents to amend** (review extended the inventory):
`specs/001-rdlt-ingestion-engine/contracts/persisted-formats.md:29` **plus a
migration note** in §2 (Principle IX requires the note, not just a restatement;
the section already carries an amendment header as precedent),
`specs/001-rdlt-ingestion-engine/data-model.md:142`,
`specs/001-rdlt-ingestion-engine/plan.md:29` and `:118`,
`2026-07-18-rdlt-engine-design.md:{44,122,312}`, and `wal/mod.rs:1`.

**Reconciliation with D3**: the spec's wording says "a streaming record-batch
container". The IPC *file* format is selected instead, and this supersedes that
phrasing — it is the arm `PERF_ANALYSIS` actually implemented, measured and
output-verified, and the stream format is rejected on the truncation argument
above.

---

## 3. Story 3 — build profile and allocator

**Decision**: declare `[profile.release]` with `lto = "fat"`, `codegen-units = 1`,
explicit `opt-level = 3`, adopted from a recorded 4-arm sweep (stock / cgu1-only
/ thin+cgu1 / fat+cgu1).

**Review correction**: pairing `lto = "thin"` with `codegen-units = 1` defeats
ThinLTO's whole advantage, which is that its LTO step parallelises across
codegen units. The sweep's thin arm must not pin cgu=1, or it answers nothing.

**Decision**: reject `panic = "abort"` entirely, in every profile. Add
`[profile.dist] inherits = "release"` carrying `strip = "symbols"` and nothing
else, plus a `make dist` verb.

**Rationale**: cargo silently forces unwind for test and bench units
(`UnitFor::new_test`, `PanicSetting::AlwaysUnwind`), so `PERF_ANALYSIS`'s stated
reason ("cargo test cannot use an abort profile") is imprecise — the real
objections are that library embedders inherit the setting and that the CLI's
documented exit-code taxonomy has not been shown to survive abort.

**Decision (contested)**: pin `[profile.bench]` to `lto = false,
codegen-units = 16` so the iai instruction-count baselines do not shift and
`perf-baselines.json` is untouched by this story.

**Review correction — this is a real cost, not a detail**: pinning
`[profile.bench]` to settings that *diverge* from `[profile.release]` means the
two share no build artifacts. CI's perf-gate job runs `make bench TARGET=iai`
and then needs a release binary; today they share, after the pin they do not.
Against a job that already takes **39m19s**, that is a large regression.
**Unresolved** — the alternative is to let the baselines shift and re-record
them deliberately (which PI1 explicitly permits, and which §0.3 shows is safe
because the gate is one-sided). See §9.3.

**Decision**: split the cold-start check out of `make bench TARGET=iai` into its
own verb. The review confirmed a latent break behind this: `212edf5` wired
cold-start into the iai target, `ci.yml:75-76` runs it, **no CI workflow installs
hyperfine**, and `check-cold-start.sh:25-28` exits 1 without it — plus the script
itself demands a quiet machine, which a CI runner is not.

**Decision (allocator)**: a 2×2 factorial (neither / arena only / trim only /
both) from one env-var-gated throwaway build.

**Rationale, and a genuinely new fact**: `main.rs:41` sets `M_TRIM_THRESHOLD` to
`128*1024` — **which is glibc's own default**. The call's only real effect is its
documented side effect: disabling glibc's dynamic mmap/trim threshold
adaptation. That explains the measured `sys` 0.21 → 0.12 s and makes the
factorial the right shape rather than a formality.

**Decision**: add **no** allocator crate. Recommend against mimalloc and
jemalloc in this story and probably permanently; record it as a bounded
follow-up whose measurement is only meaningful after Stories 4 and 6 land.
Note that the cheapest route to deleting the sanctioned `unsafe` is deleting the
`mallopt` call, not replacing the allocator.

**Review correction**: `build_profile: Option<String>` does not solve the
provenance problem it was introduced for — after this story every release
measurement is profile *"release"* whether `[profile.release]` says `lto = "fat"`
or says nothing, so a stock artifact and an LTO artifact remain
indistinguishable. Provenance must record the codegen *settings*, not the
profile name.

---

## 4. Story 4 — the Postgres COPY encoder

**4.1 Encoder layer — decided: `ToSql::to_sql` on concrete borrowed values.**

Per column the encoder matches once on the wire kind and calls the concrete impl
(`<&str as ToSql>::to_sql(&s, &Type::TEXT, buf)`), so every call is monomorphic
and inlinable and there is **no `dyn` anywhere**. `postgres-protocol` is **not**
adopted as a direct dependency: `ToSql` gets the same bytes with zero new
dependencies, and a second independently-versioned third-party surface in a crate
Principle III says wraps its driver at one boundary is a cost with no benefit.

**4.2 Borrowed cell representation — decided: a per-column enum of typed array
references**, built once per batch and indexed per row:

```rust
enum ColumnView<'b> { Bool(&'b BooleanArray), Int8(&'b Int64Array), Text(&'b StringArray), … }
```

There is no per-cell value type at all — the enum arm *is* the encode decision.
This removes the per-cell `Box`, the two per-row `Vec`s, the `String::to_owned`
for text, the per-cell `downcast_ref`, and the virtual `is_null`.

**4.3 Framing and buffering — ours, and the only hand-written part.**
One reused `BytesMut`; after each completed tuple, flush at 64 KiB into
`CopyInSink<Bytes>`; trailer then `finish()`.

**Review correction to the mechanics** (the shape is right, four API claims were
wrong): `Client::copy_in<T, U>(&self, statement: &T)` takes the *statement* type
first, so the call is `client.copy_in::<_, Bytes>(&sql).await?`, and the sink
must be pinned (`futures::pin_mut!`, exactly as `commit.rs:353` already does)
and driven as `sink.as_mut().feed(chunk).await?`.

**Review correction to the constant**: 64 KiB was justified by pointing at the
CLI's `M_TRIM_THRESHOLD`. That is a CLI-only setting which library embedders do
not get, *and* Story 3 is actively re-measuring it. The chunk size must be
justified on its own terms.

**4.4 UUID — the `uuid` crate is REJECTED. This is the owner directive working
in both directions.**

The design proposed adopting `uuid::Uuid::try_parse` and deleting the
hand-written `parse_uuid_text`. The review killed it on four grounds and the
kill is accepted:

1. **No measured benefit.** `PERF_ANALYSIS` §F5 attributes the 2.3× to boxes,
   Vecs, `to_owned`, the per-cell downcast and the virtual `is_null`. UUID
   parsing appears nowhere in the profile or the cost table. FR-001 and D1's
   "measured-better" test are not met.
2. **It is a genuinely new dependency.** `uuid` has no `[workspace.dependencies]`
   entry; `rdlt-connector-postgres` builds without iceberg. Principle I's
   default answer is no.
3. **It narrows accepted input.** `Uuid::try_parse` accepts a smaller set than
   both today's parser and PostgreSQL's own `uuid_in`. Principle IV forbids
   silent narrowing of semantics; no FR authorises this one and there is no
   migration note.
4. **It invalidates pinned assertions** (`encode.rs:476-482`) for no gain.

**Instead**: fix the real defect in the existing parser in place — it accepts a
hyphen at a position it should not — by rejecting a hyphen whose `hex_seen`
equals the last boundary at which one was consumed. No dependency, no narrowing,
no pin change.

*The general rule this establishes*: "use off-the-shelf crates" means **do not
hand-write what a crate does**; it does not mean **take a crate for code that
already works, is tested, and does not appear in the profile**. Both halves are
in contract PI3.

**4.5 numeric — stays hand-written, confirmed.** No `numeric_to_sql` exists in
`postgres-protocol`; `rust_decimal`'s 96-bit mantissa cannot represent
`Decimal128`'s 38 digits; `bigdecimal` allocates per value. Rewrite
`numeric_wire_bytes` as `write_numeric(value, scale, &mut BytesMut)` using
integer divmod into a `[u16; 16]` stack array — which also removes the decimal
*string* rendering entirely, so `itoa` is not needed either.

**4.6 Byte-identity proof — the original sequencing was impossible.**

The design proposed pinning a complete COPY stream from today's encoder in a
preceding commit. But today's framing lives inside `BinaryCopyInWriter`, whose
only constructor takes a `CopyInSink`, which cannot be built without a live
server (all fields private). The only way to emit a complete-stream fixture from
today's code is to hand-write the framing in the test — i.e. to write the very
thing the fixture is supposed to validate.

**Corrected**: split the proof along the line the code permits.
- **Value bytes** *can* be pinned offline from today's shipped code —
  `encode::cell_value(...)` then `ToSql::to_sql` into a `BytesMut`, plus
  `numeric_wire_bytes` called directly. Commit 1 pins a per-type, per-boundary,
  null-and-non-null fixture across all twelve wire kinds.
- **Framing** is pinned against a live server through the existing
  dest-conformance path, and by the source decoder as round-trip oracle.

**4.7 Measurement**: add `dest::testhook::{bench_batch, bench_encode}` and a
`pg_copy_encode_10k` iai case, mirroring the existing
`source::testhook::bench_wire` precedent and its `pg_copy_decode_10k` baseline.
**The instrument must land on the shipped path first**, in the same commit as
the value-byte fixture, so there is a *before* number — FR-001 needs a delta,
not an after.

**Unflagged obligations the review added**: the rewrite touches
`crash_point!("pg.stage.copy")` and the abort-on-drop staging invariant, so
`tests/dest_crash_sweep.rs` and `TARGET=sweep` are in the gate; every new error
path needs an explicit typed constructor (the `Box<dyn Error>` from
`ToSql::to_sql` must map to `DestinationError::fatal`); and two comments
carrying live invariants must be restated at their new homes — the i128 overflow
rationale (`encode.rs:197-200`) and the typed-NULL rule (`encode.rs:106-107`).

---

## 5. Story 5 — full-refresh publish

**Decision**: COPY straight into the target inside one explicit unit
transaction — `BEGIN ISOLATION LEVEL READ COMMITTED`, `TRUNCATE target` (once
per load, under the existing durable receipt guard), `COPY target FROM STDIN
BINARY` ×N, publish steps, `COMMIT`. No table swap. **Applies to Replace and
Append.** `INSERT … SELECT` disappears from the publish entirely.

**Rationale**: FR-024 requires the target's indexes, constraints, grants and
dependent objects to survive, which rules the swap out — `PERF_ANALYSIS` F4
records that it loses the target's OID, breaking dependent views and foreign
keys.

**Decision**: drive the transaction with literal `BEGIN`/`COMMIT`/`ROLLBACK`
through `Client::batch_execute`, not `tokio_postgres::Client::transaction()`.
The off-the-shelf `Transaction` type exists and is rejected on a borrow fact —
it holds `&'a mut Client`, so it cannot be stored in the session across calls.
(`self_cell` / `ouroboros` would make it storable and are rejected as
dependencies bought to defeat a borrow rule.)

**Decision**: stage tables are no longer created for non-merge tables at all —
`ensure_table`'s two-leg loop creates the stage leg only for `WriteMode::Merge`,
so the `UNLOGGED` table and its `__rdlt_arrival BIGSERIAL` disappear for
Replace/Append.

**Decision**: the sqlcore **planner** changes but the `Step` enum does not.
`CommitCtx` gains `full_load_publish: FullLoadPublish` (`Staged` |
`DirectToTarget`, `#[non_exhaustive]`) and `cleared_targets`. DuckDB stays
`Staged` and its emitted program is byte-identical; direct-append for duckdb is
a named deferral.

**Decision**: `ClearTarget` runs exactly once per (load, target), as the first
statement of the unit transaction that first writes that target. Units 2..N emit
no clear. No fallback path.

**Four new failure modes, recorded, none blocking**: `TRUNCATE` holds ACCESS
EXCLUSIVE for the whole load rather than the ~740 ms publish, so readers block
longer; the unit transaction retains `xmin` for its duration, delaying vacuum
database-wide; a stalled load holds both.

**Atomicity**: correct for READ COMMITTED readers by construction; a REPEATABLE
READ or SERIALIZABLE reader whose snapshot predates the `TRUNCATE` sees the
target empty. **This is today's behaviour, not something the story introduces** —
pin it with a test and a self-contained comment.

**Crash points**: `pg.stage.copy` is **renamed** to `pg.unit.write` (no alias,
per D1); `pg.publish.begin` narrows from "before BEGIN" to "at commit(), before
the first publish step"; `pg.unit.begin` and `pg.target.clear` are new.

**Prerequisite**: the session must know the unit's `load_id` before its first
write, which requires restructuring `recover_wal` so WAL replay uses a dedicated
session opened with the recovered span's load id.

---

## 6. Story 6 — shred path

**6.1 FR-029 as written is unsatisfiable — the spec has been corrected.**

`RowIdBuilder::update_lp` (`crates/rdlt-core/src/identity.rs:61-64`) feeds the
canonical rendering's **length before its bytes**. The total length must
therefore be known before any byte is hashed, so no streaming walk can reproduce
the same hash input without first producing the whole rendering. Since D6 freezes
the emitted identity, the rendering stays. **Verified independently at plan
time**; FR-029 now reads "MUST NOT allocate a fresh buffer per row".

What remains recoverable: `content_hash` (`table.rs:127`) allocates a **fresh**
`Vec` per root while the child path already threads a reusable one. Thread the
single scratch through `row_identity` into the keyless arm and delete the
allocating wrapper.

**6.2 `build_batch` — decided: single-pass scatter, not a transpose.**

Not row-major. Iterate each row's entries **once**, resolve the key to a column
index through a per-batch map, write into a column-major slot buffer, then call
the existing `build_column` per column unchanged. The Arrow builders stay
exactly as they are — they are the off-the-shelf column construction.

**6.3 Child-table memo**: `child_tables: Vec<(String, usize)>` on `TableBuffer`,
resolved eagerly inside the observation loop while the borrowed key is live.
`std` collections; RF5 stands, no faster-hasher crate is warranted at this
cardinality.

**6.4 Arena**: **not** reusable across pushes (it borrows the slab) — pre-size it
per push instead. The `rollback_snapshot` clone is per-push, does not appear in
the profile, and should be left alone rather than claimed as a win.

**6.5 One dependency proposed, measurement-gated: `smallvec 1.15`.**
Zero new lock entries (already at 1.15.2 via hyper/moka/idna), zero new
transitive deps, `default-features = false`, explicitly **not** `union`.
Justification: the three remaining per-object buffers hold arena borrows and
cannot be hoisted; the hand-written inline-capacity alternative needs
`MaybeUninit`, which `unsafe_code = "deny"` makes invalid under FR-007. So the
choice is smallvec or leave ~2.2M malloc/free pairs in place. **Gate: land it
only if `shred_nested_10k`'s instruction count actually moves.**

**6.6 `identity.rs` is unchanged.** Hasher reuse to avoid per-row
`blake3::Hasher::new` is recorded as a negative result so it is not chased twice.

**6.7 The oracle**: a committed verbatim golden listing of every emitted identity
over a hazard corpus, generated from the pre-change build and pinned **before**
the first line changes, plus a cross-view proptest. The existing tests are not
an oracle and cannot be made into one. Highest-value untested hazard today:
**null slots in a child list**.

**6.8 memcmp**: do not touch the canonical key-sort comparator until the memcmp
callers are attributed — decisions 6.2 and 6.3 remove callers and may recover
most of the 5.48% for free.

---

## 7. Story 7 — output-format configuration

**Decision**: a new rdlt-owned `ParquetOptions` in the SPI crate
(`rdlt_connector::output`), following the `Secret` precedent exactly
(`crates/rdlt-connector/src/secret.rs:16-19` — one shared type, schemars behind
the existing `schema` feature, re-exported from each connector's own config
path). **The SPI gains no parquet dependency**: the type is plain data and each
connector translates it into `WriterProperties` at its own boundary
(Principle III).

**Setters verified** at the exact lines: `set_compression` (properties.rs:980),
`set_dictionary_enabled` (:990), `set_dictionary_page_size_limit` (:1006),
`set_data_page_size_limit` (:1022), and `set_max_row_group_size` — which is
`#[deprecated(since = "58.0.0")]` (:726-727), so the replacement
`set_max_row_group_row_count` is used, and it `assert_ne!(value, Some(0))`
(:741), a genuine panic guard that the config must reject before reaching.

**`with_unsigned_payload` exists** — `object_store-0.12.5/src/aws/builder.rs:863`,
wired as `sign_payload: !self.unsigned_payload.get()?` at `:1159`. Take it as an
explicitly separable, opt-in sub-item.

**Four breaks the review found, all real:**

1. **`#[serde(default)]` does not do what the design said.** Bare
   `#[serde(default)]` calls `Default::default()` on the *field type*, so an
   omitted `dictionary_enabled: bool` deserializes to `false`, not `true`, and
   the `usize` limits to `0`. That inverts the headline claim that "specifying
   nothing reproduces today's geometry". The repo's correct spelling is already
   in two of the cited files: `#[serde(default = "default_path_style")]`.
2. **Cross-field validation cannot live in `ParquetOptions::validate`** — a
   `parquet:` block under `format: jsonl` needs the sibling `format` field.
3. **The story is unreachable from a pipeline YAML as designed.**
   `crates/rdlt/src/pipeline_spec.rs:126-134` carries a *separate*
   `DestSpec::File { path, format, location, partition_by }` mirror enum and
   `:389-408` rebuilds `FileDestConfig` field by field. Adding `parquet:` to
   `FileDestConfig` alone leaves it invisible to the CLI **and to every bench
   cell**. This kills US7 acceptance scenario 2 and half of FR-032 on the primary
   surface. Iceberg is unaffected (`DestSpec::Iceberg(Box<IcebergConfig>)` takes
   the whole config).
4. **The defaults contradict the story's own acceptance criterion** — see §9.2.

**Also**: `benches/cells/e2e.toml:50` and `:54` give **both** the `dlt` and
`dlt-pyarrow` variants the same `"lake", "dlt"` prefix, so per-arm
bytes-written attribution (FR-035) needs the cell changed first — four arms
currently share three prefixes.

---

## 8. Story 9 — the serial-path ceiling

**8.1 Do not open with a design; open with falsifying measurement.**

The strongest evidence in the tree says the recorded 3.5× may be the fixture:
`benches/bench-setup.sh:49-50` starts a **stock `postgres:16`** with no tuning
at all — default `shared_buffers`, `fsync` on, default `max_wal_size`. FR-039
already requires this; the protocol is four arms (E0 server capability with no
rdlt, E1 post-improvement baseline, E2 non-saturating destination, E3 optional
span attribution) each with a stop rule.

**Review corrections**: the original stop-rule inference divided a single
backend's *unlogged staging* throughput by an *end-to-end pipeline* aggregate
and concluded the server was already at 91% — different workloads, and the
arithmetic gave 90% anyway. E0 needs a **logged-target arm** as well as the
unlogged one (the unlogged arm answers the Merge question, the logged arm
answers the post-Story-5 Replace question). And the `file:` jsonl destination
does not measure "the engine ceiling" — `FileSession::write` runs an
Arrow→JSON serializer whose cost is nowhere in `PERF_ANALYSIS`. Run **both**
jsonl and parquet arms; their difference *is* the encoder term, which must be
subtracted before the residue is called an engine ceiling.

**8.2 `LoadSession` does NOT need to change — the version window stays closed.**

Parallel staging lives entirely behind the existing signature. This is the
single most consequential Phase 0 outcome for governance: **D5's conditional
does not fire**, `cargo semver-checks` reports no break, and the 0.2 → 0.3
window stays closed — recorded as an outcome per PI8.

**Review correction to the mechanism**: a `FuturesUnordered`/`JoinSet` stored as
a `PgSession` field cannot hold futures borrowing `self.client`. The mechanism
is `Arc<Client>` plus a `JoinSet` (or explicit slot structs owning their
client), which means per-slot encoding moves off the loader task.

**Review correction, and it is the important one**: the proposed
`TableWriter` SPI shape (if the break were taken anyway) **does not compile** as
given — `Box<dyn TableWriter>` carries no lifetime tie to the session, yet every
implementor's per-table work mutates session state (`FileSession.staged`,
`IcebergSession.pending_files`). `finish` would have to return the per-table
accumulation for the session to absorb, which puts a new vocabulary type on the
semver-sacred surface.

**8.3 `__rdlt_arrival BIGSERIAL` does NOT order correctly across concurrent COPY
connections** and must become an engine-assigned ordinal: the stage column
becomes `BIGINT NOT NULL`, the session reserves `[base, base + num_rows)` per
batch at the moment `write()` accepts it — i.e. in the loader's serial order, so
interleaving cannot reorder it. This changes the stage DDL and adds a column to
the wire tuple, which means **Story 4's byte-identity pin must be deliberately
re-pinned when Story 9 lands** (FR-003 requires a reviewable diff).

**8.4 Backpressure (FR-043) — the proposed fix does not close the hole.**
The design has `write()` return as soon as a slot is free, i.e. *before* the COPY
completes, so holding the byte permit until `write()` returns releases it exactly
when the batch enters flight — leaving N in-flight batches unaccounted, which is
verbatim the situation it set out to fix. The permit must be transferred **into
the staging slot** and dropped when that slot's COPY completes. That requires
either an engine type (`OwnedSemaphorePermit`) to cross the semver-sacred SPI, or
a session-local byte budget seeded from the engine's. **Unresolved** — see §9.1.

**8.5 Single-core safety (FR-044)**: slots = `min(configured, available_parallelism())`,
with one slot taking the *same* code path rather than a preserved serial branch
(D1 forbids the dual path). `std::thread::available_parallelism`, no
connection-pool crate — `deadpool`/`bb8`/`r2d2` all solve dynamic sizing and
recycling, which is not the problem here.

**8.6 A defect that appears the moment a second connection exists**: every
additional client must repeat the session setup the first one got —
`Postgres::open` runs `SET search_path TO {schema}` on the single client
(`dest/mod.rs:107-121`) and stage names are used unqualified throughout. A second
client without that `SET` writes to the wrong schema.

---

## 9. Conflicts found by Phase 0 — all four now resolved

Three were genuine forks and were put to the owner; the fourth was a factual
correction. All are settled and the spec carries the changes.

### 9.1 Story 5 forecloses Story 9's primary lever on the flagship cell

`PERF_ANALYSIS` §3.3 proposes parallel staging connections *precisely because*
"staging COPY runs on `self.client` outside the publish transaction and is
therefore already auto-committed". **After Story 5 that sentence is false for
Replace and Append** — there is no staging table for them at all.
`benches/cells/pipelines/pg-to-pg.yaml` is `write_mode: replace`.

So on the flagship 1M-row cell, Story 9's recommended lever applies to
**merge-mode staging only**, and **SC-005** cannot be satisfied there by that
lever.

**RESOLVED — SC-005 is re-targeted to the merge cell.** Both stories stay as
designed. Full-refresh single-pipeline throughput remains bounded by one
bulk-load connection, and that consequence is recorded in the spec rather than
left implicit. Two-phase commit across connections was considered and rejected:
it is a large addition to the exactly-once path that `PERF_ANALYSIS` never
priced, and prepared-transaction leaks are an operational hazard (they block
vacuum indefinitely). Re-scoping Story 5 to preserve staging was also rejected —
it would trade a measured ~350 ms win for a speculative one.

**Consequence for tasks**: Story 9's value must be re-derived from the corrected
dedup cell, not from `pg-to-pg-1m`, and the ceiling experiment's E0 arm needs
both logged and unlogged targets to answer the two regimes separately.

### 9.2 Story 7's shipped defaults cannot deliver its own acceptance criterion

US7 scenario 3 requires encoder processor time to fall **≥ 25% with the shipped
defaults**. The design keeps every non-compression default at parquet's own and
adds snappy — so encoding CPU strictly **rises**. The measured CPU win comes from
*lowering* `dictionary_page_size_limit` (44.8 ms/batch dictionary-on vs 8.0 off),
which the design exposes but leaves at parquet's default.

Separately **SC-007** requires the volume written to land within 25% of dlt's
73.7 MB — a 2.3× reduction from 210.0 MB, attributed to snappy alone and
unestablished.

**RESOLVED — the defaults change.** Ship snappy *and* a reduced
`dictionary_page_size_limit`, so the measured encoder win reaches users rather
than sitting behind a knob nobody sets. The asymmetry is what makes this safe: a
lower cap only affects columns whose dictionary would exceed it — precisely the
high-cardinality case that is pathological — while low-cardinality columns keep
full dictionary encoding and their file-size benefit. FR-033 now carries this.

**Consequence for tasks**: the default limit is a *measured* choice, not a
guess. Sweep it against the bench source (which has ~1M-cardinality text
columns) and against a low-cardinality shape, and record both. SC-007's 2.3×
volume reduction must be confirmed, not assumed from the codec alone.

### 9.3 Story 3's `[profile.bench]` pin doubles an already-39-minute CI job

Pinning `[profile.bench]` to settings that diverge from `[profile.release]`
removes artifact sharing between `make bench TARGET=iai` and `make release` in
the same job.

**RESOLVED — no `[profile.bench]` pin.** Bench inherits the new release profile,
the instruction counts shift once, and they are re-recorded in the same commit
with the reason in the message. PI1 permits exactly this and forbids only the
alternative of widening the tolerance. It is safe because the gate is one-sided
(§0.3): a shift in the improving direction cannot fail the build, so nothing
breaks silently while the re-record is prepared.

**Consequence for tasks**: the re-record happens in Story 3's own commit, before
Stories 4 and 6 measure against those baselines — which is why Story 3 is
sequenced ahead of them. Provenance must record the codegen *settings*, not the
profile name (§3), or a stock and an LTO baseline stay indistinguishable.

### 9.4 Two spec acceptance figures do not match the measured evidence

- Story 2 AC-1 requires peak memory to fall ≥ 12% on "a 1M-row pipeline".
  Measured: pg-to-pg-1m 150 → 121 MB (−19%), **pg-to-s3parquet-1m 158 → 143 MB
  (−9.5%)**. The criterion is met on one cell and not the other.
- SC-004 requires ≥ 15% on **both** 1M-row workloads. Story 2 alone does not
  deliver that on the parquet cell.

**RESOLVED — the criteria are corrected to the measured evidence.** Story 2's
acceptance splits into two cell-specific scenarios (≥ 15% on the relational
copy, ≥ 8% on the lake extract) and SC-004 carries the same split. This is a
factual correction, not a relaxation: the original figures were written from the
relational cell's number and applied to both.
