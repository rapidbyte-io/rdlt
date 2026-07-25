# Tasks: Performance Improvements — Measured Wins and the Serial-Path Ceiling

**Input**: Design documents from `/specs/019-performance-improvements/`

**Prerequisites**: plan.md, spec.md, research.md, data-model.md, contracts/performance-improvements.md, quickstart.md

**Tests**: this feature's gate is the existing workspace suite plus three kinds of pin that contract PI4 demands — byte-identity fixtures captured **from the code being replaced, before it is deleted**; crash-point sweeps over the ten in-scope points; and golden statement pins re-pinned deliberately where statements change. New tests appear only where a clause requires one. **Measurements are deliverables, not tests** (PI1): every story ends with a recorded before/after.

**Organization**: one phase per user story. The spec's stories are the plan's increments, so story order is execution order — each merges independently with the full gate green.

## Format: `[ID] [P?] [Story] Description`

- **[P]**: parallelisable (different files, no dependency on an incomplete task)
- **[Story]**: US1–US9, mapping to the spec's user stories
- Every task names its files

## Ordering constraints that are NOT priority

Three real dependencies override the P1/P2/P3 reading:

- **US1 first.** Every later measurement compares against the matrix; the matrix must be truthful before it is a comparator.
- **US3 before US4 and US6.** US3 re-records `benches/perf-baselines.json` under the new build profile. Any story that adds or compares an instruction-count case must measure on the far side of that re-record, or its before/after straddles two codegen configurations.
- **US9 last.** US2/US4/US5 shorten the serial path it must parallelise, and FR-039 requires the ceiling to be re-measured against the post-improvement baseline before its design is fixed.

US5 additionally **removes the staging table** that US9's lever depends on for full-refresh loads — which is why SC-005 was re-targeted to the merge cell (research §9.1). US9's value must be derived from the corrected dedup cell, never from `pg-to-pg-1m`.

---

## Phase 1: Setup

- [X] T001 Create `specs/019-performance-improvements/close-out.md` with one row per contract clause (PI1–PI8) and one per user story, columns: item / story / disposition / evidence — the 017/018 pattern. Seed the "superseded values" column from the committed artifacts that this feature will correct: `benches/results/pg-to-pg-dedup-1m.json` (`rdlt.rows = 3000000`, `verify.actual_rows = 1000000`, `median_ms = 14813.79`) and the three live bars in `benches/bars.toml`
- [X] T002 [P] Record the pre-change instrument state that later tasks compare against, in close-out.md: every entry of `benches/perf-baselines.json` with its current value, the current `[profile.release]` state (absent — cargo defaults `lto=false`, `codegen-units=16`, `opt-level=3`), the release binary size, and the cold-start median. These are the "before" numbers for US3 and they cannot be recovered once the profile changes

---

## Phase 2: Foundational (blocks every measured claim)

**⚠️ No story may record a result before T003 completes.**

- [X] T003 Re-establish the baseline of record (FR-002, PI1): full five-cell three-way recorded session on the unmodified tree via `make bench TARGET=e2e`, quiet guard passing, fixture identity verified against the recorded hashes (`events` `e840f51738a6b4b15f9f085ea85e3df8`, `events_v2` `7e208273f4d5333658fff2fa1c9839d9`). Record every cell's wall/CPU/RSS in close-out.md beside the spec's table and note each deviation. If a cell deviates by more than the ~3% band, the LOCAL figure becomes the comparator for every later delta and that substitution is recorded
- [X] T004 Fix the latent CI break that would otherwise mask every later gate claim: `benches/check-cold-start.sh` needs `hyperfine` (it exits 1 without it, `:25-28`), `ci.yml:75-76` runs it via `make bench TARGET=iai`, and **no workflow installs hyperfine**. Either install it in the perf-gate job or complete the split in T028 first — decide and record which, in close-out.md

**Checkpoint**: baseline recorded, gate genuinely green. Stories may begin.

---

## Phase 3: User Story 1 — The benchmark measures what it claims (Priority: P1) 🎯 MVP

**Goal**: the keep-in-sync cell delivers only what it declares; the config hole that hid it is closed; the harness can never let it recur; the published record is corrected.

**Independent Test**: the corrected cell's destination holds exactly `events_merged` at 1,000,000 rows and nothing else; the harness rejects a cell whose delivered stream set differs from its declared set; the re-run three-way session records a win; RESULTS.md and bars.toml carry the correction with a policy entry.

- [X] T005 [US1] Redefine the empty table list in `crates/rdlt-connector-postgres/src/source/config.rs`: delete the rejection at `:567-570` ("`tables` present but empty — omit it to discover all") and rewrite the field doc at `:34` to the uniform rule — `tables` present ⇒ exactly this list, possibly empty; absent ⇒ discover every table in `schema`. Per D1 the spelling changes meaning outright; no second spelling is added. Verify the downstream paths already behave (`source/mod.rs:403-411` yields only queries for `Some(empty)`, `mod.rs:302-310` yields no CDC tables, `source/reflect.rs:258-266` reflects unchanged, `config.rs:616-618` returns no per-table config)
- [X] T006 [US1] Add the two new configuration-time rejections in `crates/rdlt-connector-postgres/src/source/config.rs` (FR-011, PI7): in `validate_tables` (`:538`) reject `tables` present-and-empty **with** no queries, naming both remedies; in `validate_cdc` (`:510-534`) reject a `cdc:` block combined with an empty table list, because CDC with no tables captures nothing and would otherwise be a silent no-op (Principle IV). Both messages self-contained, no clause IDs
- [X] T007 [P] [US1] Add the discovery warning in `crates/rdlt-connector-postgres/src/source/mod.rs`: one `tracing::warn!` from `streams()` when `tables` is absent while `queries` are declared, naming the discovered table count and names and stating the remedy. Self-contained wording
- [X] T008 [P] [US1] Scope both dedup pipelines: add `tables: []` to `benches/cells/pipelines/pg-to-pg-dedup.yaml` and `pg-to-pg-dedup-load1.yaml`
- [X] T009 [US1] Change the cell verification shape in `crates/rdlt-bench/src/cells.rs`: `Cell.verify` becomes a table→rows map (`[cell.verify]` with one `name = count` line per table); delete `struct Verify`. In `Cell::check` (`:110-126`) reject at LOAD time any cell carrying a `pipeline` but no non-empty verify map, naming the cell and its file before any container starts
- [X] T010 [US1] Enforce the delivered-vs-declared stream set in `crates/rdlt-bench/src/runner.rs` (FR-009/FR-010, PI6): in `run_once_subprocess`, right after the report parses, compare `RunReport.tables` keys against the cell's declared map and fail the run naming the surplus and missing tables. The delivered set needs no new channel — the harness already indexes `report["tables"]` in `report_totals`/`report_table_rows`
- [X] T011 [US1] Update the artifact in `crates/rdlt-bench/src/artifact.rs`: `verify` becomes the same table→rows map (delete `VerifyOutcome`), and `ARTIFACT_FORMAT_VERSION` goes 2 → 3 with the reader rejecting v2 by version. **This is the feature's ONE artifact bump — US7's `artifact_bytes` fields land under the same version** (T072), so coordinate rather than bumping twice. Delete the vestigial `StreamAttribution` and `RdltSide.streams` at the same time (documented "always empty since subprocess is the only run behavior" — dead weight in a recorded format, PI2)
- [X] T012 [US1] Guard the Trends delta in `crates/rdlt-bench/src/report.rs` (`trends_table`, `:315-345`): when two compared points carry different row counts, render the row counts instead of a percentage. A wall-clock delta between runs that moved different volumes is not a speedup, and the corrected dedup cell is exactly that case
- [X] T013 [US1] Re-measure: recorded three-way session for `pg-to-pg-dedup-1m` (and the untimed LOAD 1 verified by hand once — the harness gate covers the measured run only, research §1). Confirm the destination holds only `events_merged`. Then re-derive the cell's bar from the corrected session's own floor per Principle VIII, or record its absence, in `benches/bars.toml`
- [X] T014 [US1] Correct the public record in `benches/RESULTS.md`: policy-log entry (newest first) naming the defect, the superseded values (14.81 s median ±8% vs dlt 12.48 s = 0.8×; 14.75 s vs 12.55 s = 0.9× in the two-way session), why they were not comparable, and the corrected result. Amend Caveats; regenerate the Matrix and Trends blocks; fill close-out rows PI6 and US1

**Checkpoint**: the matrix is a valid comparator. Every later measurement depends on this.

---

## Phase 4: User Story 2 — Crash safety stops costing a quarter of the engine (Priority: P1)

**Goal**: recovery-log segments stop being encoded as a columnar analytics format, with the guarantee unchanged and the disk write off the async runtime.

**Independent Test**: the two 1M-row cells improve to their floors; the crash sweep is green over the four WAL points; a v1 log is refused by version and degrades to re-extraction; a truncated segment is refused rather than replayed short; no parquet reader remains in the engine.

- [X] T015 [US2] Swap the segment writer in `crates/rdlt-engine/src/wal/mod.rs`: `write_segment` (`:214-223`) becomes `arrow::ipc::writer::FileWriter::try_new(File::create(path)?, batch.schema_ref())` → `write(batch)` → `finish()`. **Unbuffered** — no `BufWriter`, no `try_new_buffered`. Delete the `ArrowWriter` import. Zero new dependencies: `arrow`'s default features already carry `ipc` and `arrow-ipc 58.3.0` is in `Cargo.lock`
- [X] T016 [US2] Swap the segment reader in `crates/rdlt-engine/src/wal/resume.rs`: `open_segment` (`:16-25`) returns `arrow::ipc::reader::FileReader`, which is the same `Iterator<Item = Result<RecordBatch, _>>` shape as the displaced `ParquetRecordBatchReader`. **Delete the parquet reader entirely** — no fallback path (PI2)
- [X] T017 [US2] Remove the dependency: delete `parquet = { workspace = true }` from `crates/rdlt-engine/Cargo.toml:14`. Verify exhaustively that nothing else in the crate uses it — grep for `parquet` over `crates/rdlt-engine/` must return only the unrelated test *name* `sweep_parquet_destination` (`tests/crash_sweep.rs:154`). A direct Principle I win, recorded in close-out.md
- [X] T018 [US2] Bump and re-gate the format version in `crates/rdlt-engine/src/wal/mod.rs` and `resume.rs`: `WAL_FORMAT_VERSION` 1 → 2; change the resume gate from `found > supported` to **exact match**, with a new outcome `Scan::Unsupported { found, supported }` so a version refusal is distinguishable from corruption without substring matching (Principle V). Keep `initial_wal_version()` returning 1 and rewrite its doc to say what it now means
- [X] T019 [US2] Update everything the bump breaks — the design's "nothing else changes" claim was false: `resume.rs:282-286` asserts `WAL_FORMAT_VERSION == 1` with the message "bump deliberately, with a migration note" (a tripwire doing its job); `resume.rs:307` pins `matches!(run(V+1), Scan::Damaged(_))` and must re-point at `Scan::Unsupported`; the doc at `resume.rs:290-292`; the mutation-closure doc at `resume.rs:263-266`; and `runtime/run.rs:353-356` needs a **new** match arm for the new outcome
- [X] T020 [P] [US2] Rename the segment extension to `.arrow` in `crates/rdlt-engine/src/wal/mod.rs:141`. Nothing keys on it — verified: `mark_committed` unlinks exact paths (`:191-194`), `clear` removes the whole directory (`:226-228`), the WAL never globs
- [X] T021 [US2] Move segment I/O off the async runtime (FR-016): make `Wal::record` / `sync_for_commit` / `mark_committed` async and offload only the disk-touching bodies via `tokio::task::spawn_blocking`, awaited inline. **`sync_for_commit` needs TWO hops, not one** — `crash_point!` expands to `fail::fail_point!($name, |_| { $err })` (`crates/rdlt-core/src/failpoint.rs:24`), whose closure form is a `return` from the **enclosing function**, so no crash point may move inside a `spawn_blocking` closure; and `crash_point!("wal.manifest.fsync")` sits at `mod.rs:175-181`, inside the range. Hop 1 = the `pending_sync` fsync loop plus `manifest.flush()`; the crash point stays on the async side; hop 2 = `manifest.sync_all()` on a `try_clone`d handle. Do **not** pipeline the WAL write against the destination write — the manifest's on-disk order IS the replay order
- [X] T022 [P] [US2] Offload replay pass 1 in `crates/rdlt-engine/src/wal/resume.rs:172-188` with the same helper (the decode-and-drop validation loop contains no `await` and moves wholesale). Leave pass 2 (`:213-229`) inline with a self-contained comment saying why — it runs once per crash
- [X] T023 [US2] Add the three deterministic file-mutation tests no crash point can produce, in `crates/rdlt-engine/tests/`: a truncated segment (must be refused, not replayed short — this is the whole reason the file container beats the stream container); a manifest/segment row-count disagreement; and an unsupported format version. Add the empty-batch round-trip pin — a zero-row batch must survive write→read as one 0-row batch, which works via `RecordBatchOptions::new().with_row_count(...)` (`arrow-ipc-58.3.0/src/reader.rs:556`). Note the swap *removes* a live-vs-replay asymmetry: parquet's `ArrowWriter::write` short-circuits on zero rows, `FileWriter::write` does not
- [X] T024 [US2] Run the crash sweep (`make test TARGET=sweep`) over the four WAL points — `wal.segment.write`, `wal.segment.fsync`, `wal.manifest.append`, `wal.manifest.fsync` — none of which changes meaning provided T021's rule holds. Confirm duplicate-free recovery at each
- [X] T025 [US2] Amend the frozen documents (Principle IX requires a **migration note**, not just a restatement): `specs/001-rdlt-ingestion-engine/contracts/persisted-formats.md:29` plus a §2 migration line recording that v1 logs are refused by version (the section already carries an amendment header as precedent); `specs/001-rdlt-ingestion-engine/data-model.md:142`; `specs/001-rdlt-ingestion-engine/plan.md:29` and `:118`; `2026-07-18-rdlt-engine-design.md:{44,122,312}`; and the module doc at `wal/mod.rs:1`. Record the D3 reconciliation — the spec says "streaming record-batch container", the IPC **file** format is selected instead, because a truncated stream replays short and succeeds (`read_meta_len` returns `Ok(None)` on `UnexpectedEof`) while the file container's validated footer makes truncation unrepresentable, at a cost of 5.8 vs 5.9 ms/batch
- [X] T026 [US2] Measure and record (PI1): interleaved A/B on `pg-to-pg-1m` and `pg-to-s3parquet-1m`, ≥ 5 pairs, medians. Floors — wall ≥ 15% on both; peak memory ≥ 15% on the relational copy and ≥ 8% on the lake extract (AC-1/AC-1b, corrected at plan time from the measured 150 → 121 MB and 158 → 143 MB). No `perf-baselines.json` re-record is needed: no iai benchmark touches the WAL. Fill close-out rows PI4 (the authorised bump), PI5 and US2

**Checkpoint**: a quarter of the engine's processor time is back, with recovery unchanged.

---

## Phase 5: User Story 3 — Shipped builds are actually optimized (Priority: P2)

**Goal**: declare the release profile, settle the two allocator knobs by measuring them separately for the first time, and make the instruction-count gate's provenance honest.

**⚠️ Must complete before US4 and US6** — it re-records the baselines they measure against.

**Independent Test**: the release build shows the CPU floor, cold start and binary size both improve, the instruction baselines are re-recorded with recorded provenance, and the allocator comment matches what was measured.

- [ ] T027 [US3] Run the 4-arm build sweep and record all four in close-out.md: stock / `codegen-units=1` only / `lto="thin"`+cgu default / `lto="fat"`+`codegen-units=1`. **Do not pair `thin` with `codegen-units=1`** — ThinLTO's advantage is that its LTO step parallelises across codegen units, so pinning cgu=1 makes that arm answer nothing. Record CPU on `pg-to-pg-1m`, wall, link time, and binary size per arm
- [ ] T028 [US3] Adopt the winning profile in the workspace `Cargo.toml`: `[profile.release]` with `lto`, `codegen-units` and an explicit `opt-level = 3`. **Add no `[profile.bench]` pin** — bench inherits release, the instruction counts shift once, and T029 re-records them (research §9.3: a divergent bench profile removes build-artifact sharing from a perf-gate job that already takes 39m19s)
- [X] T029 [US3] Re-record `benches/perf-baselines.json` under the adopted profile (`benches/compare-iai.sh --record`), with the reason in the commit message — PI1 permits exactly this and forbids only widening the tolerance. Safe to stage because the gate is one-sided: `compare-iai.sh:105-106` appends a failure only when `delta > TOLERANCE`, so an improving shift cannot fail the build
- [X] T030 [US3] Record codegen provenance so a stock and an LTO measurement are distinguishable: add a `codegen` string beside `toolchain` in `benches/perf-baselines.json`, written by `--record` and refused-on-mismatch by the comparator (the same pattern `compare-iai.sh:82-93` already implements for the toolchain). **Do not** use a `build_profile: Option<String>` on the artifact `Fingerprint` — after this story every release measurement is profile *"release"* either way, so the name records nothing
- [X] T031 [P] [US3] Add `[profile.dist] inherits = "release"` carrying `strip = "symbols"` and **no `panic` key in any profile**, plus a `make dist` verb building `cargo build --profile dist -p rdlt-cli`. `panic = "abort"` is rejected outright: library embedders inherit it, and cargo silently forces unwind for test/bench units (`UnitFor::new_test`, `PanicSetting::AlwaysUnwind`) rather than erroring, so the usual justification is imprecise. Record the rejection and its reason
- [X] T032 [US3] Split the cold-start check out of `make bench TARGET=iai` into its own verb in the `Makefile`, invoked by `make check` and the recorded session but not by CI's perf-gate job — closing the latent break identified in T004 (no workflow installs hyperfine, and the script demands a quiet machine, which a CI runner is not). Update `.github/workflows/ci.yml` accordingly
- [X] T033 [US3] Run the allocator 2×2 factorial from ONE env-var-gated throwaway build of `crates/rdlt-cli/src/main.rs:35-43` — arms: neither / arena only / trim only / both — interleaved, on `pg-to-pg-1m` and `s3jsonl-to-s3parquet-200k`, recording wall, CPU, sys and peak RSS for each. **A fact that reframes the experiment**: `main.rs:41` sets `M_TRIM_THRESHOLD` to `128*1024`, which is glibc's own default — the call's only real effect is its documented side effect of disabling glibc's dynamic threshold adaptation, which is what explains the measured `sys` 0.21 → 0.12 s
- [X] T034 [US3] Settle the allocator from T033's evidence (FR-038): choose the shipped setting, and **rewrite the comment at `main.rs:34-43` to state what was measured** — it currently claims "no measured wall-time cost", which the evidence contradicts (up to 9% wall on the JSONL cell for ~21% RSS). Add **no** allocator crate; record mimalloc/jemalloc as a bounded follow-up whose measurement is only meaningful after US4 and US6, and note that the cheapest route to deleting the sanctioned `unsafe` is deleting the `mallopt` call, not replacing the allocator
- [X] T035 [US3] Measure and record (PI1): CPU on `pg-to-pg-1m` ≥ 10% below baseline; cold start at or below its prior median and ≤ 40 ms; binary size recorded. Fill close-out row US3

**Checkpoint**: the measurement substrate is fixed. US4 and US6 may now measure against stable baselines.

---

## Phase 6: User Story 4 — The hot loops stop allocating once per value (Priority: P2)

**Goal**: the Postgres wire encoder stops allocating per value and stops framing at 4 KiB, with byte-identical output.

**Independent Test**: value bytes match the pinned fixture for every wire kind; a 1M-row load's contents are unchanged; blocking waits fall by an order of magnitude; no boxing path remains.

- [X] T036 [US4] **PIN BEFORE DELETE (PI4 ordering).** In a commit that PRECEDES any encoder change, add `crates/rdlt-connector-postgres/tests/fixtures/pg_copy_values.hex` — a per-type, per-boundary, null-and-non-null fixture across all twelve `ColumnWire` variants, generated from the code as it ships today by calling `encode::cell_value(...)` then `ToSql::to_sql` into a `BytesMut`, plus `numeric_wire_bytes` directly. **The complete-stream fixture the design first proposed is impossible**: today's framing lives inside `BinaryCopyInWriter`, whose only constructor takes a `CopyInSink`, which cannot be built without a live server — so emitting it would mean hand-writing in the test the very framing the fixture is meant to validate
- [X] T037 [US4] In the same pre-change commit, add the measurement instrument on the SHIPPED path so FR-001 has a *before* number: `dest::testhook::{bench_batch, bench_encode}` in `crates/rdlt-connector-postgres/src/dest/`, driving today's `cell_value` + `ToSql::to_sql`, plus a `pg_copy_encode_10k` case in `crates/rdlt-connector-postgres/benches/iai_pg.rs`, mirroring the existing `source::testhook::bench_wire` / `pg_copy_decode_10k` precedent. Record the baseline with `--record`
- [X] T038 [US4] Introduce the borrowed per-column view in `crates/rdlt-connector-postgres/src/dest/encode.rs`: an enum of typed array references (`Bool(&BooleanArray)`, `Int8(&Int64Array)`, `Text(&StringArray)`, …) built once per batch and indexed per row. There is no per-cell value type — the enum arm IS the encode decision. This removes the per-cell `Box`, both per-row `Vec`s, the `String::to_owned` for text, the per-cell `downcast_ref` (12M per run) and the virtual `is_null`
- [X] T039 [US4] Encode values through the off-the-shelf layer: `postgres_types::ToSql::to_sql` on concrete borrowed values, so every call is monomorphic and inlinable and there is **no `dyn` anywhere**. `postgres-protocol` is deliberately **not** taken as a direct dependency — `ToSql` yields the same bytes with zero additions, and a second independently-versioned third-party surface in a crate Principle III says wraps its driver at one boundary is a cost with no benefit. Keep chrono in the timestamp/date/time paths, feeding the existing `with-chrono-0_4` impls
- [X] T040 [US4] Make `numeric_wire_bytes` allocation-free in `crates/rdlt-connector-postgres/src/dest/encode.rs:195-249`: `write_numeric(value, scale, &mut BytesMut)` doing base-10⁴ grouping by integer divmod into a `[u16; 16]` stack array. This also removes the decimal *string* rendering entirely. **It stays hand-written and that is justified by fact** (PI3): `postgres-protocol` has no `numeric_to_sql`; `rust_decimal`'s 96-bit mantissa cannot represent `Decimal128`'s 38 digits; `bigdecimal` allocates per value. Keep the existing proptest round-trip against the source decoder and the i128-extremes structural test
- [X] T041 [US4] Fix the uuid parser in place at `crates/rdlt-connector-postgres/src/dest/encode.rs:296-335` — it accepts a hyphen at a position it should not; reject a hyphen whose `hex_seen` equals the last boundary at which one was consumed. **The `uuid` crate is deliberately NOT adopted** (PI3, both directions): it appears nowhere in the measured profile so D1's "measured-better" test is unmet, it would be a genuinely new dependency, and `Uuid::try_parse` accepts a narrower set than both today's parser and PostgreSQL's own `uuid_in` — a silent narrowing Principle IV forbids and no requirement authorises. Record the decision and its reasoning
- [X] T042 [US4] Replace the framing and buffering in `crates/rdlt-connector-postgres/src/dest/commit.rs:312-375`: one reused `BytesMut`; 19-byte header, per-tuple `i16` field count then per field `i32` length or `-1`, `i16` `-1` trailer; flush at ~64 KiB into `CopyInSink<Bytes>`. Mechanics — `let sink = client.copy_in::<_, Bytes>(&sql).await?` (the *statement* type parameter comes first), `futures::pin_mut!(sink)` exactly as `:353` already does, then `sink.as_mut().feed(chunk).await?` and `sink.finish().await?`. **Justify the 64 KiB on its own terms**, not by reference to the CLI's `M_TRIM_THRESHOLD` — that is a CLI-only setting library embedders never get, and T033/T034 are actively re-measuring it. Delete `BinaryCopyInWriter` usage and `encode::cell_value` (PI2)
- [X] T043 [US4] Handle the error path correctly (FR-021): a value the encoder cannot represent returns `Err` and lets the `CopyInSink` drop **without** calling `finish()`, relying on tokio-postgres' own abort-on-drop protocol — the encoder must have no cleanup path that finishes on error. Map every new failure explicitly through the typed constructor (`DestinationError::fatal`), including the `Box<dyn Error>` that `ToSql::to_sql` returns and the date/time range guards; no substring matching (Principle V)
- [X] T044 [US4] Restate the comments that carry live invariants at their new homes (Principle VI): the i128 overflow rationale from `encode.rs:197-200` ("a 38-digit i128 times 10^pad exceeds u128::MAX") and the typed-NULL rule from `:106-107` ("binary COPY checks the ToSql type against the column's wire type"). Both rules still hold; deleting their comments would strand them
- [X] T045 [US4] Re-pin what this story deliberately changes (FR-003, PI4): the numeric tests call `numeric_wire_bytes` directly (`encode.rs:384`, `:405`, `:422`) and must move to `write_numeric`; the uuid assertions at `:476-482` change with T041. These are deliberate, reviewable pin diffs, not incidental test churn
- [X] T046 [US4] Run the crash sweep for the rewritten write path: the story rewrites `PgSession::write`, which opens with `crash_point!("pg.stage.copy")` (`commit.rs:317-320`) and whose abort-on-drop behaviour is the at-least-once staging invariant (`crates/rdlt-connector/src/lib.rs:119-122`). Run `tests/dest_crash_sweep.rs` and `make test TARGET=sweep`
- [X] T047 [US4] Measure and record (PI1): the `pg_copy_encode_10k` instruction count against T037's baseline (expect > 2× reduction); interleaved A/B on `pg-to-pg-1m` for wall and CPU; and **`%w` from `/usr/bin/time`** — the voluntary-context-switch count is the clean discriminator for the framing half, and must fall by at least an order of magnitude from the recorded 113,552. Fill close-out row US4

**Checkpoint**: ~12M allocations per load are gone and the wire stream is framed sanely.

---

## Phase 7: User Story 5 — Full-refresh loads stop writing every row twice (Priority: P2)

**Goal**: Replace and Append COPY straight into the target inside one unit transaction; the staging table disappears for non-merge tables.

**Independent Test**: the publish `INSERT … SELECT` is gone; a concurrent reader sees only complete states; the crash sweep is green over the renamed points; merge behaviour and its golden statements are untouched.

- [X] T048 [US5] Restructure `recover_wal` in `crates/rdlt-engine/src/runtime/run.rs:295-360` so the session knows its unit's load id before the first write: run `wal::resume::scan` first and, on `Scan::Recover(span)`, open a DEDICATED session with `OpenCtx::new(pipeline, span.load_id)`, replay into it, commit, and drop it — then open the run's own session normally. This is a prerequisite, not an optimization
- [X] T049 [US5] Extend the planner in `crates/rdlt-connector-sqlcore/src/`: `CommitCtx` gains `full_load_publish: FullLoadPublish` (`Staged` | `DirectToTarget`, `#[non_exhaustive]` per the config-enum policy) and `cleared_targets: &BTreeSet<TableName>`; add `prepare_target(...)`. **The `Step` enum does not change.** `ClearTarget` is emitted exactly once per (load, target), as the first statement of the unit transaction that first writes that target
- [X] T050 [US5] Hold the unit transaction in `crates/rdlt-connector-postgres/src/dest/commit.rs`: literal `BEGIN ISOLATION LEVEL READ COMMITTED` / `COMMIT` / `ROLLBACK` through `Client::batch_execute`, opened lazily at the first `write()` of a unit. `tokio_postgres::Client::transaction()` is rejected on a borrow fact, not preference — it holds `&'a mut Client` and cannot be stored in the session across calls (and `self_cell`/`ouroboros` are rejected as dependencies bought to defeat a borrow rule). Move `load_committed_before` (`:413-424`) to the first statement after `BEGIN`, cached per unit; leave `replayed` (`:396-407`) and the full-feed `EXISTS` probes (`:426-441`) at `commit()`
- [X] T051 [US5] Stop creating stage tables for non-merge tables in `crates/rdlt-connector-postgres/src/dest/commit.rs:188-238`: the two-leg loop creates the stage leg only for `WriteMode::Merge`, so the `UNLOGGED` table and its `__rdlt_arrival BIGSERIAL` (`:198-202`) are not created for Replace/Append, and the planner emits no `TruncateStage` for them
- [X] T052 [P] [US5] Keep DuckDB on the staged path: supply `FullLoadPublish::Staged` at its one call site in `crates/rdlt-connector-duckdb/src/dest/commit.rs` so its emitted program, DDL, staging and sweep stay byte-identical. Record direct-append for duckdb as a named deferral behind its own experiments
- [X] T053 [US5] Rename and re-scope the crash points (PI5): `pg.stage.copy` → `pg.unit.write` (**no alias**, per D1); `pg.publish.begin` keeps its name but narrows from "before BEGIN" to "at commit(), before the first publish step"; add `pg.unit.begin` and `pg.target.clear`. Update the registered list in `crates/rdlt-connector-postgres/src/dest/mod.rs` and the expected list in `tests/dest_crash_sweep.rs`, and update each point's self-contained comment to what it now brackets
- [X] T054 [US5] Pin the isolation-level behaviour with a test and a self-contained comment: `TRUNCATE`+`COPY` in one transaction is atomic for READ COMMITTED readers by construction, and a REPEATABLE READ or SERIALIZABLE reader whose snapshot predates the `TRUNCATE` sees the target empty. **That is today's behaviour, not something this story introduces** — pin it so it cannot drift silently
- [X] T055 [US5] Record the four new failure modes in the module doc, none blocking: `TRUNCATE` holds ACCESS EXCLUSIVE on the target for the whole load rather than the ~740 ms publish, so readers block longer; the unit transaction retains `xmin` for its duration, delaying vacuum database-wide; a stalled load holds both. Self-contained wording
- [X] T056 [US5] Re-pin deliberately (FR-003): the sqlcore step-program pins re-pin **additively**; `crates/rdlt-connector-postgres/tests/golden_sql.rs` does **not** change (merge statements are untouched); add one NEW text pin in the postgres crate for the non-merge publish statements
- [X] T057 [US5] Run the crash sweep over the renamed and new points plus `session.after_write`, confirming exactly-once and that no rows from a rolled-back attempt survive; verify a Replace load spanning several commit units clears exactly once and units 2..N emit no clear
- [X] T058 [US5] Measure and record (PI1): interleaved A/B on `pg-to-pg-1m` — server-side publish cost ≥ 40% below baseline and cell wall ≥ 10% below. Verify FR-024 by inspection: indexes, constraints, grants and dependent objects survive the publish. Record the deliberate trade in close-out.md — this story removes the staging that US9's parallel-staging lever depends on for full loads, which is why SC-005 targets the merge cell (research §9.1). Fill close-out row US5

**Checkpoint**: full-refresh rows reach the server once.

---

## Phase 8: User Story 6 — Semi-structured ingestion, identities unchanged (Priority: P2)

**Goal**: shred-path allocations and repeated work removed, with every emitted `_rdlt_id` byte-identical.

**Independent Test**: the golden identity listing is unchanged over the hazard corpus; `shred_nested_10k` instruction count falls; the JSONL cell's CPU falls.

- [X] T059 [US6] **PIN BEFORE CHANGE (PI4).** In a commit that PRECEDES any shred change, add a committed verbatim golden listing of every emitted identity over a hazard corpus, generated from the pre-change build, plus a cross-view proptest. The corpus must cover nested objects, arrays, scalar lists, nulls, absent fields, duplicate keys, children at depth, the keyed path — and **null slots in a child list**, the highest-value case with no test today. The existing tests are not an oracle and cannot be made into one
- [X] T060 [US6] Reuse the identity scratch buffer in `crates/rdlt-engine/src/shred/table.rs`: thread the single `hash_scratch` through `row_identity` into the keyless arm and **delete the allocating `content_hash` wrapper** (`:127`) so `content_hash_with` is the only entry point. This is FR-029 as corrected — eliminating the canonical rendering is impossible while identities are frozen, because `RowIdBuilder::update_lp` (`crates/rdlt-core/src/identity.rs:61-64`) feeds its **length before its bytes**
- [X] T061 [US6] Remove the keyed path's per-field allocation in `crates/rdlt-engine/src/shred/`: replace `render_scalar`'s `Option<String>` return on that path with a form that borrows for string values and renders into a reusable buffer otherwise; delete the allocating variant. **Preserve the float trap**: `render_scalar` uses Rust's `Display` for floats while `canonical_json_bytes` goes through serde_json — the two renderings differ on purpose and both are load-bearing for persisted values
- [~] T062 [US6] **NOT TAKEN — measured worse, see close-out D-13.**  Replace the column-major probe in `crates/rdlt-engine/src/shred/build.rs:67` with a **single-pass scatter**, not a transpose: iterate each row's entries once, resolve the key to a column index through a per-batch map, write into a column-major slot buffer, then call the existing `build_column` per column unchanged. The Arrow builders stay exactly as they are — they are the off-the-shelf column construction (PI3)
- [X] T063 [P] [US6] Memoize the child-table index in `crates/rdlt-engine/src/shred/tape.rs:231`: `child_tables: Vec<(String, usize)>` on `TableBuffer`, resolved eagerly inside the observation loop (`:173-179`) while the borrowed key is still live. `std` collections only — no faster-hasher crate is warranted at this cardinality (a handful of tables per stream)
- [X] T064 [P] [US6] Reduce per-document allocations in `crates/rdlt-engine/src/shred/tape.rs`: pre-size the per-push `Arena` (`:98`) rather than trying to reuse it — it borrows the slab and is **not** reusable across pushes; carry integers instead of owned keys and node vectors; hoist the BFS queue. Leave the `rollback_snapshot` clone (`:96`) alone — it is per-push, not per-row, and does not appear in the profile
- [X] T065 [US6] Evaluate `smallvec = { version = "1.15", default-features = false }` as a direct dependency of `rdlt-engine`, **measurement-gated**: land it only if `shred_nested_10k`'s instruction count actually moves. It covers the three buffers that cannot be hoisted because they hold arena borrows. Justification if taken (PI3/Principle I): zero new lock entries (already at 1.15.2 via hyper/moka/idna), zero new transitive deps, and the hand-written inline-capacity alternative needs `MaybeUninit`, which `unsafe_code = "deny"` forbids under FR-007. Explicitly do **not** enable `union`. If the count does not move, do not land it and record that
- [X] T066 [US6] Record two negative results in close-out.md so they are not chased twice: `crates/rdlt-core/src/identity.rs` is **unchanged** — reusing a `blake3::Hasher` across rows is not pursued; and do not touch the canonical key-sort comparator until the `__memcmp` callers are attributed, because T062 and T063 remove callers and may recover most of the 5.48% for free
- [X] T067 [US6] Measure and record (PI1): the golden identity listing byte-identical over the corpus; `shred_nested_10k` instruction count; interleaved A/B on `s3jsonl-to-s3parquet-200k` for CPU ≥ 10% below baseline with output unchanged. Fill close-out rows PI4 (identity half) and US6

**Checkpoint**: the flagship differentiator is cheaper and its persisted identities are provably unchanged.

---

## Phase 9: User Story 7 — Users can choose what their output files look like (Priority: P3)

**Goal**: parquet writer properties become configurable with defaults that actually deliver the measured win, and the benchmark starts comparing equivalent artifacts.

**Independent Test**: a destination with no settings writes compressed files; explicit settings reach the writer through a pipeline YAML; encoder CPU falls on high-cardinality data with the shipped defaults; the parquet cell records bytes per arm.

- [X] T068 [US7] Add `ParquetOptions` and `ParquetCompression` to a new `crates/rdlt-connector/src/output.rs`, following the `Secret` precedent exactly (`src/secret.rs:16-19` — one shared type, `schemars` behind the existing `schema` feature, re-exported from each connector's own config path). **The SPI gains no parquet dependency**: the type is plain data and each connector translates it into `WriterProperties` at its own boundary (Principle III). Struct `#[serde(deny_unknown_fields)]`, enums `#[non_exhaustive]`
- [X] T069 [US7] Spell the defaults correctly — this is where the design was wrong: bare `#[serde(default)]` calls `Default::default()` on the **field type**, so an omitted `dictionary_enabled: bool` would deserialize to `false` and the limits to `0`, inverting the intent. Use `#[serde(default = "…")]` per field, following the repo's own `#[serde(default = "default_path_style")]` precedent. Defaults: compression `snappy`, **and a reduced `dictionary_page_size_limit`** so high-cardinality columns abandon dictionary encoding rather than interning every distinct value (FR-033 as extended — snappy alone makes encoder CPU *rise*)
- [X] T070 [US7] Sweep the dictionary limit and choose the default from evidence, not taste: measure encoder CPU and output size against the bench source (~1M-cardinality text columns) and against a low-cardinality shape, and record both in close-out.md. The asymmetry is the whole point — a lower cap must not degrade low-cardinality columns
- [X] T071 [US7]  Wire the options into `crates/rdlt-connector-file/src/dest/{config.rs,session.rs}` and `crates/rdlt-connector-iceberg/src/dest/{config.rs,writer.rs}` as `#[serde(default)] pub parquet: Option<ParquetOptions>`, translating to `WriterProperties::builder()` with the verified setters: `set_compression` (properties.rs:980), `set_dictionary_enabled` (:990), `set_dictionary_page_size_limit` (:1006), `set_data_page_size_limit` (:1022), and `set_max_row_group_row_count` — **not** `set_max_row_group_size`, which is `#[deprecated(since = "58.0.0")]` (:726-727). Note `set_max_row_group_row_count` asserts non-zero (:741), so the config must reject 0 before reaching it
- [X] T072 [US7] **Make it reachable from a pipeline YAML** — without this the story is invisible to the CLI and to every bench cell, killing FR-032's "explicit settings" half: `crates/rdlt/src/pipeline_spec.rs:126-134` carries a **separate** `DestSpec::File { path, format, location, partition_by }` mirror enum and `:389-408` rebuilds `FileDestConfig` field by field. Add `parquet` to both. Iceberg needs nothing — `DestSpec::Iceberg(Box<IcebergConfig>)` takes the whole config
- [X] T073 [US7] Implement FR-034 validation where the fields it needs are in scope: a `parquet:` block under a non-parquet format needs the sibling `format` field, so that rule lives on `FileDestConfig`, not on `ParquetOptions::validate`. Reject a compression level set for a codec that takes none, and a zero row-group count. Every message names the offending setting, self-contained, no clause IDs
- [ ] T074 [P] [US7] Add bytes-written to the artifact: `artifact_bytes: Option<u64>` on `RdltSide` and the `CompetitorSide::Ok` variant in `crates/rdlt-bench/src/artifact.rs`, both `#[serde(default, skip_serializing_if = "Option::is_none")]`, landing **under the same `format_version = 3`** that T011 introduces — one bump for the feature, not two. Populate from a per-cell measurement seam plus a stdlib-only `benches/fixtures/s3_prefix_bytes.py` summing `ListObjectsV2` sizes over an arm's prefix
- [X] T075 [US7] Fix the prefix collision that blocks per-arm attribution: `benches/cells/e2e.toml:50` and `:54` give **both** the `dlt` and `dlt-pyarrow` variants the same `"lake", "dlt"` prefix, so `pg-to-s3parquet-1m` has four arms over three prefixes and two would report whichever ran last. Give each arm its own prefix before FR-035 can mean anything
- [X] T076 [US7] Evaluate S3 unsigned payloads as a separable opt-in: `AmazonS3Builder::with_unsigned_payload(bool)` exists (`object_store-0.12.5/src/aws/builder.rs:863`, wired at `:1159`). Expose it explicitly — never as a default — and measure its effect against the recorded 6.72% `ring` SHA-256 share
- [X] T077 [US7] Record the user-visible behaviour change (Principle IX position: written parquet is the user's data product, not an rdlt-internal persisted format, so no format version is introduced): a README entry stating that output is compressed by default and how to restore uncompressed. Note this repo has no CHANGELOG, so README is the record
- [ ] T078 [US7] Re-derive the affected bar: `s3jsonl-to-s3parquet-200k` carries `min_ratio = 45.0` against a 60.1× floor, and its rdlt arm writes parquet — so D4 adds compression CPU to rdlt's side only while dlt's destination already writes compressed. Re-measure and re-derive from the new session floor, or record its removal, in `benches/bars.toml` with a policy entry
- [ ] T079 [US7] Measure and record (PI1): default output is compressed; explicit settings reach the files; encoder CPU ≥ 25% below baseline on high-cardinality data **with the shipped defaults** (AC-3 — achievable only because of T069/T070); bytes written per arm recorded on `pg-to-s3parquet-1m` against dlt's 73.7 MB (SC-007). Fill close-out rows PI7 and US7

**Checkpoint**: users control their output, and the parquet cell compares like with like.

---

## Phase 10: User Story 8 — The small, safe wins (Priority: P3)

**Goal**: three individually-minor costs with no correctness surface, taken together.

**Independent Test**: the decoder allocates a constant number of scratch buffers; the merge sort runs under a transaction-scoped memory setting; a multi-statement merge evaluates its dedup once.

- [ ] T080 [P] [US8] Hoist the per-row scratch in `crates/rdlt-connector-postgres/src/source/copy_decode.rs:407`: `let mut ranges: Vec<Option<(usize, usize)>> = Vec::with_capacity(self.plans.len())` allocates once per ROW — 1M per `pg-to-pg-1m` run, on the tokio I/O thread. Make it a reusable field cleared per tuple
- [ ] T081 [P] [US8] Add `SET LOCAL work_mem` to the publish transaction in `crates/rdlt-connector-postgres/src/dest/commit.rs` so the merge dedup sort stops spilling at Postgres's 4 MB default. `SET LOCAL` scopes it to the transaction, so it cannot affect any other session — state that rule in the comment
- [ ] T082 [US8] Evaluate the dedup subquery once per publish rather than once per statement in `crates/rdlt-connector-sqlcore/src/plan/arms.rs`: the scd2 arm interpolates `deduped(…)` into three statements and the hard-delete upsert arm into two, so the server re-sorts each time. Zero effect on all five benchmarked cells (none uses scd2 or hard-delete) but a real multiplier for workloads that do — so measure it on a purpose-built workload, not on the matrix
- [ ] T083 [US8] Re-pin `crates/rdlt-connector-postgres/tests/golden_sql.rs` for T082's statement changes — a deliberate, reviewable diff (FR-003, PI4)
- [ ] T084 [US8] Measure and record (PI1): the decoder change on `pg-to-pg-1m` (expect CPU movement, ~0 wall); `work_mem` on the corrected dedup cell (measured ~−4%, do not oversell it); T082 on its purpose-built workload with the number recorded. Fill close-out row US8

**Checkpoint**: the cheap wins are banked.

---

## Phase 11: User Story 9 — One pipeline uses the machine the way four do (Priority: P3)

**Goal**: establish what the real ceiling is, then take the parallelism that survives the earlier stories.

**⚠️ Opens with measurement, not design (FR-039).** The recorded 3.5× may be the fixture: `benches/bench-setup.sh:49-50` starts a **stock `postgres:16`** with no tuning — default `shared_buffers`, `fsync` on, default `max_wal_size`.

**Independent Test**: the ceiling is recorded against a non-saturating destination and the post-improvement baseline; a merge-mode pipeline exceeds one core; ordering, exactly-once and backpressure survive concurrency.

- [ ] T085 [US9] Run ceiling arm E0 — server capability, no rdlt: N concurrent `COPY … FROM STDIN BINARY` replaying a captured payload, N ∈ {1,2,4,8}, into **both** an UNLOGGED stage table (the merge regime) **and** a LOGGED target (the post-US5 Replace regime). Two arms, because the two regimes now differ. Record rows/s per arm and the `synchronous_commit` setting under which it was taken
- [ ] T086 [US9] Run ceiling arm E1 — the post-improvement pipeline baseline: re-run the N-scaling curve (N ∈ {1,2,4,6,8}) on the tree with US1–US8 merged, so the design targets the shortened serial path rather than the original one
- [ ] T087 [US9] Run ceiling arm E2 — a destination that does not itself saturate: the `file:` destination at a local tmpfs path, in **both** `jsonl` and `parquet` form with writer properties frozen. The difference between the two arms IS the encoder term, which must be subtracted before the residue is called an engine ceiling. Report as "engine + source + writer", never as "the engine ceiling"
- [ ] T088 [US9] Fix the design target from E0–E2 and record it in close-out.md, including the honest statement that the 1.5M rows/s aggregate is end-to-end pipeline throughput whose binding term was unattributed. **If the evidence says the fixture was the ceiling, say so and re-scope this story** rather than building against a number that does not exist
- [ ] T089 [US9] Replace `__rdlt_arrival BIGSERIAL` with an engine-assigned ordinal in `crates/rdlt-connector-postgres/src/dest/commit.rs:200`: the stage column becomes `BIGINT NOT NULL` and the session reserves `[base, base + num_rows)` per batch at the moment `write()` accepts it — i.e. in the loader's serial order, so interleaving cannot reorder it. **A sequence does NOT order correctly across concurrent COPY connections**, and this ordering is what makes merge dedup last-wins. This changes the stage DDL and adds a column to the wire tuple
- [ ] T090 [US9] Re-pin US4's byte-identity fixture for the added wire column (FR-003, PI4) — a deliberate, reviewable diff. Flagged here because it is easy to miss: T036's pin is against the pre-US9 tuple shape
- [ ] T091 [US9] Implement parallel staging for **merge-mode** tables in `crates/rdlt-connector-postgres/src/dest/`: `Arc<Client>` plus a `JoinSet` (a `FuturesUnordered` stored as a session field cannot hold futures borrowing `self.client`), slot admission via `tokio::sync::Semaphore`, quiesce before `BEGIN`. Slots = `min(configured, available_parallelism())` via `std::thread::available_parallelism`, with one slot taking the **same** code path rather than a preserved serial branch (D1 forbids the dual path). No connection-pool crate — `deadpool`/`bb8`/`r2d2` solve dynamic sizing and recycling, which is not the problem here
- [ ] T092 [US9] Fix the session-setup defect that appears the moment a second connection exists: `Postgres::open` runs `SET search_path TO {schema}` on the single client (`crates/rdlt-connector-postgres/src/dest/mod.rs:107-121`) and stage names are used unqualified throughout, so a second client without that `SET` writes to the wrong schema. Route every slot's client through the same setup path
- [ ] T093 [US9] Close the backpressure hole properly (FR-043): transfer the byte permit **into the staging slot** and drop it when that slot's COPY completes — not when `write()` returns, which is when the batch *enters* flight and would leave N in-flight batches unaccounted. Decide and record which seam carries it: an engine `OwnedSemaphorePermit` crossing the semver-sacred SPI, or a session-local byte budget seeded from the engine's. If the permit cannot cross, the session-local budget is the answer and the reason is recorded
- [ ] T094 [US9] Add a crash point for the new failure state (crash with k of N slot COPYs complete), register it in `crates/rdlt-connector-postgres/src/dest/mod.rs` and the expected list in `tests/dest_crash_sweep.rs`, and restate in `crates/rdlt-engine/tests/crash_sweep.rs` what `session.after_write` now brackets. Classify slot failures through the typed constructors at the new seam — with `write()` returning before its COPY completes, classification moves from inside `write()` to quiesce time inside `commit()` (Principle V)
- [ ] T095 [US9] Verify the ordering and safety invariants under concurrency: per-table ordering preserved (FR-041) via the upstream-stamped ordinal and the `ensure_table` quiesce; exactly-once at every crash point (FR-004); peak memory within 25% of baseline (FR-043); and **no throughput regression at `available_parallelism() == 1`** or under a cpu-quota'd container (FR-044)
- [ ] T096 [US9] Record the semver outcome (PI8): `LoadSession` is **unchanged**, `cargo semver-checks` reports no break on `rdlt-connector`, and **the recorded 0.2 → 0.3 window stays closed**. Record that outcome explicitly in `benches/GOVERNANCE.md` and close-out.md — an unexercised window is a result, not an omission
- [ ] T097 [US9] Measure and record (PI1): a **merge-mode** pipeline above one core of utilisation and ≥ 50% more rows/s than the baseline of record (SC-005 as re-targeted). Record explicitly that full-refresh single-pipeline throughput remains bounded by one bulk-load connection, and why. Fill close-out row US9

**Checkpoint**: the ceiling is known and what was reachable has been taken.

---

## Phase 12: Polish & Close-out

- [ ] T098 Run the final recorded three-way session (`make bench TARGET=e2e`) on the merged tree, quiet guard passing, fixture identity verified — this is the session the published matrix quotes. Regenerate `benches/RESULTS.md` (`make bench TARGET=report`)
- [ ] T099 Re-derive every bar in `benches/bars.toml` from the final session's own floors per Principle VIII — at most one per cell, each below its cited floor, each with a `RESULTS.md` policy-log entry. Cells at parity or behind carry no bar and that is recorded
- [ ] T100 [P] Verify PI2 mechanically: a search of the shipped tree finds no surviving copy of anything this feature replaced — the parquet WAL writer/reader, `encode::cell_value` and the boxing path, `BinaryCopyInWriter` usage, `content_hash`, `struct Verify`/`VerifyOutcome`, `StreamAttribution`. Record the searches and their zero results (SC-012)
- [ ] T101 [P] Verify PI3 mechanically: every hand-written component this feature introduced or retained carries its recorded fact-based justification (numeric encoder, tuple framing, the uuid parser retention), and every dependency change is recorded with version, feature path and tree cost — one removal (`parquet` from `rdlt-engine`), at most one addition (`smallvec`, only if T065's gate opened), one rejection (`uuid`)
- [ ] T102 Measure coverage baseline-first (`make coverage`) and record it against the ≥ 80% floor with any exclusions named
- [ ] T103 Complete `specs/019-performance-improvements/close-out.md`: every PI clause and every story disposed with evidence, zero uncited claims, deviations named. Confirm cold start ≤ 40 ms and the full gate green on the merged tree

---

## Dependencies

```
Setup (T001–T002)
   └─> Foundational (T003–T004)  ← blocks every measured claim
          └─> US1 (T005–T014)    ← the matrix must be truthful first
                 └─> US2 (T015–T026)
                        └─> US3 (T027–T035)   ← re-records iai baselines
                               ├─> US4 (T036–T047)
                               ├─> US6 (T059–T067)
                               └─> US5 (T048–T058)
                                      └─> US9 (T085–T097)   ← needs 2/4/5 landed
   US7 (T068–T079)  ─ independent, any time after Foundational
   US8 (T080–T084)  ─ independent, any time after Foundational
                                             └─> Polish (T098–T103)
```

**Hard edges**:

- T003 before every measurement task (T013, T026, T035, T047, T058, T067, T079, T084, T097).
- T029 (baseline re-record) before T037 and T067 — those compare instruction counts.
- T036 and T037 (pins + instrument) strictly **before** T038–T042 — PI4 requires the oracle to be captured from the code being deleted.
- T059 (identity golden listing) strictly **before** T060–T065.
- T011 and T074 share one artifact `format_version` bump — whichever lands first owns it.
- T051 (stage removal) creates the constraint that T091 addresses merge-mode only.
- T089 (ordinal) before T090 (re-pin) before T091 (parallel staging).

**Parallel opportunities**: within US1 (T007, T008); US2 (T020, T022); US3 (T031); US5 (T052); US6 (T063, T064); US7 (T074); US8 (T080, T081); Polish (T100, T101). US7 and US8 can run as whole phases alongside US2–US6 by different hands — they touch disjoint files.

---

## Implementation Strategy

**MVP = US1 alone.** It corrects a published claim, needs no engine change, and turns the matrix's only recorded loss into a 2.6× win. It is also the prerequisite for trusting every later number.

**Then the serial-path cluster in order**: US2 (largest measured engine win, −18%/−21% wall) → US3 (fixes the measurement substrate) → US4 and US5. These four are where wall-clock actually moves, because they sit on the critical path.

**US6, US7, US8 are independent** and can proceed in parallel with the cluster by different hands.

**US9 last, and honestly.** It opens with three measurement arms whose purpose is to *falsify* the recorded 3.5×. If E0–E2 show the ceiling belonged to the benchmark's stock Postgres rather than to the engine, the right outcome is to record that and re-scope — not to build parallelism against a number that does not exist.

**Every story ends with a recorded measurement** (PI1). A story whose measurement shows no improvement is reported as such and either dropped or justified on non-performance grounds; it is not re-run until the number is favourable.
