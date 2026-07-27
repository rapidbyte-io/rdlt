# Tasks: Audit Remediation — Silent Losses Closed, the Record Made True

**Input**: Design documents from `/specs/020-audit-remediation/`

**Prerequisites**: plan.md, spec.md, research.md, data-model.md, contracts/audit-remediation.md, quickstart.md

**Tests**: this feature's gate is the existing workspace suite plus one kind of pin that contract **AR1** demands — a regression pin **demonstrated to fail against the pre-fix build**. A pin only ever observed green is not evidence. Two consequences bind task shape: a **skipping test is green**, so a container-gated test can never be the AR1 pin (capture it container-free); and where a defect is reachable only from an embedding application, the pin is **synthetic and recorded as synthetic**. Measurements in US11 are deliverables, not tests (**AR8**): each ends with a recorded number and a decision.

**Organization**: one phase per user story, in spec priority order. US3 splits into three phases and US5 into two commits (research sequencing), giving 13 implementation increments across 11 stories. Each merges independently with the full local gate green.

## Format: `[ID] [P?] [Story] Description`

- **[P]**: parallelisable (different files, no dependency on an incomplete task)
- **[Story]**: US1–US11, mapping to the spec's user stories
- Every task names its files

## Ordering constraints that are NOT priority

Four real dependencies override the P1/P2/P3 reading:

- **US1 first.** It is doc-only and zero-risk, and it is what makes every later increment plan against facts rather than against a file that says the last feature is unimplemented.
- **US3's resume integrity (Phase 6) before US11's skip-fetch (T171).** The fetch reorder changes what the planner sees; landing it first would put the integrity check on a code path that no longer exists in the shape it was written for.
- **US2 before US8's decimal grammar table (T131).** The table pins the grammar, which US2 does not change; the precision-refusal rows belong to US2 as its own red-before-green pin.
- **US11 last.** Measurement capacity is the scarce resource and every earlier increment changes code that US11 measures.

**US5 additionally makes the 0.3 bump required** (it deletes a public field from semver-sacred `rdlt-core`). US9's local semver check must run **after** US5, so the break is expected and recorded rather than discovered.

---

## Phase 1: Setup

- [X] T001 Create `specs/020-audit-remediation/close-out.md` with one row per contract clause (AR1–AR8), one per user story, and one per audit item, columns: item / story / disposition / evidence — the 017/018/019 pattern
- [X] T002 Seed the close-out's disposition ledger with **every** item enumerated in `NEXT_STEPS.md` (all eight sections, ~120 items, plus the 18 Appendix A refutations pre-marked as non-goals), so AR8's "none silent" is mechanically checkable rather than trusted
- [X] T003 [P] Check host disk headroom and prune container/`target/` residue before starting, recording the reclaimed figures in close-out.md — 017 recorded the gate turning red twice from exactly this (168 GB of podman residue; `target/` at 851 GB)
- [X] T004 [P] Record the pre-change instrument state in close-out.md — every entry of `benches/perf-baselines.json`, the four bar statuses from `benches/bars.toml`, the committed artifact `format_version`, and the release binary size — as the "before" column US8 and US11 compare against
- [X] T005 [P] Verify which container images and runtimes are available (podman, postgres, rustfs, polaris, the pyiceberg venv) and record in close-out.md which test legs can actually run on this machine, so a later green is never mistaken for coverage it did not have

---

## Phase 2: Foundational (blocks every "green" claim)

**⚠️ No increment may claim the gate green before T006 completes.**

- [X] T006 Run the full local gate (`make check`) on the merge base **twice**, clean, and record both runs in close-out.md. If either is red, fix or record the cause before any increment starts — a story cannot be credited with a gate it inherited broken
- [X] T007 Record the AR1 procedure in close-out.md — how a red-before-green pin is captured (stash or merge-base checkout, the command, what the red run printed) — so every later pin cites one consistent method, and note the two exceptions AR1 names (container-gated tests cannot be the pin; embedder-only defects get synthetic pins, recorded as synthetic)

**Checkpoint**: gate genuinely green, ledger seeded, evidence method fixed. Stories may begin.

---

## Phase 3: User Story 1 — The repository stops misinforming its own readers (P1)

**Goal**: every status, measurement, format and behaviour claim the project makes about itself agrees with the artifacts, and the repository ships the license it declares.

**Independent test**: pick any status claim in the project instructions, the 019 completion record, the performance analysis or the benchmark documentation and check it against the artifacts; every claim holds. `git ls-files | grep -i license` is non-empty.

**Doc-and-comment only — zero behaviour change, zero test change.**

- [X] T008 [P] [US1] Add `LICENSE` at the repo root with the verbatim Apache-2.0 text and the boilerplate appendix filled in (the owner supplies the copyright holder string). No NOTICE file — nothing in the tree is vendored or carries an attribution obligation (research R1.1)
- [X] T009 [US1] Rewrite the 019 block inside the `<!-- SPECKIT START -->` region of `CLAUDE.md` in the COMPLETE style of 013–018: recorded three-way session 2026-07-25, per-cell standing vs dlt (13.2x / 1.7x / 95.0x / 63.6x / 2.6x), the honest misses (US2 wall −14.3% vs ≥15%, US2 RSS −7.5% vs ≥8%, US6 cell-CPU −4.9% vs ≥10%, T047 4.0x vs 10x), the US9 re-scope (T089–T095 not built; 1.19M rows/s; 8.43x; Amdahl 1.29x vs SC-005's 1.5x), and the close-out pointer
- [X] T010 [US1] Correct the 018 block in `CLAUDE.md`: replace the superseded medians (1.95/1.62/1.15/0.99/14.60 s) and delete the "0.9x LOSS — dedup merge path = optimization target" framing, which 019 turned into a 2.6x barred win
- [X] T011 [P] [US1] Fix `specs/019-performance-improvements/close-out.md:7` — header status IN PROGRESS → COMPLETE
- [X] T012 [P] [US1] Rewrite the paragraph at `specs/019-performance-improvements/close-out.md:958` asserting T098/T099 are not done into past tense pointing at the session recorded at `:1055`, resolving the document's internal contradiction
- [X] T013 [P] [US1] Close the PI5 contract row at `specs/019-performance-improvements/close-out.md:179` ("Remaining: T094") — T094 was re-scoped away with US9 (`tasks.md:234`), so the row needs a terminal disposition
- [X] T014 [P] [US1] Set `specs/019-performance-improvements/spec.md:7` Status from Draft to the terminal value 017 and 018 use, and check 017/018 spec.md status lines for the same staleness
- [X] T015 [US1] Amend FR-016 in place at `specs/019-performance-improvements/spec.md:553` with the measured inversion (offload cost +7.0% wall, 6.7 ms/batch — D-03 at `close-out.md:358-386`) and the standing re-trigger: the offload becomes worth re-measuring only when a freed thread has work
- [X] T016 [US1] Add the EXECUTED banner to `PERF_ANALYSIS.md` in the exact shape `REFACTORING.md` and `BENCH_REFINMENT.md` already carry, pointing at the 019 close-out and naming the claims its own execution falsified: F3's under-one-core (measured CPU/wall 1.6), §F8's allocator wall-cost claim (D-05 factorial), F6's 12.41% recoverability (D-13), and the ~3.5x headroom (T088)
- [X] T017 [US1] Document the measured aggregate-throughput characteristic (FR-018) in `README.md` and `benches/RESULTS.md`: ~1.19M rows/s single pipeline, 8.43x at 8 concurrent pipelines, and the deliberate trade — a full-refresh load is one transaction on one connection by construction, so aggregate throughput comes from pipeline-level concurrency. This is the deliverable US9's re-scope named and never produced
- [X] T018 [P] [US1] Fix the `benches/bars.toml` header (~:18) still claiming the dedup cell "carries NO bar" while the file defines one, and the parity framing for `pg-to-s3parquet-1m` (now 1.7x, still deliberately unbarred)
- [X] T019 [P] [US1] Fix `benches/README.md:39` — artifacts are `format_version` 3, not 2 (verify against a committed artifact under `benches/results/`)
- [X] T020 [P] [US1] Fix the four contradicted `Makefile` header claims (research R10.9): the `TARGET=deep` scope claim at `:18` vs the recipe at `:84-94`, the missing `coverage` verb in the header block, and the coverage recipe's scope comment
- [X] T021 [P] [US1] Add the 0.2 → 0.3 window standing section to `benches/GOVERNANCE.md` — the window is the next publish and its queue was empty as of 019; note that feature 020 US5 makes the bump required
- [X] T022 [P] [US1] Correct `crates/rdlt-connector-file/src/dest/config.rs:51-53` — the `partition_by` doc claims Hive-style `<column>=<value>` directories; `final_tail` (`dest/layout.rs:61-64`) writes a bare `<value>`, which the README already states correctly
- [X] T023 [P] [US1] Correct the stale comments and silently-ignored-knob documentation in `crates/rdlt-connector-file/src/source/config.rs` (~:40), stating for each of `primary_key`, `validate` and `type_hints` whether it is honoured or ignored (FR-020; verify which by reading the source, do not assume the audit's list)
- [X] T024 [P] [US1] Correct the `json_type` capability contract comment at `crates/rdlt-engine/src/load/lowering.rs:19` against what lowering actually does, and state the WAL-before-validation invariant for merge-key NULL checks at `crates/rdlt-engine/src/load/mod.rs:166` (replay has no such check)
- [X] T025 [P] [US1] Document the snapshot-retention dependency of Iceberg replay detection at `crates/rdlt-connector-iceberg/src/dest/commit.rs:46`, and fix the stale module doc at `crates/rdlt-connector-postgres/tests/direct_publish_guarantees.rs:5` claiming an ignored, unfixed defect
- [X] T026 [US1] Run the full local gate; record US1's close-out row with the list of every corrected claim and the artifact each was checked against

**Checkpoint**: the record is true. Every later increment plans against it.

---

## Phase 4: User Story 2 — Values reach the destination or are counted (P1)

**Goal**: no value the shredder cannot represent disappears silently — it is delivered, refused, or counted.

**Independent test**: a corpus of full-range unsigned integers, hint-violating objects, over-precision decimals and unparseable hinted values ingests with every value present, counted, or typed-refused; `shred_identities.txt` byte-identical.

**Two schema-affecting changes ship here and are recorded, not discovered.**

- [X] T027 [US2] Capture and freeze the identity evidence: run `cargo nextest run -p rdlt-engine --test shred_identity_pin` on the merge base and record the hash of `crates/rdlt-engine/tests/fixtures/shred_identities.txt` in close-out.md. **Add no case to this corpus in this increment** — adding cases while changing the path hides movement (research R2.7)
- [X] T028 [P] [US2] Write the red pin for full-range unsigned integers in `crates/rdlt-engine/tests/`: a column of values above `i64::MAX` must arrive intact, not as NULL. Demonstrate it FAILS on the merge base and record what it printed (AR1)
- [X] T029 [P] [US2] Write the red pin for the type-hint override in `crates/rdlt-engine/tests/`: a hinted column receiving an object keeps its hinted type. **Assert the stored value, not just the resolved type** — pre-fix the column holds the serialized subtree, post-fix NULL with the discard counted (research R2.2)
- [X] T030 [P] [US2] Write the red pins for decimal precision refusal and for hint validation (`Decimal{precision:200}` must be a typed config error, not a panic). Both are embedder-shaped — no shipped connector can put `LogicalType::Decimal` in `type_hints` — so build them in `crates/rdlt-engine/tests/` on `MemorySource` + `StreamSpec::with_type_hint` and **record the evidence as synthetic** (research R2.6)
- [X] T031 [US2] Change `Kind::UInt(_)` to observe `LogicalType::Utf8` in `crates/rdlt-engine/src/shred/infer.rs:49-54` and drop the `saw_inexact_int` assignment. No range condition — a range-conditional rule makes the resolved type depend on value arrival order. Replace the comment with the live invariant (research R2.1)
- [X] T032 [US2] Delete the now-unreachable `Some(Kind::UInt(u)) => Some(u as f64)` arm from `scalar_float64` in `crates/rdlt-engine/src/shred/build.rs:253` in the same change — greenfield, and it is an inexact conversion of exactly the class the escalation rule exists to refuse
- [X] T033 [US2] Guard the shape-conflict arm on pinned-ness in `ColState::observe` (`crates/rdlt-engine/src/shred/infer.rs:112`), adding `ScalarState::is_pinned()`, so a hinted column keeps its type when an object or array arrives
- [X] T034 [US2] Add the precision check to `parse_decimal` (`crates/rdlt-engine/src/shred/build.rs:444`): take `precision`, reject any value whose scaled magnitude reaches `10^precision`, at one point covering both the integer and string arms
- [X] T035 [US2] Add misfit counting to `build_batch` (`crates/rdlt-engine/src/shred/build.rs:123-132`) as a **positional** count of present-non-null inputs that produced NULL cells — never a difference of totals, which underflows on an ordinary nullable list column and panics in debug (research R0.2/R2.4). Return it alongside the batch; guard the push on a non-zero count
- [X] T036 [US2] Update all four `LoadItem::Discarded` construction sites together (`shred/mod.rs:135`, `shred/mod.rs:297`, `shred/passthrough.rs:97`, and the new one) and emit the new producer with the **existing** reason string — a second free-form string would leave substring-matching as the only way to separate them, which AR5 forbids
- [X] T037 [US2] Validate embedder type hints in `validate_streams` (`crates/rdlt-engine/src/runtime/run.rs:238-293`), returning `RdltError::config` naming stream and column, enforcing `1 <= precision <= 38` and `0 <= scale <= precision` — closing the `scale as i8` wrap that slips arrow's own validation
- [X] T038 [US2] Verify `shred_identities.txt` is byte-identical to the T027 hash and record it; if it moved, stop and diagnose before proceeding — identity is frozen (AR4)
- [X] T039 [US2] Record in close-out.md: the two schema-affecting changes with before/after types (FR-027); the **correction to the audit** (the type-hint defect creates no child table — it turns a preserved-verbatim JSON column into a NULLed one, worse and in the other direction, with `type_hints: {c: json}` as the escape hatch); that representability misfits are **not separable** from policy discards in the report; and the residual at `build.rs:184` (an explicit null in a list column still becomes a valid empty list, uncounted, and is deliberately not changed here)
- [X] T040 [US2] Record in `close-out.md` the typed `DiscardReason` enum as a named deferral with the trigger "the next feature that opens the version window for another reason" (AR6)
- [X] T041 [US2] Run the full local gate including `TARGET=prop`; record US2's close-out row

**Checkpoint**: the shredder's "counted, never silent" rule is true.

---

## Phase 5: User Story 3a — File destination ownership and truncation (P1)

**Goal**: a full refresh removes everything the destination previously wrote, and object identification cannot be confused by data values.

**Independent test**: a destination reconfigured across format and partitioning contains exactly one load's rows after a full refresh; a partition value equal to the table name completes truncation and commit.

- [X] T042 [P] [US3] Write the red pin for the key mis-split in `crates/rdlt-connector-file/tests/`: table `events` partitioned by a column whose value renders as `events`, then a Replace load — pre-fix the commit fails and the object survives. Demonstrate red (AR1)
- [X] T043 [P] [US3] Write the red pin for ownership across a config change in `crates/rdlt-connector-file/tests/`: write partitioned jsonl, reconfigure to unpartitioned parquet, Replace, assert zero stale rows. Demonstrate red
- [X] T044 [US3] Replace the `rfind` search in the S3 arm of `keys_of_table` (`crates/rdlt-connector-file/src/location/mod.rs:304`) with a strip against the known table root; add `S3Location::key_of_table_root`; a listed key not under the listed prefix becomes a **typed fatal**, not a silent drop, because a silent drop makes the ownership listing incomplete (research R4.1)
- [X] T045 [US3] Replace `owns_tail(tail, ext, partitioned)` with `owns_part(tail)` in `crates/rdlt-connector-file/src/dest/truncate.rs:22-30`: the last segment starts `part-` and ends with **any** extension this destination can write, at depth 1 or 2, unconditionally. Delete the now-dead parameters (greenfield)
- [X] T046 [US3] Add `DestFormat::ALL` beside `extension()` in `crates/rdlt-connector-file/src/dest/config.rs:22-27` so the exhaustive match forces a new variant to be considered, and pin `ALL`'s completeness against the schemars-generated enum in `dest_config_schema()`
- [X] T047 [US3] Run the full local gate including the file-family container legs; record US3a's close-out row noting that ownership is now independent of current configuration

---

## Phase 6: User Story 3b — Resume integrity for grown parquet inputs (P1)

**Goal**: a resume never proceeds from a recorded position the input's current content does not justify, and a legitimate append still resumes.

**Independent test**: append to a parquet file → resume succeeds; rewrite an earlier row group while growing → typed refusal.

**Must precede US11's skip-fetch (T171).**

- [X] T048 [US3] Add the fixture helper `write_parquet_groups(path, groups: &[&[i64]])` to `crates/rdlt-connector-file/tests/` using `ArrowWriter` with an explicit `flush()` between batches — verified not to emit a trailing empty row group, so `write_parquet_groups(&[&[1,2,3]])` is exactly one group (research R3.7)
- [X] T049 [P] [US3] Write pin P2 (red) in `crates/rdlt-connector-file/tests/`: a file grown *and* rewritten in its consumed prefix must fail typed. Demonstrate it fails on the merge base — pre-fix it silently resumes and loads wrong rows
- [X] T050 [P] [US3] Write pin P1 in `crates/rdlt-connector-file/tests/`: a file grown by legitimate appending must still resume and deliver every row (FR-033 — the check must not over-refuse)
- [X] T051 [P] [US3] Write pin P4 in `crates/rdlt-connector-file/tests/`, the regression pin for the design defect Phase 0 caught: run 1 on the pre-fix build (hash-less cursor), run 2 post-fix against an appended file (unverified resume that records a hash), run 3 against a further append — **must reach the full rowcount**. This goes red under the naive design and green only under the unconditional descriptor (research R0.3)
- [X] T052 [US3] Add `row_groups_hash: Option<String>` to `FileProgress` (`crates/rdlt-connector-file/src/source/types.rs`) with `#[serde(default, skip_serializing_if = "Option::is_none")]`. **Do NOT bump `CURSOR_FORMAT_VERSION`** — `etag` and `tail_hash` were added to this struct with this shape and no bump, `format_version` is serialized unconditionally so a bump would change jsonl and csv documents this fix does not touch, and combined with a refusal gate it would make the increment non-revertible (research R3.4)
- [X] T053 [US3] Replace `FileTask.tail_check: Option<(u64, String)>` with `resume_check: Option<ResumeCheck>` (`TailBytes` | `RowGroupPrefix`) in `crates/rdlt-connector-file/src/source/types.rs`, deleting the old field (greenfield)
- [X] T054 [US3] Implement the prefix descriptor in `crates/rdlt-connector-file/src/formats/parquet.rs`: blake3 over, per row group `0..done`, the loop index, `num_rows`, `total_byte_size`, `num_columns` (a `usize`), then per column chunk the dictionary page offset, data page offset and compressed size — all from the footer already parsed, zero additional parses. Do **not** use `byte_range()` (it asserts on negative offsets and would panic on a hostile footer)
- [X] T055 [US3] Build the descriptor **unconditionally** at the top of `read_task` in `crates/rdlt-connector-file/src/formats/parquet.rs`, before the group loop, walking `0..task.start` from the already-held metadata on every task with `start > 0` — whether or not a check is present. Verification forks that hasher and compares. State the invariant as a comment at the site (research R3.3). This is the fix for the poisoned-hash defect
- [X] T056 [US3] Emit the check only when `done_units > 0`, mirroring jsonl's arming filter (`jsonl.rs:155`), and return a **typed fatal** — never an arithmetic operation — on `groups == 0` with a check present, `start > total_groups`, a negative offset or size in the footer, `end` past the file length, or a short read. Convert the currently-silent empty loop at `parquet.rs:46` to a typed fatal in the same change
- [X] T057 [US3] Add pin P5 in `crates/rdlt-connector-file/tests/` bounding the recorded narrowing: rewrite with the same logical prefix but different `WriterProperties` plus an appended group, and assert the typed refusal — pinning the behaviour as decided rather than discovered
- [X] T058 [US3] Record in close-out.md as an FR-002 deviation: a grow-by-rewrite performed by a different writer or different writer properties is now refused, because the hash covers absolute offsets and a whole-file re-encode is not an append. Also record the migration note (additive optional field, no version change, parquet entries carry no integrity value until the next checkpoint rewrites them)
- [X] T059 [US3] Run the full local gate including the S3/rustfs legs; record US3b's close-out row

---

## Phase 7: User Story 3c — Classification, formats, retention (P1)

**Goal**: one rulebook decides transient-versus-fatal; two-pass races fail typed for every inferred type; unbounded state is bounded or documented with its cost.

**Independent test**: a deterministic storage failure is not retried; a CSV file changed between passes fails typed including for boolean; a rotating-file pipeline's recorded state behaves as documented.

- [X] T060 [P] [US3] Write the red pins in `crates/rdlt-connector-file/tests/`: a deterministic `object_store` failure must not consume the retry budget; a boolean column whose CSV changes between passes must fail typed rather than coerce to `false`
- [X] T061 [US3] Invert `is_recoverable` (`crates/rdlt-connector-file/src/location/s3.rs:303-310`) to an allow-list — `object_store::Error::Generic` and nothing else — then route `S3Location::classify`'s severity decision through it so exactly one place decides transient-versus-fatal for all three call sites. Update the stale rulebook comments at `location/mod.rs:30-33` and `s3.rs:299-302`
- [X] T062 [P] [US3] Fix the inferred-Bool arm at `crates/rdlt-connector-file/src/formats/csv.rs:244` to match `"true"`/`"false"` and return the existing `two_pass` typed error otherwise, mirroring the declared-hint arm exactly
- [X] T063 [P] [US3] Make the temp fetch directory own its cleanup: add an RAII guard in `crates/rdlt-connector-file/src/source/mod.rs` whose `Drop` removes the directory, and **delete** the manual cleanup at `:193-195` in the same change (greenfield — no second cleanup path)
- [X] T064 [P] [US3] Distinguish "is a directory" from "does not exist" in `resolve_files` (`crates/rdlt-connector-file/src/source/mod.rs:59-81`) with an actionable typed error naming a pattern the operator could use instead; give the S3 missing-object message the same closing hint without adding a probe request
- [X] T065 [US3] Bound the destination commit log: add `CommitLog::retain_recent(current_load)` in `crates/rdlt-connector-file/src/dest/layout.rs`, called from `write_state_and_receipt`, retaining the current load's receipts plus the one immediately preceding load. Both readers key on the session's own load id. Do **not** bump `LAYOUT_FORMAT_VERSION`
- [X] T066 [US3] Take FR-038's documented-growth branch for per-file cursor entries: do **not** prune and do **not** add a knob. Document on `FileCursor` (`crates/rdlt-connector-file/src/source/cursor.rs`) and in `crates/rdlt-connector-file/README.md` what is retained, why (a pruned entry whose file reappears re-reads from zero, duplicating rows under Append), what it costs, and the operator's lever. Add a pin asserting the stated rule so a future accidental prune fails loudly
- [X] T067 [US3] Run the full local gate; record US3c's close-out row with the retention decision and its cost figures

---

## Phase 8: User Story 4 — A stalled server cannot hang the pipeline (P2)

**Goal**: no configuration can produce an unbounded wait, and a pagination setup that cannot advance is refused before the first request.

**Independent test**: a deliberately stalling endpoint produces a typed failure within a bounded time on both the data and token paths; an unadvanceable pagination config is rejected at validation.

- [X] T068 [P] [US4] Write the red pin for the unbounded hang in `crates/rdlt-connector-rest/tests/`: a stalling test server must produce a typed error within a bounded time. Demonstrate that pre-fix it hangs (bound the test itself so a red run terminates)
- [X] T069 [P] [US4] Write the red pin for POST pagination in `crates/rdlt-connector-rest/tests/`: a `method: post` stream with a non-object body plus a paginator must be rejected at config validation, not after ten thousand duplicate pages
- [X] T070 [US4] Add `request_timeout_secs: u64` to `RestConfig` (`crates/rdlt-connector-rest/src/source/config.rs`, after `retry_after_cap_secs`) with `#[serde(default)]` = 300, and **reject 0** in `validate` with `ConfigError::Invalid` — 0 must not mean "disabled", because SC-007 forbids any configuration producing an unbounded wait
- [X] T071 [US4] Build exactly **one** `reqwest::Client` in `RestSource::build` (`crates/rdlt-connector-rest/src/source/mod.rs:65-82`) via `Client::builder().timeout(..)`, mapping the build error to `ConfigError::Invalid` (FR-041); pass a clone to the auth provider. Make `build` fallible (all four callers already return `Result`). **Delete** `Client::new()` at `client/mod.rs:56` and the per-fetch client at `auth.rs:113`
- [X] T072 [US4] Add `validate_post_body_pagination` to `RestConfig::validate` in `crates/rdlt-connector-rest/src/source/config.rs` rejecting exactly: `method == Post` AND a non-object body AND a paginator among the four that produce non-empty page params
- [X] T073 [US4] Add `httpdate = "1"` to `[workspace.dependencies]` and to `crates/rdlt-connector-rest/Cargo.toml`, and extend `client::retry_after` (`client/mod.rs:164-171`) to fall back to `httpdate::parse_http_date` when the delta-seconds parse fails, converting to a duration (a past date yields no wait). Record the registry facts in close-out.md: `httpdate 1.0.3` is already in `Cargo.lock` via `hyper`, so this edge costs **zero new tree entries**
- [X] T074 [US4] Clamp the **reported** `SourceError::RateLimited { retry_after }` by `retry_after_cap` at the single site in `driver.rs:164`, leaving `client::retry_after` returning the raw duration so `send`'s existing over-cap semantics are untouched — both forms then pass through one clamp
- [X] T075 [US4] Replace `Mutex<Option<CachedToken>>` with `Mutex<TokenState { generation, cached }>` in `crates/rdlt-connector-rest/src/source/client/auth.rs:19`, thread `Option<u64>` through `attach`/`send`, and make `on_unauthorized` clear only when the failing generation matches the cached one. Reuse the existing single-flight mutex — add no new synchronisation primitive
- [X] T076 [US4] Split `resolve::substitute` (`crates/rdlt-connector-rest/src/source/read/resolve.rs:72-78`) into the raw form (query and body) and a new `substitute_path` that percent-encodes each substituted **value** (never the template's own slashes) with the RFC 3986 unreserved set; call it from the path site only. Hand-rolled — no new dependency
- [X] T077 [P] [US4] Extend `RESERVED` in `crates/rdlt-connector-rest/src/source/config.rs:68-78` to the 13 exact, case-insensitive credential header names (research R5.7), changing the array to a slice so its length is not hand-maintained. No substring rule; not extended to query params. Update the field docs and README to match
- [X] T078 [P] [US4] Delete the invariant body term from the request fingerprint at `crates/rdlt-connector-rest/src/source/read/driver.rs:121` and replace the block's comment with the live invariant. Ship with **no performance claim and no measurement** — the value is provably constant across every page of a sequence, so it cannot change a guard outcome (research R5.8)
- [X] T079 [US4] Run the full local gate including the REST sweep; record US4's close-out row

---

## Phase 9: User Story 5 — A declared schema contract means what it says (P2)

**Goal**: the engine's schema-policy behaviour and its documented contract agree, and no persisted field is written and never read.

**Independent test**: a frozen stream run twice with drift introduced between runs behaves as documented; a run legitimately observing fewer columns is not reported as drift; a new nested collection mid-run is policed like a new column.

**This increment makes the 0.3 bump required. Two commits: baseline, then enforcement.**

### Commit A — the baseline, written but not yet read (safe alone, independently revertible)

- [X] T080 [US5] Replace `schema_hashes` with `schemas: BTreeMap<TableName, TableSchema>` (`#[serde(default)]`) in `StateDoc` (`crates/rdlt-core/src/state.rs`) and bump `STATE_FORMAT_VERSION` 1 → 2. Delete the digest field — nothing reads it and a digest can only prove inequality, which is the FR-031 false-positive trap
- [X] T081 [US5] **Stamp the version**: assign `STATE_FORMAT_VERSION` where the recovered document is adopted (`crates/rdlt-engine/src/runtime/run.rs:349-350` and the replay leg at `:387-389`), and replace the hardcoded `format_version: 1` in `crates/rdlt-testkit/src/fixtures.rs:51` with the constant. Without this the bump is inert and a v1 engine hits a serde "missing field" routed through `fatal` instead of the typed refusal (research R6.4)
- [X] T082 [US5] Make `apply_delta` (`crates/rdlt-engine/src/load/apply.rs:31-34`) **merge** rather than overwrite: baseline columns first in baseline order with types joined against the incoming ones, then incoming columns absent from the baseline appended in observed order. State the merge rule for `nullable` and `provenance` explicitly — both participate in the content hash
- [X] T083 [P] [US5] Pin the migration in `crates/rdlt-core/src/state.rs`: a hand-written v1 document (with `schema_hashes`, without `schemas`) deserializes with `check_readable().is_ok()` and an empty `schemas`; a v2 document round-trips; a document written by this build declares `format_version == 2`
- [X] T084 [US5] Measure and record in close-out.md the serialized `StateDoc` size for the widest bench cell — a recorded number, not a threshold gate (research R6.8)
- [X] T085 [US5] Run the full local gate (`make check`) and record it in `close-out.md`; commit A is independently revertible because the baseline is written and not yet read

### Commit B — enforcement

- [X] T086 [P] [US5] Write the red pins in `crates/rdlt-engine/tests/us4_policies.rs` (all four fail on the pre-fix build): (i) two-run drift under Freeze must fail typed with the violated column named; (ii) run 2 whose first drain observes a **subset** and whose second drain re-sights a baseline column must **succeed** — this is the pin that kills the design Phase 0 rejected; (iii) the same shape under DiscardRow must discard nothing; (iv) `per_table["t"] = Freeze` plus a list field appearing at drain 2 must produce a typed violation naming the child table
- [X] T087 [US5] Add `StreamBaseline { schemas: Arc<..>, established: bool }` in `crates/rdlt-engine/src/schema/` constructed once at run start (`crates/rdlt-engine/src/runtime/run.rs`) from `StateDoc.schemas` and reachable from the shred context. The registry is **not** seeded — it keeps its within-run semantics and the emitted `LoadItem` stream is unchanged
- [X] T088 [US5] Implement the **two-diff** governance on both paths (`crates/rdlt-engine/src/shred/mod.rs:150-161` and `shred/passthrough.rs:63-71`): `emit = registry.diff(&observed)` drives LoadItems unchanged; `governed = diff_against(union(registry, baseline), &observed)` drives policy. Empty `governed` → nothing policed and every emitted change is Evolve (this also removes the panic path); `governed == [CreateTable]` → exempt iff bootstrapping, else `policy.action_for(&table, None)`; otherwise per governed change. Delete any single-arm `establishment_changes` shape
- [X] T089 [US5] Replace the `or_else` registry/baseline lookup in `shred/passthrough.rs:54-60` with the same **union**, so a column first seen at drain 2 or later is still widened to its baseline type
- [X] T090 [US5] Add child-table policy inheritance to `SchemaPolicy::action_for` (`crates/rdlt-core/src/policy.rs:87`): resolve `per_table[child]`, then `per_table[root]`, then `default`, carrying the root table name alongside the child. Without it, freezing a parent does not freeze the child table a new nested collection creates (FR-030)
- [X] T091 [US5] Close the table-level discard hole: `enforce_discards` (`crates/rdlt-engine/src/shred/mod.rs:246-248`) skips changes with no column, so a refused table creation silently degenerates to Evolve. Handle the column-less case explicitly
- [X] T092 [US5] Decide the v1-document hole explicitly in `crates/rdlt-core/src/state.rs` (research R6.6): a recovered document with an empty `schemas` under a non-Evolve policy is refused with a **typed** variant naming the pipeline and telling the operator to re-establish once under Evolve — the alternative is a silent one-run Freeze bypass on first upgrade. Add its pin
- [X] T093 [US5] Update the contract documents so the promise is precise rather than narrowed: `2026-07-18-rdlt-engine-design.md:293-294`, `specs/001-*/data-model.md`, `specs/001-*/spec.md` — Freeze is judged against the schema the pipeline has established, and a run observing a subset of established columns is not drift
- [X] T094 [US5] Extend `crates/rdlt-engine/tests/us4_policies.rs` with the cross-run cases and confirm the existing within-run cases still pass unchanged — within-run drift is preserved because the union includes the registry
- [X] T095 [US5] Run the full local gate; run `cargo semver-checks` locally and record the **expected** break on `rdlt-core` in close-out.md, with the note that this converts the standing 0.2 → 0.3 publish-time bump into a required one and that nothing is published, so no consumer breaks
- [X] T096 [US5] Record US5's close-out row including the rejected alternative (narrowing the contract) with its reasoning, and the design Phase 0 overturned with the concrete failure it would have shipped

**Checkpoint**: Freeze means what the design document says it means.

---

## Phase 10: User Story 6 — Nested types work against a real catalog (P2)

**Goal**: an advertised capability is exercised end to end, and an unchanged nested-type stream loads twice without reporting drift.

**Independent test**: a struct + list stream loads into a live catalog, reads back, and loads again unchanged without drift.

- [ ] T097 [US6] Pin the Polaris image at increment start by **live probe** — pull the candidate, read `org.opencontainers.image.version` and the digest off the pulled image, edit the single site at `crates/rdlt-connector-iceberg/tests/common/mod.rs:124`, and prove it by running the iceberg suite twice green. Pin by digest if no immutable version tag is published. **Do not invent a tag** (research R7.6)
- [ ] T098 [US6] Parameterize `crates/rdlt-connector-iceberg/src/dest/test_support.rs:20-51` to take a schema (replacing the hardcoded single-column body; one implementation, no duplicate fixture) — it already builds through the very normalizer that causes the defect
- [ ] T099 [US6] Write the **container-free** red pin in `crates/rdlt-connector-iceberg/src/dest/ensure.rs` tests using T098: ensure a struct-bearing schema twice and assert the second settles without drift. Demonstrate it fails on the merge base. A skipping live test is green and is inadmissible as the AR1 pin (research R7.4)
- [X] T100 [US6] Add the ID-insensitive recursive comparison to `crates/rdlt-connector-iceberg/src/dest/schema.rs` — the module that assigns the IDs, so the invariant and its insensitivity live together — with a `Drift { Type | NestedFields | Nullability }` result; keep the drift policy in `ensure.rs`
- [X] T101 [US6] Replace the ID-sensitive `current_field.field_type != field.field_type` comparison at `crates/rdlt-connector-iceberg/src/dest/ensure.rs:163` with the structural comparison, and pin that a genuinely contradictory nested change is **still** refused typed (FR-048)
- [X] T102 [US6] Add the **asymmetric** nullability rule to the comparison in `crates/rdlt-connector-iceberg/src/dest/schema.rs`: `live.required && !wanted.required` is drift (the write cannot honour it); the reverse is tolerated. Pin both directions (FR-051)
- [X] T103 [US6] Add `crates/rdlt-connector-iceberg/tests/nested_types.rs` (skip-not-fail on both runtime and venv) running the engine **twice** against the same config over a stream with a struct column, a scalar-list column and a hinted decimal, asserting both runs succeed and the catalog shows both loads' rows; extend the existing pyiceberg read-back leg to cover the nested columns
- [X] T104 [US6] Record in close-out.md: the audit's claim upgraded from **plausible to confirmed and guaranteed** (with the three upstream facts), and the nested **additive** evolution ceiling as a named deferral with its trigger — after this fix a struct that gains a child is refused, which is a strict improvement over refusing every struct re-ensure but is still a ceiling for JSON sources. Make the typed error say precisely what happened
- [X] T105 [P] [US6] Record in `close-out.md` both re-probed phase-2 doors as **still closed with registry evidence**: `Transaction` in iceberg 0.10.0 exposes exactly eight actions with no overwrite/rewrite/delete action file, and no client middleware for SigV4. Keep the deferrals; do not open scope
- [X] T106 [US6] Run the full local gate including the iceberg live suite and its sweep; record US6's close-out row

---

## Phase 11: User Story 7 — The engine's remaining sharp edges (P2)

**Goal**: values are refused rather than silently altered, failures are classified consistently, and diagnostics are attributed correctly.

**Independent test**: each of the sixteen items has a pin that fails on the pre-fix build; the group merges as one increment with the gate green.

- [X] T107 [P] [US7] Delete the Decimal arm at `crates/rdlt-connector-postgres/src/dest/encode.rs:42-44` and the redundant Time arm at `:47-49` so both fall through to the representation match at `:63-66`, which reads the scale off the **array** — the scale the payload is actually stored at — and already applies the negative-scale typed fatal. Pin that a schema/array scale divergence no longer rescales values
- [X] T108 [P] [US7] Bounds-check Time64 (`0..86_400_000_000`) before the cast at `crates/rdlt-connector-postgres/src/dest/encode.rs:246`, returning the typed fatal 019's FR-021 promises; use checked/i64 arithmetic for the date epoch shift at `:241`; replace `unwrap_or(i16::MAX)` at `:350` with `expect`. All inside the `field!` closure so a rejected value aborts before any length prefix is backfilled
- [X] T109 [P] [US7] Swap `map_err(fatal)` → `map_err(classify)` at the nine enumerated sites in `crates/rdlt-connector-duckdb/src/dest/commit.rs` (probe, target and stage DDL, add-column, alter-type, scd2 validity DDL, legacy drop-index, the non-constraint index arm) and pin that a transient file lock now retries at ensure as it already does at write
- [X] T110 [P] [US7] Migrate the two async span sites to `tracing::Instrument` (`crates/rdlt-engine/src/runtime/run.rs:497-498` and `load/mod.rs:120-121`), leaving the two `spawn_blocking` `enter()` calls unchanged. Zero new dependencies. Record that no test pins span attribution today and that this item is verified by inspection
- [X] T111 [P] [US7] Add a `Scan::Discard` sibling variant returned from `crates/rdlt-engine/src/wal/resume.rs:165` when a manifest was read but produced nothing replayable, and clear the directory from `recover_wal` (`runtime/run.rs:317`) — the same call the `Damaged` and `Unsupported` arms already make. Pin that repeated crash-before-first-checkpoint does not accumulate residue, and that replayable data is never cleared
- [X] T112 [P] [US7] Reclassify the five internal-invariant sites to `RdltError::internal` (`load/lowering.rs:97,117,129`, `shred/passthrough.rs:157`, `shred/mod.rs:211`), leaving every other `RdltError::config` in the engine unchanged. Update any test asserting the old variant **by variant, not by message text**
- [X] T113 [P] [US7] Map `RdltError::Internal` and the catch-all to exit code 70 (EX_SOFTWARE) in `crates/rdlt-cli/src/main.rs:102`, add `CliError::Io` → 74 (EX_IOERR) for the report write at `:195-196`, and update the exit-code taxonomy in the file header to cover the fallback case
- [X] T114 [US7] Make lowering total for `decimal`: recurse into `ColumnType::Struct` when `caps.structs && !caps.decimal`, and handle `ScalarList { item: Decimal }`, in both `lower_column` and `flatten_array` (`crates/rdlt-engine/src/load/lowering.rs`). Test vehicle is the testkit memory destination with custom capabilities; record the evidence as synthetic (no shipped destination declares the combination)
- [X] T115 [P] [US7] Shorten the suffix to fit the bound in `normalize_ident` and `suffixed` (`crates/rdlt-core/src/naming.rs:49-54`, `:120-126`) rather than changing `IdentRules`; keep the public `ident_hash` clamp. Add the boundary test for `max_len < 9`
- [X] T116 [P] [US7] Log the tokio-postgres driver's terminal error via one shared helper replacing both copies at `crates/rdlt-connector-postgres/src/tls/connect.rs:78-80` and `:89-91`; log EventStream lag at `crates/rdlt-engine/src/lib.rs:92`. Warn, do not propagate
- [X] T117 [P] [US7] Report the event-feed task's failure on stderr as a **warning** at `crates/rdlt-cli/src/main.rs:190` — the run already succeeded and a broken renderer must not turn a successful load into a non-zero exit
- [X] T118 [P] [US7] Make a read or parse failure on an **existing** report a hard error in `crates/rdlt-bench/src/runner.rs:254-259` so "absent" means genuinely absent, and stop treating a failed container inspect as container-exit in `crates/rdlt-bench/src/competitors.rs:364-367`
- [X] T119 [P] [US7] Annotate or exclude forced runs in the bench history so a forced median cannot enter Trends as evidence (`crates/rdlt-bench/src/report.rs:249`) — the `forced` flag exists precisely to prevent this and is dropped one layer over
- [X] T120 [US7] Run the full local gate including all sweeps; record US7's close-out row with one line per item

---

## Phase 12: User Story 8 — The gate becomes as strong as the project claims (P2)

**Goal**: the mutation record describes code that exists, every named pin kills its target, and test residue is reclaimable.

**Independent test**: every survivor has a terminal disposition; each named pin fails on a deliberately broken build; one command reclaims every container the suite starts.

- [X] T121 [US8] Add `crates/rdlt-connector-sqlcore/src/**/*.rs` to `examine_globs` in `.cargo/mutants.toml` (+~205 mutants, +~21 min)
- [X] T122 [US8] Run mutation testing **fresh**: `rm -rf mutants.out mutants.out.old && RDLT_TESTKIT_FORCE_NO_CONTAINERS=1 TARGET=mutants make test` in distrobox. The `--iterate` cache is 100% dead across the 017 renames, so this is a full run — budget 60–90 min (80–115 with sqlcore). Leave `--iterate` in the recipe: it is a no-op on a fresh run and is the right resume mechanism if the run is killed
- [X] T123 [US8] Triage the survivor list in close-out.md in this order: every mutant in a file this feature changed gets a terminal disposition, no exceptions; then the named ones below; the remainder may be a **named deferral with its trigger** but never an untriaged list. Commit the refreshed `mutants.out`
- [X] T124 [P] [US8] Pin `LoadItem::byte_size` by its **consequence** (`crates/rdlt-engine/src/load/mod.rs`): drive a real byte channel and assert backpressure — a Batch at budget sends, a second stays pending under a timeout, it sends after `recv()` frees the permit, and a Checkpoint sends on a zero budget. **Correct the false comment at `crates/rdlt-engine/tests/mutation_closures.rs:24-25`** which claims coverage that does not exist (Principle VI)
- [X] T125 [P] [US8] Split, do not patch, the misnamed WAL test: rename `run_header_serializes_current_version_and_segments_are_sequential` (`crates/rdlt-engine/src/wal/resume.rs:387`) to what it asserts and delete the second clause of its doc comment; add a new test module in `crates/rdlt-engine/src/wal/mod.rs` recording two items and asserting two distinct sequential segment files with their own contents, named in order by the manifest
- [X] T126 [P] [US8] Pin `lower_batch` under **mixed** capabilities in `crates/rdlt-engine/src/load/lowering.rs`: one batch carrying both a struct and a decimal column, asserting `caps(true,false)` preserves the struct while rendering the decimal, and `caps(false,true)` flattens the struct while preserving the decimal
- [X] T127 [P] [US8] Extend T126's mixed-batch test in `crates/rdlt-engine/src/load/lowering.rs` with a non-nullable top-level field and a non-nullable struct child, asserting lowered nullability is false for the top level and true for the flattened child — killing both the `||`→`&&` and `delete !` mutants in one shot; add exact `render_decimal` boundary cases
- [X] T128 [P] [US8] Pin `violation_for` over all three `SchemaChange` variants in `crates/rdlt-engine/src/schema/contracts.rs`, asserting the `from`/`to` fields by value, not by rendered text
- [X] T129 [P] [US8] Pin the `EverySeconds` commit-policy boundary by constructing a `Loader` in an in-crate test module in `crates/rdlt-engine/src/load/mod.rs` with a back-dated `last_commit_at` — wide margins on both sides, no sleeps, no clock control
- [X] T130 [P] [US8] Add an integration test in `crates/rdlt-engine/tests/` asserting a clean run removes the WAL directory, with the companion assertion that a failed run does not
- [X] T131 [US8] Add the first test module to `crates/rdlt-engine/src/shred/build.rs` covering `parse_decimal`'s **grammar** (`".5"`, `"5."`, `"+5"`, `"1e5"`, whitespace, over-scale, i128 boundaries). The precision-refusal rows belong to US2 — this lands **after** US2 shipped that behaviour
- [X] T132 [P] [US8] Pin `SchemaPolicy::freeze()` in `crates/rdlt-core/src/policy.rs` (keep it — it is public API, not dead code)
- [X] T133 [P] [US8] Add the `DestSpec::File` ↔ `FileDestConfig` parity pin in `crates/rdlt-connector-file/tests/config_schema.rs` as a **schemars field-set** assertion with a failure message naming the required action, so it survives US10's embedding refactor
- [X] T134 [US8] Unit-test `drain_loader` directly in `crates/rdlt-engine/src/runtime/run.rs` for the `saw_cancelled` precedence — a dropped sender so the loader completes `Ok`, and a JoinSet holding one already-completed cancelled task. **Do not build a testkit source variant**; the existing sleep-timed closure is why the mutant survives
- [X] T135 [US8] Add `.with_label("rdlt-test", "1")` at every container start site in `crates/rdlt-testkit/src/containers.rs` and the rdlt-bench container code (`testcontainers` 0.23.3 exposes `ImageExt::with_label`), add a documented reclaim verb to the Makefile that removes containers and volumes by that label, and note the convention in the testkit module doc
- [X] T136 [US8] Verify the Makefile reclaim verb after a **deliberately aborted** run, recording the result in `close-out.md` — start the suite, kill it so `Drop` is skipped, then reclaim and assert nothing labelled remains
- [X] T137 [US8] Add the `pg.tx.acked` crash point immediately after the unit commit succeeds and before the unit is taken (`crates/rdlt-connector-postgres/src/dest/commit.rs:894-901`), register it in `FAIL_POINTS`, and add it to the sweep — closing the recorded blind spot where the destination committed and the client never learned. **Do not** build a drops-connection fail-point action (research R9.15)
- [X] T138 [US8] Add `[profile.flake]` to `.config/nextest.toml` with retries and JUnit output, plus a small tool that appends nextest's flaky classifications to a committed log, so the six recorded container flakes become data rather than a re-run convention
- [ ] T139 [US8] Run the full local gate plus the extended sweep; record US8's close-out row with the survivor counts before and after, and the reclaim verification

---

## Phase 13: User Story 9 — The tree is ready to be published (P3)

**Goal**: every publishable crate has accurate, complete metadata, builds in each consumer-selectable configuration, and produces a warning-free documentation build.

**Independent test**: a packaging dry run per crate succeeds with correct metadata; `make docs` is clean.

**CI cannot run — every CI-only verification lands recorded as unperformed.**

- [X] T140 [P] [US9] Correct the two wrong package descriptions — `crates/rdlt-cli/Cargo.toml:3` (says TOML; the binary parses YAML at `main.rs:135`) and `crates/rdlt-connector-file/Cargo.toml` (says "file source"; it has been source+dest with CSV and S3 since 015) — plus the one incomplete and one inconsistent description research R10.3 names
- [X] T141 [US9] Add `keywords`, `categories` and `documentation` to all 12 publishable `crates/*/Cargo.toml`, and a per-crate `readme` — **not** in `[workspace.package]`, because an inherited `readme` resolves relative to the workspace root, not the crate
- [X] T142 [US9] Verify per-crate license inclusion with `cargo package --list -p <crate>` for each publishable crate; a root `LICENSE` satisfies the repository and GitHub detection but **not** the `.crate` tarballs. Add what is missing
- [X] T143 [US9] Add `#![warn(missing_docs)]` to `crates/rdlt-core/src/lib.rs`, `crates/rdlt-connector/src/lib.rs` and `crates/rdlt/src/lib.rs` — warn, not deny; per-crate attribute, not `[workspace.lints]` — and document the public items it flags. Record the real count, which only appears when the lint first runs
- [X] T144 [US9] Add a `docs` verb to the Makefile running `RUSTDOCFLAGS="-D warnings" cargo doc --workspace --no-deps --all-features`, list it in the header block, wire it into `check` after `lint`, and fix the broken intra-doc links it surfaces
- [X] T145 [US9] Verify each publishable crate builds in its consumer-selectable feature configurations (`crates/rdlt/Cargo.toml`, `crates/rdlt-connector/Cargo.toml`, `crates/rdlt-testkit/Cargo.toml`) (the facade's narrowed connector features, the SPI's `schema`, testkit's `containers`), locally rather than as a CI job; record the method and the result
- [X] T146 [P] [US9] Change the CI semver job from `-p rdlt-core -p rdlt-connector` to `--workspace` in `.github/workflows/ci.yml:100-108`. **Record the verification as unperformed** — the job cannot execute (AR7)
- [X] T147 [P] [US9] Regenerate `fuzz/Cargo.lock` (a standalone workspace) so it stops recording `parquet` as an rdlt-engine dependency, stale since 019 US2
- [X] T148 [P] [US9] Replace the no-op pattern in `tools/interop/.gitignore` with `.venv/` — a slash-containing pattern in a nested ignore file is anchored to that directory and matches nothing
- [X] T149 [P] [US9] Honour `CARGO_TARGET_DIR` at both hardcoded sites — `crates/rdlt-bench/src/paths.rs:38` and `benches/check-cold-start.sh:18`
- [X] T150 [P] [US9] Bound the readiness wait in `benches/bench-setup.sh:55` with a timeout and a typed failure message, and remove the hardcoded mise kubectl fallback
- [X] T151 [P] [US9] Document the `hyperfine` and `python3` prerequisites of `make check` in the Makefile header rather than softening the hard failure
- [X] T152 [US9] Run the full local gate plus `make docs`; record US9's close-out row, listing every item whose verification is CI-blocked and therefore unperformed

---

## Phase 14: User Story 10 — Every recorded deferral is taken or re-recorded (P3)

**Goal**: no correctness invariant is hand-maintained in two places, and the deferral ledger closes.

**Independent test**: for each named deferral the repository contains either the taken change or a fresh record with a new trigger.

- [ ] T153 [US10] Add the generic byte-budget channel core to `crates/rdlt-connector/src/channel.rs` — `ByteSized`, `ByteTx<T>`, `ByteRx<T>`, `Permitted<T>` — parameterizing the message cap, sender `Clone`, `ByteSized` sizing and the close-wake
- [ ] T154 [US10] Rewire the engine to the SPI core and **DELETE** `crates/rdlt-engine/src/runtime/channel.rs` in the same change (D17, greenfield). Verify a tree-wide search for the deleted implementation returns zero hits (AR6)
- [ ] T155 [US10] Fix the drifted nullability at `crates/rdlt-engine/src/load/lowering.rs:138` (hardcoded `true`) to the schema side's rule, and record the severity honestly as latent-unreachable rather than promoting it
- [ ] T156 [US10] Add the lowering parity property test in `crates/rdlt-engine/src/load/lowering.rs`: over generated `TableSchema` × the four capability combinations, using zero-row batches, assert exact `arrow::datatypes::Schema` equality between `lower_batch(batch).schema()` and the arrow schema of `lower_schema(schema)` — converting a hand-maintained parity into a machine-checked invariant
- [X] T157 [US10] Replace `DestSpec::File`'s struct variant with a newtype embedding `Box<FileDestConfig>` in `crates/rdlt/src/pipeline_spec.rs`, and replace the field-by-field rebuild at `:397-419` with construction from the embedded config — the shape the Iceberg arm already uses
- [X] T158 [P] [US10] Re-record in `close-out.md` the pg and duckdb `DestSpec` mirrors with a new trigger: neither connector has a deserializable destination config (both are builder-shaped), so the embedding cannot follow. Note that their `DestOptions` leg is already mechanically guarded
- [ ] T159 [P] [US10] Route `flagged_roots` through the dialect dedup seam at `crates/rdlt-connector-sqlcore/src/plan/arms.rs:152-158`, verifying the golden pins stay byte-identical
- [ ] T160 [US10] Move `create_index_sql` and the duplicate-merge-key diagnosis into sqlcore from `crates/rdlt-connector-duckdb/src/dest/commit.rs:68-81` and `crates/rdlt-connector-postgres/src/dest/ddl.rs:52-66`, **adding the golden pin they lack today**, with both executors' emitted SQL byte-identical
- [X] T161 [P] [US10] Re-record in `close-out.md` the shared `ensure_table` choreography extraction with the trigger "the next feature that adds a third SQL destination, or that changes the index-ensure protocol in either executor"
- [X] T162 [P] [US10] Reject D19 in `close-out.md` with its recorded reason (its premise changed — it is a quartet, and the code it names is not a correctness invariant), naming the shape that would close it and a new trigger
- [X] T163 [P] [US10] Remove the verified-unused dependencies: `arrow-schema` from `crates/rdlt-core/Cargo.toml` (and correct the charter comment at `src/lib.rs:10`), `futures` from rdlt-engine, `bytes` and `futures` from rdlt-testkit, demote `tokio` to a dev-dependency of the facade (keeping `tokio-util`), and delete the redundant dev-dependency duplicates
- [ ] T164 [US10] Give `WalRecord::Segment.rows` a consumer in `crates/rdlt-engine/src/wal/resume.rs` rather than deleting it: a pass-1 replay cross-check accumulating decoded rows and, on mismatch, warning and degrading to source re-extraction as the existing damage arms do. **No `WAL_FORMAT_VERSION` bump.** Pin the mismatch path
- [X] T165 [P] [US10] Rewrite the 13 tagged comments across the 8 test files under `crates/rdlt-connector-duckdb/tests/` so each states what the file covers, with no feature number, story number, task ID or contract clause ID (Principle VI)
- [X] T166 [US10] Triage 017's eight verified-but-cut review residuals (`specs/017-workspace-refactoring/close-out.md:222-225`): take three, fold one, re-record four with new triggers
- [ ] T167 [US10] Run the full local gate plus both golden suites; record US10's close-out row and confirm the count of fired-but-undisposed deferrals is **zero** (SC-014)

---

## Phase 15: User Story 11 — The performance queue is answered, not assumed (P3)

**Goal**: every queued question ends with a recorded number and a decision. A queue of recorded negatives is a successful outcome.

**Independent test**: no performance change ships without a measured win; every item has a disposition with a number.

**Last: measurement capacity is scarce and the code under measurement must stop changing first.**

- [ ] T168 [US11] Capture the owed `EXPLAIN (ANALYZE, BUFFERS)` for the merge arm manually via `auto_explain` on the bench fixture database, inside the unit transaction with its `SET LOCAL work_mem`; record the plan verbatim in close-out.md. **rdlt-bench is not extended.** No change is proposed against that path until the plan is in hand (FR-077)
- [ ] T169 [US11] Capture blocked-time attribution into `close-out.md` with a throwaway build instrumenting the ~6 await sites in `crates/rdlt-engine/src/runtime/run.rs` and `load/mod.rs` (tokio-console is rejected — not reachable without a new dependency and a rebuilt binary; sched-tracepoint profiling is verified unavailable from this container). Record what it decides: whether further CPU reductions buy wall or only headroom (FR-078)
- [ ] T170 [US11] Run the free step-1 allocator profile (`perf record` on two cells), recording it in `close-out.md`, and apply its stop condition: if libc allocator symbols are under ~10% of cycles, **record the negative and stop**. Only if they still rank, run the mimalloc A/B, adopting only on a wall or CPU win with RSS within a few percent — the memory edge over dlt is a headline result
- [ ] T171 [US11] Implement the S3 skip-fetch: thread the already-decoded cursor into `resolve_inputs` (`crates/rdlt-connector-file/src/source/mod.rs:214`) and skip etag-matched complete objects, synthesizing their metadata from recorded progress. **Requires Phase 6 to have landed.** Measure the cell before and after and record it (FR-081)
- [ ] T172 [US11] Prototype the D-08 fixed-width COPY fast path (`crates/rdlt-connector-postgres/src/dest/encode.rs`) in a throwaway build; gate on the `pg_copy_encode_10k` iai baseline and the byte-identity fixture, then an interleaved cell CPU A/B. Ship only on a measured win; if it does not clear the threshold, record the negative with its numbers and a site comment
- [ ] T173 [US11] Run the WAL residual-cost A/B (`workdir=None` vs default) on both 1M cells and record the number in `close-out.md`. **019's D2 still binds**: an automatic all-Replace skip is not taken, because D2 rejected precisely that on the ground that recovery becomes a full source re-extraction — expensive against a rate-limited or paid-per-request source. Record the residual cost and the standing decision
- [ ] T174 [US11] Heap-profile the file destination's whole-part buffering (`crates/rdlt-connector-file/src/dest/session.rs`) with `valgrind --tool=dhat` (already a prerequisite of the iai gate; heaptrack is not installed and not worth installing). If the encode buffer dominates the peak, evaluate a single staged part streamed through multipart upload, preserving the one-named-part-per-batch replay protocol. **Record the D18 disposition either way** so it stops being an open trigger
- [ ] T175 [US11] Move the WAL recovery path's blocking work off the runtime thread (`crates/rdlt-engine/src/wal/resume.rs` scan and both replay passes) and verify with a **starvation test**, not a timing — this is async hygiene for embedders and carries no throughput claim
- [ ] T176 [P] [US11] Run the netem 2 ms RTT experiment on the loopback and record it in `close-out.md`; take commit-preamble coalescing only if the measurement justifies it
- [ ] T177 [P] [US11] Probe the canonical-JSON per-object allocation (`crates/rdlt-engine/src/shred/canon.rs`) with D-13/D-21 as the explicit null hypothesis and a high bar to take; record the result either way
- [ ] T178 [P] [US11] Price the merge stage's 1M `nextval()` calls server-side, recording the number in `close-out.md` (two stage-shaped tables, with and without the serial column) and record the number with its expected not-taken disposition
- [ ] T179 [P] [US11] Add an iai bench under `benches/` for the partitioned-write per-row string rendering (`crates/rdlt-connector-file/src/dest/session.rs`), then decide from the number
- [X] T180 [P] [US11] Record in `close-out.md` the reqwest 0.12/0.13 double tree as **rejected with reason** — verified impossible to deduplicate without an upstream version change — with a re-trigger
- [ ] T181 [US11] Dispose every remaining smaller performance item in the close-out, three of them without new measurement; confirm the count closed by assertion or omission is **zero** (SC-015)
- [ ] T182 [US11] Verify all four enforcement bars in `benches/bars.toml` still pass and no cell is worse than its standing of record (`make bench TARGET=gate`); record the matrix (FR-082, SC-016)
- [ ] T183 [US11] Record US11's close-out rows as D-entries following the 019 pattern, with a site comment at each negative so it is not attempted a third time

---

## Phase 16: Polish & Close-Out

- [ ] T184 Verify SC-001/SC-002 in `close-out.md`: all 29 confirmed defects have a terminal disposition with a pin demonstrated red on the pre-fix build; **zero** of the 18 refuted claims appear as implemented work, each appearing exactly once as a recorded non-goal
- [ ] T185 Verify AR6 mechanically and record in `close-out.md`: a tree-wide search for each replaced implementation returns zero hits (the engine channel, the old ownership predicate, the deleted encoder arms, the removed dependencies)
- [ ] T186 Measure coverage baseline-first with `make coverage`, confirm ≥ 80%, and record it in `close-out.md` (FR-012)
- [ ] T187 Complete the close-out matrix: every item from T002's ledger in exactly one terminal state, zero uncited dispositions (AR8, SC-017)
- [ ] T188 Record the feature's deviations in the close-out — the schema-affecting shred changes, the parquet narrowing, the required 0.3 bump, the corrections to `NEXT_STEPS.md` itself, and every CI-blocked verification recorded as unperformed
- [ ] T189 Run the full local gate (`make check`) twice clean and record both runs in `close-out.md`; confirm every increment merged green and each is independently revertible (SC-018)

---

## Dependencies & Execution Order

**Phase order**: Setup → Foundational → US1 → US2 → US3a → US3b → US3c → US4 → US5 → US6 → US7 → US8 → US9 → US10 → US11 → Polish.

**Hard edges** (violating these ships a defect):

- T006 blocks every "gate green" claim.
- Phase 6 (T048–T059) blocks T171.
- Phase 4 (T027–T041) blocks T131.
- Phase 9 (T080–T096) blocks T146 and the semver record in T095.
- T027 blocks T031–T037 (identity evidence must be captured before the path changes).
- T098 blocks T099 (the parameterized fixture is what makes the red pin container-free).
- T121 blocks T122 (mutation scope before the run).
- T153 blocks T154 (the SPI core exists before the engine copy is deleted).
- Within every story, the red pins precede their fixes — that is AR1, not a preference.

**Story independence**: US1 and US4–US11 have no cross-story code dependency; US2 and US3 share no files. Any of them could be developed on its own branch and merged alone.

## Parallel Opportunities

- **Setup**: T003, T004, T005 together.
- **US1**: T008, T011–T014, T018–T025 — twelve independent files.
- **US2**: the three red-pin tasks (T028, T029, T030) together, before any fix.
- **US3c**: T062, T063, T064 — three independent one-file edits.
- **US4**: T077, T078 alongside the client work.
- **US7**: T107–T113 and T115–T119 — thirteen independent single-file fixes; this story parallelises better than any other.
- **US8**: T124–T133 — ten independent pins.
- **US9**: T140, T146–T151.
- **US10**: T158, T159, T161, T162, T163, T165.
- **US11**: T176–T180, subject to one measurement session at a time on a quiet machine.

## Implementation Strategy

**MVP**: Phases 1–3 (Setup, Foundational, US1). Doc-only, zero risk, and it stops the project misinforming every future session — including the next planning round. Deliverable on its own.

**Then, in severity order**: US2 and US3 close the classes where data is silently wrong today; US4 closes the unbounded hang. Stopping after those four is a coherent release.

**Then**: US5 (the contract), US6 (the advertised capability), US7 (sharp edges), US8 (the gate).

**Last**: US9–US11 — readiness, deferrals, and the measurement queue, none of which is urgent and all of which benefit from landing after the code stops moving.

**A note on what "done" means here**: for US11 in particular, a phase that ends with several recorded negatives has succeeded. AR8 forbids closing an item without a number; it does not require the number to be favourable.
