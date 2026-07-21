# Tasks: Unified Benchmark Framework

**Input**: Design documents from `/specs/012-bench-harness/`

**Prerequisites**: plan.md, research.md (R1–R8), data-model.md,
contracts/bench-harness.md (BH1–BH8), quickstart.md

**Tests**: included — the harness's own logic (loaders, stats, gate,
report splicing, artifact schema) gets unit cells welded to each task;
the protocol gets a container-free self-test cell. This feature's
"tests" for US3 are the paired re-measure itself (continuity record).

**Organization**: tasks grouped by user story; US order is build order
(plan Design Notes). Nothing existing is deleted before its replacement
has measured in-band (R7).

## Phase 1: Setup

- [ ] T001 Scaffold the harness crate: add `crates/rdlt-bench` to
  `Cargo.toml` members; create `crates/rdlt-bench/Cargo.toml`
  (`publish = false`, deps ONLY from existing workspace deps: rdlt
  full-featured, serde, serde_json, toml, tokio — BH8) and
  `crates/rdlt-bench/src/main.rs` with house-style arg parsing and
  `list|run|gate|report` dispatch stubs; `cargo check -p rdlt-bench`
  green; verify `git diff Cargo.toml` adds only the member line and
  `cargo tree -p rdlt-bench` pulls zero crates not already in the tree.

## Phase 2: Foundational (blocking all stories)

- [ ] T002 Cell + bar loaders welded to their typed-error cells:
  `crates/rdlt-bench/src/cells.rs` (data-model §1 schema,
  `deny_unknown_fields`, duplicate-id error naming both files) and the
  Bar model (§5) with cross-validation — gated cell without a bar and
  bar without a gated cell are load-time typed errors naming the
  offender (BH1); unit cells in the same files cover every error arm
  plus a happy-path fixture parse.
- [ ] T003 Measurement protocol as code:
  `crates/rdlt-bench/src/protocol.rs` — quiet-machine loadavg guard
  (refuse-or-annotate for gated runs), warmups, N runs,
  median/p95 statistics (BH2); unit cells for the stats math
  (odd/even N, p95 index) and the guard's annotate-vs-refuse decision.

## Phase 3: User Story 1 — One harness, cells as data (P1) 🎯 MVP

**Goal**: a declared cell runs through one uniform protocol with zero
per-cell code; the matrix is listable/filterable.

**Independent test**: the container-free self-test cell runs under
nextest end-to-end (declare → filter → warmup+runs → medians +
dataset identity recorded).

- [ ] T004 [P] [US1] Fixture lifecycle in
  `crates/rdlt-bench/src/fixtures.rs`: kinds per data-model §2
  (postgres-container via podman/docker autodetect, generated-files,
  mock-rest, none); create → seed with content-identity hashes → share
  within an invocation where isolation allows (destination-schema drop
  between runs) → teardown on exit; unit cells for the `none` and
  `generated-files` kinds (container kinds are exercised operator-side
  in US3).
- [ ] T005 [US1] Subprocess runner + `list`/`run` commands in
  `crates/rdlt-bench/src/main.rs` (+ pipeline-template substitution
  into a workdir): spawn the release CLI per protocol.rs, wall time via
  `Instant`; `list` prints coordinates/class/competitors, `run`
  accepts `--class` and `--filter` (glob on id); add the SELF-TEST cell
  (`benches/cells/selftest.toml`, `fixture kind = "none"`, trivial
  subprocess) and a nextest cell that drives it end-to-end proving the
  US1 independent test.

**Checkpoint**: US1 delivers a working harness; everything after adds
metrics or migrates content.

## Phase 4: User Story 2 — Metrics, artifacts, enforced gates (P2)

**Goal**: every run yields the full BH3 metric set in a committed
versioned artifact; bars enforce; tables generate.

**Independent test**: one gated cell's artifact carries every FR-003
metric + fingerprint; tightening its bar makes `gate` exit nonzero
naming the cell; `report` regenerates tables leaving narrative
byte-identical.

- [ ] T006 [P] [US2] Procfs sampler in
  `crates/rdlt-bench/src/sample.rs`: thread polling
  `/proc/<pid>/status` (VmHWM) + `/proc/<pid>/stat` (utime/stime) at
  ~50 ms, last-seen + coarse time-series; unreadable → `null` with
  reason, never fabricated (R3, BH3); unit cell self-observes a spawned
  sleep-and-allocate child.
- [ ] T007 [P] [US2] Library mode in
  `crates/rdlt-bench/src/library_mode.rs`: in-process run via the
  `rdlt` crate; RunReport rows/bytes → rows/s + MB/s (exact),
  `events()` timestamps → per-stream attribution
  (StreamStarted/BatchLoaded/StreamFinished) (R3); unit cell over a
  tiny file→parquet in-process pipeline asserts attribution ordering
  and non-estimated totals.
- [ ] T008 [P] [US2] Artifact schema v1 in
  `crates/rdlt-bench/src/artifact.rs` + fingerprint collection
  (cpu_model, kernel, rustc -V, competitor pin, dataset hashes,
  loadavg) (R5, BH5); write to `benches/results/<cell-id>.json`, raw
  time-series to gitignored `benches/results/raw/` (add the .gitignore
  entry); serde round-trip + format_version cells.
- [ ] T009 [US2] Competitor module in
  `crates/rdlt-bench/src/competitors.rs` + `benches/competitors/dlt/`
  (absorb `benches/baseline/`: Dockerfile, entry scripts, seed SQL;
  add `variants.toml` — dlt-pyarrow baseline, dlt-sqlalchemy +
  dlt-connectorx context): self-timed wall (continuity), cgroup v2
  CPU/RSS delta via podman-inspect path (R4); MISSING status loud, and
  ratio bars over a missing baseline FAIL (BH4); unit cells for
  variants parsing and missing-image handling.
- [ ] T010 [US2] Gate in `crates/rdlt-bench/src/gate.rs` +
  `benches/bars.toml` transcribing the 8 gated RESULTS.md rows
  (flagship ≥10×, peak-RSS ≤1/5, shred ≥10×, REST→PG ≥5×, passthrough
  ≥2×, pg→DuckDB ≥6×, pg→PG ≥6×, cold ≤40 ms absolute) with
  tolerances + policy pointers (R6); nonzero exit naming cell, bar,
  measured value; wall-median bars only — CPU/RSS recorded not gated
  (BH6); verdict unit cells incl. tightened-bar failure and
  missing-baseline failure.
- [ ] T011 [US2] Report in `crates/rdlt-bench/src/report.rs`: splice
  generated tables between `<!-- rdlt-bench:BEGIN/END -->` markers in
  `benches/RESULTS.md`, narrative outside markers byte-for-byte
  preserved (BH7); unit cells: marker splice idempotence,
  narrative-untouched assertion, refusal on missing markers.

**Checkpoint**: US2 delivers the full instrument; US3 fills it with the
real matrix.

## Phase 5: User Story 3 — Migration with continuity (P3)

**Goal**: every existing cell migrated, scripts retired, recorded
numbers proven continuous (in-band) or version-policy re-derived.

**Independent test**: gated set green under `rdlt-bench gate` with the
continuity record showing every cell in-band (or an explicit policy
entry); no run-*.sh remain.

- [ ] T012 [P] [US3] Migrate e2e cells → `benches/cells/e2e.toml` +
  `benches/cells/pipelines/` templates: jsonl→DuckDB (+ its gated
  peak-RSS bar), parquet→parquet, parquet→DuckDB context row,
  mock-REST→Postgres, shred-only, and cold-start as `mode =
  "hyperfine"` shelling the recorded 20-run/3-warmup protocol
  (R8) — replacing `benches/run-e2e.sh`'s coverage (script deleted in
  T017, not here).
- [ ] T013 [P] [US3] Migrate Postgres-source cells →
  `benches/cells/pg.toml` (pg-wide→DuckDB gated, pg-wide→PG gated,
  jsonb→DuckDB scoreboard; dlt-pyarrow baseline + sqlalchemy/connectorx
  context variants) with the `benches/fixtures/` seed carried from
  `baseline/seed_pg.sql` — replacing `benches/run-pg.sh`.
- [ ] T014 [P] [US3] Migrate CDC cells → `benches/cells/cdc.toml`
  (change-apply throughput 500k-backlog, catch-up latency; the
  settle-before-timing note preserved in cell workload config) —
  replacing `benches/run-cdc.sh`.
- [ ] T015 [P] [US3] Migrate merge cells → `benches/cells/merge.toml`
  (merge-index two regimes, delete-insert vs upsert, scope-replace,
  ordered-dedup) — replacing `benches/run-merge-index.sh`,
  `run-merge-strategies.sh`, `run-merge-refinements.sh`.
- [ ] T016 [US3] The paired re-measure session (reference machine,
  quiet, release build, dlt image built): run the full gated set
  same-session paired via `rdlt-bench run --class gated`; write the
  continuity record `specs/012-bench-harness/evidence/continuity.md`
  (per-cell recorded vs new median, delta %, in-band verdict per R7 —
  out-of-band cells get diagnosis and, only if accepted, a
  version-policy entry); commit the artifacts under `benches/results/`;
  `rdlt-bench gate` green.
- [ ] T017 [US3] Retire the scripts + rewire entry points: delete the
  six `benches/run-*.sh`; rewrite `benches/README.md` for the new
  layout (quickstart-aligned); Makefile bench verbs delegate to
  rdlt-bench (`TARGET=e2e` → gated run, new `TARGET=matrix` → full
  sweep, `TARGET=iai` untouched; gate/report verbs) keeping the
  intent-verb style; `compare-iai.sh` + `perf-baselines.json` retained
  unchanged (BH8).

## Phase 6: Polish & close-out

- [ ] T018 Close-out: insert RESULTS.md generated-section markers +
  regenerate tables via `rdlt-bench report` (narrative + History
  preserved, add the 012 history entry); full green sweep — `make
  check`, `cargo nextest run`, `cargo test --doc`, semver-checks ("no
  update required" for rdlt-core/rdlt-connector), `git diff` shows
  zero runtime-crate manifest changes; walk quickstart.md commands
  verbatim.

## Dependencies

- T001 → T002/T003 → US1 (T004, T005)
- US2: T006/T007/T008 [P] after US1; T009 after T008; T010 after
  T008+T009 (missing-baseline verdict); T011 after T008
- US3: T012–T015 [P] after US2; T016 after T012–T015; T017 after T016
  (nothing deleted before in-band); T018 last
- US order is build order — US2 needs US1's runner; US3 needs US2's
  artifacts/gate.

## Parallel example (US3)

Launch T012, T013, T014, T015 together — four different cell files +
their pipeline templates, no shared files; then a single T016 session
measures them all paired.

## Implementation strategy

MVP = Phase 1–3 (a working declarative harness proven by the self-test
cell, no containers needed). Then US2 completes the instrument
(metrics/artifacts/gate/report) with unit-testable logic. US3 is the
operator-heavy phase: cell authoring is parallel and mechanical, but
T016 is ONE quiet-machine session and is the feature's acceptance bar —
budget it like the 004/006 re-measure sessions. If T016 lands
out-of-band on any cell, stop and diagnose before touching T017.
