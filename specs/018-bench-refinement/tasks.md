# Tasks: Benchmark Refinement — Three-Way E2E Matrix

**Input**: Design documents from `/specs/018-bench-refinement/`

**Prerequisites**: plan.md, spec.md, research.md, data-model.md, contracts/bench-refinement.md, quickstart.md

**Tests**: the workspace gate + the harness selftest suite are the behavior pins; new tests appear only where a contract clause demands one (artifact-v1 loud rejection, variants-collision load error, vocabulary sweep). Measurement sessions are deliverables, not tests.

**Organization**: grouped by user story (US1–US5 = phases P0–P4; the spec's stories ARE the plan's phases, so story order is execution order — each phase merges independently with the full gate green).

## Format: `[ID] [P?] [Story] Description`

## Phase 1: Setup

- [X] T001 Create the feature close-out record at specs/018-bench-refinement/close-out.md (one row per BR clause + per phase deliverable, columns: item / phase / disposition / evidence — the 017 pattern) and capture the pre-migration state facts it must cite: current HEAD hash (the archive commit for Milestones), the 8 bar values from benches/bars.toml, and the final recorded medians for the retiring claims (from benches/results/ artifacts: jsonl-duckdb-200k 13.5×+RSS, shred-only 12.0×, rest-pg 6.7×, parquet-passthrough 3.5×, pg-wide-duckdb 7.8×, pg-wide-pg 7.6×, cold-start 24.2 ms, cdc catch-up ~72k changes/s)

## Phase 2: Foundational

No foundational tasks — the existing harness and gate are the foundation; US1's own first task (the governance amendment) is the only ordering constraint and lives inside US1 (amend-then-delete, research D-02).

**Checkpoint**: archive facts recorded; user stories may begin.

---

## Phase 3: User Story 1 — The benchmark says one thing, simply (Priority: P1) 🎯 MVP — plan phase P0

**Goal**: one five-cell matrix and nothing else; taxonomy/suites/modes deleted; cold-start relocated; presentation rebuilt; governance amended FIRST.

**Independent Test**: quickstart P0 block — vocabulary sweep zero hits, amend-before-delete visible in git log, cold-start check enforced on instruments, report regenerates diff-clean, full gate green.

- [X] T002 [US1] GOVERNANCE FIRST (BR2): apply Amendment A verbatim to .specify/memory/constitution.md (Principle VIII → cells/bars wording, version 1.0.0→1.1.0, Sync Impact Report in the header comment) and Amendment B verbatim to specs/012-bench-harness/contracts/bench-harness.md; commit BEFORE any vocabulary deletion (both texts in specs/018-bench-refinement/contracts/bench-refinement.md)
- [X] T003 [US1] Harness collapse, schema side: remove `class` and `suite` from the cell schema and `Mode`→Subprocess-only (delete the enum + `mode` key entirely per research D-05) in crates/rdlt-bench/src/cells.rs; artifact format_version 1→2 in crates/rdlt-bench/src/artifact.rs (`class` removed, optional `extra` object, `forced` annotation; reader REJECTS v1 with a message naming the archive commit from T001); update crates/rdlt-bench/tests/selftest.rs to the new shapes; add the v1-rejection test
- [X] T004 [US1] Harness collapse, behavior side: delete all class/suite branches from crates/rdlt-bench/src/{runner.rs,gate.rs,report.rs,main.rs,protocol.rs}; quiet guard becomes the one classless rule (refuse/wait for ANY run; RDLT_BENCH_FORCE=1 → `forced: true` in the artifact) in runner.rs; bar cross-validation reduces to every-bar-references-an-existing-cell in gate.rs
- [X] T005 [US1] Delete crates/rdlt-bench/src/library_mode.rs, its module wiring in main.rs/lib path, the Mode::Library arm remnants, and the bench-side shared_parity_specs_all_parse test; UPDATE the header comment of benches/parity_specs.yaml to name rdlt-cli as the remaining pin consumer (FR-005 — the fixture and the CLI parse/build pins survive; verify `cargo nextest run -p rdlt-cli -E 'test(parity)'` still green)
- [X] T006 [US1] Cold-start → instruments (FR-006): create benches/check-cold-start.sh (the recorded hyperfine protocol from the deleted cell, asserting ≤ 40 ms, non-zero exit on breach, quiet-machine note); wire it into the Makefile instruments verbs (runs with TARGET=iai make bench and therefore make check); delete benches/competitors/dlt/cold_start.py with the cell in T007
- [X] T007 [US1] THE MIGRATION COMMIT (BR1, ONE commit): delete the 25 legacy cells (benches/cells/{pg.toml,merge.toml,cdc.toml} entirely; e2e.toml stripped to empty-for-now; keep selftest.toml), their pipeline specs under benches/cells/pipelines/, the 10 fixtures + seed_merge_index.sql + seed_refine.sql + polaris_bootstrap.py from benches/fixtures/, ALL benches/results/*.json v1 artifacts, dlt scripts {cold_start,normalize_only,pipeline_jsonl_duckdb,pipeline_pg_duckdb,pipeline_rest_pg}.py and the dlt-sqlalchemy variant from benches/competitors/dlt/variants.toml, and all 8 bars from benches/bars.toml (leave empty-with-header); the commit message cites every retired cell's final value + the archive commit (facts from T001)
- [X] T008 [US1] Presentation rebuild (FR-014): rewrite benches/RESULTS.md to the new skeleton (methodology+policy-log header with the matrix-rebuild/bar-retirement entry; ONE generated matrix region with BEGIN/END markers; hand-written Caveats; generated Trends region; hand-written Milestones seeded with the retired claims + archive commit per research D-13); create benches/GOVERNANCE.md with the relocated coverage/semver/exclusion records; rebuild crates/rdlt-bench/src/report.rs for the new sections (matrix table: cell/medians-with-spread/ratios/bar/status, caption from cell note; Trends from benches/history.jsonl); add the history-append (one line per cell×variant per recorded invocation, ts from the artifact) to runner.rs
- [X] T009 [US1] US1 verification: quickstart P0 block — BR1 vocabulary sweep zero hits; amend-before-delete order visible in git log; check-cold-start.sh enforced; `cargo run -p rdlt-bench -- report` regenerates diff-clean; full workspace gate (`cargo nextest run`, doc-tests, `make lint`) green; close-out rows BR1/BR2/BR3(P0 half) filled

**Checkpoint**: US1 = plan P0, independently mergeable. The benchmark is empty-but-coherent (5-cell skeleton pending US3), all claims preserved in Milestones.

---

## Phase 4: User Story 2 — Feasibility is proven before machinery is built (Priority: P2) — plan phase P1

**Goal**: the five probes answered with recorded evidence on THIS machine; go/no-go each; zero harness code.

**Independent Test**: specs/018-bench-refinement/spike/ holds five evidence-backed records; the runtime probe has an explicit decision; no crates/ or benches/ code changed in this phase.

- [x] T010 [US2] Probe 1 — RUNTIME (#1 risk): attempt `abctl local install --low-resource-mode` under rootless podman (`KIND_EXPERIMENTAL_PROVIDER=podman`); record evidence + decision in specs/018-bench-refinement/spike/01-runtime.md. If podman fails: DOCUMENT the docker path and STOP for recorded owner approval before any system-level install (BR5); a no-go records the absent-with-reason fallback shape for US4
- [x] T011 [US2] Probe 2 — networking (depends on T010 go): from a kind pod, reach the host's postgres and RUSTFS fixture ports; record the working address form (host.docker.internal vs gateway IP) in spike/02-networking.md
- [x] T012 [US2] Probe 3 — API fields (depends on T010 go): throwaway connection via the public API, one sync, `GET /v1/jobs/{id}`; pin the exact status/timing/recordsSynced/bytesSynced field names in spike/03-api-fields.md
- [x] T013 [P] [US2] Probe 4 — quiet-guard fit (depends on T010 go): idle 1-min loadavg with the kind cluster up vs the guard threshold; decision (fits / recorded allowance / stop-cluster-between-arms) in spike/04-quiet-guard.md
- [x] T014 [P] [US2] Probe 5 — reset fidelity (depends on T010 go): sync → reset + destination schema drop → row counts prove initial state restored; record in spike/05-reset.md
- [x] T015 [US2] Spike summary: spike/00-summary.md with the five go/no-go decisions and the resulting US4 shape (3-way, or 2-way + absent-with-reason); close-out row BR5 filled

**Checkpoint**: US2 = plan P1. If probe 1 is no-go, US3 still proceeds; US4 blocks with the recorded fallback.

---

## Phase 5: User Story 3 — The new matrix measures rdlt against dlt (Priority: P3) — plan phase P2

**Goal**: five real cells, consolidated fixtures, slimmed dlt module, and the FIRST RECORDED SESSION (rdlt vs dlt, 10 arms, rowcount-verified, unenforced).

**Independent Test**: session artifacts exist for 5 cells × (rdlt + dlt), 10/10 verifications pass, the generated matrix renders them, bars.toml still empty.

- [X] T016 [US3] Fixture reshape (research D-07): rewrite benches/fixtures/fixtures.toml to the two fixtures — `pg` (one postgres:16 container; database `src` seeded via seed_pg.sql; empty dest_rdlt/dest_dlt/dest_airbyte databases created at start; per-run reset = drop/recreate the measured arm's destination schema) and `rustfs` (pin 1.0.0-beta.11; bucket `raw` session-seeded with gen_jsonl.py output; bucket `lake` with per-product prefixes, prefix-delete reset); adapt the seeding plumbing in crates/rdlt-bench/src/fixtures.rs
- [X] T017 [P] [US3] The five rdlt pipeline specs in benches/cells/pipelines/: pg-to-pg.yaml (pg source → pg dest, replace), pg-to-s3parquet.yaml (pg → file dest parquet+s3 lake/rdlt), s3jsonl-to-pg.yaml (file source jsonl s3 raw → pg dest), s3jsonl-to-s3parquet.yaml (file → file), pg-to-pg-dedup.yaml (merge upsert, key id) — all consuming the fixture conn/endpoint templates
- [X] T018 [US3] The five cells in benches/cells/e2e.toml per data-model §1 (id/fixtures/pipeline/expected_rows/note/competitors; the dedup cell's generate step prepares load-1 + the 50%-changed load-2 dataset and the measured run is LOAD 2 ONLY per research D-08; its regime note recorded)
- [X] T019 [P] [US3] dlt module slim + extend (research D-11, plan list): Dockerfile +s3fs +connectorx −duckdb extras; adapt pipeline_pg_pg.py, rewrite pipeline_parquet.py → pipeline_s3jsonl_s3parquet.py, add pipeline_pg_s3parquet.py / pipeline_s3jsonl_pg.py / pipeline_pg_pg_dedup.py; variants.toml: `dlt` (connectorx backend for pg sources) + `dlt-pyarrow` (context), delete remaining retired script references
- [x] T020 [US3] FIRST RECORDED SESSION (BR4): quiet machine, `make release`, build the dlt image, `cargo run -p rdlt-bench -- run` — 5 cells × (rdlt + dlt + dlt-pyarrow context); every arm rowcount-verified; artifacts committed (format v2); `report` renders the matrix + Trends gains its first lines; policy log gains the connectorx-scoping entry; close-out rows BR3/BR4/BR7(dlt half) filled

**Checkpoint**: US3 = plan P2. The matrix is live two-way; numbers recorded, nothing enforced.

---

## Phase 6: User Story 4 — The matrix goes three-way (Priority: P4) — plan phase P3

**Goal**: driver competitor kind + Airbyte module + the first recorded three-way session (or absent-with-reason per US2's decisions).

**Independent Test**: session artifacts carry all three products' arms for the five cells (measured or Missing{reason}); per-product timing boundaries stated in the rendered matrix.

- [x] T021 [US4] Driver kind (BR6, research D-10): variants discovery from benches/competitors/*/variants.toml (flat namespace; duplicate id = load-time error naming both files — with a test) in crates/rdlt-bench/src/competitors.rs; `kind = "self_timed_container" | "driver"` variant field; driver runs execute the module's host-side driver via the venv convention and consume the existing last-line JSON (+`extra{}` pass-through into the artifact); `Missing{reason}` when the module's prerequisite probe fails; per-variant `runs` honored (airbyte 3)
- [x] T022 [US4] Airbyte module in benches/competitors/airbyte/: setup.py (idempotent: abctl install-if-needed per spike/01 decision, five connections via the API, ids → gitignored state.json), driver.py (trigger sync via POST /v1/jobs, poll GET /v1/jobs/{id} with the spike/03-pinned fields, emit seconds=job-wall + rows + extra.sync_s + cluster cgroup rss via the existing read_cgroup_via_exec path), variants.toml (`airbyte`, pinned version, kind=driver, runs=3), README.md (fairness policy, prerequisites, spike pointers); add the state.json gitignore entry
- [ ] T023 [US4] FIRST 3-WAY SESSION: quiet machine (per spike/04's decision), `cargo run -p rdlt-bench -- run` — 15 arms (or absent-with-reason for probe-ruled-out cells); artifacts committed; matrix renders three columns + ratios; caveats gain the Airbyte fairness text (what these cells measure and don't); close-out rows BR6/BR7(airbyte half) filled
- [ ] T024 [US4] US4 verification: absence path proven — run once with abctl stopped/absent → affected arms record Missing{reason}, all other arms still measure; full gate green

**Checkpoint**: US4 = plan P3. Blocked only by a US2 no-go, in which case the recorded fallback ships instead.

---

## Phase 7: User Story 5 — Enforcement returns measurement-first (Priority: P5) — plan phase P4

**Goal**: ≤ 1 bar per cell, each below its recorded three-way session floor, each with a policy entry; the gate green against the justifying session.

**Independent Test**: `cargo run -p rdlt-bench -- gate` passes against the session cited by every bar's policy entry; no cell has two bars; no bar binds a cluster statistic.

- [ ] T025 [US5] Set bars measurement-first (BR8, research D-14): from the recorded 3-way session, add ≤ 5 bars to benches/bars.toml (`ratio_vs dlt` per cell where the floor supports it; optionally ONE rss_ratio_vs dlt on pg-to-pg-1m), each below its session floor with a policy entry in RESULTS.md; run `gate` green against the same session; record the explicitly-NOT-taken 3-way Iceberg cell decision in the policy log; close-out row BR8 filled

**Checkpoint**: US5 = plan P4. Enforcement restored with evidence.

---

## Phase 8: Polish & Close-Out

- [ ] T026 [P] Re-run the BR1 vocabulary sweep + the amend-order check at final HEAD; verify every RESULTS.md number is generated-or-cited (SC-003) and Milestones cite the archive commit for every retired claim
- [ ] T027 Complete specs/018-bench-refinement/close-out.md — every BR row terminal with evidence; note BENCH_REFINMENT.md with an executed-disposition header pointing at the close-out (the 017 pattern); update the CLAUDE.md SPECKIT block if any plan decision changed in flight
- [ ] T028 Final full gate at HEAD: `cargo nextest run` + doc-tests + `make lint` + `TARGET=iai make bench` (instruments incl. relocated cold-start) + `cargo run -p rdlt-bench -- gate` (SC-008)

---

## Dependencies & Execution Order

### Phase Dependencies

- **Setup (T001)**: first — the archive facts feed T007's migration message and T008's Milestones.
- **US1 (P0)**: T002 STRICTLY FIRST (amend-then-delete); T003→T004→T005 sequential (same crate); T006 [P]-able with T005; T007 after T003–T006 (deletes what the code no longer references); T008 after T007; T009 last.
- **US2 (P1)**: independent of US1's code but runs after it merges (phase order); T010 gates T011–T014; T013/T014 parallel; T015 last.
- **US3 (P2)**: needs US1's harness shapes; independent of US2. T016→T018 (cells reference fixtures); T017/T019 parallel; T020 last (the session).
- **US4 (P3)**: needs US2's spike decisions AND US3's cells/fixtures. T021→T022→T023→T024.
- **US5 (P4)**: needs US4's recorded session (or the recorded 2-way fallback decision, in which case bars bind dlt ratios only and the policy entries say so).
- **Polish**: last.

### Parallel Opportunities

- US1: T006 alongside T005; within T003/T004 the schema and behavior edits are one-crate sequential.
- US2: T013 + T014 after the runtime go.
- US3: T017 + T019 while T016/T018 land.
- Measurement tasks (T020, T023) are NEVER parallel with anything — quiet-machine rule.

## Implementation Strategy

- **MVP = US1** (plan P0): the cleanup alone delivers the credibility story with zero new measurements — independently shippable.
- **Honest degradation**: a US2 runtime no-go re-shapes US4 to absent-with-reason and US5 to dlt-only bars; nothing silently pretends to be 3-way.
- **Measurement discipline**: T020/T023/T025 are deliberate quiet-machine acts; artifacts are committed evidence, and no enforcement precedes its recorded session (BR8).
