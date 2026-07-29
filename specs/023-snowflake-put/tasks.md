# Tasks: Snowflake Internal-Stage Ingestion, and the Retirement of Two Paths

**Input**: Design documents from `/specs/023-snowflake-put/`
**Prerequisites**: plan.md, spec.md, research.md (D1–D10 + 6 open questions),
data-model.md, contracts/snowflake-put.md (SP1–SP8), quickstart.md

**Conventions**: every increment merges on a green full local gate, run as
`env -u RUSTUP_TOOLCHAIN make check` (that variable silently overrides the
1.96.0 pin); tests via `cargo nextest run`; live legs gate skip-not-fail on
their own credential and ANNOUNCE the skip; never assert success from a
filtered command whose failure the filter cannot see; never re-record perf
baselines to clear a toolchain-mismatch refusal; the other SQL destinations'
golden pins stay byte-identical at every merge.

**Ordering principle**: nothing is deleted before its replacement is proven.
The new path is built and cut over first; the old paths are removed after.

---

## Phase 1: Setup

- [X] T001 Adopt the fork in the workspace: set `snowflake-connector-rs` at
  `Cargo.toml:89` to `{ git = "https://github.com/rapidbyte-io/snowflake-connector-rs.git", rev = "<40-char rev of feat/put-file-upload>", default-features = false, features = ["key-pair-auth", "put"] }` —
  the `version` key DELIBERATELY OMITTED, because a dependency carrying both
  publishes silently with the git source stripped (verified). Record the exact
  revision. Confirm the workspace builds and the existing 022 suite is
  unaffected.
- [X] T002 Verify the fork's feature surface against the workspace: confirm
  `put` exists as a feature in the fork's manifest, that `default-features =
  false` still yields a working key-pair session, and that no new transitive
  dependency conflicts with the workspace pins (`cargo tree -d`). Record the
  dependency-tree delta in research.md as an addendum.
- [X] T003 Correct the `Cargo.toml:32` comment "Internal crates (path deps;
  version for future publishing)" — it becomes false for the snowflake/facade/
  CLI chain while the git dependency stands. State the exception and point at
  `tools/allowed-git-deps.toml`.

---

## Phase 2: Foundational (blocking prerequisites)

**These block every user story.** The record repair authorises the work; the
boundary method and service-fact pins are what every later task builds on.

- [ ] T004 Record repair, part one — the uncited claim: 022's SD1 asserts the
  internal-stage gap was deferred "with the issue filed" and T040 required the
  issue URL in `specs/022-snowflake-dest/close-out.md`; no URL exists anywhere.
  Either cite it or record in close-out.md that it was never filed. Do this
  FIRST: `spec.md:499-501` conditions the fork route on upstream having
  stalled, and "stalled" cannot be claimed against an issue that cannot be
  cited.
- [ ] T005 Record repair, part two — the narrowing: `specs/022-snowflake-dest/spec.md:499-501`
  authorised BOTH closure routes (upstream contribution OR a maintained fork if
  upstream stalls) while `parity.md:15` narrowed it to upstream only. Record
  the narrowing as a correction in 022's close-out, so the fork route is
  authorised by the record rather than by this feature's assertion.
- [ ] T006 Harden and wire the distribution check: take
  `specs/023-snowflake-put/drafts/check-git-deps.sh` and
  `drafts/allowed-git-deps.toml` into `tools/`, FIX the known gap (implicit
  workspace members are never scanned — the precise silent pass the check
  exists to prevent), and wire it into `make lint` ahead of clippy. It must
  fail on: a git dep carrying `version` (never allowlistable — it publishes
  silently against the registry); an unrecorded git dep; a moving reference
  (branch/tag rather than rev); a stale allowlist entry; and a recorded blast
  radius that no longer matches the graph.
- [ ] T007 Prove the check catches what it claims, with cases: construct a
  scratch member carrying each failure form and assert the check fails naming
  it; assert it passes on the recorded arrangement from T001. Include an
  implicit-member case, since that is the gap T006 fixes.
- [ ] T008 Record the arrangement in `tools/allowed-git-deps.toml`: dependency,
  declaring manifest and table, git URL, exact rev, the REASON the fork is
  required, the named EXIT that retires it, and the crates it renders
  unpublishable — the last being a claim the check verifies against the
  resolved graph, not a comment.
- [ ] T009 Extend the ONE library boundary in
  `crates/rdlt-connector-snowflake/src/dest/client.rs`: the `Executor` trait
  gains a method that runs a statement and returns named columns per row as
  plain strings. The existing four methods (`execute`, `scalar_u64`,
  `sum_column`, `column_names`) cannot carry an upload's per-row outcome —
  verified. Keep library types behind the boundary; the new method returns
  owned strings. Update `SessionExecutor`, `DmlOnly` and the test `Recorder`.
- [ ] T010 [P] Pin the measured service facts as live checks in
  `crates/rdlt-connector-snowflake/tests/live_semantics.rs`, each failing by
  NAMING the assumption if the service changes: (a) the upload does not commit
  an open transaction — assert the rolled-back count is zero AND the
  transaction id is unchanged across the upload, because a zero count alone is
  also consistent with the statement having aborted the unit; (b) creating the
  staging object does not commit the unit while DROPPING it does; (c) an
  already-compressed part passes through untouched (no `.gz` suffix, reported
  compression unchanged).
- [ ] T011 [P] Pin the partial-failure shape in
  `crates/rdlt-connector-snowflake/tests/live_client.rs`: an upload matching
  two files where one is unreadable returns SUCCESS with a mixed result set
  (one uploaded, one error), and returns an error only when every part failed.
  This is the hazard SP2 exists for and the fork has no test covering it.

**Checkpoint**: the record authorises the work, the dependency is recorded and
mechanically guarded, the boundary can read per-row outcomes, and every service
fact the design rests on fails loudly if it changes.

---

## Phase 3: User Story 1 — Loads land with nothing to configure (P1) 🎯 MVP

**Goal**: rows land through service-provided storage, with no bucket, no
storage credential, no mode and no threshold.
**Independent test**: a load with a configuration carrying no storage settings
lands exact totals; a re-run publishes nothing; awkward values survive.

- [ ] T012 [US1] Resolve open question 1 (per-part size bound) before writing
  the backend: determine what bounds a part across sources, not just the one
  whose byte budget cuts batches. Read the engine's batching and the
  connector's `write()`. If the transfer's per-file ceiling is reachable,
  decide the enforcement and make its refusal name something the user can
  change. Record the answer in research.md.
- [ ] T013 [US1] Internal staging backend in
  `crates/rdlt-connector-snowflake/src/dest/stage.rs`: build one part at a time
  into a per-load local directory, upload it, delete the local file
  immediately, retain only the staged name. Peak local usage is ONE part.
  Preserve the load-scoped ownership discipline the deleted path arrived at the
  hard way — two loads of one pipeline must never derive the same name.
- [ ] T014 [US1] Name the staging object per pipeline and create it as schema
  work, strictly before any unit opens. A named object is required: the
  per-user area has no scoping at all and the per-table area can only load its
  own table (both measured). Teardown never runs inside a unit, because
  dropping the object commits one.
- [ ] T015 [US1] Build the file list from the upload's REPORTED target, relative
  to the prefix the load statement names — never the local name, never the
  listing's name (which doubles the prefix and is lower-cased). Retain
  case-insensitive column matching: parts carry lower-case encoded names
  against an upper-case catalog, and 022 pinned this deliberately.
- [ ] T016 [US1] Per-part verification per SP2 in
  `crates/rdlt-connector-snowflake/src/dest/session.rs`: inspect EVERY returned
  row's status; any non-success abandons the unit with a typed error naming the
  part and carrying the service's message. Keep the existing rows-loaded
  verification; both must hold.
- [ ] T017 [US1] Typed local-storage failures per SP6: map out-of-space,
  read-only filesystem, permission denied and path-length conditions to the SPI
  taxonomy, each naming the condition. Decide transient versus fatal per
  condition and justify at the site — out-of-space is arguable and the reasoning
  belongs in the code.
- [ ] T018 [US1] Local reclamation: a later run removes its OWN load's residue
  unconditionally and another load's only when demonstrably stale, mirroring the
  discipline the deleted path needed. Prove two concurrent loads of one pipeline
  cannot delete each other's in-flight files.
- [ ] T019 [US1] Cut over: the internal path becomes the path `write()` uses.
  The old paths remain compiled but unreachable at this point — deletion is the
  next story, so this increment is independently reviewable and revertible.
- [ ] T020 [US1] Live acceptance in
  `crates/rdlt-connector-snowflake/tests/live_load.rs`: exact totals; a re-run
  publishes nothing; awkward values (quotes, backslashes, multi-byte text,
  NULLs) survive; a load delivering no rows still commits its position.
- [ ] T021 [US1] Resolve open question 3 (crash-point set): decide whether the
  local-write and upload moments are distinguishable to any durable observer.
  If not, they earn ONE point, not two. Any point that exists must carry an
  assertion the sweep can actually make — verify the plumbing exists before
  proposing it. Update `FAIL_POINTS` in `src/dest/mod.rs` and the pinned
  registry list in `tests/crash_sweep.rs` together.
- [ ] T022 [US1] Resolve open question 2 (staged-object reclaim): determine what
  the listing exposes about age for the new staging area. If reclaim is weaker
  than the deleted path's modification-time rule, record the cost rather than
  claiming parity.
- [ ] T023 [US1] Crash sweep on the single path in
  `crates/rdlt-connector-snowflake/tests/crash_sweep.rs`: every point × three
  actions × Append and Replace, each cell crashing, crashing AGAIN during
  recovery, then running clean to exact totals, with the armed-fire pin proving
  each point fired. Delete `reachable()` if one path makes it an identity
  function. Record the new cell count against today's 30 and the new wall clock
  against 72 minutes (SC-012 requires both to fall).
- [ ] T024 [US1] Decide open question 4 explicitly: whether the sweep gains
  Merge mode (shipped in 022, absent from the sweep). Cost it in cells against
  SC-012's requirement that the total fall, and record the decision either way.
- [ ] T025 [US1] US1 gate: full local gate green; close-out story row; any
  deviation cited.

**Checkpoint**: MVP — rows land through service storage with nothing
configured, and exactly-once is re-proven on a smaller matrix.

---

## Phase 4: User Story 2 — The configuration surface shrinks, visibly (P1)

**Goal**: the storage vocabulary is gone, and a document still carrying it is
refused by name.
**Independent test**: a document with the removed block is refused naming it; a
document without it runs; the generated schema contains no storage vocabulary.

- [ ] T026 [US2] Delete the storage configuration from
  `crates/rdlt-connector-snowflake/src/dest/config.rs`: the `stage` field, the
  `Stage` and `S3Stage` types, their constructors, validation and accessor, and
  every test exercising them. No tombstone field — the existing rejection of
  unknown fields already refuses the removed block by name, and a tombstone
  would leave the vocabulary in the generated schema.
- [ ] T027 [US2] Delete the external-stage machinery from
  `src/dest/stage.rs`: the object-store handle, the credentialed create
  statement and its redaction path, the bucket reclaim, and the S3 error
  classification. Retain what the internal path needs — the part record, the
  file-list construction, the load-scoped naming.
- [ ] T028 [US2] Delete the statement-rendering path from
  `src/dest/encode.rs`: the value renderers, the decimal literal, the escaping
  helper, the statement builders and the measured byte budget. CHECK FIRST
  whether the escaping helper is used by the state and receipt statements in
  `session.rs` — if it is, it stays and only its INSERT callers go.
- [ ] T029 [US2] Delete the path-selection branch in `src/dest/session.rs`, so
  no branch exists that could select among mechanisms (SC-003), and the
  testhook entries in `src/dest/mod.rs` that exist only for deleted paths.
- [ ] T030 [P] [US2] Delete `tests/live_stage.rs` and `tests/batch_knee.rs`
  entirely; edit `tests/ingestion_session.rs`, `tests/differential_oracle.rs`,
  `tests/conformance.rs`, `tests/live_economy.rs` and `tests/secret_hygiene.rs`
  to the single path. Verify each edit by reading the file — do not apply line
  ranges from the research drafts, which were found wrong.
- [ ] T031 [P] [US2] Delete the bucket credential gate from
  `crates/rdlt-testkit/src/snowflake.rs`: `StageCreds`, its resolver, the
  dotenv parsing and their tests. The account gate and the skip announcement
  stay.
- [ ] T032 [P] [US2] Remove the storage block from `benches/parity_specs.yaml`
  and the CLI parse pin in `crates/rdlt-cli/src/main.rs`; verify both fixtures
  still parse and build (they are pinned by count assertions — read the file
  before editing).
- [ ] T033 [US2] Drop the dependencies that fall out of
  `crates/rdlt-connector-snowflake/Cargo.toml`, each verified by use-search
  first: object store, bytes, futures, chrono. Keep parquet and the arrow
  crates. DO NOT remove the SPI's `object-store` feature or its shared
  recoverability rule — the file connector still uses them (verify at its call
  sites and say so in the commit).
- [ ] T034 [US2] Refusal and schema checks: a document carrying the removed
  block is refused naming it; the generated schema contains zero storage
  vocabulary; and a mechanical residue search finds no configuration type,
  renderer, constant, test or dependency that existed only for the deleted
  paths (SC-007).
- [ ] T035 [US2] US2 gate: full gate green; the other SQL destinations' golden
  pins byte-identical; close-out row.

**Checkpoint**: one path, one configuration, no residue.

---

## Phase 5: User Story 3 — Every authentication method is proven (P2)

**Goal**: all four unattended methods exercised against the real account.
**Independent test**: with all credentials present all four run; with any one
absent that one alone skips, announces, and the suite stays green.

- [ ] T036 [US3] Provision on the qual account: a password-capable test user
  (not a service-type user, which refuses passwords) and an OAuth security
  integration with a token. Record ONLY the convention in committed files —
  never the values.
- [ ] T037 [US3] Turn the two written-but-skipping legs green in
  `crates/rdlt-connector-snowflake/tests/live_auth_matrix.rs`: one load per
  method, each gated on its OWN credential entry.
- [ ] T038 [P] [US3] Corrupted-secret shape per method: a deliberately wrong
  secret fails with an error naming the account and login and containing no
  secret material.
- [ ] T039 [US3] Close the two unperformed entries in 022's close-out table,
  citing the runs; US3 gate.

---

## Phase 6: User Story 4 — The record says what is true (P2)

**Goal**: every shipped claim about ingestion matches shipped behaviour.
**Independent test**: each ingestion claim in the documents is checked against
behaviour and matches.

- [ ] T040 [US4] Rewrite `specs/022-snowflake-dest/parity.md`: the internal-stage
  row is no longer a gap; line 18's claim that "dlt requires a stage; rdlt's
  default path needs no infrastructure at all" becomes false and is replaced by
  the honest distinction — no bucket is needed, but cloud-storage egress is.
- [ ] T041 [US4] Amend the contract explicitly: SP1–SP8 supersede 022's SD1 and
  SD6, stated in `specs/023-snowflake-put/contracts/snowflake-put.md` and
  cross-referenced from 022's contract so a reader of either finds the
  amendment.
- [ ] T042 [US4] Resolve open question 5's documentation half — the egress
  probe: run the allowlist query, identify which entries are storage hosts, and
  establish what a network permitting only the account host experiences. Do NOT
  apply firewall rules: this session shares the host network namespace. If no
  safe in-session method exists, say so and record what the allowlist alone
  establishes.
- [ ] T043 [US4] Rewrite the crate README for one path: delete the two-path
  table and the bucket configuration; add the egress prerequisite beside the
  existing s3-compatible allowlist caveat; add the upgrade note telling a user
  with a storage block to delete it and why.
- [ ] T044 [US4] Verify `quickstart.md` verbatim against the account and fold
  corrections back; `make docs` clean.
- [ ] T045 [US4] US4 gate: every ingestion claim in README, quickstart,
  parity.md, the contracts and CLAUDE.md checked against behaviour; close-out
  row.

---

## Phase 7: User Story 5 — The path that ships is the one measured (P3)

**Goal**: supersession established by numbers, recorded and gating nothing.
**Independent test**: the close-out carries the comparison with dataset identity
and configuration.

- [ ] T046 [US5] Rewrite `crates/rdlt-connector-snowflake/tests/ingestion_session.rs`
  for the single path, keeping 022's 12-column row shape BYTE-FOR-BYTE — the
  recorded figures refer to it, and "improving" it destroys the comparison.
- [ ] T047 [US5] Record the session against 022's figures (582 rows/s for
  statements; 2,191 rows/s at 250k and 1,941 at 1M for the bucket path), with
  dataset identity, configuration, wall clock and rows/s. Record whatever comes
  out, including a result that does not favour the new path.
- [ ] T048 [US5] Resolve or honestly re-record 022's open question: the bucket
  path at 1M ran 11% slower per row than at 250k on one run each, which cannot
  separate a multi-part effect from variance. Determine what actually controls
  part count before designing the comparison — do not assume the engine config
  exposes it. If the instrument cannot separate them, say so.
- [ ] T049 [US5] Confirm the measurement gates nothing: no bar proposed, the
  bench gate untouched and green (Principle VIII); US5 gate.

---

## Phase 8: Polish & Close-out

- [ ] T050 [P] Coverage at or above the 80% floor, measured baseline-first and
  recorded.
- [ ] T051 [P] Semver: confirm the SPI and core need no change; record that the
  connector's configuration change is breaking, that nothing is published, and
  that the distribution constraint from the git dependency is inherited by the
  publish feature.
- [ ] T052 Close-out matrix: SP1–SP8 all terminal, story matrix complete, every
  deviation cited, every unperformed verification named with its reason, and
  every one of research.md's six open questions carried to a terminal
  disposition.
- [ ] T053 Delete `specs/023-snowflake-put/drafts/` once its content is either
  adopted into `tools/` or rejected with a reason — an unverified draft left in
  the tree is exactly what it was moved out of `tools/` to avoid.
- [ ] T054 Final gate: `env -u RUSTUP_TOOLCHAIN make check` TWICE clean on a
  quiet machine, with the SC-005 secret sweep re-run at the final commit and
  both results recorded in close-out.md.

---

## Dependencies

- **T001–T003 → everything**: nothing compiles against the fork until it is
  adopted.
- **T004–T005 → all code work**: the record authorises the fork route; do not
  build on an unevidenced claim.
- **T006–T008 → any merge**: the distribution constraint must be guarded before
  the arrangement hardens, since no other gate can see it.
- **T009 → T016**: per-part verification needs the boundary method.
- **T010–T011 → T013**: build the backend against pinned facts, not assumed ones.
- **T012 → T013**; **T021–T022 → T023** (decide the point set before sweeping).
- **US1 → US2**: nothing is deleted until its replacement is proven.
- **US1+US2 → US3, US4, US5**: certify, document and measure what actually ships.
- Within US2, T030/T031/T032 are parallel; T033 follows them (dependencies fall
  out only once every use is gone).

## Parallel opportunities

- T010 and T011 (different test files, both pinning facts).
- T030, T031, T032 (test files, testkit, fixtures — disjoint).
- T038 alongside T037 (different concerns in one suite).
- T050 and T051 (coverage and semver are independent).

## Implementation strategy

**MVP = Phases 1–3.** A Snowflake destination loading through service-provided
storage with nothing configured, exactly-once re-proven. The old paths are still
compiled at that point but unreachable — deliberately, so the cutover is
reviewable and revertible on its own.

Each later phase is independently mergeable on a green gate. Deletion (US2) is
the second increment rather than the first because a deletion that precedes its
replacement is a regression with a plan attached.

Six open questions from research.md are assigned to specific tasks — T012 (part
size), T022 (reclaim strength), T021 (crash points), T024 (merge in the sweep),
T004 (the upstream issue), T006 (the check's implicit-member gap) — and T052
requires each to reach a terminal disposition rather than quietly lapsing.
