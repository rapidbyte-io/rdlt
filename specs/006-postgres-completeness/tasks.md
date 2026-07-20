# Tasks: Postgres Source Completeness — Parity + TLS

**Input**: Design documents from `/specs/006-postgres-completeness/`

**Prerequisites**: plan.md, spec.md (incl. parity table), research.md
(R1–R8), data-model.md, contracts/{tls-policy, type-hints,
query-streams, merge-structured}.md, quickstart.md

**Tests**: INCLUDED — the spec's success criteria are test-defined
(TLS matrix SC-001, conformance SC-002/003, suppression-proof
visibility SC-004, round-trip schemas SC-005). Safe Rust only; the
rustls verifiers are safe trait impls, quarantined + documented.

**Organization**: by user story; US1 (TLS) is the MVP; US2/US3/US4 are
mutually independent after Phase 1 (US2 and US1 both touch the source
`config.rs` — calendar-ordered, not parallel).

## Format: `[ID] [P?] [Story] Description`

## Phase 1: Setup

- [X] T001 Crate merge (owner decision, research R1 — amends 005 R9):
      create `crates/rdlt-postgres` with `source`/`dest` feature-gated
      modules (both default) and move `rdlt-source-postgres` →
      `src/source/`, `rdlt-dest-postgres` → `src/dest/` intact;
      per-direction `FAIL_POINTS` consts preserved; tests move with
      `dest_` prefixes on colliding binaries; iai_pg bench moves;
      update workspace members/deps, facade (`rdlt::postgres` /
      `rdlt::postgres_source` re-exports + features), CLI, Makefile
      sweep/iai lines, fuzz/Cargo.toml, CI references. Add workspace
      deps for this feature: `tokio-postgres-rustls`, `rustls`,
      `rustls-pemfile`, `rustls-native-certs`, `schemars`; dev
      `rcgen`. FULL suite green after the move (pure relocation — no
      behavior change; sweeps + gate prove it).

## Phase 2: User Story 1 — TLS, the full sslmode matrix (Priority: P1) 🎯 MVP

**Goal**: five sslmode levels with libpq semantics for BOTH postgres
connectors, custom roots, typed verification errors; 005's "TLS not
wired" rejection gone.

**Independent Test**: spec US1 — the cert matrix (match / mismatch /
unknown CA × five modes × two connectors) behaves per the contract
table, `prefer` falls back, `require` connects against self-signed.

- [X] T002 [US1] `TlsPolicy` + root resolution in
      `crates/rdlt-postgres/src/tls.rs`: mode enum (serde +
      conn-sslmode interop), `RootCert` (path | inline PEM), root
      loading (custom else `rustls-native-certs`), typed config errors
      (unreadable/unparseable root names the path; verify-* with no
      resolvable roots); unit tests on the error paths with rcgen-made
      and corrupted PEMs.
- [X] T003 [US1] Verifiers in `crates/rdlt-postgres/src/tls_verify.rs`
      (quarantined, loudly documented): `require` accept-any verifier;
      `verify-ca` wrapper delegating chain checks to the webpki
      verifier while waiving ONLY hostname mismatch; unit tests against
      rcgen chains (good chain passes both; unknown CA fails verify-ca
      but passes require; name mismatch fails full, passes ca).
- [X] T004 [US1] Connector construction + error taxonomy in
      `crates/rdlt-postgres/src/tls.rs`: policy → NoTls | rustls
      connector per mode (prefer relies on tokio-postgres's native
      fallback), and a mapping from rustls/tokio-postgres errors to
      the contract's distinguished connect failures (trust-anchor /
      chain / hostname / server-refused-TLS); unit tests for the
      mapping.
- [X] T005 [US1] Source wiring: `crates/rdlt-postgres/src/source/config.rs`
      gains the `tls:` block (mode + root_cert; contradiction vs conn
      `sslmode` = typed config error; verify-* only via block) with
      schema-visible docs; `src/source/mod.rs::connect` builds the policy
      (conn sslmode as default, block override-with-consistency) and
      REMOVES the 005 rejection; config unit tests updated (the
      sslmode=require rejection tests become acceptance tests).
- [X] T006 [P] [US1] Destination wiring:
      `crates/rdlt-postgres/src/dest/mod.rs` `Postgres::tls(TlsPolicy)`
      builder + connect path through the shared `tls` module;
      `crates/rdlt-cli/src/main.rs` `[destination.postgres]` gains
      optional `tls = { mode, root_cert }`.
- [X] T007 [US1] TLS test rig in
      `crates/rdlt-postgres/tests/common/mod.rs`: rcgen CA +
      server certs (SAN localhost/127.0.0.1 + a wrong-SAN pair), TLS
      postgres container via entrypoint shim (certs copied 0600,
      `ssl=on`, optional hostssl-only pg_hba), returning conn info +
      cert paths.
- [X] T008 [US1] Matrix conformance in
      `crates/rdlt-postgres/tests/tls_matrix.rs` (both directions in
      one suite — same crate now): five modes × {match, mismatch, unknown CA} for
      source AND destination; `prefer` fallback on a plaintext server;
      `require` success on self-signed; hostssl server rejects
      `disable`; each negative asserts the DISTINGUISHED typed error.
      Contract/doc cleanup: 005 contract TLS notes + crate READMEs +
      research R1 rejection language replaced by pointers to
      contracts/tls-policy.md.

**Checkpoint**: production databases reachable — independently
shippable MVP.

## Phase 3: User Story 2 — Type hints + query streams (Priority: P2)

**Goal**: per-column overrides via the closed conversion table; query
streams with described schemas and full incremental semantics.

**Independent Test**: spec US2 — hinted text→timestamp lands typed; a
join query lands with described schema + working incremental; invalid
hints/queries fail typed at open.

- [ ] T009 [US2] Hint vocabulary + closed conversion table in
      `crates/rdlt-postgres/src/source/types.rs`: `HintType` (shared
      vocabulary incl. `decimal(p,s)`), `apply_hint(source_info, hint)
      -> Result<MappedType>` implementing contracts/type-hints.md
      exactly (undefined pair = typed error; [documented-lossy]
      flagged); exhaustive unit tests keyed to the contract rows incl.
      rejections.
- [ ] T010 [US2] Table hints end-to-end:
      `crates/rdlt-postgres/src/source/config.rs` `tables[].type_hints`
      + open-time validation (column exists, pair allowed, hinted
      cursor stays cursor-capable) in `src/source/{mod,reflect}.rs`;
      conformance in `tests/conformance.rs`: text→timestamp_tz lands
      typed downstream, unconstrained-numeric→decimal(p,s) hint
      restores decimality, cast-failure surfaces as a typed copy-phase
      error naming the column.
- [ ] T011 [US2] Query streams core:
      `crates/rdlt-postgres/src/source/config.rs` `queries[]` (name
      uniqueness across tables+queries, cursor/primary_key/type_hints);
      describe-based schema in `src/source/reflect.rs` (prepare
      `SELECT * FROM (sql) AS q`, map column OIDs via the existing
      contract, all-nullable, typmod-unknown ⇒ textual numeric policy);
      `src/source/{sqlgen,mod}.rs` read path over the wrapped FROM
      with unchanged incremental/checkpoint machinery.
- [ ] T012 [US2] Query conformance in
      `crates/rdlt-postgres/tests/query_streams.rs`: join query
      snapshot + schema assertions; incremental on a query stream
      (delta, boundary dedup via declared primary_key, mid-run
      checkpoints); mutating SQL (INSERT/UPDATE/data-modifying CTE)
      rejected typed BEFORE data moves; cursor-absent-from-output and
      name-collision rejections; hint-on-query case.

**Checkpoint**: the two dlt capability gaps closed, independently
testable.

## Phase 4: User Story 3 — Merge for keyed structured streams (Priority: P3)

**Goal**: the recorded B4 lift — updates converge to one row per key
on both SQL destinations, exactly-once under the crash model.

**Independent Test**: spec US3 — update-heavy incremental with merge
converges (count == source count, newest values win) on both SQL
destinations; keyless/non-capable rejections stand; sweep green in
merge mode.

- [ ] T013 [US3] The contract event FIRST:
      `specs/001-rdlt-ingestion-engine/contracts/connector-spi.md`
      (D8 + E7 redelivery note) and the feature-002 clause references
      gain pointers to
      `specs/006-postgres-completeness/contracts/merge-structured.md`
      (amendment, not rewrite); rejection-message text agreed here.
- [ ] T014 [US3] Engine lift in `crates/rdlt-engine/src/` (locate the
      plan-time B4 rejection): accept Merge for structured streams
      with non-empty declared key AND `capabilities().merge`; keep
      typed rejections (keyless → message points at keyed
      alternative; non-capable unchanged); write-time NULL-in-key
      validation (typed error naming column); pass declared key
      columns to the destination commit path; unit tests for
      accept/reject matrix + NULL keys.
- [ ] T015 [P] [US3] DuckDB merge-by-key in
      `crates/rdlt-dest-duckdb/src/lib.rs`: generalize the keyed
      delete+insert from `_rdlt_root_id` to configured key columns
      (multi-column keys); conformance: update-heavy convergence,
      idempotent re-commit (D3).
- [ ] T016 [P] [US3] Postgres merge-by-key in
      `crates/rdlt-postgres/src/dest/mod.rs`: same generalization +
      conformance.
- [ ] T017 [US3] Merge under fire:
      `crates/rdlt-postgres/tests/crash_sweep.rs` gains a Merge
      mode loop (keyed incremental source; armed-fire assertions
      extended); `crates/rdlt-postgres/tests/dest_crash_sweep.rs` +
      engine sweep pins extended where merge mode reaches new
      boundaries; keyless + parquet rejections re-asserted;
      `tests/incremental.rs` merge-rejection test updated to the new
      keyed-acceptance reality.

**Checkpoint**: the biggest workflow gap closed with crash-model
proof.

## Phase 5: User Story 4 — Trust surfaces (Priority: P4)

**Goal**: lossy mappings visible, schemas generated from truth, the
three test advisories closed.

**Independent Test**: spec US4 — suppression-proof lossy signal;
example configs validate / invalid fail against generated schemas for
all three sources; advisory tests fail when their regressions are
injected.

- [ ] T018 [P] [US4] Lossy visibility in
      `crates/rdlt-postgres/src/source/mod.rs`: one
      `tracing::warn!(target: "rdlt::lossy", …)` per
      [documented-lossy] column per read (policy rows + textual
      fallback + lossy hints); capture-subscriber test (exactly once;
      silent when clean); amend the 005 type-mapping contract's "run
      report" wording to name this surface.
- [ ] T019 [P] [US4] Config schemas: `schemars::JsonSchema` derives on
      the config families of `crates/rdlt-postgres` (source),
      `crates/rdlt-source-rest`, `crates/rdlt-source-file`; each crate
      exposes `config_schema()` and fills
      `ConnectorSpec.config_schema` in `spec()`; round-trip tests per
      crate (documented examples validate; unknown-field configs fail;
      schema-valid ⇒ parses over the test corpus).
- [ ] T020 [P] [US4] Advisory closures: differential multi-batch
      variant in `crates/rdlt-postgres/tests/differential.rs`
      (`batch_max_rows: 3`, larger row sets, arrow-select concat
      before compare); `tests/memory_bound.rs` honors `RDLT_HEAVY=1`
      (missing prereqs FAIL with instructions; Makefile sweep/deep
      targets export it); container-kill test polls for ≥1 committed
      row before killing (unconditional integrity assertion).

## Phase 6: Polish & close-out

- [ ] T021 Close-out: complete the spec's parity table (every "006
      action" row → done; OUT rows carry reasons — SC-007);
      `make check` + `cargo test --doc` + `cargo semver-checks
      check-release --baseline-rev origin/main -p rdlt-core -p
      rdlt-connector` green; gate within tolerance (TLS off-path
      verified); implementation-notes block at the top of this file;
      READMEs + quickstart truthful against shipped surfaces.

## Dependencies & Execution Order

```text
T001 ─► US1: T002 → T003 → T004 → T005 ∥ T006 → T007 → T008
     ├► US2: T009 → T010 → T011 → T012      (after T005 lands — shared config.rs)
     ├► US3: T013 → T014 → (T015 ∥ T016) → T017   (independent of US1/US2)
     └► US4: T018 ∥ T019 ∥ T020             (T018/T020 after US2 files settle)
                    all ─► T021
```

- T001 (the crate merge) is a pure-relocation gate: full suite must
  be green on it BEFORE any feature work stacks on top.
- US1 is the MVP and blocks nothing except by file contention
  (source/config.rs: T005 before T010/T011).
- US3 touches engine + destinations only — fully parallel to US1/US2
  by files, calendar-ordered with them for review sanity.
- T015/T016 are genuinely parallel (different crates).

## Implementation Strategy

- **MVP = Phase 1–2**: TLS alone makes the connector production-
  reachable; ship-worthy checkpoint.
- US3's contract text (T013) lands BEFORE its code — semantics pinned
  in review-able prose first (plan rule).
- Every suite lands with the code it nets; armed-fire pins are updated
  in the same change as any new fail-point reachability.

## Notes

- 21 tasks: Setup 1, US1 7, US2 4, US3 5, US4 3, Polish 1.
- Format validated: checkbox + sequential ID on every task; [P] only on
  genuinely disjoint files; story labels only in Phases 2–5.
