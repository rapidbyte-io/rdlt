# Tasks: Postgres Source Completion (pre-CDC)

**Input**: Design documents from `/specs/007-postgres-source-completion/`

**Prerequisites**: plan.md, spec.md (as amended by research R4/R7),
research.md (R1–R10), data-model.md,
contracts/{tls-client-auth, cursor-lag, connstring-portability}.md,
quickstart.md

**Tests**: INCLUDED — every success criterion is test-defined (mTLS
matrix SC-001, lag conformance SC-002, conn-string corpus SC-003,
cursor-edge conformance SC-004, discovery totals SC-005/006, schema
round-trips SC-007, unchanged gate SC-008). Safe Rust only; zero new
dependencies; zero engine/SPI changes.

**Organization**: by user story. US1 (mTLS) is the MVP. US1 → US3 are
calendar-ordered (both rework `tls.rs`); US2, US4, US5 are mutually
independent of the TLS chain and of each other.

## Implementation notes (close-out, 2026-07-21)

All 15 tasks done. Gates: `make check` green (lint, 253/253 workspace
tests, engine + postgres crash sweeps, iai perf gate within tolerance —
every 007 change is off the hot path as planned), doc-tests green,
semver-checks vs origin/main clean for rdlt-core and rdlt-connector
("no update required" — zero SPI changes, as promised). Discoveries
worth keeping:

- **tokio-postgres Display opacity, third encounter**: auth-phase 28000
  rejections render as just "db error" — the ClientCert classifier reads
  `as_db_error()` for the real server message. Anywhere classification
  keys off message text, go through the DbError, never Display.
- **`sslmode=verify-ca|verify-full` in conn strings**: the driver itself
  rejects these libpq spellings — discovered when the first
  production-shaped URL corpus entry failed. The gate now translates
  them into the policy mode (block may keep or strengthen, never
  weaken), which US3's spec text didn't anticipate but its "working
  libpq URL just works" goal requires.
- **Split credentials forced a validation move**: cert-from-URL +
  key-from-block is legitimate (P2), so both-or-neither validation
  moved from `resolve_policy` (block-only view) to `parse_conn`
  (post-merge view). `parse_conn` is now the ONLY entry to policy
  resolution for both connectors.
- **rustls ambient-provider pitfall recurred in TEST code**: the
  pg_stat_activity probe built its own ClientConfig and panicked at
  runtime — any rustls construction anywhere in the tree needs the
  pinned-provider builder, tests included.
- **Lag closed-flag subtlety**: stored open-boundary finals carry no
  boundary keys, which would render `>` on resume — with lag configured
  the window re-read forces closed (`>=`) regardless, or the lagged
  window would silently exclude the watermark row itself.
- **NullPolicy::Error costs one `null_count()` per batch** (Arrow keeps
  it precomputed) — the zero-cost-when-clean claim is real, not
  aspirational.

## Format: `[ID] [P?] [Story] Description`

## Phase 1: Setup

No setup tasks — no new dependencies, no crate or workspace changes
(plan Technical Context). Work starts directly in US1.

## Phase 2: User Story 1 — Mutual TLS (Priority: P1) 🎯 MVP

**Goal**: client-certificate authentication for BOTH postgres
connectors through the shared tls module; typed errors at every layer.

**Independent Test**: spec US1 — cert-required server: valid cert+key
syncs (source and destination), no-cert and wrong-CA-cert fail with
the distinguished `ClientCert` error, mismatched key is a config error
before any connection.

- [X] T001 [US1] Config + validation in
      `crates/rdlt-postgres/src/tls.rs`: `TlsPolicy` gains
      `client_cert`/`client_key` (both `Option<RootCert>`, path or
      inline PEM); `resolve_policy` enforces both-or-neither (error
      names the missing counterpart), credential+`mode: disable`
      contradiction, unreadable/unparseable/encrypted-key errors
      naming the input (contract C2); unit tests for the full
      rejection matrix with rcgen-made and corrupted PEMs.
- [X] T002 [US1] Handshake + classification in
      `crates/rdlt-postgres/src/tls.rs`: the three `client_config()`
      arms swap `with_no_client_auth()` for `with_client_auth_cert`
      when credentials resolve (key parsed via existing
      `rustls-pemfile`; PKCS#8/RSA/SEC1); new
      `TlsFailure::ClientCert` in `classify_connect_error` fed from
      rustls certificate alerts (via the 006 `get_ref()` downcast
      chain) AND auth-phase SQLSTATE 28000 (contract C3, research R2);
      unit tests for the mapping arms.
- [X] T003 [US1] Test rig in
      `crates/rdlt-postgres/tests/common/mod.rs`: `TlsPki` issues
      client certs from the test CA and the wrong CA (distinct-CN
      discipline); `TlsPgFixture` initdb script installs the CA as
      `ssl_ca_file` and writes a `hostssl … cert` pg_hba variant
      (research R3) selectable by the test.
- [X] T004 [US1] Matrix conformance in
      `crates/rdlt-postgres/tests/tls_matrix.rs`: the five contract
      cells — valid cert+key syncs via SOURCE and DESTINATION;
      no-credential → `ClientCert` naming the missing credential;
      wrong-CA client cert → `ClientCert`; mismatched key → config
      error before connect; credential offered-but-unused against the
      plain hostssl server still syncs (SC-001).

**Checkpoint**: cert-required deployments unblocked — shippable MVP.

## Phase 3: User Story 3 — libpq conn-string portability (Priority: P3)

**Goal**: no working libpq URL ever dies with a bare parse error; the
TLS parameter trio translates into the policy. (Calendar-ordered after
US1: same file, and `sslcert=`/`sslkey=` target the US1 fields.)

**Independent Test**: spec US3 — `sslrootcert=` URL syncs with an
empty tls block; contradictions and unknown parameters fail typed
naming both sides / the parameter.

- [X] T005 [US3] Conn-string front-end in
      `crates/rdlt-postgres/src/tls.rs` (the one shared parse gate):
      extract/strip `sslrootcert`/`sslcert`/`sslkey` from key=value
      AND URL-query forms; translate into the TlsPolicy resolution
      with the existing agree-or-error contradiction rule (P1–P3);
      `sslrootcert=system` → native roots; re-wrap every residual
      driver rejection into an error NAMING the parameter with
      alternative pointers (`sslpassword`, `gssencmode` — P4);
      pass-through byte-identical otherwise (P6);
      `application_name=rdlt` default when absent, user value wins
      (A1–A2); unit corpus over real-world URL shapes asserting zero
      bare parse errors (SC-003 unit half).
- [X] T006 [US3] Container proof in
      `crates/rdlt-postgres/tests/tls_matrix.rs`: an `sslrootcert=`
      URL (no tls block) syncs against the TLS fixture via source AND
      destination; during a sync, `pg_stat_activity.application_name`
      shows `rdlt` (SC-003 container half + SC-006).

**Checkpoint**: paste-a-URL first contact works.

## Phase 4: User Story 2 — Cursor lag (Priority: P2)

**Goal**: late-arriving rows captured via a read-side window; saved
watermark never regresses; exact totals under keyed Merge.

**Independent Test**: spec US2 — row committed behind the watermark
inside the lag loads next run; totals exactly match the source across
three further runs; rejection matrix all typed.

- [X] T007 [P] [US2] Lag vocabulary + validation:
      `crates/rdlt-postgres/src/source/config.rs` gains `Lag` (custom
      FromStr/Display/serde + manual JsonSchema mirroring the pattern
      — HintType precedent) and `CursorConfig.lag`; open-time
      validation in `crates/rdlt-postgres/src/source/mod.rs`: closed
      boundary required, subtractable cursor family (time/date/
      integer/decimal; text/uuid typed error naming column+type),
      whole-days-only for `date`, stream primary key required
      (research R4 / contract L2); unit rejection matrix.
- [X] T008 [US2] Read-path wiring:
      `crates/rdlt-postgres/src/source/sqlgen.rs` renders
      `($watermark - lag)` per cursor family (`- interval 'N
      seconds'`, native magnitude, days) as the closed lower bound;
      `mod.rs` passes lag ONLY into the read bound — saved watermark
      advances unchanged (L1); unit tests pin the rendered SQL per
      family and that state handling is untouched.
- [X] T009 [US2] Conformance in
      `crates/rdlt-postgres/tests/incremental.rs`: late-arrival under
      keyed Merge — sync, insert a row behind the watermark inside
      the window, sync → captured; destination totals exactly equal
      source across three further runs (idempotent window re-merge);
      beyond-window row NOT loaded (L5 pin) (SC-002).

**Checkpoint**: the silent-loss class for `updated_at` cursors closed.

## Phase 5: User Story 4 — Cursor edge policies (Priority: P4)

**Goal**: NULL-cursor `error` policy; inclusive end bound; defaults
byte-identical.

**Independent Test**: spec US4 — NULL row fails typed under `error`,
loads under the old policies; inclusive boundary row loads exactly
once.

- [X] T010 [P] [US4] `NullPolicy::Error`:
      `crates/rdlt-postgres/src/source/config.rs` third variant;
      raise in the tracker
      (`crates/rdlt-postgres/src/source/cursor.rs`) on first NULL —
      typed FATAL naming stream+column, zero cost when clean (N1);
      conformance in `tests/incremental.rs`: fails typed, no
      duplicates after retry-then-fix (N2), exclude/include pins
      unchanged (N3) (SC-004).
- [X] T011 [P] [US4] Inclusive end bound:
      `CursorConfig.end_bound` (exclusive default) in `config.rs`;
      direction-aware `<=`/`>=` arm in `sqlgen.rs` upper-bound matrix
      (E1); conformance in `tests/incremental.rs`: boundary row loads
      exactly once, beyond-bound row does not, exclusive default
      unchanged (E2) (SC-004).

## Phase 6: User Story 5 — Discovery + observability (Priority: P5)

**Goal**: one-predicate hierarchy exclusion with the NEW
explicit-listing override; truthful docs.

**Independent Test**: spec US5 — INHERITS hierarchy loads each row
exactly once; a listed child reads as its own stream; docs state
foreign-table non-discovery.

- [X] T012 [US5] Discovery filter in
      `crates/rdlt-postgres/src/source/reflect.rs`: replace
      `NOT c.relispartition` with the `pg_inherits` NOT-EXISTS
      predicate + listed-name exception parameter (research R7);
      conformance in `tests/conformance.rs` IN THE SAME TASK (plan
      rule: a pin updated apart from its behavior change is a broken
      tripwire): update `partitioned_tables_load_once_via_parent` to
      the unified rule, add classic-INHERITS and mixed
      inheritance+partition hierarchies (exact totals, children not
      streams), add explicit-listing override cells for a partition
      leaf AND an INHERITS child (SC-005).
- [X] T013 [P] [US5] Docs truthfulness in
      `crates/rdlt-postgres/README.md`: discovery scope (partitions +
      INHERITS excluded, listing override, foreign tables never
      discovered — FR-012), mTLS + conn-string parameter sections,
      lag semantics incl. the Append at-least-once property (L3),
      `application_name` note; quickstart cross-check.

## Phase 7: Polish & close-out

- [X] T014 Config schemas in
      `crates/rdlt-postgres/tests/config_schema.rs`: examples/corpus
      gain `client_cert`/`client_key`, `lag`, `end_bound`,
      `nulls: error`; schema-valid ⇒ parses, unknown fields fail
      both, bad `lag` strings stopped by the pattern (SC-007).
- [X] T015 Close-out: `make check` + `cargo test --doc` +
      `cargo semver-checks check-release --baseline-rev origin/main
      -p rdlt-core -p rdlt-connector` (must stay "no update
      required"); perf gate within tolerance — no bar or baseline
      moves (SC-008); implementation-notes block at the top of this
      file; mark the 006 audit items closed in
      `specs/006-postgres-completeness/spec.md` parity-table
      neighborhood if referenced; tasks all [X].

## Dependencies & Execution Order

```text
Phase 2 (US1, MVP): T001 → T002 → T003 → T004
Phase 3 (US3):      T005 → T006          (after US1 — same file, uses its fields)
Phase 4 (US2):      T007 → T008 → T009   (independent of TLS chain)
Phase 5 (US4):      T010 ∥ T011          (independent)
Phase 6 (US5):      T012 ∥ T013          (independent)
Phase 7:            T014 (after T001+T007+T010+T011) → T015 (last)
```

- T007/T010/T011 all edit `config.rs` — parallel-safe only across
  different working sessions; within one session run sequentially.
- The TLS chain (US1→US3) and the cursor/discovery stories can
  proceed in parallel by different sessions; single-session order:
  US1, US3, US2, US4, US5, polish.

## Implementation Strategy

MVP = Phase 2 alone (cert-required deployments unblocked). Each later
phase is an independently shippable increment; nothing outside
`crates/rdlt-postgres` changes, so the blast radius of any stop-point
is one crate. Perf proof is the EXISTING gate (SC-008) — if any cell
drifts past tolerance, the offending change is off-path by design and
the diff is the suspect, not the baseline.
