# Research: Workspace Refactoring Program

Phase 0 decisions. Every unknown from Technical Context is resolved here —
either with a code fact verified at plan time (cited), or as a designed
probe-with-fallback at the increment's first task (the 015/016 pattern:
probe recorded, fallback designed, never silent). No NEEDS CLARIFICATION
markers remain.

## D-01. B3 delivery: stopgap sync now, structural fix at increment 12

**Decision**: Increment 1 syncs the bench crate's diverged spec-struct copy
(adds the missing `File`/`Iceberg` destination variants) with a parity test
that fails if the two copies drift again; increment 12 retires both copies
behind one `rdlt::pipeline_spec` module consumed by CLI and bench.

**Rationale**: the live bug (library-mode bench cannot parse `file:`/
`iceberg:` pipelines) is fixed in increment 1 per "bugs first, on unmoved
code"; the extraction is a structural move that belongs with the CLI/bench
splits and makes Principle II structurally true.

**Alternatives considered**: (a) full extraction in increment 1 — rejected:
it drags R4 CLI-split concerns into the bug-fix increment; (b) fix only at
step 12 — rejected: leaves a live defect unfixed for ~10 increments.

## D-02. B5 DuckDB constraint-violation classification

**Decision**: classify via duckdb-rs structured errors —
`duckdb::Error::DuckDBFailure(ffi::Error { code, extended_code }, _)`
(verified present in the pinned duckdb-rs 1.10505.0 source). First task of
increment 1 probes which `code`/`extended_code` a live upsert-precondition
violation produces and pins it in a test.

**Fallback (designed)**: if the failure surfaces without a usable code,
narrow the match to the full `"Constraint Error"` prefix (drop the broad
`"violate"` needle) and keep the probe test pinning duckdb's exact message
so any rewording fails CI loudly (constitution Principle V's pin escape
hatch).

**Alternatives considered**: keeping substring matching with both needles —
forbidden by Principle V; matching Display prefix only without a pin —
silent-drift risk the catalogue explicitly calls out.

## D-03. B6 Iceberg auth-rejection classification

**Decision**: classify on the `status` context value carried by
`iceberg::Error` (the crate's own error tests already construct
`.with_context("status", ...)` — verified at
`crates/rdlt-connector-iceberg/src/dest/errors.rs:97,111`). First task of
increment 1 probes that a live REST-catalog 401 carries the context value.

**Fallback (designed)**: if the live 401 lacks the context, narrow the
needle to the exact rendered status line, pin it with a probe test, and
record the upstream issue link in the close-out matrix.

**Alternatives considered**: keeping `to_string().contains("401
Unauthorized")` — the exact silent-degradation Principle V forbids.

## D-04. R8 `DestError::RateLimited` is additive, not breaking

**Decision**: add `RateLimited` to `DestError` now — the enum is
`#[non_exhaustive]` (verified `crates/rdlt-connector/src/error.rs:40`), so
external matches already carry wildcards; in-workspace exhaustive matches
surface as compile errors and get real handling (engine retry loop honors
retry-after like the source path).

**Rationale**: REST-catalog and warehouse destinations are rate-limited in
practice (catalogue R8); the variant closes the source/destination
asymmetry without touching the 0.3 window.

**Alternatives considered**: documenting a waiver — rejected, the iceberg
destination already maps 429/503 into Transient today, losing retry-after
semantics the engine could honor.

## D-05. R3 shared `Secret` home and feature shape

**Decision**: `rdlt_connector::secret::Secret` in the SPI crate (newtype +
masking Debug/Display + transparent serde + From impls), with the schemars
impl behind a new SPI feature `schema` — the SPI crate has serde but not
schemars today (verified `crates/rdlt-connector/Cargo.toml`); the three
connectors (rest, file, iceberg) enable the feature and re-export the type
from their current paths (old paths kept as re-exports, no API break).

**Rationale**: the SPI crate is the one crate all connectors already depend
on; an optional feature keeps SPI lean for engine-only consumers
(Principle I).

**Alternatives considered**: rdlt-core — rejected: core is
engine-vocabulary, connectors-only concerns live in the SPI (catalogue R3
names the SPI crate); a new micro-crate — rejected: Principle I.

## D-06. R2 commit-unit protocol planner shape

**Decision**: sqlcore gains a pure planner —
`commit_script(tables, options, replayed) -> Vec<Step>` where `Step` is a
closed enum (replay-check, single-unit guard/mark, per-table publish arm,
stage truncation, state upsert, receipt insert) — and each destination
executes steps through its existing session/dialect. Prerequisite: split
`PgSession::commit` and `DuckDbSession::commit` (R4) first so the two copies
are visibly identical before lifting. The mechanical helpers (`quote`,
`column_list`, `root_of` + its named constant, index-name formula,
`hard_delete` resolution, `MergePlan` construction) move in the same
increment. Existing golden-SQL pins must pass unchanged; new pins cover the
emitted step scripts for both dialects.

**Rationale**: the planner owns decisions (the correctness-critical half the
catalogue identifies); destinations keep execution, so no driver types cross
into sqlcore (Principle III) and drift becomes impossible by construction.

**Alternatives considered**: a shared trait with default methods — rejected:
decision logic would still be overridable per-destination, which is the
drift vector; sharing SQL text only — rejected: the drift already observed
is in bookkeeping decisions, not SQL text.

## D-07. R6 shared apply helpers (live path vs WAL replay)

**Decision**: extract `apply_delta` (lower_schema → ensure_table →
record schema hash) and `apply_batch` (lower_batch → session.write) into one
engine module used by both `Loader::process` and `wal::resume::replay`;
replay's redundant double table-ensure collapses in the same change. The
crash-sweep suite is the behavior pin.

**Alternatives considered**: leaving replay hand-rolled with a comment —
rejected: this is the catalogue's "highest-stakes duplication in the
engine" and Principle IV territory.

## D-08. B10 two-pass replay

**Decision**: pass 1 validates the uncommitted span (opens/decodes segments,
builds readers, surfaces damage reasons — also fixing the swallowed
`Err(_) => Ok(None)` diagnostic loss); pass 2 streams segments through the
session one at a time under the byte budget. Ordered after the R6/R4 engine
work (increment 5) because the split makes the streaming rewrite small.

**Alternatives considered**: capping the buffered vec — rejected: still
violates the budget contract for spans over the cap; single-pass streaming
with rollback — rejected: needs session rollback semantics destinations
don't promise.

## D-09. R7 file-crate location unification

**Decision**: one `location::Location` abstraction with read and write
halves (absorbing dest `Store`), one shared error-classification function
(source's transient/fatal split becomes the single rulebook, closing B9),
one "keys belonging to table T" ownership helper (closing B2's root cause),
and `FileMeta`/`FileTask`/`FileProgress` move into `location/` types so
shared layers no longer import upward from `source/`.

**Alternatives considered**: keeping two abstractions with shared helpers —
rejected: B2/B9 both arose from the fork; the catalogue's split plan (R4
file row) already assumes the unified layer.

## D-10. R10 naming/semver strategy

**Decision**: three buckets. (1) Non-breaking renames (locals, fields with
serde renames preserving wire format, private types) — applied in increment
11. (2) Breaking-but-aliasable (`DestinationError`/`DestinationCapabilities`
aliases, `OAuth2ClientCredentials` serde alias, `Pagination::BodyCursor`
serde alias) — new name introduced, old name kept as deprecated alias for
one semver window. (3) Breaking-without-alias (`merge_key` → `merge_scope`
config vocabulary) — named deferral to the recorded 0.2→0.3 window; this
feature does not open it (workspace verified at 0.2.0).

**Alternatives considered**: opening the 0.3 window now — rejected by spec
assumption (deferral named instead); skipping the naming pass — rejected:
the non-breaking majority costs nothing and R10 rows are already drifting
into new code.

## D-11. D1–D5 testkit containers/fixtures design

**Decision**: new `rdlt_testkit::containers` module — `runtime_available()`
(superset probe: env override, docker/podman sockets, `podman ps` last) and
`PgFixture::start() -> Option<PgFixture>` (skip-not-fail posture,
`16-alpine` tag and conn string defined once) — plus
`rdlt_testkit::fixtures` owning `batch_of`/`schema_for`/`meta_for`. All
seven duplication sites across postgres/duckdb/file/iceberg tests consume
them; the posture rule becomes: a missing runtime always skips visibly,
never panics (matching the documented skip-not-fail intent, Principle VII).

**Alternatives considered**: per-crate `tests/common` cleanup without
testkit — rejected: cross-crate drift (duckdb copying postgres's fixture) is
the observed failure mode; only testkit is shared.

## D-12. D6–D10 CI approach

**Decision**: local composite action `.github/actions/free-disk/action.yml`
referenced by all 5 jobs; semver job gets the step too (its build is the
heaviest two-tree case — the exemption theory is undocumented and unproven);
drop `dtolnay/rust-toolchain@stable` installs and rely on
`rust-toolchain.toml` (keep `@nightly` only for fuzz); keep one canonical
copy of the disk-constraint rationale in the composite action's README with
one-line pointers elsewhere; document the deep-checks RUSTFLAGS divergence
in place.

**Alternatives considered**: a reusable workflow — rejected: composite
action is the lighter mechanism for a step, and stays repo-local.

## D-13. D7/D13/D14 manifest facts

**Decision**: `iai-callgrind` moves to `[workspace.dependencies]` pinned
`=0.16.x` to match the runner version in CI, with a cross-linking comment
both places (a version-agreement check in the perf-gate job guards the
pair); `libc` inherits; `rust-version = "1.96"` enters `[workspace.package]`
and is inherited per-crate (toolchain file pins 1.96.0 — verified; the
stale "1.94 floor" prose in CLAUDE.md gets corrected, along with the arrow
"58.4" vs pinned 58.3 drift); redundant implied features
(`postgres-source`, `file`) drop from CLI/bench feature lists.

**Alternatives considered**: deriving the runner version from the manifest
in CI via `cargo metadata` — acceptable variant; decided at implementation,
recorded in the close-out matrix either way.

## D-14. Red-first regression evidence method

**Decision**: each B-item regression test is committed in the same increment
as its fix, and the close-out matrix cites the red run: the test is executed
once against the pre-fix tree (`git stash` of the fix or a first commit
ordering test-before-fix) with the failing output captured in the matrix
row. Where a defect needs infrastructure to reproduce (B6 needs a 401ing
catalog), the probe fixture from D-03 doubles as the reproduction.

**Rationale**: SC-001 demands demonstrated-red evidence, not asserted-red.

## D-15. Coverage baseline-first and matrix mechanics

**Decision**: before increment 1, run the coverage target once on the merge
base and record the number in the close-out matrix header (the 015/016
"baseline-first" pattern). The close-out matrix lives at
`specs/017-workspace-refactoring/close-out.md`, one row per catalogue item
(B1–B12; R1–R13 expanded to their Part 3 sub-items; D1–D15 plus Part 5
low-severity notes), columns: item, increment, disposition
(applied / shimmed / deferred-with-name / overtaken-by-events), evidence
citation. FR-023's zero-uncited rule is checked by grepping the matrix for
empty evidence cells at close-out.
