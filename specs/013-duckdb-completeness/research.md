# Research: DuckDB Destination Completeness

## R1 — Shared core location: a new internal crate, SPI untouched

**Decision**: new workspace member `crates/rdlt-connector-sqlcore` —
the destination-agnostic merge layer: the options vocabulary
(`MergeStrategy`, table options, scd2 options), two-layer validation,
the plan shapes (dedup/survivor ordering, scope replacement, strategy
arms, per-table single-commit-unit state), and the `MergeDialect`
trait. Both SQL connectors depend on it. Dependencies: `serde` +
`thiserror` only (existing workspace deps — zero new external
runtime deps, FR-010).

**Rationale**: rdlt-connector (SPI) is frozen — adding public items
there breaks semver-checks "no update required". A crate keyed by
ROLE (`-sqlcore`, clearly not a `<system>` connector) sits naturally
beside the family without polluting the system-name pattern.

**Alternatives considered**: module inside rdlt-connector (rejected:
SPI freeze); duckdb depending on rdlt-connector-postgres (rejected:
drags tokio-postgres/rustls into an embedded connector and inverts
the family shape); copy-paste (rejected: the point is destination #4
becoming a dialect, not a connector).

## R2 — API stability: types move, paths stay

**Decision**: the option types move to sqlcore under neutral names
(`DestOptions`, `TableOptions`, `MergeStrategy`, `Scd2Options`);
rdlt-connector-postgres re-exports them at the existing paths
(`PgDestOptions`/`PgTableOptions` become aliases). The CLI's YAML
shape, the facade paths, and serde behavior are unchanged; DuckDB
gains the same `.options(...)` builder hook postgres has.

## R3 — Extraction proof: golden-SQL pins BEFORE the refactor

**Decision**: before any code moves, unit cells capture the EXACT SQL
strings the postgres dest generates for a representative plan matrix
(each strategy × dedup_sort × merge_key × hard_delete × scd2 absent
modes). The extraction must reproduce those strings byte-for-byte
through the postgres dialect — the golden pins plus the existing
suites/sweeps/gated bars are the FR-001 "provably unchanged" proof.

**Rationale**: "behavior-preserving refactor" claims are cheap; byte-
identical SQL is checkable and makes review trivial.

## R4 — DuckDB dialect feasibility (probe-verified at implementation)

Current knowledge, each pinned by a probe test before the arm ships
(any miss → typed capability gap per FR-002, never approximation):

- **Dedup/survivor**: DuckDB supports postgres-style `DISTINCT ON`
  (and `QUALIFY` as fallback) — the 010 ordered-dedup shape carries.
- **Upsert**: DuckDB supports `INSERT … ON CONFLICT DO UPDATE`
  targeting a primary key or UNIQUE ART index; the M5 auto-ensured
  unique merge-identity index pattern carries (`CREATE UNIQUE INDEX`).
- **Delete-insert / scope replace**: plain `DELETE … WHERE (…) IN` +
  `INSERT … SELECT` — the duckdb dest already emits this shape today.
- **scd2**: `UPDATE`/`INSERT … SELECT` with `IS DISTINCT FROM` all
  supported; boundary timestamp via R5.
- **Stage→target already flows through SQL** (`INSERT INTO target
  SELECT … FROM stage`), so per-column CASTs (R6) and strategy arms
  slot into the existing seam.

## R5 — scd2 boundary timestamp on DuckDB

**Decision**: each dialect supplies its transaction-timestamp
expression; DuckDB's `now()` is transaction-stable and the commit
unit already executes inside one transaction, so the 008 rule ("one
boundary per commit unit, redelivery-stable") holds. A cell pins that
two scd2 statements in one unit observe the same boundary.
Postgres' expression is untouched (FR-001).

## R6 — Native JSON on DuckDB

**Decision**: `Json`-typed columns create as DuckDB `JSON` (the JSON
extension is statically bundled with the `bundled` duckdb-rs build);
staging stays VARCHAR (Arrow appender path unchanged) and the
stage→target `INSERT … SELECT` applies the cast. `json_type` flips to
true; round-trip proven via the postgres jsonb escape-hatch path and
DuckDB's own `json_extract` in the cell. If the bundled build turns
out to lack the extension, the capability stays false and the finding
is recorded (honesty either way) — the probe decides.

## R7 — Cross-destination differential oracle

**Decision**: `crates/rdlt/tests/dest_differential.rs` — facade-level
(the only crate that naturally sees both destinations). Identical
in-memory feeds (testkit MemorySource scripts: appends, redeliveries,
duplicates, deletes via hard_delete flag, scoped loads, NULL-in-key
rejects) run through postgres (testcontainers) and DuckDB (temp file);
equivalence = same rows per table under a canonical SELECT (ordered,
normalized), same typed-error classes for rejection cases, modulo a
short documented type-affinity table (e.g. numeric width). Gated by
the same container-optional discipline as existing pg live tests.

## R8 — Verification + benchmarks

**Decision**: 011 protocol scoped to the duckdb crate — matrix at
`specs/013-duckdb-completeness/matrix.md` (citations first; the 006/
008/010 suites already cover much of the shared behavior), coverage
via `cargo llvm-cov nextest -p rdlt-connector-duckdb` with baseline
measured in T001 and an ≥80% recorded floor; crash sweeps extend the
duckdb failpoints to the new arms (armed-fire pins, crash/rerun
convergence per strategy). Benchmarks: two SCOREBOARD cells in the
012 harness (`duckdb-strategy-delete-insert-1m`,
`duckdb-strategy-upsert-1m` — pg-src fixture → DuckDB file dest,
load-2-timed with `{{run}}`-unique 50% updates, mirroring the pg
strategy cells); no new gates, all existing bars must stay green.
dlt parity record follows the 010 format against pin dlt 1.29.0.
