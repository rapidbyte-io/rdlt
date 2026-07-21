# Research: Postgres Destination Completion

Decisions verified against the code on branch `008-postgres-dest-completion`
(main @ de8c197) and the dlt inventory recorded in this feature's spec
Context. File/line facts checked this session.

## R1 — Native NUMERIC/JSONB/UUID encoding: hand-rolled wire encoders, zero new dependencies

**Decision**: the destination keeps its `BinaryCopyInWriter` bulk path and
gains small `ToSql` wrapper types for the three new wire formats, written
in-crate:
- `NumericWire(i128, scale)` — the Postgres numeric binary format
  (ndigits/weight/sign/dscale + base-10000 digit groups). We already
  DECODE this format in `src/source/copy_decode.rs`; the encoder is its
  mirror, tested against the decoder round-trip (encode → decode →
  identical i128/scale) AND against a live server.
- `JsonbWire(&str)` — version byte `1` + UTF-8 text (the source's
  `Decode::JsonbText` strips exactly this byte).
- `UuidWire([u8; 16])` — parsed from the canonical text form the engine
  ships (`LogicalType::Uuid` arrow repr is Utf8).

**Rationale**: tokio-postgres supports these types only through optional
dependency features (`rust_decimal`, `uuid`, `serde_json`); the project
already owns the numeric/jsonb wire knowledge in the source decoder, and
a hand-rolled `ToSql` impl is safe Rust with no new deps — the same
house call as 005's decoder. Mismatch risk is covered by the
encoder↔decoder round-trip property test.

**Alternatives considered**: `rust_decimal` + tokio-postgres features —
rejected (new dependency for a format we already implement; i128→Decimal
conversion adds its own edge cases). Text-format COPY fallback for the
new types — rejected (two code paths, and CSV/text COPY is the slower
path we deliberately avoid).

## R2 — Capability flips: data-only, engine untouched, ZERO user configuration

**Decision**: `DestCapabilities { decimal: true, json_type: true }` for
the postgres destination. These are CODE-LEVEL declarations inside the
connector's own `capabilities()` method (the struct that already says
`merge: true`) — not configuration. No config key exists or will exist
for them; users of the connector sync a decimal/JSON/UUID column and
get the native type, full stop (owner clarification, 2026-07-21). Verified: engine lowering is entirely
capability-driven (`crates/rdlt-engine/src/load/lowering.rs:59,125` —
`if !caps.decimal` lowers Decimal128 to rendered text; with the flag on,
Decimal128 passes through untouched). `LogicalType::Uuid` needs no
capability (its arrow repr stays Utf8; the DESTINATION chooses the
`uuid` column type and parses at encode time). NOT NULL comes from
`ColumnDef.nullable` (verified present, `rdlt-core/src/schema.rs:69`) —
emitted for CREATE only; adding NOT NULL to existing columns is a
non-additive change and stays out (FR-003).

**Consequences pinned in conformance**: structured postgres→postgres
round trip carries `numeric(p,s)` end to end (source produces
Decimal128 for constrained numerics); shredded JSON documents land as
`jsonb`; SUM equality proves no float detour.

## R3 — Pre-existing text columns: NO fallback (owner decision, greenfield)

**Decision (amended during implementation, 2026-07-21)**: the originally
planned catalog-detection + per-column text-fallback + `rdlt::lossy`
visibility machinery was IMPLEMENTED, then REMOVED on owner decision:
rdlt is greenfield — there is no pre-008 installed base whose tables
need protecting, and the fallback added a wire variant, a session map,
stage-recreation logic, and a conformance test for a population of
zero. What remains: the additive-only rule (existing columns are never
silently retyped — unchanged, enforced by the existing D5 migration
path), and hand-created mismatched tables fail LOUDLY at publish with
the server's typed error (SQLSTATE 42804 class) through `describe()`.
Contract dest-types.md T7 amended to match.

## R4 — Upsert strategy: conflict-update from the stage, index ensured first

**Decision**: `INSERT INTO target (cols) SELECT ... FROM (SELECT
DISTINCT ON (key) * FROM stage ORDER BY key, arrival DESC) dedup ON
CONFLICT (key) DO UPDATE SET col = EXCLUDED.col, ...` — in-batch
last-wins via the existing DISTINCT ON/arrival pattern, matched keys
update in place, one statement inside the existing publish transaction
(D2/D3 receipts give exactly-once and idempotent re-commit unchanged).
`ensure_table` creates `CREATE UNIQUE INDEX IF NOT EXISTS` on the key
first; a unique-violation there (pre-existing duplicate keys, SQLSTATE
23505) is a typed error naming the key columns. Keyed structured AND
shredded identity merges both get the strategy (identity = `_rdlt_id`
key).

**Alternatives considered**: native `MERGE INTO` (dlt's upsert) —
rejected: requires PG15+, `ON CONFLICT` is semantically sufficient for
key-based upsert and works on every supported version.

## R5 — Hard-delete column

**Decision**: per-table config `hard_delete: <column>`; validated at
ensure time (column exists in the schema; boolean → `= TRUE` condition,
any other type → `IS NOT NULL`, the dlt-compatible shape). Commit-time:
flagged keys `DELETE FROM target WHERE (key) IN (SELECT key FROM stage
WHERE <cond>)` and the insert/upsert SELECT excludes flagged rows.
Deletes of never-loaded keys are naturally no-ops. Works under both
delete-insert and upsert strategies.

## R6 — SCD2: destination-local semantics on keyed streams

**Decision**: per-table strategy `scd2` (keyed streams only — keyless
typed-rejected at ensure). The destination adds validity columns
(default names `_rdlt_valid_from`/`_rdlt_valid_to`, configurable,
collision-checked against the schema); active version = `valid_to IS
NULL`. Change detection in SQL: staged row vs active version compared
column-wise with `IS DISTINCT FROM` over non-key data columns (no row
hash to maintain). Per commit unit, inside the publish transaction:
retire (set `valid_to = boundary`) active versions whose key is staged
with different values, then insert staged rows (in-batch last-wins
dedup first) as new active versions with `valid_from = boundary`.
Boundary timestamp = `now()` at first execution of that (load_id,
commit_seq) — crash-redelivery stability comes from the EXISTING D3
receipt idempotency: a re-delivered commit unit returns the recorded
receipt and re-executes nothing, so the boundary cannot be minted
twice (verified: `_rdlt_commits` receipts precede any merge SQL).
Absence policy: `absent: keep` (default — incremental feeds are
partial) or `absent: retire` (full-feed semantics: retire active keys
not present in the stage). Unchanged staged rows (all columns
IS-NOT-DISTINCT) are skipped entirely — no churn versions.

**Alternatives considered**: dlt-style row-version hash column —
rejected (adds a stored hash column and a hashing contract;
`IS DISTINCT FROM` is exact, needs no storage, and NULL-safe). Validity
from load timestamps per ROW — rejected (a load must have ONE boundary
per commit unit or ranges interleave).

## R7 — Supporting indexes for merge paths

**Decision**: at ensure time, merge-mode tables get
`CREATE INDEX IF NOT EXISTS` on the merge identity: `_rdlt_id` for
shredded roots (and `_rdlt_root_id` for children), key columns for
keyed structured tables; upsert's UNIQUE index (R4) subsumes the plain
index for that strategy. Names are deterministic
(`rdlt_ix_<table>_<identity-hash>`) so idempotency holds across
sessions. The before/after effect is MEASURED once on a large keyed
merge (drop-index baseline vs indexed run, same session, quiet
machine) and recorded as a scoreboard entry (FR-009/SC-005) — no new
gate.

## R8 — Error chains (review F6 debt)

**Decision**: `dest` grows the same discipline as `tls.rs`: a single
`fn describe(e: &tokio_postgres::Error) -> String` that prefers
`as_db_error()` (server message + SQLSTATE) and falls back to the
source chain walk; `transient()`/`fatal()` route through it. The
existing SQLSTATE transient heuristic (08/53/57/40 classes) moves in
unchanged. Regression test: a forced constraint violation surfaces the
server message + SQLSTATE in the pipeline error.

## R9 — Modularization: pure relocation FIRST, then edits

**Decision**: commit 1 of the implementation splits `dest/mod.rs` (613
lines) into `dest/{mod.rs, config.rs, ddl.rs, encode.rs, commit.rs}` —
mod.rs keeps `Postgres`/`PgSession` orchestration + Destination/
LoadSession impls; config.rs the builder + new options types; ddl.rs
`sql_type`/create/migrate/index; encode.rs `copy_type`/`cell_value` +
the R1 wire encoders; commit.rs the publish transaction + strategy SQL.
The relocation commit is MOVES ONLY, gated by the full suite passing
byte-identical (FR-011/SC-007); all feature edits land in later
commits. This mirrors the 006 crate-merge discipline.

## R10 — Configuration surface

**Decision**: a serde-facing `PgDestOptions` (per-destination defaults +
per-table overrides): `merge_strategy: delete_insert | upsert | scd2`,
`hard_delete: <column>`, `scd2: { valid_from, valid_to, absent }`.
Builder API: `Postgres::options(PgDestOptions)` plus convenience
methods; CLI TOML `[destination.postgres]` gains the same fields;
`from_value`/serde entry point for embedders; schemars derive +
round-trip tests extend the 006 schema discipline (FR-012/SC-008).
Zero rdlt-core/rdlt-connector changes — verified the strategy concept
never crosses the SPI (WriteMode stays `Merge { key }`; strategies are
how THIS destination executes it; SCD2 is documented as a
destination-local semantic extension).

## R11 — Benchmarks

**Decision**: `benches/run-pg.sh` gains two scoreboard cells run with
the existing 5-run median protocol: merge-heavy (1M rows, 50% updates,
delete-insert vs upsert, indexed) and the R7 unindexed-vs-indexed
single measurement. Existing gated bars (pg→pg ≥6×, iai) unchanged and
must stay within tolerance (SC-006) — native-type encoding rides the
same bulk path, so the expected drift is noise-level; if the gate
disagrees, the encoding is the suspect, not the baseline.
