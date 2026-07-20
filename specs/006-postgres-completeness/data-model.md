# Data Model: Postgres Source Completeness — Parity + TLS

**Feature**: 006-postgres-completeness | **Date**: 2026-07-20

## 1. TlsPolicy (shared, `rdlt-pg-tls`)

| Field | Values | Rules |
|---|---|---|
| `mode` | `disable` \| `prefer` (default) \| `require` \| `verify_ca` \| `verify_full` | conn-string `sslmode` covers the first three; verify-* requires the config block; block↔conn contradiction = typed config error |
| `root_cert` | path or inline PEM, optional | used by verify-*; unreadable/unparseable = typed config error naming it; absent ⇒ platform trust store |

Consumed by BOTH postgres connectors (source config `tls:` block;
destination builder + CLI TOML). Error taxonomy per
contracts/tls-policy.md: config-shaped failures at open, verification
failures at connect (trust-anchor / chain / hostname, distinguished).

## 2. Type hints (source config, per table)

`tables[].type_hints: {column → hint}`; hint vocabulary shared with
rest/file. Compiles to (server-side cast, decode) per the CLOSED table
in contracts/type-hints.md. Validation at open: column exists;
(source type → hint) pair is listed; hinted cursor columns stay
cursor-capable. Lossy hints join the lossy-visibility surface (§5).

## 3. Query stream (source config)

`queries: [{name, sql, cursor?, primary_key?, type_hints?}]`
— name unique across tables+queries; `sql` always wrapped
`SELECT * FROM (sql) AS q` (read-only enforcement + predicate surface +
snapshot); schema from prepare/describe → same type mapping (typmod
unknown ⇒ numerics take the textual policy row unless hinted;
nullability unknown ⇒ nullable). Cursor column must be in the described
output. Otherwise a first-class stream: same incremental state
(watermark + boundary keys), same checkpoints, same crash guarantees.

## 4. Keyed structured merge (engine + SQL destinations)

- **Plan-time acceptance**: structured stream + `Merge` requested +
  non-empty declared key + destination `capabilities().merge` — else
  the existing typed rejections stand.
- **Write-time validation**: NULL in any key column of any batch =
  typed error (keys are identities).
- **Commit semantics**: per commit unit, delete-by-key then insert from
  staging (generalization of the existing `_rdlt_root_id` machinery to
  configured key columns); idempotent per (load_id, commit_seq) as
  today (D3).
- **State transition**: downstream holds exactly one row per key;
  redelivery inside the crash window is dedup-safe in this mode
  (documented E7 note update).

## 5. Lossy-visibility record (runtime, observable)

One warn-level structured trace per [documented-lossy] column per
stream read: `{stream, column, source_type, rule}`, target
`rdlt::lossy`, exactly once (not per batch). Covers policy rows,
textual fallback, and representation-changing hints.

## 6. Config schema (per source crate)

Generated from the config structs (schemars); exposed as
`config_schema()` and carried in `ConnectorSpec.config_schema`
(existing field). Invariants (round-trip tested): every documented
example validates; unknown-field configs fail (additionalProperties:
false); schema-valid ⇒ serde-parses for the test corpus.

## Validation rules (cross-entity)

- A `tls.mode` of verify-* with no resolvable trust root (no custom
  root AND empty platform store) fails at open, not at first packet.
- Hints and query streams compose: hint validation for query streams
  runs against the DESCRIBED schema.
- Merge + query stream requires `primary_key` declared on the query
  (nothing to reflect) — same keyed rule as tables.
- Every new failure path carries phase + stream/column context (FR
  language: typed, phase-tagged).
