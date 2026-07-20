# Research: Postgres Source Completeness — Parity + TLS

**Feature**: 006-postgres-completeness | **Date**: 2026-07-20

Inputs: spec.md (incl. the measured parity table); the committed 005
dlt review; current SPI surface facts verified this session:
`DestCapabilities.merge: bool` exists, `ConnectorSpec.config_schema:
Option<serde_json::Value>` exists (defaulting None), `PipelineEvent` is
`#[non_exhaustive]`.

## R1 — TLS: one shared policy crate, rustls, libpq semantics

**Decision**: new internal crate `rdlt-pg-tls`:
`TlsPolicy { mode: Disable|Prefer|Require|VerifyCa|VerifyFull, root_cert: Option<RootCert> }`
(`RootCert` = file path or inline PEM). It builds the
`tokio-postgres-rustls` connector for a policy:

- `disable` → NoTls plaintext.
- `prefer` → TLS connector offered; tokio-postgres falls back to
  plaintext when the server refuses (its native Prefer behavior).
- `require` → encrypted, certificate NOT validated (libpq semantics)
  via a quarantined safe-Rust `ServerCertVerifier` that accepts any
  chain (documented loudly; never a default).
- `verify-ca` → chain verified against roots, hostname NOT checked
  (wrapper around the webpki verifier that treats only the
  name-mismatch error as success).
- `verify-full` → stock rustls verification (chain + hostname) —
  the recommended production mode.
- Roots: custom root when configured, else platform trust store
  (`rustls-native-certs`); a missing/unreadable/unparseable custom
  root is a typed CONFIG error naming the path; a verification
  failure is a typed CONNECT error distinguishing trust-anchor vs
  chain vs hostname (mapped from rustls error variants).

**Mode sourcing**: the conn string's `sslmode` covers
disable/prefer/require (what `tokio_postgres::Config` parses — it
REJECTS verify-*; verified in 005). The config's `tls:` block is the
full-fidelity surface (all five modes + root); when both are present
the block must not CONTRADICT the conn string (typed config error),
because silently out-ranking an explicit `sslmode` is how plaintext
surprises happen. verify-* therefore requires the block — documented in
the contract with examples. No string surgery on conn strings (the 005
lesson: parse, never pattern-match).

**Destination symmetry**: `rdlt-dest-postgres` gains the same
`TlsPolicy` (builder method + CLI TOML fields); both connectors call
the shared crate — they cannot drift.

**Alternatives**: openssl (FFI, platform pain — rejected);
native-tls (weaker control over the verify-ca/require distinction —
rejected); duplicating TLS per crate (drift — rejected); making
`require` verify certificates (breaks the ecosystem vocabulary
operators rely on — rejected, verify-full is the loud recommendation
instead).

## R2 — Type hints: server-side casts from a closed conversion table

**Decision**: `tables[].type_hints: {column: hint}` with the SAME hint
vocabulary as rest/file (`bool,int64,float64,decimal(p,s),utf8,binary,
timestamp_tz,timestamp_naive,date,time,uuid,json`). A hint compiles to
a server-side cast in the COPY projection plus the matching decode —
the wire still only carries the lossless decode set. The conversion
table (contracts/type-hints.md) is CLOSED: every (source type → hint)
pair is either listed with its cast or is a typed config error at open.
Universal row: any type → `utf8` (canonical text). Text-family →
typed hints use strict casts (`::timestamptz` etc.); a value that
fails the cast aborts the COPY with a typed copy-phase error naming
the column — per engine clause E7 structured streams have no
value-level discard, so "schema policy" for structured means: typed
error, never silent (spec US2-AS1 satisfied in its E7 form). Hinted
cursor columns must remain cursor-capable (existing validation reused).

**Alternatives**: client-side per-value parsing with discard policies
(contradicts E7's no-value-discard rule for structured streams;
slower; rejected); open-ended cast passthrough (unbounded failure
surface; rejected — closed table only).

## R3 — Query streams: subquery wrapping + describe-based schema

**Decision**: config gains `queries: [{name, sql, cursor?, primary_key?}]`.
The user SQL is ALWAYS wrapped: `SELECT * FROM ( <sql> ) AS q` — one
wrapper serving three jobs: (1) read-only enforcement (Postgres rejects
data-modifying statements/CTEs in a subquery — validated before any
data moves by the describe step), (2) a stable surface for the
incremental predicate/ORDER BY (`WHERE q.<cursor> …`), (3) per-statement
snapshot consistency identical to tables. Schema comes from
prepare/describe of the wrapped statement: column names + type OIDs
feed the SAME type-mapping contract; typmod is not available from
describe, so query-stream numerics are unconstrained → the textual
policy row (documented; a type hint can override). Nullability is
unknowable from describe → all columns nullable. Name collisions
(query vs table stream, duplicate query names) are typed config errors.
Cursor column must appear in the described output.

**Alternatives**: raw SQL splicing without a wrapper (loses read-only
enforcement and makes predicate composition fragile — rejected);
statement-kind allowlisting by parsing SQL (a parser is a bigger attack
surface than the server's own subquery rules — rejected).

## R4 — Merge for keyed structured streams: the B4 amendment

**Decision**: recorded amendment (contracts/merge-structured.md,
pointer added to the feature-002 contract): plan-time, `Merge` on a
structured stream is ACCEPTED when the stream declares a non-empty
`primary_key` AND the destination declares `capabilities().merge`
(existing flag — no SPI change); otherwise the existing typed
rejections stand (keyless; non-capable destination — parquet stays
append/replace). Semantics: per commit unit, delete-then-insert by the
declared key over the staged rows (the destinations' existing
`_rdlt_root_id` merge machinery generalized to configurable key
columns). The ENGINE validates at write time that key columns carry no
NULLs (typed error — merge keys are identities). Crash model unchanged:
staging invisibility + idempotent commit make merge exactly-once; the
crash sweeps gain a Merge mode with armed-fire pins extended.
Redelivery window note (E7) updates: keyed structured streams in Merge
mode ARE dedup-safe across redelivery (the key replaces), which
actually CLOSES the documented at-least-once caveat for this mode.

**Alternatives**: engine-computed row hash as implicit key (hides
identity semantics, surprises on updates — rejected); source-side
dedup state (wrong layer; destinations own visibility — rejected);
deferring to 007 (leaves the top workflow gap open; the machinery
reuse makes the scope manageable — rejected by user intent
"complete high quality").

## R5 — Lossy-mapping visibility: tracing, once per column per read

**Decision**: at read start the source emits one
`tracing::warn!(target: "rdlt::lossy", stream, column, source_type, rule)`
per [documented-lossy] column (policy rows + textual fallback + lossy
hints). Tested with a capturing subscriber (fires exactly once per
column; silent when nothing is lossy). The 005 contract's "run report
notes the column" wording is amended to name this surface (tracing is
the engine's documented observability floor; `PipelineEvent` is
engine-emitted and sources have no event channel — adding one is SPI
work deferred with the FK-lineage backlog item).

## R6 — Config schemas: schemars from the parsing structs

**Decision**: derive `schemars::JsonSchema` on the three source config
struct families; each crate exposes `pub fn config_schema() ->
serde_json::Value` and `Source::spec()` fills the EXISTING
`ConnectorSpec.config_schema` field. `deny_unknown_fields` maps to
`additionalProperties: false`, so schema-validation agreement with
serde is structural; round-trip tests validate every documented example
config against the schema and assert documented-invalid ones fail —
plus a property check that schema-valid ⇒ parses for a sample corpus.

## R7 — Test-advisory closures

- **Differential multi-batch**: a second proptest variant runs with
  `batch_max_rows: 3` and larger row sets, concatenating batches
  (arrow-select) before comparison — chunk/batch boundaries become
  differentially covered.
- **Memory test loud-fail**: honor `RDLT_HEAVY=1` (set by the sweep/
  deep make targets): with it, missing prerequisites (prlimit, release
  CLI) are FAILURES with instructions; without it, the skip prints and
  the test is marked skipped, never silently green in the environment
  that promises coverage.
- **Container-kill determinism**: kill only after the destination
  observes ≥1 committed row (poll), removing the `if count > 0` escape
  and the fast-run flake window.

## R8 — TLS test rig

**Decision**: `rcgen` (dev-dep) generates a CA + server cert (SANs:
`localhost`, `127.0.0.1` for the match case; a second cert with a
wrong SAN for the mismatch case) at test time; the postgres container
starts via a wrapped entrypoint that copies certs into place with
postgres-owned 0600 perms and `exec`s the stock entrypoint with
`ssl=on` (+`hostssl`-only pg_hba for the "TLS required" case). The
matrix then drives all five modes × {match, mismatch, unknown-CA} for
BOTH connectors through shared cases. Self-skips only where container
runtime is genuinely absent (same posture as every other suite).
