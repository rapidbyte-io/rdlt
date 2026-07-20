# Research: Postgres Source Completion (pre-CDC)

All decisions verified against the current code on branch
`007-postgres-source-completion` (stacked on 006) — file/line facts
checked this session, not recalled.

## R1 — mTLS via the existing rustls path (no new dependencies)

**Decision**: Client credentials plug into the three existing
`ClientConfig` construction sites in `crates/rdlt-postgres/src/tls.rs`
(`client_config()`), replacing `with_no_client_auth()` with
`with_client_auth_cert(certs, key)` when a credential is configured.
Key material parses via the already-present `rustls-pemfile`
(PKCS#8 / RSA / SEC1 accepted). `TlsPolicy` gains
`client_cert: Option<RootCert>` and `client_key: Option<RootCert>` —
reusing the `RootCert` path-or-inline-PEM newtype for both (rename NOT
needed; its doc already says "a PEM input").

**Rationale**: zero new dependencies; the builder API offers
`with_client_auth_cert` at exactly the same stage for both the stock
webpki verifier and our `dangerous()` custom verifiers (AcceptAnyCert,
ChainOnly), so all five sslmode levels compose with client auth
untouched. Mode `disable` + client credential = the FR-002 typed
contradiction (checked in `resolve_policy`).

**Alternatives considered**: separate `ClientCredential { cert, key }`
struct — rejected; two optional fields with a both-or-neither
validation produce simpler config YAML/JSON and schemars output.
Encrypted keys — out of scope (spec assumption); `rustls-pemfile`
does not decrypt, and the typed error names the limitation.

## R2 — Classifying "server rejected our certificate"

**Decision**: extend `classify_connect_error` with a new
`TlsFailure::ClientCert` arm fed from two paths: (a) rustls
`AlertReceived(CertificateRequired | BadCertificate |
UnknownCa | HandshakeFailure)` surfaced during handshake (TLS 1.3
servers), reached through the existing `get_ref()` downcast chain
(the io::Error::source() skip discovered in 006 applies here too);
(b) Postgres auth-phase SQLSTATE `28000` ("connection requires a valid
client certificate") for servers that accept the handshake and reject
at pg_hba `cert`/`clientcert` evaluation. Both map to the SAME typed
failure so users see one story: "server rejected the client
credential", distinguished from `TrustAnchor`/`Chain`/`Hostname`
(which are about US verifying THEM).

**Rationale**: postgres rejects client certs at two different layers
depending on `pg_hba.conf` (`cert` auth method vs `clientcert=` option
on another method); a classification that only caught one layer would
flake by server configuration.

## R3 — mTLS test rig

**Decision**: extend `TlsPki` in `tests/common/mod.rs` with
client-cert issuance from the SAME test CA (plus one from the existing
"rdlt WRONG CA" — distinct CN discipline from 006 R-note applies) and
extend `TlsPgFixture`'s `zz-tls.sh` initdb script: install the CA as
`ssl_ca_file`, and write a `hostssl ... cert` pg_hba line (cert auth =
handshake identity is the login). The matrix adds cells: valid
cert+key syncs (source AND destination), no-cert typed failure,
wrong-CA-cert typed failure, mismatched key = config error before
connect.

**Rationale**: `cert` auth exercises the strictest server posture and
needs no password plumbing; the 006 fixture already rebuilds pg_hba in
the initdb hook, so this is an additive block, not a new fixture.

## R4 — Cursor lag: semantics and the duplicate-absorption truth

**Decision** (amends spec FR-004/SC-002 wording — recorded here as the
sanctioned refinement): lag lowers ONLY the read-side lower bound:
`effective_lower = saved_watermark - lag`, computed **in SQL** as
`($watermark::type - $lag_literal)` so Postgres does the typed
arithmetic (`timestamptz - interval`, `int8 - int8`, `date - int`).
The SAVED watermark advances exactly as today and is never lowered.
Validation at open: lag requires (a) a closed lower boundary (open +
lag = typed contradiction — open exists to skip re-reads), (b) a
cursor type with defined subtraction (timestamp/timestamptz/date/
int2/4/8/numeric-decimal; text/uuid = typed error naming column and
type), (c) a stream with a primary key (reflected or declared).

**The duplicate story, honestly**: the 005 checkpoint dedup
(`boundary_keys`) covers only watermark-EQUAL rows — it cannot absorb
a whole window (verified: `sqlgen::incremental_clauses` lower-closed
`>=` + tracker dedup of boundary keys only). Rows strictly inside the
window re-deliver on every run BY DESIGN (same as dlt's lag). Exact
destination totals therefore come from keyed Merge (the 006
merge-by-declared-key path — the pg source reflects PKs, so the common
case needs zero extra config). Under Append, lag is at-least-once
within the window; this is a DOCUMENTED property, and the primary-key
requirement (c) exists so Merge is always available. Conformance
(SC-002) runs under Merge and asserts exact totals + convergence of
`count(*)` — the spec's "re-runs move zero rows" is amended to
"destination totals remain exactly equal to the source" (window rows
re-deliver and merge idempotently).

**Alternatives considered**: recording ALL window keys in checkpoint
state for source-side dedup — rejected: unbounded state growth on busy
tables (a 5-minute window can hold 10⁵ keys), against the bounded-
state discipline; client-side watermark arithmetic in Rust — rejected:
per-type reimplementation of what Postgres does correctly, and the
interval subtraction must match server semantics (DST, month lengths)
exactly.

**Config form**: `lag` on `CursorConfig` as a string in the cursor's
native vocabulary — duration form (`"5m"`, `"300s"`, `"1h"`, `"2d"`)
for time cursors rendered as a `- interval 'N seconds'` (days for
`date`), plain integer/decimal magnitude for numeric cursors. Custom
FromStr/Display + schemars pattern, same pattern as 006's HintType.

## R5 — libpq conn-string portability (pre-parse and translate)

**Decision**: a small conn-string front-end in `tls.rs` (shared by
both connectors — the ONE connect path from 006 makes this a single
site): before `tokio_postgres::Config::from_str`, extract-and-strip
`sslrootcert`, `sslcert`, `sslkey` from both libpq forms (key=value
and URL query), translating them into the TlsPolicy resolution with
the SAME contradiction rule sslmode already has (conn-string value vs
tls-block value that disagree = typed error naming both). Any
remaining parameter tokio-postgres rejects is re-wrapped into a typed
error naming the parameter (FR-007) — the current bare
"invalid connection string" parse gate (005 review finding) is
replaced at the same choke point. `sslrootcert=system` (libpq 16+)
maps to our native-roots default; the special libpq file defaults
(`~/.postgresql/root.crt`) are NOT emulated (documented — explicit
config only).

**Rationale**: tokio-postgres hard-errors on unknown parameters
(verified against its Config parser), so pass-through is impossible;
stripping exactly the TLS trio keeps us byte-compatible with
tokio-postgres for everything else and avoids maintaining a full
libpq-conninfo parser.

## R6 — application_name

**Decision**: after parsing, if `application_name` is unset, set
`rdlt` on the `tokio_postgres::Config` (source and destination —
again one site via the shared connect path). User-provided values
(conn string param, which tokio-postgres supports natively) win.

## R7 — INHERITS exclusion + the explicit-listing override

**Decision**: replace `AND NOT c.relispartition` in `REFLECT_SQL` with
`AND NOT EXISTS (SELECT 1 FROM pg_inherits i WHERE i.inhrelid = c.oid)`
— declarative partitions ARE pg_inherits children, so one predicate
covers both hierarchies (including mixed ones). **Fact check**: the
spec's "explicit listing overrides exclusion, same rule as partitions"
described a precedent that does NOT exist — today's filter is
unconditional and `partitioned_tables_load_once_via_parent` pins that
leaves cannot be streams at all. The override is genuinely useful
(reading one partition/child for a backfill), so 007 IMPLEMENTS it for
both kinds: explicitly listed table names are passed to the reflection
query as an exception list (`... OR c.relname = ANY($3)`), and the 005
conformance pin is updated to assert the (unchanged) schema-wide
behavior plus the new explicit-listing acceptance.

**Rationale**: one predicate, one round trip, no new config surface —
and the spec scenario becomes true instead of silently aspirational.

## R8 — NULL-cursor "error" policy

**Decision**: third `NullPolicy` variant `Error`. SQL emits no NULL
filter (like `Include`); the decode path already touches every cursor
value for watermark tracking, so the tracker raises the typed error
(stream + column) on the first NULL — zero cost when rows have no
NULLs, no extra pre-flight query. Retry safety: the error is Fatal
(config/data contract violation, not transient), and staged batches
roll back per the existing commit protocol — no duplicate state
(spec US4-AS1) because nothing past the last checkpoint commits.

## R9 — Inclusive end bound

**Decision**: `CursorConfig` gains `end_bound: exclusive (default) |
inclusive`; `incremental_clauses` upper predicate becomes `<=`/`>=`
under inclusive (direction-aware), mirroring the existing lower-bound
closed/open matrix. Watermark semantics unchanged (the end bound is a
read filter, not resume state).

## R10 — Schema and gate integration

**Decision**: all new fields (`client_cert`, `client_key`, `lag`,
`end_bound`, `NullPolicy::Error`) ride the 006 schemars derives
automatically; the 006 `config_schema.rs` round-trip suites gain the
new-field examples (SC-007 extends, not duplicates). Perf: every 007
change is off the hot decode path (lag/end-bound only edit the WHERE
clause; mTLS only the handshake; application_name only connect), so
the existing iai gate + e2e bars are the SC-008 proof — no new cells.

## Spec amendments recorded by this research (sanctioned refinements)

1. **FR-004 / SC-002 (lag duplicates)**: "absorbed by the existing
   closed-boundary dedup" → the R4 design: closed boundary + primary
   key required; exact totals under keyed Merge (conformance mode);
   Append re-delivers window rows (documented). SC-002's "re-runs move
   zero rows" → "destination totals remain exactly equal to the
   source across three consecutive re-runs".
2. **US5-AS3 (listing override)**: described as an existing rule; it
   was not one. 007 implements it for partitions AND inheritance
   children (R7), updating the 005 conformance pin accordingly.
