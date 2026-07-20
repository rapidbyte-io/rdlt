# Implementation Plan: Postgres Source Completion (pre-CDC)

**Branch**: `007-postgres-source-completion` | **Date**: 2026-07-20 | **Spec**: [spec.md](spec.md)

**Input**: Feature specification from `/specs/007-postgres-source-completion/spec.md`

## Summary

Close every non-CDC gap from the 006 completeness audit in one small
story, touching ONLY `crates/rdlt-postgres` (plus tests/docs) — zero
engine or SPI changes. Mutual TLS plugs client credentials into the
three existing `ClientConfig` construction sites in the shared `tls`
module (both connectors inherit through the one connect path; no new
dependencies — research R1/R2). Cursor lag lowers only the read-side
lower bound via SQL-side typed arithmetic; the saved watermark never
regresses; exact totals ride the 006 keyed-Merge path, which is why
lag validation requires a primary key (R4 — the honest dedup story,
recorded as a sanctioned spec amendment). libpq portability pre-parses
the TLS parameter trio out of conn strings at the single shared parse
gate and re-wraps every unknown-parameter rejection with the parameter
name (R5). The small items — NULL-cursor `error` policy (decode-time,
zero-cost when clean, R8), inclusive end bound (one WHERE-clause
matrix entry, R9), default `application_name=rdlt` (R6), and a
`pg_inherits`-based discovery filter that unifies partitions +
INHERITS children AND introduces the explicit-listing override the
spec scenario needs (R7 — upgrading the 005 rule, conformance pin
updated). All new config fields ride the 006 generated-schema
round-trips (R10).

## Technical Context

**Language/Version**: Rust stable (2024 edition), workspace v0.2.0

**Primary Dependencies**: none new — `rustls`/`rustls-pemfile` (mTLS),
`tokio-postgres` (conn config), `schemars`/`jsonschema` (schemas) are
all already in the tree; `rcgen` (dev) already issues the test PKI

**Storage**: n/a — checkpoint/state formats UNCHANGED (lag is a
read-side bound; watermark semantics untouched)

**Testing**: extend the existing surfaces in place — `tls_matrix.rs`
gains the client-cert cells (server posture: `ssl_ca_file` +
`hostssl … cert` via the existing initdb hook), `incremental.rs` gains
lag/late-arrival + NULL-error + inclusive-end conformance,
`conformance.rs` gains the INHERITS/mixed-hierarchy and
explicit-listing cells, config tests gain conn-string translation +
contradiction cases, `config_schema.rs` suites gain the new fields

**Target Platform**: Linux; reference machine unchanged

**Project Type**: Rust library workspace + dev CLI; net-zero crates

**Performance Goals**: no new cells; every change is off the hot
decode path (WHERE-clause edits, handshake-time credentials,
connect-time application_name) — SC-008 is proven by the existing iai
gate + gated e2e bars staying within tolerance

**Constraints**: safe Rust only (`unsafe_code = "deny"`, no new
exceptions — client-auth wiring is plain rustls builder API); zero
`rdlt-core`/`rdlt-connector` changes (semver-checks vs origin/main
must stay "no update required"); checkpoint/state compatibility with
005/006 WALs preserved (new config fields are all optional with
today's behavior as default); 006 benchmark records untouched

**Scale/Scope**: one crate's source + tls modules; ~5 config fields
(2 TLS, 3 cursor), 1 policy variant, 1 SQL predicate swap + exception
list, 1 conn-string front-end; contracts: 3 new + 2 amended (tls
policy, source-config); 2 recorded spec amendments (research R4, R7)

## Constitution Check

Constitution file remains the unfilled template; governing principles
carried from features 001–006. **Seams sacred**: PASS — zero SPI or
engine changes; both recorded spec amendments (R4 lag-dedup truth, R7
listing override) go through research + spec-edit, not silent drift.
**No silent failures**: PASS — every new failure mode is typed and
named (missing key counterpart, cert-with-disable, server-rejected-
client-cert distinguished from server-verification failures, lag on
undefined subtraction, lag+open-boundary, lag-without-key, NULL cursor
under `error`, unknown conn parameter BY NAME); the one at-least-once
property (lag under Append) is documented, never silent. **Correctness
before speed**: PASS — lag's semantics were redesigned at plan time
when the "existing dedup absorbs it" claim proved false (R4);
conformance pins exact totals under the supported mode. **Measured,
not asserted**: PASS — SC-008 rides the armed perf gate; no bar or
baseline moves. **Safe Rust**: PASS — no unsafe anywhere in scope.

Post-design re-check: PASS — the design added no new crates, no SPI
surface, no unsafe, and no state-format changes.

## Project Structure

### Documentation (this feature)

```text
specs/007-postgres-source-completion/
├── plan.md              # This file
├── research.md          # R1–R10 + the two sanctioned spec amendments
├── data-model.md        # Config-surface deltas + validation rules
├── quickstart.md        # User-facing recipes for each story
├── contracts/
│   ├── tls-client-auth.md        # mTLS: config, handshake, error taxonomy
│   ├── cursor-lag.md             # lag + NULL policy + end bound semantics
│   └── connstring-portability.md # libpq TLS-trio translation rules
└── tasks.md             # /speckit-tasks output (NOT created by /speckit-plan)
```

### Source Code (repository root)

```text
crates/rdlt-postgres/
├── src/
│   ├── tls.rs            # TlsPolicy {+client_cert, +client_key};
│   │                     #   client_config() with_client_auth_cert;
│   │                     #   classify: +ClientCert failure (alerts + 28000);
│   │                     #   conn-string front-end: strip/translate TLS trio,
│   │                     #   named unknown-parameter errors, application_name
│   ├── tls_verify.rs     # unchanged (verifiers compose with client auth)
│   ├── source/
│   │   ├── config.rs     # CursorConfig {+lag, +end_bound}; NullPolicy::Error;
│   │   │                 #   Lag vocabulary type (FromStr/Display/schemars)
│   │   ├── sqlgen.rs     # lower bound minus lag (SQL-side); inclusive upper
│   │   ├── reflect.rs    # pg_inherits filter + listed-name exception ($3)
│   │   ├── cursor.rs     # tracker: NULL under Error → typed fatal
│   │   └── mod.rs        # open-time lag validation (boundary/type/key)
│   └── dest/mod.rs       # inherits mTLS + portability via shared tls path
└── tests/
    ├── common/mod.rs     # TlsPki client certs; fixture ssl_ca_file + cert auth
    ├── tls_matrix.rs     # + client-cert cells (both directions)
    ├── incremental.rs    # + lag late-arrival (Merge), NULL-error, inclusive end
    ├── conformance.rs    # + INHERITS/mixed hierarchy + listing override
    │                     #   (updates the 005 partition pin)
    └── config_schema.rs  # + new fields in examples/corpus
```

## Design Notes (delta-level)

- **mTLS (US1)**: `TlsPolicy` gains `client_cert`/`client_key`
  (both `RootCert` path-or-inline form; both-or-neither validated in
  `resolve_policy`, cert-with-`disable` a contradiction). All three
  `client_config()` arms swap `with_no_client_auth()` for
  `with_client_auth_cert` when credentials are present — the custom
  verifiers (`AcceptAnyCert`, `ChainOnly`) compose unchanged.
  Classification: rustls `AlertReceived(CertificateRequired |
  BadCertificate | UnknownCa | HandshakeFailure)` via the existing
  `get_ref()` downcast chain, plus SQLSTATE `28000` at auth phase →
  one `TlsFailure::ClientCert`, distinct from server-verification
  failures (R2 — pg rejects at either layer depending on pg_hba).
- **Lag (US2)**: `effective_lower = $watermark - lag` rendered in SQL
  (`- interval 'N seconds'` for time cursors, native magnitude for
  numeric, days for date) so Postgres owns the typed arithmetic. Saved
  watermark NEVER lowered. Open-time validation: closed boundary,
  subtractable cursor type, primary key present. Exact totals under
  keyed Merge (006 path); Append documented at-least-once within the
  window (R4). Clamping is inherent: a lowered bound only widens the
  window; `initial_value` floors nothing on run 1 (no watermark yet).
- **Portability (US3)**: one front-end at the shared parse gate:
  extract `sslrootcert`/`sslcert`/`sslkey` from key=value and URL
  forms, feed the remainder to tokio-postgres, translate extractions
  into the TlsPolicy resolution with the existing contradiction rule;
  re-wrap residual unknown-parameter rejections naming the parameter.
  `sslrootcert=system` → native roots; libpq's implicit
  `~/.postgresql/*` file defaults NOT emulated (documented).
- **Cursor edges (US4)**: `NullPolicy::Error` raises from the tracker
  (decode already visits every cursor value — zero added cost when
  clean; Fatal, so the commit protocol guarantees no partial dupes).
  `end_bound: inclusive` is one new arm in the upper-bound matrix.
- **Discovery + observability (US5)**: `NOT EXISTS (… pg_inherits …)`
  replaces `NOT relispartition` (covers both hierarchies, R7);
  explicitly listed names ride an exception array parameter — NEW
  capability, 005 conformance pin updated deliberately.
  `application_name=rdlt` set post-parse when absent, both connectors.

## Verification Map (story → proof)

| Story | Proof surface |
|---|---|
| US1 mTLS | tls_matrix client-cert cells: valid syncs (src+dest), no-cert typed, wrong-CA-cert typed, mismatched-key config error (SC-001) |
| US2 lag | incremental late-arrival under Merge: capture + exact totals + 3-run convergence; rejection matrix (open boundary, text cursor, keyless) (SC-002) |
| US3 conn strings | unit corpus of real libpq URL shapes: translation, contradiction, named rejections — zero bare parse errors (SC-003) |
| US4 edges | NULL-error + inclusive-end conformance, defaults-unchanged pins (SC-004) |
| US5 discovery | INHERITS + mixed-hierarchy exact totals; listing override; pg_stat_activity name probe (SC-005/006) |
| Schemas | config_schema suites extended per field (SC-007) |
| No-regression | make check + gated bars, unchanged (SC-008) |

## Phase 2 note for /speckit-tasks

Stories are independent after the config/tls groundwork; suggested
order US1 → US3 (both touch tls.rs — calendar-ordered, not parallel),
US2, US4, US5, close-out. The R7 conformance-pin update MUST land in
the same task as the filter change (a pin updated separately from its
behavior change is a broken tripwire).
