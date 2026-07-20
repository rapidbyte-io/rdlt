# Data Model: Postgres Source Completion (pre-CDC)

All deltas are additive and optional — every existing config parses
unchanged with identical behavior. No checkpoint/state-format changes.

## TlsPolicy (shared, `crates/rdlt-postgres/src/tls.rs`)

| Field | Type | Default | Rules |
|---|---|---|---|
| `mode` | TlsMode | prefer | unchanged |
| `root_cert` | Option<RootCert> | None | unchanged |
| `client_cert` | Option<RootCert> *(new)* | None | path or inline PEM; requires `client_key`; contradiction with `mode: disable` |
| `client_key` | Option<RootCert> *(new)* | None | path or inline PEM (PKCS#8/RSA/SEC1, unencrypted); requires `client_cert` |

Validation (in `resolve_policy`, before any connection):
- `client_cert` XOR `client_key` present → `TlsConfigError` naming the
  missing counterpart.
- credential + `mode: disable` → contradiction error.
- unreadable/unparseable material → error naming the path (or "inline
  PEM"), consistent with `root_cert` errors.
- encrypted key → typed error naming the unencrypted-PEM limitation.

New connect-failure class: `TlsFailure::ClientCert` — "server rejected
the client credential" (rustls certificate-related alerts ∪ SQLSTATE
28000), disjoint from `TrustAnchor`/`Chain`/`Hostname`.

## Conn-string TLS parameters (translated, never stored)

| libpq parameter | Destination | Contradiction rule |
|---|---|---|
| `sslrootcert=PATH` | `TlsPolicy.root_cert` | conflicts with a DIFFERENT block `root_cert` |
| `sslrootcert=system` | native-roots default | conflicts with block `root_cert` |
| `sslcert=PATH` | `TlsPolicy.client_cert` | conflicts with a different block value |
| `sslkey=PATH` | `TlsPolicy.client_key` | conflicts with a different block value |
| any other unknown | rejected | error NAMES the parameter (+ pointer to alternative when one exists) |

Same-value duplication (string and block agree) is NOT an error —
mirrors the existing sslmode consistency rule.

## CursorConfig (`crates/rdlt-postgres/src/source/config.rs`)

| Field | Type | Default | Rules |
|---|---|---|---|
| `column` | String | — | unchanged |
| `initial_value` | Option<String> | None | unchanged |
| `boundary` | closed \| open | closed | unchanged |
| `direction` | max \| min | max | unchanged |
| `end_value` | Option<String> | None | unchanged |
| `end_bound` *(new)* | exclusive \| inclusive | exclusive | read filter only; direction-aware (`<=`/`>=`) |
| `nulls` | exclude \| include \| **error** *(new variant)* | exclude | `error`: first NULL cursor value = typed Fatal naming stream+column, raised in the tracker |
| `lag` *(new)* | Option<Lag> | None | see Lag |

## Lag (new vocabulary type)

String form, cursor-native:
- time cursors (`timestamp`, `timestamptz`): duration — `"90s"`,
  `"5m"`, `"2h"`, `"1d"` → rendered `- interval 'N seconds'`
- `date` cursors: whole days — `"3d"` (sub-day durations rejected)
- integer/decimal cursors: plain magnitude — `"1000"`, `"0.5"`
- text/uuid cursors: rejected at open (no defined subtraction)

Open-time validation (all typed, naming column and type):
- requires `boundary: closed` (lag + open = contradiction)
- requires the stream to have a primary key (reflected or declared) —
  the keyed-Merge dedup path must exist (research R4)
- unit form must match the cursor family

Semantics: `effective_lower = saved_watermark - lag` (SQL-side
arithmetic); the SAVED watermark advances normally and is never
lowered; run 1 (no watermark) ignores lag.

## Discovery filter (`reflect.rs`)

`NOT c.relispartition` → `NOT EXISTS (SELECT 1 FROM pg_inherits i
WHERE i.inhrelid = c.oid) OR c.relname = ANY($listed)` — one predicate
excludes declarative partitions AND classic INHERITS children;
explicitly listed names override (new capability, applies to both
kinds; 005 conformance pin updated in the same change). Foreign tables
(`relkind 'f'`) remain undiscovered — documented.

## Connection identity

`application_name` defaults to `rdlt` (set post-parse when absent);
user values — conn-string or future config — always win. Applies to
source and destination through the shared connect path.

## Schema integration

Every new field/variant rides the 006 schemars derives; `Lag` gets a
manual `JsonSchema` (string + pattern) mirroring its `FromStr`, the
HintType precedent. The 006 `config_schema.rs` round-trip suites gain
examples per field (schema-valid ⇒ parses; unknown fields fail both).
