# Feature Specification: Postgres Source Completion (pre-CDC)

**Feature Branch**: `007-postgres-source-completion`

**Created**: 2026-07-20

**Status**: Draft

**Input**: User description: "Postgres source completion (pre-CDC): close the remaining non-CDC gaps found in the 006 completeness audit — mutual TLS (client certificate + key in the TLS policy, both postgres connectors, wired through the shared rustls path with matrix tests); cursor lag/attribution window (re-scan a configured window behind the resumed watermark so late-arriving rows are picked up, overlap absorbed by the existing closed-boundary dedup); libpq connection-string portability (accept sslrootcert= by translating it into the TLS policy root, typed pointers to the tls block for sslcert=/sslkey= until mTLS config lands, never a bare parse error on a working libpq URL); on_cursor_value_missing raise policy (NULL cursor row = typed error, alongside existing exclude/include); closed end_value boundary option (inclusive upper bound, parity with dlt range_end); default application_name=rdlt for observability in pg_stat_activity; exclude classic INHERITS children from discovery the same way declarative partitions are excluded (no double-read); document foreign-table (relkind f) non-discovery. Explicitly OUT: CDC/logical replication (own future feature), cross-table snapshot export, parallel stream reads."

## Context

The feature-006 completeness audit (recorded in the 006 parity table and the
post-close review) found that the Postgres source is at full parity with dlt's
`sql_database` on every gated capability, but left a short list of non-CDC
gaps that stand between "parity" and "honestly complete". This feature closes
all of them in one small story. CDC / logical replication is deliberately NOT
here — it is protocol-design work and gets its own future feature.

## User Scenarios & Testing *(mandatory)*

### User Story 1 - Connect to certificate-authenticated databases (mutual TLS) (Priority: P1)

A data engineer whose organization requires client-certificate authentication
(common for managed and enterprise Postgres) configures the connection with
their client certificate and private key alongside the existing trust
settings. Both the Postgres source AND the Postgres destination present the
certificate during the TLS handshake and the server admits them; without the
certificate the same server refuses the connection.

**Why this priority**: This is the only item in the set that BLOCKS a class of
deployments outright — a cert-required server cannot be used at all today.
Everything else in this feature improves fidelity; this one gates access.

**Independent Test**: Against a database configured to require client
certificates: connection succeeds with a valid certificate+key, fails with a
typed error without one, fails with a typed error with a wrong-CA client
certificate — for source and destination alike.

**Acceptance Scenarios**:

1. **Given** a server that requires client certificates, **When** the user
   supplies a valid client certificate and key in the TLS settings, **Then**
   reads and writes succeed over the encrypted, mutually-authenticated
   connection.
2. **Given** the same server, **When** no client certificate is configured,
   **Then** the connection fails with a typed error that names the missing
   client credential (not a generic handshake failure).
3. **Given** a client certificate the server does not trust, **When**
   connecting, **Then** the failure is typed and distinguishes "server
   rejected our certificate" from server-verification failures.
4. **Given** a client key that does not match the certificate, or an
   unreadable/unparseable certificate or key file, **Then** the failure is a
   typed configuration error naming the offending input BEFORE any
   connection attempt.

---

### User Story 2 - Late-arriving rows are captured (cursor lag) (Priority: P2)

A data engineer syncing a table on an `updated_at` cursor knows that
long-running transactions commit rows whose cursor value is OLDER than rows
already synced (the classic attribution-window problem). They configure a lag
window on the cursor (e.g. "5 minutes"). Every incremental run re-scans that
window behind the saved watermark, so late-committed rows are picked up on the
next run; rows already loaded in the overlap are not duplicated.

**Why this priority**: Without lag, late commits below the watermark are
permanently invisible — a silent-data-loss class for the most common cursor
choice (`updated_at`). This is the highest-value correctness improvement in
the set for existing users.

**Independent Test**: Load a table incrementally; insert a row with a cursor
value BEHIND the current watermark but inside the lag window; the next run
loads exactly that row and nothing else, and re-runs stay stable (no
duplicates, count converges).

**Acceptance Scenarios**:

1. **Given** an incremental stream with a configured lag and a saved
   watermark, **When** a row commits with a cursor value inside the lag
   window (behind the watermark), **Then** the next run loads it exactly
   once.
2. **Given** the same setup, **When** a row commits with a cursor value
   OLDER than the lag window, **Then** it is not loaded (documented,
   unchanged behavior — the window bounds the guarantee).
3. **Given** rows already loaded that fall inside the lag window, **When**
   subsequent runs re-scan the window, **Then** they are not duplicated
   (re-runs converge; totals match the source).
4. **Given** a lag configured on a cursor type where subtracting the window
   is meaningless or unsupported, **Then** configuration fails with a typed
   error at open, naming the cursor column and its type.
5. **Given** a timestamp cursor with a lag, **When** the watermark minus the
   lag would precede the initial value or the epoch of the type, **Then**
   the lower bound clamps safely and the run succeeds.

---

### User Story 3 - Existing connection strings just work (libpq portability) (Priority: P3)

A data engineer pastes their organization's existing libpq connection URL —
which contains `sslrootcert=/path/ca.pem` — into the rdlt config. Instead of
a parse error, the trust root is honored exactly as if it had been set in the
TLS settings block. URLs carrying `sslcert=`/`sslkey=` are accepted and
translated to the client-certificate settings from User Story 1. A
contradiction between URL parameters and the TLS block remains a typed
configuration error naming both sides.

**Why this priority**: First-contact experience — today a working production
URL is rejected with a bare parse error, which reads as "rdlt can't talk to
my database". No data-correctness impact, but high adoption friction.

**Independent Test**: A conn string with `sslrootcert=` (and optionally
`sslcert=`/`sslkey=`) connects successfully against a matching TLS-enabled
server with no TLS block configured; contradictory settings fail typed.

**Acceptance Scenarios**:

1. **Given** a conn string containing `sslrootcert=`, **When** the source or
   destination opens, **Then** the file is used as the trust root and
   verification behaves per the sslmode in the same string.
2. **Given** a conn string containing `sslcert=` and `sslkey=`, **When**
   opening, **Then** the client certificate is presented (equivalent to
   configuring User Story 1 via the TLS block).
3. **Given** both a conn-string TLS parameter and a conflicting TLS-block
   setting, **Then** the open fails with a typed error naming the parameter
   and the block field that disagree.
4. **Given** a conn string with any other unrecognized parameter, **Then**
   the error names the parameter (never a bare "invalid connection string").

---

### User Story 4 - Cursor edge policies and range parity (Priority: P4)

A data engineer who treats NULL cursor values as a data bug configures the
NULL-cursor policy to fail the run with a typed error naming the table and
column (alongside the existing "exclude" and "include" behaviors). Another
engineer backfilling a fixed window configures an INCLUSIVE upper bound so
that the window `[start, end]` is expressible directly.

**Why this priority**: Small parity items; each is one decision surfaced to
config with existing machinery underneath.

**Independent Test**: A table containing a NULL cursor row fails typed under
the new policy and loads under the old policies; a backfill with an inclusive
end bound loads boundary rows exactly once.

**Acceptance Scenarios**:

1. **Given** the NULL-cursor policy set to error, **When** a run encounters
   a NULL cursor value, **Then** the run fails with a typed error naming the
   stream and column, and no partial duplicate state results on retry.
2. **Given** an inclusive end bound, **When** rows exist exactly AT the
   bound, **Then** they load; rows beyond it do not; re-runs stay stable.
3. **Given** existing configs using the current policies and exclusive end
   bounds, **Then** their behavior is unchanged (defaults untouched).

---

### User Story 5 - Operational visibility and discovery correctness (Priority: P5)

A DBA inspecting the database's live-session view can identify rdlt
connections by name, from both connectors, without any configuration. A data
engineer with a legacy inheritance hierarchy (classic `INHERITS`, not
declarative partitioning) syncs the schema and gets each row exactly once —
child tables are not double-read through their parent. The documentation
states plainly that foreign tables are not discovered.

**Why this priority**: Small operational trust items — cheap, visible,
zero-config.

**Independent Test**: The session view shows the connector's name during a
sync; a parent-with-INHERITS-children schema loads with exact row counts (no
duplicates); the docs contain the foreign-table statement.

**Acceptance Scenarios**:

1. **Given** any sync, **When** a DBA queries the live-session view, **Then**
   rdlt connections carry an identifying application name (user-overridable
   via the standard connection parameter).
2. **Given** a schema with a classic INHERITS hierarchy, **When** discovery
   runs, **Then** child tables are excluded the same way declarative
   partitions are, the parent read covers all rows, and totals match the
   source exactly.
3. **Given** a child table explicitly listed in the tables config, **Then**
   it is read as its own stream — an override 007 INTRODUCES for both
   inheritance children and declarative partitions (research R7: no such
   rule existed before; the 005 partition conformance pin is updated).
4. **Given** a schema containing foreign tables, **Then** discovery skips
   them and the user documentation says so.

---

### Edge Cases

- Inline PEM vs file path for client certificate and key (same dual form the
  trust root already supports); key formats in common PEM encodings.
- Client certificate supplied without a key (or vice versa) — typed config
  error at validation, not at connect.
- mTLS combined with every sslmode: meaningful only when TLS is in play;
  configuring a client certificate with TLS disabled is a typed
  contradiction.
- Lag on a numeric/integer cursor (units are cursor-native), on a text
  cursor (unsupported — typed error), on a date cursor (whole days).
- Lag configured together with `initial_value`: first run is unaffected
  (no watermark yet); the clamp applies from the second run on.
- Lag window larger than the entire table's cursor range — degrades to a
  full re-scan with dedup, correct though slow (documented).
- A conn string whose `sslrootcert=` file does not exist — typed error
  naming the path, consistent with the TLS block's root errors.
- INHERITS child that is ALSO a declarative partition parent (mixed
  hierarchies) — each row still loads exactly once.
- Application name containing characters requiring escaping — passed through
  safely.

## Requirements *(mandatory)*

### Functional Requirements

- **FR-001**: The TLS settings for BOTH Postgres connectors MUST accept a
  client certificate and private key (file path or inline PEM, matching the
  existing trust-root forms), presented during the TLS handshake for every
  mode where TLS is active.
- **FR-002**: Client-credential errors MUST be typed and early: missing
  counterpart (cert without key or key without cert), unreadable or
  unparseable material, and certificate-with-TLS-disabled are configuration
  errors at open; server rejection of the client certificate is a connect
  failure DISTINGUISHED from server-verification failures.
- **FR-003**: Incremental cursor configuration MUST accept an optional lag
  (attribution window) expressed in cursor-native units (duration for
  time-typed cursors, magnitude for numeric cursors); each resumed run
  lowers the effective lower bound by the lag while the SAVED watermark
  continues to advance normally (never lowered by lag).
- **FR-004** *(amended per research R4)*: Lag requires a closed lower
  boundary AND a stream with a primary key (reflected or declared) — both
  validated typed at open (open boundary + lag is a contradiction; no key
  means no dedup path exists). Destination totals MUST equal the source
  exactly under keyed Merge write mode (the conformance mode for lag);
  under Append, rows inside the window re-deliver each run — a documented
  at-least-once property of the window, never silent.
- **FR-005**: Lag on cursor types where the subtraction is undefined (e.g.
  text) MUST fail typed at open, naming the column and type; boundary
  clamping (before initial_value or type minimum) MUST be safe.
- **FR-006**: Connection strings containing `sslrootcert=` MUST be accepted:
  the value becomes the trust root exactly as if configured in the TLS
  block; `sslcert=`/`sslkey=` MUST map to the client credentials of FR-001.
  Contradictions between conn-string TLS parameters and the TLS block MUST
  fail typed, naming both sides — consistent with the existing
  sslmode-vs-block consistency rule.
- **FR-007**: No syntactically valid libpq connection string may fail with a
  bare parse error: unsupported parameters MUST be rejected with an error
  naming the parameter (and pointing at the supported alternative when one
  exists).
- **FR-008**: The NULL-cursor policy MUST gain an "error" variant: a NULL
  cursor value fails the run with a typed error naming stream and column;
  existing "exclude" (default) and "include" behaviors and defaults are
  unchanged.
- **FR-009**: The incremental end bound MUST support inclusive semantics as
  a config option; the current exclusive behavior remains the default;
  boundary rows load exactly once under either setting.
- **FR-010**: Both connectors MUST set an identifying application name on
  their connections by default, overridable through the standard connection
  parameter; it MUST be visible in the database's live-session view.
- **FR-011**: Schema discovery MUST exclude classic INHERITS children by
  default (rows arrive via the parent read), exactly mirroring the
  declarative-partition rule, including the explicit-listing override; row
  totals for inheritance hierarchies MUST match the source exactly.
- **FR-012**: User documentation MUST state that foreign tables are not
  discovered, alongside the existing discovery-scope notes.
- **FR-013**: All new configuration surfaces MUST appear in the generated
  config schema (the 006 schemas-from-structs guarantee holds: schema-valid
  implies parseable, unknown fields fail both).

### Key Entities

- **Client credential**: certificate + private key pair (each a path or
  inline PEM) attached to the existing TLS policy; participates in
  validation before any connection.
- **Lag window**: optional per-cursor magnitude; interacts with watermark
  (read-side lower bound only), boundary mode (closed required), and
  initial/end values (clamping).
- **Connection-string TLS parameters**: `sslrootcert`, `sslcert`, `sslkey`
  — translated into the TLS policy with contradiction detection.
- **NULL-cursor policy**: now three-valued (exclude | include | error).
- **End-bound mode**: exclusive (default) | inclusive.

## Success Criteria *(mandatory)*

### Measurable Outcomes

- **SC-001**: Against a client-certificate-required database, both
  connectors complete a full sync; the without-certificate and wrong-CA
  attempts each fail with their distinguished typed error — demonstrated by
  an automated matrix covering source and destination.
- **SC-002** *(amended per research R4)*: In an automated late-arrival
  scenario (row committed behind the watermark, inside the lag), the row
  loads on the next run and destination totals remain exactly equal to the
  source across three consecutive re-runs (window rows re-deliver and merge
  idempotently — no duplicates, newest values win).
- **SC-003**: A production-shaped libpq URL carrying `sslrootcert=` (and one
  carrying `sslcert=`/`sslkey=`) connects with an empty TLS block; every
  rejected parameter error names the parameter. Zero bare parse errors
  across the test corpus of real-world URL shapes.
- **SC-004**: The NULL-cursor error policy and inclusive end bound each have
  conformance coverage proving exact-once boundary behavior and unchanged
  defaults for existing configs.
- **SC-005**: An INHERITS hierarchy (including a mixed
  inheritance+partition case) syncs with row totals exactly equal to the
  source, verified against a real database.
- **SC-006**: rdlt connections are identifiable by name in the live-session
  view during a sync from either connector, with zero configuration.
- **SC-007**: Generated config schemas round-trip every new field (examples
  validate; schema-valid parses; unknown fields fail) — extending the 006
  schema tests, not duplicating them.
- **SC-008**: The full existing gate stays green (tests, crash sweeps, perf
  gate within tolerance) — this feature adds no measurable cost to the hot
  paths (lag adds work only when configured).

## Assumptions

- The lag window is fixed per cursor config (a constant magnitude), not
  adaptive; adaptive/statistical windows are out of scope.
- Lag semantics for time cursors use a duration string form consistent with
  the existing typed-literal conventions of cursor config; numeric cursors
  interpret lag as a plain magnitude in the cursor's own units.
- mTLS scope is client authentication to the server only; certificate
  revocation checking (CRL/OCSP) is out of scope, consistent with the 006
  TLS policy scope.
- Encrypted (passphrase-protected) private keys are out of scope for this
  feature; a typed error names the limitation.
- `sslpassword=`, `gssencmode=`, `krbsrvname=` and other libpq parameters
  outside the TLS trio remain unsupported — but fail with the FR-007 named
  error, never a bare parse failure.
- The INHERITS exclusion follows the exact precedent of the 005 partition
  rule (default-exclude, explicit-listing override) — no new configuration
  surface.
- Explicitly OUT of scope (future features): CDC / logical replication,
  cross-table snapshot consistency (exported snapshots), parallel stream
  reads.
