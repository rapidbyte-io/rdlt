# Feature Specification: Snowflake Internal-Stage Ingestion, and the Retirement of Two Paths

**Feature Branch**: `023-snowflake-put`

**Created**: 2026-07-29

**Status**: Draft

**Builds on**: 022 (Snowflake destination — COMPLETE, 43/43, merged at `1ef4860b`)

## Why this exists

Feature 022 shipped the Snowflake destination with **two** ways to get rows in:
batched `INSERT` statements (the default, needing no infrastructure) and a bulk
path that wrote files to a bucket the user supplied. Neither is the way the
service itself recommends. Both existed because the recommended way — uploading
a file to storage Snowflake provides — was **unreachable** through the library
the connector is built on, which 022 verified in source and recorded as its one
substantive gap against the comparable tool.

That constraint is now lifted. With the upload reachable, the bulk path's whole
reason for existing disappears — it was a substitute for storage the user had to
supply themselves — and the statement path loses the argument that justified it,
because Snowflake-provided storage needs no user infrastructure either.

So this feature is not "add a third option". It is: **make the recommended way
the only way, and delete the two workarounds** — the configuration they
required, the credentials they handled, and the test surface they carried.

A user's Snowflake destination stops having an ingestion decision in it at all.

## User Scenarios & Testing *(mandatory)*

### User Story 1 - Loads land with nothing to configure (Priority: P1) 🎯 MVP

A user points a pipeline at Snowflake with an account, a login, a database and a
schema. Rows land. There is no bucket to create, no storage credential to issue,
no ingestion mode to choose, and no size threshold to reason about.

**Why this priority**: It is the whole feature. Everything else is subtraction
or verification.

**Independent test**: Run a load with a configuration carrying no storage
settings of any kind. Exact totals land; a re-run publishes nothing; the values
that survived in 022 still survive.

**Acceptance Scenarios**:

1. **Given** a configuration naming only account, user, auth, database and
   schema, **When** a load of N rows runs, **Then** exactly N rows land in the
   target and the load reports N.
2. **Given** a load that already committed, **When** the same pipeline runs
   again against the same source, **Then** no row is published a second time.
3. **Given** a load where one part fails to upload while others succeed,
   **When** the unit would otherwise commit, **Then** the load fails with a
   typed error naming the failed part, and nothing is published.
4. **Given** a table of awkward values (quotes, backslashes, multi-byte text,
   NULLs), **When** the load completes, **Then** every value reads back
   identical to what was sent.
5. **Given** a machine whose temporary storage is full or read-only, **When** a
   load runs, **Then** it fails with a typed error naming that condition rather
   than a generic I/O failure.

---

### User Story 2 - The configuration surface shrinks, visibly (Priority: P1)

A user upgrading finds that the storage block their document used to carry is
gone. They are told so, by name, when the document is loaded — not by having it
silently ignored while their data quietly takes a different route.

**Why this priority**: Ignoring a removed field is how a user comes to believe
something untrue about where their data went. The refusal is the feature, not a
side effect of it.

**Independent test**: Feed a document containing the removed storage block. It
is refused with an error naming the field and stating it no longer exists.

**Acceptance Scenarios**:

1. **Given** a document carrying the removed storage block, **When** it is
   loaded, **Then** it is refused with a message naming that block.
2. **Given** a document carrying no storage block, **When** it is loaded,
   **Then** it is accepted and the load runs.
3. **Given** the generated configuration schema, **When** it is inspected,
   **Then** it contains no storage vocabulary at all.

---

### User Story 3 - Every authentication method is proven, not merely implemented (Priority: P2)

An operator choosing how a pipeline authenticates can see that each supported
method has actually been exercised against a real account, rather than written
and assumed to work.

**Why this priority**: Two of the four methods shipped implemented and
unit-tested, with their live checks written and skipping. Closing them is cheap,
and an unexercised authentication path fails on the day it is first needed.

**Independent test**: With credentials present for all four methods, all four
live checks execute and pass. With any one absent, that one alone skips, says
so, and the rest still run.

**Acceptance Scenarios**:

1. **Given** credentials for each of the four unattended methods, **When** the
   suite runs, **Then** one load per method completes against the real account.
2. **Given** a deliberately corrupted secret for any method, **When** a load is
   attempted, **Then** it fails with an error naming the account and login and
   containing no secret material.
3. **Given** no credential for one method, **When** the suite runs, **Then**
   that method's check announces its skip and the suite stays green.

---

### User Story 4 - The record says what is true (Priority: P2)

Someone reading the project's own documents can tell what the connector does,
what it requires of their network, and which recorded commitments have been
discharged — without discovering that a claim was quietly outdated.

**Why this priority**: Several recorded statements become false the moment this
ships, and one recorded claim was never true. A project whose discipline is
"every disposition cited" cannot let either stand.

**Independent test**: Every claim about ingestion in the shipped documents is
checked against shipped behaviour and matches.

**Acceptance Scenarios**:

1. **Given** the parity record's claim that the connector's default path needs
   no infrastructure, **When** the single path ships, **Then** that claim is
   rewritten to the honest distinction: no bucket is needed, but reaching cloud
   storage is.
2. **Given** the contract clauses describing the library boundary and the bulk
   path, **When** this feature ships, **Then** both are amended explicitly
   rather than by implication.
3. **Given** a recorded assertion that an upstream issue was filed, **When** the
   record is checked, **Then** either the issue is cited or the record states it
   was never filed.
4. **Given** a user whose network reaches only the account host, **When** they
   read the documentation, **Then** they learn the storage requirement there
   rather than from a failed load.

---

### User Story 5 - The path that ships is the one that was measured (Priority: P3)

The claim that the single path supersedes both old ones is backed by numbers
taken on the real service, not by argument.

**Why this priority**: The old paths carry recorded measurements. Replacing them
on assertion would be a regression in method, in a project whose previous
feature overturned its own expectation by measuring.

**Independent test**: A recorded session compares the single path against the
previously recorded figures on comparably shaped data.

**Acceptance Scenarios**:

1. **Given** the previously recorded figures, **When** the single path is
   measured on comparably shaped data, **Then** the comparison is recorded with
   its dataset identity and configuration.
2. **Given** a measurement that does not favour the single path, **When** it is
   recorded, **Then** it is recorded as it came out.
3. **Given** the benchmark governance, **When** these numbers are recorded,
   **Then** they gate nothing.

### Edge Cases

- A part larger than the transfer's per-file ceiling: refused with a typed error
  naming the limit, never silently truncated.
- Two loads of one pipeline running close together: neither removes the other's
  in-flight local files or staged objects.
- A crash between building a local part and uploading it; between uploading and
  loading; between loading and the load becoming durable. Each converges to
  exactly-once totals on recovery, including when the crash recurs during
  recovery.
- A load that delivers no rows: commits its position, uploads nothing.
- Temporary files left by a process that died: reclaimed by a later run of the
  same pipeline without touching a concurrent one's.
- An upload reporting success overall while individual parts failed: treated as
  failure of the whole unit.
- A network permitting the account host but not cloud storage: fails
  diagnosably, and the requirement is documented in advance.

## Requirements *(mandatory)*

### Functional Requirements

- **FR-001**: The connector MUST load rows through storage the service itself
  provides, requiring no storage account, bucket, or storage credential from the
  user.
- **FR-002**: The connector MUST offer exactly one ingestion mechanism. No
  configuration field, size threshold, or fallback may select among mechanisms.
- **FR-003**: The connector MUST verify per-part upload outcomes individually
  and MUST fail the unit when any part did not upload, including where the
  underlying transfer reports overall success.
- **FR-004**: The connector MUST verify that the number of rows the service
  loaded equals the number staged, and MUST abandon the unit on any difference.
- **FR-005**: Staging MUST remain inside the atomic unit, so a failed load
  publishes nothing.
- **FR-006**: The connector MUST refuse, by name, a configuration carrying the
  removed storage vocabulary; it MUST NOT accept and ignore it.
- **FR-007**: The superseded mechanisms and every artefact existing only to
  serve them — configuration types, credential handling, encoding routines,
  tuning constants, test suites, and dependencies — MUST be removed in the same
  change that supersedes them. No aliases, shims, or deprecated re-exports.
- **FR-008**: Infrastructure shared with other connectors MUST NOT be removed
  merely because this connector stopped using it.
- **FR-009**: Local temporary artefacts MUST be scoped so concurrent loads of
  one pipeline cannot delete one another's, and MUST be reclaimed after a
  process dies without removing a live load's.
- **FR-010**: Local storage failures MUST classify as typed errors naming the
  condition.
- **FR-011**: Each service behaviour the design depends on MUST be pinned by a
  check that fails, naming the assumption, if the service changes: that
  uploading does not end an open transaction; that already-compressed payloads
  survive the transfer byte-for-byte; that the transfer does not re-compress
  them.
- **FR-012**: Every crash point MUST converge to exactly-once totals under
  repeated failure, including failure during recovery, and each point MUST be
  proven to have fired.
- **FR-013**: All four unattended authentication methods MUST have a live check
  that runs when its own credential is present and announces its skip when not.
- **FR-014**: No credential may appear in rendered output on any channel; a
  failed connection MUST name the account and login and no secret.
- **FR-015**: Recorded claims invalidated by this feature MUST be corrected
  within it, and every disposition MUST cite its evidence.
- **FR-016**: The network requirement the single path imposes MUST be stated in
  user-facing documentation as a prerequisite.
- **FR-017**: The dependency arrangement MUST be recorded together with its
  consequences for distribution, and a mechanical check MUST fail if such an
  arrangement is introduced without being noticed.
- **FR-018**: Behaviour outside ingestion — merge strategies, exactly-once,
  identifiers, type mapping, schema evolution — MUST be unchanged, and the other
  destinations' emitted statements MUST be byte-identical before and after.

### Key Entities

- **Ingestion unit**: one atomic transaction carrying staged rows, the
  idempotence receipt, and the pipeline's position; commits together or not at
  all.
- **Part**: one batch of rows encoded for transfer, carrying the count it holds
  so what arrived can be checked against what was sent.
- **Staging area**: per-pipeline storage the service provides, into which parts
  are uploaded and from which they are loaded, cleared when the unit commits.
- **Local working area**: per-load temporary space where parts are built before
  transfer, owned by one load and reclaimed deterministically.

## Success Criteria *(mandatory)*

### Measurable Outcomes

- **SC-001**: A user can load into Snowflake with a configuration containing no
  storage settings of any kind.
- **SC-002**: The connector's configuration vocabulary contains zero storage
  fields, verified against the generated schema.
- **SC-003**: Exactly one ingestion mechanism exists in the shipped connector,
  verifiable by the absence of any branch selecting among mechanisms.
- **SC-004**: For every crash point, under every failure action, a load that
  crashes, crashes again during recovery, then runs clean lands exact totals
  with no duplicate publish — and every point is proven to have fired.
- **SC-005**: A partial upload failure fails the load, demonstrated against a
  case the underlying library reports as success.
- **SC-006**: All four unattended authentication methods complete a real load,
  and each skips independently and visibly when its credential is absent.
- **SC-007**: The removed mechanisms leave no residue: no configuration type,
  encoding routine, tuning constant, test, or dependency that existed only for
  them remains, verified by mechanical search.
- **SC-008**: The full local verification suite passes twice consecutively, and
  the other SQL destinations' emitted statements are byte-identical to before.
- **SC-009**: The ingestion measurement is recorded against the previously
  recorded figures, with dataset identity and configuration, and gates nothing.
- **SC-010**: No account identifier, login name, or secret material appears in
  any committed file, verified by mechanical search at the final commit.
- **SC-011**: Every recorded claim about ingestion in the project's documents
  matches shipped behaviour, and every unperformed verification carries its
  reason.
- **SC-012**: The crash sweep's cell count and wall-clock time are lower than
  before, recorded as numbers.

## Assumptions

- The upload capability is consumed from a fork of the existing library rather
  than a rewrite, keeping the connector's single-library-boundary rule intact.
- The fork is pinned to an exact revision, not tracked as a moving branch, so
  builds stay reproducible.
- Consuming a fork by revision prevents distribution of the affected packages
  through the public registry until the change is accepted upstream or the fork
  is published under its own name. This is accepted deliberately for the
  validation period; the exit is recorded, and the future publishing work
  inherits the constraint knowingly.
- The qual account remains the live verification target, and credentials for the
  two currently unexercised authentication methods will be provisioned on it.
- Measurements against a hosted service carry network variance; they inform
  decisions and documentation but gate nothing.
- Nested and list-typed columns continue to be lowered before they arrive;
  carrying them natively is deliberately left to a later feature.
- The owner has weighed the loss of a statement-only ingestion path against the
  simplification and accepted it, on the understanding that the comparable tool
  cannot serve that environment either.

## Out of Scope

- A Snowflake source, change capture, or streams and tasks — destination only.
- The service's streaming-ingest channel APIs.
- Extracting the remaining shared unit-execution choreography into the shared
  core: its recorded trigger is unmet.
- External stages on any cloud, removed with the path they served. Accounts
  backed by other clouds are served transparently by the single path.
- Carrying nested or list-typed columns natively.
- A hermetic emulator: no fidelity-compatible one exists.
- Verification specific to private-connectivity deployments: no such environment
  is available.
- Repair of the hosted continuous-integration environment; its blocker stands
  and dependent checks are recorded unperformed, never claimed green.
