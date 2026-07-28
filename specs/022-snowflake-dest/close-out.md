# Close-out: Snowflake Destination Connector (022)

**Status**: IN PROGRESS. Every claim here cites the evidence that produced
it; no disposition is silent. Verifications this machine cannot perform are
recorded UNPERFORMED with the reason, never as green.

## Contract matrix (SD1–SD8)

| clause | status | evidence |
|---|---|---|
| SD1 — one library, one boundary | OPEN | |
| SD2 — full unattended auth vocabulary, every secret guarded | ON TRACK | T001 A3 (PAT rides the password channel, oauth rejects it), A4 (invalid secret → `kind = Auth`, secret provably absent from the rendered error) |
| SD3 — the atomic unit is pure DML, DDL strictly outside | ON TRACK | T001 A2: `CURRENT_SESSION()` identical across `query()` calls; BEGIN/INSERT/ROLLBACK across three separate calls yields count 0; the same sequence with a CREATE in the middle yields count 1 — DDL auto-commit proven a second time, on a second transport |
| SD4 — merge without enforced constraints | OPEN | |
| SD5 — identifier policy is total | ON TRACK | T001 A2 false alarm: the qual database has no `PUBLIC` schema, so session-context reliance failed and fully-qualified three-part names worked — D2's decision validated by accident |
| SD6 — ingestion verified on every shipped path | ON TRACK | T001 A5: parquet written by the workspace's OWN arrow writer (lowercase columns, embedded NULL) loaded into a quoted-upper table via `MATCH_BY_COLUMN_NAME=CASE_INSENSITIVE`, 3/3 rows, `rows_loaded` present for SD6's verification |
| SD7 — crash discipline + statement economy | OPEN | |
| SD8 — house verification standard | OPEN | |

## Story matrix

| story | status | evidence |
|---|---|---|
| US1 — exactly-once loads, one document | NOT STARTED | |
| US2 — full merge parity | NOT STARTED | |
| US3 — frugal with round trips | NOT STARTED | |
| US4 — verified like the other connectors | NOT STARTED | |
| US5 — recorded performance standing | NOT STARTED | |

## Task ledger

| task | disposition | note |
|---|---|---|
| T001 environment gate | **DONE** | six probes; two plan corrections (A1 reqwest cost, A7 fakesnow); research.md addenda A1–A8 |
| T002 close-out skeleton | **DONE** | this file |
| T003 ensure extraction | **DONE (narrowed)** | D12 + D13 — `sqlcore::ensure` plans decisions, both destinations lower them; 18 ensure pins written first and green after; no DdlDialect trait; golden suites byte-identical; 816/816 |
| T004 session extraction | **NARROWED** (in progress) | D12 — six pure items, not a skeleton; a shared async skeleton is type-system-impossible here |
| T005–T043 | OPEN | |

## Deviations and corrections

### C-01 (T001) — the recorded reqwest cost was wrong, and smaller

Research D1 stated the adopted crate brings "a second reqwest major
wherever the `iceberg` feature is off". The lock says the workspace gains
**no reqwest at all**: 0.13.4 was already present via
opendal ← iceberg-storage-opendal ← the iceberg destination. Total lock
impact is +18 crates, all the RustCrypto stack the encrypted-PKCS#8 key
path needs. The narrower true statement — a `snowflake`-on / `iceberg`-off
build gets the 0.13 line from snowflake — replaces it in D1 and in the
plan's dependency line.

### C-02 (T001) — fakesnow rejected on the envelope, not the semantics

The plan carried fakesnow as a possible hermetic leg pending a fidelity
probe. Probed: fakesnow hardcodes `queryResultFormat: "arrow"` on every
success response, and the adopted crate is JSON-only **by design** — it
rejects other formats and ships a unit test asserting exactly
`"unsupported result format: arrow"`. Neither side is configurable from
here. Its SQL semantics were fine (DDL, DML, BEGIN/ROLLBACK, and
`MERGE … QUALIFY` all executed), so the rejection is narrow and the
re-trigger is precise: fakesnow honouring a JSON result format, or the
crate gaining arrow support. No hermetic protocol leg is adopted; the mock
executor seam covers protocol-shaped tests.

### C-03 (T003/T004) — both extractions narrowed, with the reason and the trigger

The tasks committed to TAKING both fired sqlcore triggers. A four-way
structural survey (`extraction-plan.md`) found that a shared async execute
skeleton is not writable: the duckdb session commits inside a synchronous
closure holding a `MutexGuard<Connection>` while postgres is async over an
owned client, and reconciling them needs a dependency the shared core's
contract forbids or a redesign of duckdb's concurrency. The narrowing was
pre-authorized by the task text ("extract only the shared shapes and
re-record the remainder with a named trigger — never a silent partial") and
is exercised here rather than forced. What IS extracted is chosen for value,
not volume: `ReplayDisposition` alone converts an inverted, comment-only
invariant into a typed decision the third destination inherits by
construction. Full reasoning and the ordered edit plan: D12 +
`extraction-plan.md`. Trigger for the remainder is recorded in D12.

### C-04 (T003/T004 survey) — a Snowflake exactly-once trap caught before it was written

`TRUNCATE TABLE` is DDL on Snowflake and auto-commits the open transaction —
the exact hazard the pure-DML unit exists to prevent — and the Replace clear
runs inside the unit. `SnowflakeDialect::clear_table` must therefore emit
`DELETE FROM`, as the duckdb dialect already does. Found by the extraction
survey, not by a failing test, and recorded before the implementation could
inherit the bug.

### C-05 (T003 step 1) — the podman shim fix un-skipped a test that had been green by absence

Repairing the host-exec shim (the session bridge had assumed distrobox
inside a toolbox) made the iceberg Polaris fixture reachable for the first
time in this session — and one nested-types cell immediately FAILED with
`unknown resume cursor Cursor(Number(1))`.

**Not a regression, and proven so**: the identical failure reproduces on the
pre-change tree in a scratch worktree, so the postgres refactor is innocent.
The cell built a fresh source per attempt carrying only the NEWEST batch, so
the second load could not resolve the cursor its own first load had
committed. The harness refuses that rather than silently restarting from
zero — correctly, per the recorded rule that test sources must honour resume
cursors.

**The file's own doc comment warns about exactly this hazard** ("a
container-gated test skips when no runtime is present, and a skipping test
is green") and then fell into it: the defect was invisible for as long as
the fixture could not start. Fixed by giving the stream every batch it has
ever produced, which is what a real source has and what makes the cursor
resolvable. Iceberg suite now 56/56 with **0 skipped**; workspace 800/800.

Recorded here rather than in 016's close-out because it was found by this
feature's environment work; the fix is a test correction, not a behaviour
change.

### D13 (T003) — how the extraction was made safe, and what it actually shared

No test pinned ensure DDL text before this feature, so the extraction could
not have been verified after the fact. The order was therefore forced and is
worth recording as method: **hoist rendering out of execution WITHIN each
destination first, pin the rendered statement vectors container-free, and
only then move the decision logic across a crate boundary.** Red-before-green
applied to a refactor.

The 18 pins were each demonstrated able to FAIL before being trusted —
`CACHE 32` → `64`, reordering a widen before its `ADD COLUMN`, removing
duckdb's legacy index drop, and adding `NOT NULL` to duckdb's validity column
(i.e. "harmonizing" it with postgres, which DuckDB rejects outright). Two of
them caught defects in themselves while being written: a wrong assumption
that the DEFAULT merge strategy is upsert (it is delete-insert, whose index
is supporting, not arbiter), and a filter matching the scd2 INDEX because it
names a validity column.

**What the extraction actually shared**, after the survey ruled out sharing
SQL: leg selection, the within-session widen predicate, scd2 validity
ordering, index planning, and `TableFacts::of` — both destinations derived
`has_identity` and `is_child` with identical code.

**The `stages` rule is the item that justifies the task.** Ensure and commit
must agree on whether a table round-trips through a stage: a table the commit
plan publishes from a stage but ensure never created one for fails at write
time, and the reverse builds a stage that is written, truncated and never
read. That rule was derived independently at each site and free to drift; it
is now one function consulted twice.

One real defect was introduced and caught en route: the new duckdb module
landed between `#[cfg(feature = "failpoints")]` and `FAIL_POINTS`, silently
moving the guard onto the wrong item. Reattached, and named here because a
mis-scoped cfg is invisible in a green build.

## Unperformed verifications

| what | reason |
|---|---|
| hermetic protocol leg | no fidelity-compatible emulator exists today (C-02) |
| PrivateLink-specific host-override behaviour | no PrivateLink environment; the SEAM itself is proven (T001 A6 — a real login completed through `custom_base_url`) |
| CI-only checks | the recorded external blocker stands; never claimed green |
