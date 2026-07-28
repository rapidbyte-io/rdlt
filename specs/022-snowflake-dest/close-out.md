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
| T003–T043 | OPEN | |

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

## Unperformed verifications

| what | reason |
|---|---|
| hermetic protocol leg | no fidelity-compatible emulator exists today (C-02) |
| PrivateLink-specific host-override behaviour | no PrivateLink environment; the SEAM itself is proven (T001 A6 — a real login completed through `custom_base_url`) |
| CI-only checks | the recorded external blocker stands; never claimed green |
