# dlt parity: Snowflake destination

What rdlt's Snowflake destination does, against what dlt's does. Deviations
are NAMED, with their reason and — where one exists — the trigger that would
close them. A gap recorded here is a gap someone decided about; a gap not
recorded here is a defect.

Compared against dlt's `dlt/destinations/impl/snowflake/` as of the 022
research window (2026-07).

## Ingestion

| capability | dlt | rdlt | disposition |
|---|---|---|---|
| Internal named stage (`PUT` + `COPY`) | yes, the default | **no** | **DEFERRED — the one substantive gap.** `PUT` is unreachable: the SQL API refuses it (structured code 391911) and the adopted driver exposes no raw-response escape hatch. Trigger: `snowflake-connector-rs` gaining `PUT` support or raw-response access. Contributing it upstream is the recorded route. |
| External stage `COPY INTO` (S3) | yes | yes | Parity. Proven end to end against a real cross-region bucket. |
| External stage (GCS / Azure) | yes | no | Deferred. The path is storage-agnostic on the Snowflake side; only the client-side writer and the config vocabulary are S3-shaped. Trigger: a user with a GCS or Azure bucket. |
| Batched `INSERT` without any bucket | no | **yes** | rdlt does more. dlt requires a stage; rdlt's default path needs no infrastructure at all. Slower — measured, recorded in close-out — but it works for a user who cannot provision a bucket. |
| Storage integration (keys out of the stage definition) | yes | yes | Parity. |
| S3-compatible endpoints | yes, with the same caveat | yes, with the caveat documented | Parity. Snowflake requires the endpoint to be allowlisted per account by Support; neither tool can work around it. README records it as a prerequisite. |
| Per-`COPY` loaded-rowcount verification | not asserted | **yes** | rdlt does more: a mismatch fails the unit rather than committing short. |

## Write dispositions and merge

| capability | dlt | rdlt | disposition |
|---|---|---|---|
| append | yes | yes | Parity. |
| replace | yes | yes | Parity. rdlt clears inside the unit transaction, so a reader never sees a cleared-but-unfilled target. |
| merge — delete-insert | yes | yes | Parity. |
| merge — upsert | yes | yes | Parity, via `MERGE INTO` (no `ON CONFLICT` exists here). |
| merge — scd2 | yes | yes | Parity, and rdlt's boundary is exact: retired and successor versions share one captured instant, because the clock moves between statements on this service. |
| hard delete | yes | yes | Parity. |
| dedup sort | yes | yes | Parity. |
| merge scope | no | **yes** | rdlt does more. |
| Duplicate-merge-key diagnosis | driver error | **shared advice** | rdlt does more: structured code 100090 becomes the same sentence every rdlt SQL destination gives. |

## Schema

| capability | dlt | rdlt | disposition |
|---|---|---|---|
| Create table / add column | yes | yes | Parity. |
| Column drop / narrow | no | no | Parity — neither destroys data on drift. |
| `VARIANT` for nested / JSON | yes | yes | Parity. |
| Nested shapes as native structs | partial | no | Deviation, deliberate. rdlt lowers nested shapes before they reach the destination and does not claim `structs`; claiming it without a read-back proof would be a capability nobody verified. |
| Transient tables | yes | yes | Parity. |
| Iceberg-format tables | yes | no | Out of scope, recorded as a non-goal at spec time. |
| Table/column comments from schema hints | yes | no | Deferred. Cosmetic; no trigger. |
| Clustering keys | yes | no | Deferred. A cost/performance lever a user can apply with one `ALTER` outside the pipeline. Trigger: a user whose table needs it under load. |

## Authentication

| capability | dlt | rdlt | disposition |
|---|---|---|---|
| Key-pair (JWT), encrypted keys | yes | yes | Parity. Proven live. |
| Password | yes | yes | Parity, written; live leg UNPERFORMED (no provisioned user on the qual account). |
| MFA passcode | yes | yes | Parity, written; UNPERFORMED with the password leg. |
| Programmatic access token | yes | yes | Parity. Proven live — and the channel it rides was verified rather than assumed. |
| OAuth token (caller-supplied) | yes | yes | Parity, written; live leg UNPERFORMED (no security integration on the qual account). |
| External-browser SSO | yes | **no, typed** | Deliberate. It requires a human at a browser, which an unattended pipeline does not have; refused with a message saying so rather than hanging. |
| Role / warehouse / database / schema | yes | yes | Parity. |
| PrivateLink host override | yes | yes | Parity (mock-verified only; no PrivateLink account to test against). |

## Exactly-once and recovery

| capability | dlt | rdlt | disposition |
|---|---|---|---|
| Load-level idempotence | partial | **yes** | rdlt does more: receipts and state commit in the SAME transaction as the data, so a replayed unit publishes nothing. |
| Crash recovery proven by injection | no | **yes** | rdlt does more: every protocol edge is crashed deliberately, twice, and required to converge duplicate-free. |
| DDL-outside-the-unit discipline | implicit | **enforced in code** | rdlt does more: the unit's executor refuses DDL rather than trusting call sites, because DDL here commits the transaction silently. |

## Summary

One substantive gap — **internal-stage `PUT`** — with a named upstream
trigger and a recorded route to closing it. Everything else is either parity,
a deliberate non-goal recorded at spec time, or a place rdlt does more.
