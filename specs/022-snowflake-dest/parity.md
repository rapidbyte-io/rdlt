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
| Internal named stage (`PUT` + `COPY`) | yes, the default | **yes, the only path** | **Parity, and the 022 gap is CLOSED.** 022 recorded this as DEFERRED because `PUT` was unreachable — the SQL API refuses it (structured code 391911) and the released driver exposed no raw-response escape hatch. `spec.md:499-501` recorded TWO closure routes, an upstream contribution or a maintained fork if upstream stalls; this row once named only the first, corrected in close-out C-04. Closed via the fork route: a pinned revision of `rapidbyte-io/snowflake-connector-rs` implementing `PUT`. See `tools/allowed-git-deps.toml` for what that costs. |
| External stage `COPY INTO` (S3) | yes | **no** | **RETIRED in 023, deliberately.** It existed only because `PUT` did not: it asked a user to provision a bucket, hand its keys to the connector, and pay egress twice — all to reach a staging area the service already provides for free. With `PUT` reachable, keeping it would mean maintaining a second transport with its own credentials, its own failure modes and its own sweep cells for no capability. Trigger to reconsider: a user who must stage in a bucket THEY control for a reason the internal stage cannot satisfy — a data-residency or audit requirement, not a preference. |
| External stage (GCS / Azure) | yes | no | Not applicable rather than missing. It went with the external path above. Note that `PUT` works transparently on GCS- and Azure-backed ACCOUNTS: the internal stage lives in whatever storage the account was provisioned on, so a user on those clouds is served without any of this vocabulary. |
| Ingestion without provisioning anything | no | **yes** | rdlt does more, and the reason is worth stating precisely because 022 stated it wrongly. dlt requires a stage to be configured; rdlt requires nothing configured at all — the staging area is created on the user's own schema at open. What rdlt does NOT avoid is object-storage EGRESS: `PUT` uploads to the cloud storage endpoint directly, not through the account host. 022's parity claim that "rdlt's default path needs no infrastructure at all" is now FALSE as written, because the path it described is gone; the honest distinction is no bucket and no configuration, but the same network reach dlt needs. See the README's prerequisite. |
| Storage integration (keys out of the stage definition) | yes | n/a | Retired with the external path. Nothing holds storage credentials any more, which is the strongest form of this control. |
| S3-compatible endpoints | yes, with the same caveat | n/a | Retired with the external path. The account-level allowlist caveat no longer applies to anything rdlt does. |
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
