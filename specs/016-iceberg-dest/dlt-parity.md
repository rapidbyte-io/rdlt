# dlt Parity Record: Iceberg Destination (feature 016, FR-011)

Compared against **dlt 1.29.0**'s Iceberg support. dlt ships Iceberg two
ways: (a) the `filesystem` destination with `table_format: "iceberg"`
(pyiceberg-backed, filesystem/SQL catalogs) and (b) the newer
`dlt.destinations.iceberg` REST-catalog work. rdlt's 016 is a
REST-catalog-first destination built on Apache iceberg-rust — the
provider-agnostic leg (Polaris / Snowflake Open Catalog / UC) rather
than the filesystem-catalog leg. Per-surface comparison; deviations
recorded individually.

| Surface | dlt 1.29.0 | rdlt (this feature) | Verdict |
|---|---|---|---|
| catalog protocol | pyiceberg catalogs: sql/filesystem for the classic path; REST catalog in the newer leg | Iceberg REST catalog ONLY — one protocol, many providers (ID4) | parity on REST; filesystem/SQL catalogs deliberately absent (D1) |
| auth | catalog-specific (pyiceberg config passthrough) | typed vocabulary: `oauth2_client_credentials` (client-credentials against the catalog's token endpoint) + `bearer`; secrets redacted | parity, rdlt's is typed + validated rather than passthrough |
| storage credentials | explicit filesystem credentials | credential DELEGATION default (catalog config-defaults vend the S3 credentials; no keys in the dest config) + explicit S3 override with proven precedence | **rdlt ahead** on the vending default |
| append | `write_disposition: append` | WriteMode Append → fast-append snapshots, one per non-empty commit | parity |
| replace | `write_disposition: replace` (overwrite) | typed "not supported by this release" — iceberg-rust 0.10 has no overwrite transaction and ID5 forbids non-atomic delete+append emulation (T001/T008 narrowing, FR-008) | **gap, recorded** (G1) |
| merge | `merge` disposition (upsert via pyiceberg overwrite/delete machinery) | rejected by capability (`merge: false`) — append-only lakehouse posture this release | **gap, recorded** (G2) |
| exactly-once | load-package model; no snapshot-native commit identity | rdlt.pipeline/load-id/commit-seq IN snapshot summaries — exactly-once readable from table history alone; replay publishes nothing (ID2) | **rdlt ahead** |
| state | dlt state tables in the destination dataset | state doc as a property of the `_rdlt_state` marker table, written state-last; resume proven live | parity by different mechanism (both keep state in the destination) |
| concurrent writers | pyiceberg optimistic retry | bounded refresh→rebuild→commit ×4 with jitter; exhaustion typed; no foreign snapshot lost (live hammer cell) | parity |
| partitioning | column hints → identity partitions; pyiceberg transforms available | `partition_by` with identity, year/month/day/hour, bucket(N), truncate(W) transforms at CREATE; fanout writer; spec mismatch typed | parity (**D2 CLOSED** pre-review) |
| schema evolution | additive via pyiceberg | additive nullable columns via UpdateSchema in one transaction; type conflicts typed | parity |
| nested data | pyarrow structs | struct/scalar-list via the closed mapping, unique field ids | parity |
| interop proof | — (dlt IS python/pyiceberg) | pyiceberg AND Spark read-back cells over three shapes | **rdlt ahead** (adversarial cross-implementation proof) |
| catalogs: Glue | via pyiceberg glue catalog | phase-2 (no SigV4 signing seam in iceberg-catalog-rest 0.10 — T001 verdict, research R4 doors) | **gap, recorded** (G3) |
| maintenance | none (dlt defers to warehouse tooling) | none (spec out-of-scope: compaction, expire-snapshots, orphan cleanup) | parity (both defer) |
| merge-on-read / deletes | not exposed | not exposed (v2 position/equality deletes unused — append-only) | parity |

## Deviations and gaps, individually

- **D1 — REST-only catalog**: dlt's classic leg speaks
  filesystem/SQL catalogs through pyiceberg. rdlt deliberately targets
  the REST protocol only — it is the provider-agnostic surface (Polaris,
  Snowflake Open Catalog, UC, Lakekeeper…) and the one with a single
  wire contract to conformance-test against. Filesystem catalogs would
  bypass the catalog's commit arbitration entirely.
- **D2 — CLOSED (pre-review, owner request)**: `bucket(N)` /
  `truncate(W)` landed as the additive closed-enum growth the design
  anticipated — map spellings `{bucket: 16}`/`{truncate: 8}`, zero
  parameters typed eagerly, Java-convention field names
  (`col_bucket`/`col_trunc`), live fanout + metadata-visibility cells.
- **G1 — Replace**: RECORDED narrowing, not a design choice to keep:
  revisit when iceberg-rust grows an overwrite/replace transaction
  (FR-008 states the trigger). No emulation — a non-atomic
  delete+append is worse than a typed refusal (ID5).
- **G2 — Merge**: append-only this release. Iceberg upsert needs
  equality/position deletes (merge-on-read) or copy-on-write overwrite
  planning — its own feature with its own conformance net, deliberately
  not smuggled in.
- **G3 — Glue**: needs SigV4 request signing; iceberg-catalog-rest 0.10
  exposes no signing seam (only `with_client`). Doors recorded in
  research R4: upstream middleware contribution, or the native
  iceberg-catalog-glue crate (aws-sdk tree) behind its own survey.
- **No dlt bench pair**: the `iceberg-polaris-200k` scoreboard cell has
  no dlt competitor column — dlt's Iceberg write path (pyiceberg +
  filesystem destination) does not fit the competitor harness's
  pipeline shape (REST catalog + vended object store). RECORDED; the
  cell is a scoreboard (no bar) per the 004 governance.
