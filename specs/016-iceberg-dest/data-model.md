# Data Model: Iceberg Destination

## 1. Catalog connection (`IcebergConfig`)

```
IcebergConfig = {
  catalog: {
    uri: String,                       # https://…/api/catalog (REST root)
    warehouse: String,                 # provider-defined warehouse ident
    auth: Oauth2ClientCredentials { token_url?: String,
                                     client_id: String,
                                     client_secret: Secret,
                                     scopes: [String] }
        | Bearer { token: Secret },    # sigv4: PHASE-2 (recorded)
    props: BTreeMap<String,String>,    # escape hatch: extra catalog props
  },
  namespace: String,                   # dot-separated levels
  create_namespace: bool = false,      # explicit create-if-missing
  storage: Option<S3Options-shaped>,   # OVERRIDE block, family spelling;
                                       # absent = vended credentials
  tables: BTreeMap<stream, TableOptions>,
}

TableOptions = {
  name: Option<String>,                # default = stream name
  partition_by: [PartitionField],
}

PartitionField = { column: String,
                   transform: identity|year|month|day|hour }
```

Validation (eager, typed): uri http(s); warehouse/namespace non-empty;
scopes only with oauth2; partition transform spellings closed; unknown
fields rejected; generated schema round-trips. All credential fields
`Secret` (grep-proof cell). Constructors + `with_*` for the
programmatic seam (non_exhaustive vocabulary, the 014/015 posture).

## 2. Type mapping (closed — schema.rs)

| engine logical | Iceberg |
|---|---|
| Bool | boolean |
| Int64 | long |
| Float64 | double |
| Utf8 | string |
| TimestampTz | timestamptz |
| Date | date |
| Time | time |
| Uuid | uuid |
| Json | string (documented: no Iceberg JSON type in v2) |
| Decimal(p,s) | decimal(p,s) |
| Binary | binary |
| struct / list | struct / list (element rules recursive) |

Unmappable → typed at ensure_table naming the column. Field IDs:
library/catalog-assigned only.

## 3. Commit identity (snapshot-native receipts)

Snapshot summary properties written with every rdlt commit:

```
rdlt.pipeline   = <pipeline scope hash>     # the 015 scope-hash rule
rdlt.load-id    = <load id>
rdlt.commit-seq = <u64>
```

Replay detection: walk snapshot history summaries for
(pipeline, load-id, commit-seq); hit ⇒ return prior receipt, publish
nothing. State: table property `rdlt.state` = compact StateDoc JSON,
updated in the same atomic commit; `read_state` = load table → read
property → filter by pipeline.

## 4. Write modes

Append: one fast-append snapshot per non-empty commit. Replace:
Iceberg overwrite ONCE per load — durable guard = any receipt for this
load in snapshot history ⇒ no re-truncate (T001 probe may narrow v1 to
Append with Replace typed-unsupported; recorded, never silent). Merge:
rejected by capability (`merge: false`).

## 5. Session state machine

open → (per stream) ensure_table: load-or-create table via catalog
(namespace create iff configured), reconcile schema (closed mapping;
additive drift = UpdateSchema) → write: batches staged as parquet data
files via the library writer (partitioned per spec) → commit: replay
check → build transaction (append | overwrite-once-per-load) + receipt
properties + `rdlt.state` → bounded conflict retry (refresh → rebuild
→ commit, 4 attempts) → receipt. Crash points: `ice.files.write`,
`ice.commit`, `ice.receipt.visible`.

## 6. Error taxonomy

Transient: network/5xx/429 from catalog or storage, vended-credential
expiry mid-run. Fatal: auth rejection, missing warehouse/namespace
(without create flag), schema conflicts (naming table+column),
unmappable types, conflict-retry exhaustion (naming table + competing
snapshot), unsupported write mode. Every error names its subject; the
library's error type is classified in ONE place (errors.rs).

## 7. Fail points

`ice.files.write`, `ice.commit`, `ice.receipt.visible`
(`ICE_FAIL_POINTS` registry, swept live ×3 actions with a
duplicate-free snapshot-history assertion).
