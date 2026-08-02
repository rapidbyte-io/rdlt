# 029 CONTRACT INVENTORY — rdlt-connector-iceberg generation 1

Read on branch `028-snowflake-v2` (gen-1 tree identical to what 029 will rewrite).
Crate: `/var/home/netf/Repos/rapidbyte/rdlt/crates/rdlt-connector-iceberg` — 2,935 src lines
(lib.rs 22, dest/{catalog 96, commit 247, config 425, ensure 260, errors 232, mod 24,
schema 571, session 458, state 171, test_support 177, writer 139, writer_props 113}).
Everything quoted below is EXACT source spelling; the rewrite freezes it.

---

## 1. CONFIG VOCABULARY (frozen)

Entry struct: `IcebergConfig` (src/dest/config.rs). Public surface re-exported at crate
root (lib.rs): `AuthOptions, CatalogOptions, ConfigError, IcebergConfig, PartitionField,
PartitionTransform, S3Override, StorageOptions, TableOptions, config_schema`, plus
`Secret` (SPI re-export) and, from `dest::mod`, `ParquetCompression, ParquetOptions`
(SPI re-exports), `IcebergDest`, `ICE_FAIL_POINTS` (failpoints feature only).

All config structs: `#[serde(deny_unknown_fields)]`, `#[non_exhaustive]`,
`Debug, Clone, PartialEq, Serialize, Deserialize, schemars::JsonSchema`. NO serde
renames anywhere — field names ARE the YAML names. No flatten.

### IcebergConfig fields
| field | type | serde |
|---|---|---|
| `catalog` | `CatalogOptions` | required |
| `namespace` | `String` | required; dot-separated levels e.g. `raw.orders` |
| `create_namespace` | `bool` | `#[serde(default)]` = false |
| `storage` | `Option<StorageOptions>` | default None |
| `tables` | `BTreeMap<String, TableOptions>` | default empty; keyed by STREAM name |
| `parquet` | `Option<ParquetOptions>` | default None (SPI type; defaults compress = Snappy) |

### CatalogOptions
`uri: String` (required, REST catalog root e.g. `https://…/api/catalog`),
`warehouse: String` (required), `auth: AuthOptions` (required),
`props: BTreeMap<String,String>` (default empty — verbatim passthrough escape hatch;
inserted LAST in catalog_props, so user props win over generated ones, including
credential keys — documented "they win over generated ones").

### AuthOptions — struct of optional kinds, exactly-one validated
```
auth:
  oauth2_client_credentials: {token_url?, client_id, client_secret, scopes?}
  bearer: {token}
```
- `oauth2_client_credentials: Option<Oauth2ClientCredentials>` with
  `token_url: Option<String>` (default None — "Defaults to the catalog's own token
  endpoint"), `client_id: String`, `client_secret: Secret`, `scopes: Vec<String>`
  (default empty).
- `bearer: Option<BearerAuth>` with `token: Secret`.
- Builder ctors: `AuthOptions::oauth2(client_id, client_secret, scopes)`,
  `AuthOptions::bearer(token)`.

### StorageOptions / S3Override (family-S3 spelling; NO `bucket` field)
`storage: {s3: {...}}`; `s3: Option<S3Override>` (default None).
`S3Override`: `endpoint: Option<String>` (default), `region: Option<String>` (default),
`access_key: Secret` (required), `secret_key: Secret` (required),
`path_style: bool` with `#[serde(default = "default_path_style")]` = **true**.
Builders: `S3Override::new(access_key, secret_key)`, `.with_endpoint()`, `.with_region()`.
Absent `storage` = credential delegation via catalog `/v1/config` defaults (the
default, verified path).

### TableOptions (per-stream)
`name: Option<String>` (default — table name defaults to the stream name),
`partition_by: Vec<PartitionField>` (default empty).
Builders: `TableOptions::new()`, `.with_name()`, `.with_partition()`.

### PartitionField / PartitionTransform
`PartitionField { column: String, transform: PartitionTransform }` where transform is
`#[serde(with = "serde_yaml::with::singleton_map")]` + `#[schemars(with =
"PartitionTransform")]` — this is CRITICAL: YAML unit variants spell as plain strings
(`transform: day`), parameterized as single-key maps (`transform: {bucket: 16}`,
`transform: {truncate: 8}`); the singleton_map adapter is deserializer-generic so JSON
parses the same shapes. Without it serde_yaml demands `!bucket` tag syntax.
`PartitionTransform` — `#[serde(rename_all = "snake_case")]`, `#[non_exhaustive]`,
`Copy, Eq`: `Identity, Year, Month, Day, Hour, Bucket(u32), Truncate(u32)`.
Spellings: `identity`, `year`, `month`, `day`, `hour`, `{bucket: N}`, `{truncate: W}`.

### Secret-wrapped fields (grep-proof, `***` in Debug)
`catalog.auth.oauth2_client_credentials.client_secret`, `catalog.auth.bearer.token`,
`storage.s3.access_key`, `storage.s3.secret_key`.

### Parse surface (IcebergConfig)
- `from_yaml(&str)`, `from_json(&str)`, `from_value(serde_json::Value)` ("The embedder
  entry point (JSON document, no string round-trip)") — each parses THEN calls
  `validate()`.
- `validate(&self) -> Result<(), ConfigError>` public.
- Helpers: `namespace_levels() -> Vec<String>` (split on `.`),
  `table_name(stream) -> String` (tables[stream].name or the stream),
  `partition_fields(stream) -> &[PartitionField]`.
- `pub fn config_schema() -> serde_json::Value` — `schemars::schema_for!(IcebergConfig)`,
  expect msg `"schema serializes"`.
- `IcebergDest::from_config(IcebergConfig)` (re-validates!), `IcebergDest::from_yaml(&str)`.
  NOTE gen 1 has NO `IcebergDest::from_json/from_value` — the sdk's from-text family
  supplies these in gen 2.

### ConfigError (thiserror)
```
#[error("invalid iceberg destination YAML: {0}")] Yaml(#[from] serde_yaml::Error)
#[error("invalid iceberg destination JSON: {0}")] Json(#[from] serde_json::Error)
#[error("invalid iceberg destination config: {0}")] Invalid(String)
```

### Validation rules + EXACT Invalid(...) message spellings (order matters)
1. uri not http(s): `` catalog.uri `{uri}` must be an http(s) URL ``
2. warehouse empty: `catalog.warehouse must not be empty`
3. auth both set: `catalog.auth declares BOTH oauth2_client_credentials and bearer — pick one`
4. auth none set: `catalog.auth declares no scheme (expected oauth2_client_credentials or bearer)`
5. oauth token_url not http(s): `` catalog.auth.oauth2_client_credentials.token_url `{url}` must be an http(s) URL ``
6. namespace empty or an empty level (`raw..orders`): `` namespace `{ns}` must be non-empty dot-separated levels ``
7. storage present but s3 None: `` `storage` block declares no kind (expected `s3`) ``
8. parquet options invalid: the `ParquetOptions::validate()` message verbatim
9. `tables.{stream}.name` empty: `tables.{stream}.name must not be empty`
10. partition column empty: `tables.{stream}.partition_by: a field names no column`
11. `Bucket(0)`: `tables.{stream}.partition_by.{column}: bucket count must be >= 1`
12. `Truncate(0)`: `tables.{stream}.partition_by.{column}: truncate width must be >= 1`

### Catalog load-property keys (dest/catalog.rs — the `CAT_*` vocabulary)
`uri`, `warehouse`, `credential` (= `{client_id}:{client_secret.reveal()}`),
`scope` (scopes joined with a single space; only when non-empty),
`oauth2-server-uri` (token_url when set), `token` (bearer),
`s3.endpoint`, `s3.region`, `s3.access-key-id`, `s3.secret-access-key`,
`s3.path-style-access` (bool `to_string()`). Every `Secret::reveal` in the crate is
concentrated in `catalog_props()` — the single credential-audit function. Catalog built
via `RestCatalogBuilder::default().with_storage_factory(Arc::new(
OpenDalResolvingStorageFactory::new())).load(warehouse, props)`.

---

## 2. FROZEN MESSAGE SPELLINGS + ERROR TAXONOMY

One boundary: `dest/errors.rs`. `fatal(msg)` = `DestinationError::fatal(msg.to_string())`
shared crate-wide. `classify(context, iceberg::Error)` — nothing above errors.rs sees
library error types.

### classify() rules (by `iceberg::ErrorKind`)
- `Unexpected` + transport status parsed from the error's context entry:
  - 401/403 → **Fatal**: `{context}: authentication/authorization rejected — fix the credential or its grants: {rendered}`
  - 429 → **RateLimited** (retry_after None): `{context}: {rendered}`
  - other 400..500 → **Fatal**: `{context}: {rendered}` (deterministic client error)
  - 5xx or NO decodable status (network faults; a status merely quoted in a body) →
    **Transient**: `{context}: {rendered}`
- `CatalogCommitConflicts` → **Fatal**:
  `{context}: commit conflicts exhausted the bounded retry — a competing writer keeps winning: {error}`
  (reaching classify means the retry loop already exhausted; the word "exhausted"
  appears EXACTLY ONCE in the final rendering — pinned by test).
- `DataInvalid | FeatureUnsupported | PreconditionFailed | NamespaceAlreadyExists |
  TableAlreadyExists | NamespaceNotFound | TableNotFound` → **Fatal**: `{context}: {error}`
- `_` (unknown non_exhaustive kind) → **Fatal** (loud, never silent retry): `{context}: {error}`

`is_commit_conflict(&iceberg::Error)` = `matches!(kind, CatalogCommitConflicts)` drives
the retry loop.

### status_from_context (the fragile seam — pinned by its own test)
The library has no public getter for its context vec, so the status is read back off
the RENDERED Display: split once on `", context: { "`, then `split(", ")`, find entry
with prefix `"status: "`, take first whitespace token, parse u16. Anchored on the entry
KEY so a status quoted in a response BODY (renders after ` => `, outside the context
block) is never mistaken for the transport status. Statuses attached upstream via
`with_context("status", "<code> <reason>")` e.g. `401 Unauthorized`.

### Context strings passed to classify (subjects)
- catalog connect: `` catalog `{uri}` warehouse `{warehouse}` ``
- namespace ops: `` namespace `{ns.to_url_string()}` ``
- table ops (ensure/commit/write/align): `` table `{ident}` `` (ident = TableIdent
  Display, i.e. `ns.name`) — session uses `` table `{config.table_name(stream)}` ``
- state table: `` state table `{ident}` ``
- schema-change writer retirement: `` table `{name}` (schema-change writer retirement) ``

### Other frozen fatal spellings (session/ensure/schema/state)
- open, bad namespace ident: `` namespace `{ns}`: {e} ``
- arrow conversion: `{context}: arrow schema conversion: {e}`
- Merge mode: `iceberg destination does not support Merge (capabilities.merge = false)`
- Replace mode: `iceberg destination: Replace is not supported — the underlying iceberg library exposes no overwrite transaction, which Replace requires; use Append, or a SQL destination for replace semantics`
- reserved table name: `` table name `{name}` is reserved for the rdlt state marker table ``
- write before ensure: `` write before ensure_table for `{table}` `` (sdk owns this in gen 2)
- align cast failure: `` {context}: column `{name}` cannot cast {from} -> {to}: {e} ``
- align missing required column: `` {context}: the live table requires column `{name}` but the stream no longer provides it ``
- align rebuild: `{context}: aligning batch: {e}`
- state doc serialize: `state doc: {e}`; state doc parse: `state doc parse: {e}`
- missing namespace, create_namespace false: `` namespace `{ns}` does not exist and create_namespace is false — create it (or set create_namespace: true) ``
- schema build: `` table `{t}`: building iceberg schema: {e} ``
- unmappable type: `` table `{table}` column `{column}`: engine type {other:?} has no iceberg mapping (the engine-type → iceberg-type mapping is closed) ``
- unknown partition column: `` {context}: partition_by names unknown column `{column}` ``
- partition-field build: `` {context}: partition field `{column}` ({transform}): {e} ``
- partition-spec mismatch: `{context}: configured partition_by [{wanted}] does not match the live table's partition spec [{live}] — partition specs are fixed at creation (drop the table or align the config)` — rendered pairs `column:transform` (Display, never Debug) joined by `", "`
- contradictory drift: `` {context} column `{name}`: {detail} — contradictory drift is never applied `` where detail is one of:
  - Type: `stream type {wanted} conflicts with the table's {live}`
  - NestedFields: `stream shape {wanted} conflicts with the table's {live} — a nested field was added, removed or renamed, which additive evolution cannot express`
  - Nullability: `the table requires a value this stream may not supply`
- conflict-retry exhaustion prefix (from commit_with_retry's last attempt):
  context becomes `{context} ({subject} attempt {attempt}/{COMMIT_ATTEMPTS})` — subject
  is `commit` (data), `property commit` (state), `schema commit` (evolution). The
  prefix must NOT repeat "exhausted" (classify supplies it).
- crash injection: `injected crash at ice.files.write` / `injected crash at ice.commit`
  / `injected crash at ice.receipt.visible`
- writer_props (Result<_, String>, mapped through fatal at the SPI boundary):
  - `internal: {what} level requested without a value`
  - `` `compression_level` {level} is negative — {what} levels start at 0 ``
  - `` `compression_level` {level} is not valid for {what}: {e} ``
  - `` compression `{name}` is not supported by the iceberg destination ``

---

## 3. EXACTLY-ONCE DESIGN (frozen semantics)

### Commit identity (snapshot summary property keys — dest/commit.rs)
```
PROP_PIPELINE   = "rdlt.pipeline"
PROP_LOAD_ID    = "rdlt.load-id"
PROP_COMMIT_SEQ = "rdlt.commit-seq"
```
`CommitIdentity { scope, load_id, commit_seq: u64 }`. `scope` =
`ident_hash(ctx.pipeline.as_str(), SCOPE_HASH_LEN)` with `SCOPE_HASH_LEN = 12`
(rdlt_connector::core::naming::ident_hash) — NOT the raw pipeline name. commit_seq
stored as its decimal string. `summary_props()` stamps all three onto the snapshot.

### Replay detection
`already_committed(&table)`: scan `table.metadata().snapshots()` for a snapshot whose
`summary().additional_properties` matches all three keys exactly. Durability caveat
(doc-pinned): retention/expire_snapshots removes the evidence; retention must outlive
the redelivery window.

### Bounded conflict retry — `commit_with_retry` (ONE loop, three riders)
`COMMIT_ATTEMPTS: u32 = 4`. Shape: plan(current) → `CommitPlan::Settled` (return
current, commit nothing) | `CommitPlan::Commit(Box<Transaction>)` → `tx.commit(catalog)`.
On a commit-conflict error with attempt < 4: `backoff(entropy, attempt)` then
`catalog.load_table(ident)` (refresh) and re-plan (rebuild). Final-attempt conflict →
classify with context `{context} ({subject} attempt {attempt}/{COMMIT_ATTEMPTS})`.
Non-conflict error → classify(context) immediately.
Backoff: base = `50ms * 2^min(attempt,4)`; jitter = `RandomState (process-wide
OnceLock).hash_one((entropy, attempt)) % base`; sleep base+jitter. Entropy per writer:
data commits use `"{scope}:{load_id}"`, state writes use the scope, schema commits use
the context string. Riders: data append (`subject "commit"`), state property write
(`"property commit"`), schema evolution (`"schema commit"`).

### Data commit — `append_commit`
Fast-append ONLY (Append mode): `Transaction::new(current).fast_append()
.add_data_files(files).set_snapshot_properties(identity.summary_props())`, then
`apply`, `crash_point!("ice.commit")`, commit via the retry loop. The plan closure
RE-CHECKS `already_committed` each attempt (the competitor it lost to may be our OWN
replay — a second append would double the identity's snapshot) → Settled.

### Session commit choreography (session.rs `commit`)
Per table (BTreeMap iteration order): take `pending_files`, close the live writer and
extend files; empty file set → skip (EMPTY WINDOW PUBLISHES NO SNAPSHOT). Then
`load_table` FRESH; if `identity.already_committed(fresh)` → adopt fresh table and
publish NOTHING (this window's files stay orphaned + invisible — no snapshot references
them); else `append_commit`. After each table: recompute `arrow_target` from the
refreshed table (a concurrent writer's additive evolution realigns next window).
Then `crash_point!("ice.receipt.visible")`. Then — LAST, after every table's data
commit — the StateDoc write; per-table snapshot receipts make replays converge even if
the crash lands before the state write.

### StateDoc persistence (dest/state.rs) — NOT `rdlt.state` on stream tables
```
PROP_STATE_PREFIX = "rdlt.state."
STATE_TABLE       = "_rdlt_state"
state_key(scope)  = "rdlt.state.{scope}"        (ONE helper, both sides)
```
The state doc (serde_json string of `meta.state`) lives in the TABLE PROPERTIES of a
dedicated marker table `_rdlt_state` in the destination namespace, keyed per pipeline
scope. Rationale (pinned): `update_namespace` is unimplemented in iceberg-catalog-rest
0.10 (FeatureUnsupported, verified live), and stream-table properties are not
enumerable from dest config alone. NOTE: 016's plan said "table property rdlt.state
updated in the same atomic commit" — gen 1 as built uses a SEPARATE property commit on
the marker table AFTER the data commits (not the same atomic commit); replay
convergence covers the gap. Marker table schema: single optional field
`(1, "scope", String)`; created on first write (TableNotFound → create; concurrent
TableAlreadyExists → load theirs). Property write rides the shared retry
(`update_table_properties().set(key, json)`), subject `"property commit"`, entropy =
scope. Read side: `load_table(_rdlt_state)`, `.metadata().properties().get(state_key)`;
TableNotFound | NamespaceNotFound → Ok(None) (first run). `read_state` re-derives scope
with the same SCOPE_HASH_LEN and finally filters `state.pipeline == pipeline` (hash
collision safety).

### Write modes
Append → fast-append. **Replace → typed unsupported** (frozen message above; 016's ID5
fallback was TAKEN: iceberg-rust 0.10 exposes no overwrite action; emulating
delete+append would not be atomic). Merge → typed rejection + `with_merge(false)`
capability. "Replace via overwrite once-per-load with durable guard" from the 016 plan
NEVER SHIPPED — do not resurrect it from the plan text.

### File naming / recovery discipline
`session_nonce()` = `{unix_nanos:x}-{process_counter}` — unique per session; a recovery
session replaying (load, window) must never reuse a prior session's data-file names
(prior files stay orphaned when the replay check discards them). File prefix per
writer-open = `"{load_id}-{window_seq}"`; window_seq is per-table, incremented on first
write of each window, and MUST SURVIVE re-ensure (resetting it regenerates window 1's
path and overwrites a committed file). `DefaultFileNameGenerator::new(prefix,
Some(nonce), DataFileFormat::Parquet)` + `DefaultLocationGenerator::new(metadata)`.

### Mid-window schema evolution (the 028-class defect, already handled here)
`reinstall_state`: on re-ensure, if the freshly computed arrow_target differs from the
previous one, the in-flight writer is RETIRED — its closed files (valid under the prior
schema; Iceberg reads absent columns as null after additive evolution) go into
`pending_files` and join the window's commit; the next writer opens against the evolved
table. Writer survives re-ensure only while the schema is unchanged.

---

## 4. CRASH POINTS

Registry (failpoints feature, session.rs, swept by tests/sweep.rs):
```
ICE_FAIL_POINTS = ["ice.files.write", "ice.commit", "ice.receipt.visible"]
```
Arming spelling: the `crash_point!` MACRO (`rdlt_connector::core::crash_point`), value
form `crash_point!("id", Err(DestinationError::fatal("injected crash at <id>")))`.
(The 024 scanner recognises `crash_point!` and `crash_at`; this crate uses only the
macro.)
- `ice.files.write` — writer.rs:77, inside `TableWriter::open`, after location/name
  generators + parquet builder are set up, BEFORE the data-file writer builds (i.e. it
  fires at first write of a window, before any bytes are staged).
- `ice.commit` — commit.rs:173, inside the append plan closure, after the fast-append
  action is applied, immediately before returning `CommitPlan::Commit` (before
  tx.commit hits the catalog).
- `ice.receipt.visible` — session.rs:364, after ALL tables' data commits landed,
  before the state write (the receipt is visible in snapshot history; state not yet
  persisted — recovery must converge via replay detection).

---

## 5. TYPE MAPPING (closed) + field IDs + drift + partitioning

### LogicalType → Iceberg primitive (schema.rs `scalar_type`)
| engine | iceberg |
|---|---|
| Bool | Boolean |
| Int64 | Long |
| Float64 | Double |
| Decimal{p,s} | Decimal{precision: p as u32, scale: s as u32} |
| Utf8 | String |
| Binary | Binary |
| TimestampTz | Timestamptz |
| TimestampNaive | Timestamp |
| Date | Date |
| Time | Time |
| Uuid | Uuid |
| Json | **String** (Iceberg v2 has no JSON type; variant type = future work; capability `json_type: false`) |
| (future variant) | typed error, closed-table message (§2) |

`ColumnType::ScalarList{item}` → `Type::List` with `NestedField::list_element(id,
element, false)` (element NOT required — i.e. element nullable... note: third arg
`false` = not required). `ColumnType::Struct{fields}` → recursive `Type::Struct`.
Nullable column → `NestedField::optional(id, name, ty)`, else `required`.

### Field-ID discipline
Assigned SEQUENTIALLY from 1, DEPTH-FIRST, at creation time only (list element and
struct children consume IDs from the same running counter). The catalog NORMALIZES
(renumbers LEVEL-ORDER) on create — so IDs are the catalog's, never compared, never
renumbered/reused by this crate. Post-creation evolution goes through UpdateSchema
(fresh IDs, library-assigned).

### Drift comparison (schema.rs `compare_column` — id-IGNORING, structural)
`Drift::{Type{wanted,live}, NestedFields{wanted,live}, Nullability}` (strings are the
iceberg `Type` Display). Rules: primitives equal or Type-drift; structs compared by
FIELD NAME (not position), length mismatch or missing counterpart = NestedFields; lists
compare element fields; maps compare key+value fields; shape mismatch = Type drift.
Nullability ASYMMETRIC: live `required` + wanted `optional` = drift; the reverse is
fine (additive evolution deliberately creates it). Ignoring ids exists because the
catalog renumbers level-order vs our depth-first (the pinned gen-1 defect fix — second
load of a struct-bearing table used to fail as contradictory).

### Reconcile (ensure.rs)
`ensure_table`: table_exists → create with mapped schema + partition spec (concurrent
TableAlreadyExists → fall through) → `reconcile`: rides `commit_with_retry` (subject
`"schema commit"`); each attempt: check_partition_spec first, then per wanted column —
present: compare_column (drift → typed fatal); absent: `AddColumn::optional(name,
type)` (ADDITIVE ONLY — new column is nullable in the table even if the stream declares
it required, existing rows have no value). Empty addition set → Settled (converged with
a competitor adding the same column). Namespace: `ensure_namespace` — exists → ok;
missing + create_namespace false → typed; create with empty props; concurrent
NamespaceAlreadyExists → ok.

### Partitioning
Config vocabulary `tables.<stream>.partition_by` (§1). `to_partition_spec`: empty →
None. `UnboundPartitionSpec::builder().with_spec_id(0)`; partition field ids assigned
from 1000 ascending (Iceberg convention; Polaris parses the create payload STRICTLY —
spec-id and per-field field-id must be present, probed live, omitting them is a 400).
Field NAME convention: Identity keeps the column name; `{col}_year`, `{col}_month`,
`{col}_day`, `{col}_hour`, `{col}_bucket`, `{col}_trunc` (Java convention). Spec is
FIXED at creation — `check_partition_spec` compares (column-name, transform) pairs
against the live default spec (source column resolved by id, fallback `#{source_id}`),
mismatch = typed (§2). Writes: unpartitioned → plain `DataFileWriter`; partitioned →
`FanoutWriter` + `RecordBatchPartitionSplitter::try_new_with_computed_values(schema,
spec)` (partition values computed from source columns; one file per partition value per
window).

### Batch alignment (session.rs `align`)
Engine batch → table's field-id-annotated arrow schema
(`iceberg::arrow::schema_to_arrow_schema(current_schema)`, recomputed at ensure AND
commit boundaries via one `arrow_target` helper). Columns matched BY NAME
(case-sensitive); type mismatch → `arrow_cast::cast`; missing + nullable → null-filled
(`new_null_array`); missing + required → typed naming the TABLE (§2).

---

## 6. THE LIBRARY BOUNDARY

Pinned deps: `iceberg =0.10.0`, `iceberg-catalog-rest =0.10.0`,
`iceberg-storage-opendal =0.10.0` (features `opendal-s3`) — workspace Cargo.toml:84-86.

APIs used:
- Catalog: `RestCatalogBuilder::default().with_storage_factory(Arc::new(
  OpenDalResolvingStorageFactory::new())).load(name, props)` → `Arc<dyn Catalog>`;
  `namespace_exists / create_namespace / table_exists / create_table / load_table`.
- Creation: `TableCreation::builder().name().schema().partition_spec_opt().build()`;
  `Schema::builder().with_fields()`; `NestedField::{required,optional,list_element}`;
  `UnboundPartitionSpec/UnboundPartitionField` builders.
- Transactions: `Transaction::new(table)`, `.fast_append().add_data_files()
  .set_snapshot_properties()`, `.update_table_properties().set()`, `.update_schema()
  .add_column(AddColumn::optional(..))`, `ApplyTransactionAction::apply`,
  `tx.commit(catalog)`.
- Metadata reads: `table.metadata().{snapshots, current_schema, properties,
  default_partition_spec}`, `snapshot.summary().additional_properties`.
- Writers: `ParquetWriterBuilder`, `RollingFileWriterBuilder::
  new_with_default_file_size`, `DataFileWriterBuilder`, `FanoutWriter`,
  `RecordBatchPartitionSplitter`, `DefaultLocationGenerator`,
  `DefaultFileNameGenerator`, `DataFileFormat::Parquet`, `table.file_io()`.
- Arrow bridge: `iceberg::arrow::schema_to_arrow_schema`.
- Errors: `iceberg::Error`, `ErrorKind` — wrapped ONLY in errors.rs (classify /
  is_commit_conflict / status text parse).
Public surface: NOTHING from iceberg-rust crosses it (types are pub(crate); the public
API is config structs + IcebergDest + SPI re-exports). Test_support implements the full
`Catalog` trait for the ConflictCatalog mock (unit tests only).

---

## 7. TESTS CENSUS (nextest counts verified: 57 default / 59 with --features failpoints)

Unit (lib, in-src `#[cfg(test)]`): **25** — errors classification matrix + status
parser pins (5), commit retry/exhaustion (2), ensure schema-commit retry (1), schema
closed-table + nested ids (2) + partition spec building (4) + id-ignoring drift
comparison (3), session align (3), state key agreement + exhaustion (2), writer_props
(3). Container-free; ConflictCatalog mock (test_support.rs — update_table conflicts N
times; `table_with_schema` builds via `TableMetadataBuilder::from_table_creation`,
which RE-ASSIGNS field ids exactly like a REST catalog, making id-sensitivity
reproducible without a container).

Integration binaries (tests/):
- `config_schema` — **9**, container-FREE (excluded from the live group by name):
  schema/corpus round-trip, unknown-field parity, validation matrix (asserts message
  fragments `http(s)`, `warehouse`, `no scheme`, `BOTH`, `namespace`, `no kind`,
  `names no column`, `unknown variant`, `newtype variant`), secrets_never_render
  (Debug grep-proof, `***`), helper lookups, YAML + JSON transform spellings,
  bucket/truncate zero validation, dest construction/spec/capabilities,
  the_live_group_membership_is_pinned (asserts the nextest group filter membership).
- `catalog_live` — **5**: fixture smoke; engine append exact totals one snapshot per
  commit (asserts `added-records`, `rdlt.pipeline`/`rdlt.load-id`/`rdlt.commit-seq`
  stamped); empty commit publishes no snapshot; open failures typed (bad warehouse
  names `no-such-warehouse`); unreachable catalog typed.
- `exactly_once` — **4**: replayed commit publishes nothing; incremental resume from
  catalog state; narrowed stream null-fills; Replace rejected live.
- `conflict` — **1**: two live writers, 4 commits each, no snapshot lost.
- `partitioning` — **3**: fanout + spec visible in raw metadata; bucket/truncate live;
  spec mismatch typed.
- `providers` — **4**: vended credentials (no storage block) is the default path;
  explicit S3 override works; WRONG override FAILS (never silently ignored); bearer
  auth live.
- `nested_types` — **1**: nested stream loads twice without reporting drift
  (confirmation cell for the id-renumbering fix; the red pin is the unit test).
- `auth_probe` — **1**: live 401 classifies Fatal end-to-end.
- `interop` — **3**: pyiceberg (independent implementation) reads plain append /
  partitioned / after additive drift; venv at `tools/interop/.venv` (pip install -r
  tools/interop/requirements.txt = `pyiceberg[s3fs]==0.11.1`, `pyarrow==22.0.0`;
  0.11.0 rejects Polaris's config response); override via `RDLT_INTEROP_PYTHON`;
  script `tools/interop/pyiceberg_readback.py` prints JSON {count, columns,
  partition_fields, snapshots, snapshot_props}; skip-not-fail without venv.
- `spark_deep` — **1**: gated on `RDLT_DEEP=1`; `tools/interop/spark_readback.sh`;
  Makefile:227 `RDLT_DEEP=1 cargo nextest run -p rdlt-connector-iceberg -E
  'binary(spark_deep)'`.
- `sweep` — **2** (only with `--features failpoints`; `#![cfg(feature =
  "failpoints")]`): `iceberg_dest_survives_crash_sweep` — 3 points × 3 actions
  `["return", "panic", "1*off->return"]`, armed twice + recover through the ENGINE
  (rdlt_engine Engine + MemorySource, 4 rows, 2 checkpoints), exact totals 4,
  duplicate-free (pipeline, load-id, commit-seq) identity set, fired == full matrix
  pin; `the_registry_matches_the_sources` —
  `rdlt_testkit::assert_registry_matches_sources(src, &[ICE_FAIL_POINTS])`.
  Makefile:124: `cargo nextest run -p rdlt-connector-iceberg --features failpoints -E
  'binary(sweep)'` (the `make test TARGET=sweep` gate).

Container fixture (tests/common/mod.rs): plain-podman (testcontainers can't express
host network vs podman compat API), `--network host`, `--label rdlt-test=1`, `--rm`,
names `rdlt-ice-{prefix}-{pid}-{seq}`. Images PINNED:
`docker.io/rustfs/rustfs:1.0.0-beta.11` (env `RUSTFS_ADDRESS=0.0.0.0:{port}`,
`RUSTFS_ACCESS_KEY`, `RUSTFS_SECRET_KEY`) and `docker.io/apache/polaris:latest`
(NOTE: floating tag! env `POLARIS_BOOTSTRAP_CREDENTIALS=POLARIS,{id},{secret}`,
`polaris.realm-context.realms=POLARIS`, `QUARKUS_HTTP_PORT`,
`QUARKUS_MANAGEMENT_PORT`, `AWS_REGION=us-east-1`, `AWS_ACCESS_KEY_ID`,
`AWS_SECRET_ACCESS_KEY`). Constants: CLIENT_ID=`root`, CLIENT_SECRET=`s3cr3t`,
S3_KEY=`ice-key`, S3_SECRET=`ice-secret`, BUCKET=`ice`, WAREHOUSE=`rdlt`. Ports from a
PID-derived range 21000..32000 (below kernel ephemeral — bind(:0) alone races across
nextest processes, observed live). Readiness: RUSTFS = any HTTP answer; Polaris =
2xx on `/q/health` (503 DOWN while initializing). Skip-not-fail via
`rdlt_testkit::gate::runtime_available()` AND per-container podman failure (socket
probe and binary can disagree). Bootstrap: `tests/fixtures/polaris_bootstrap.py`
(stdlib-only, SigV4 PUT-bucket by hand, management API create-catalog with DUAL
endpoints, grants; args: polaris-api, rustfs-client-ep, rustfs-polaris-ep, s3 creds,
client creds, warehouse, bucket). OAuth token from
`/api/catalog/v1/oauth/tokens` form grant_type=client_credentials,
scope=`PRINCIPAL_ROLE:ALL`. Oracles: raw `GET {catalog}/v1/{warehouse}/namespaces/
{ns}/tables/{t}` for metadata + snapshot summaries (timestamp-ms sorted).

nextest grouping (.config/nextest.toml:5-10): `[test-groups.iceberg-live]
max-threads = 3`; override `filter = "package(rdlt-connector-iceberg) and not
binary(config_schema)"` → NEGATIVE filter (024: stays negative; membership asserted by
`the_live_group_membership_is_pinned`). Each cell boots its own Polaris JVM + RUSTFS.

## 8. CONSUMERS (exact locations)

- Workspace: `Cargo.toml:13` (member), `:48` (`rdlt-connector-iceberg = { path = ...,
  version = "0.3.0" }`), `:84-86` (iceberg dep pins).
- Facade `crates/rdlt/Cargo.toml:22` `iceberg = ["dep:rdlt-connector-iceberg"]`, `:38`
  optional dep. `crates/rdlt/src/lib.rs:55-56` `pub use rdlt_connector_iceberg as
  iceberg;` under `#[cfg(feature = "iceberg")]` (module `rdlt::connector::iceberg`).
- Pipeline spec: `crates/rdlt/src/pipeline_spec.rs:196-197` `DestSpec::
  Iceberg(Box<IcebergConfig>)` (full vocabulary embedded inline — NOT hand-mirrored),
  `:463-466` build arm: `IcebergDest::from_config((**config).clone()).map_err(|e|
  SpecError::resolve(format!("iceberg destination: {e}")))`, `:381` cfg-any list.
- CLI: `crates/rdlt-cli/Cargo.toml:21` (feature `iceberg` in the rdlt feature list);
  `crates/rdlt-cli/src/main.rs:251-285` `iceberg_spec_parses_from_the_yaml` test.
- Makefile: `:124` (sweep target line), `:227` (deep/spark line).
- nextest: `.config/nextest.toml:1-10` (iceberg-live group, max-threads 3).
- Bench: `crates/rdlt-bench/Cargo.toml:21` (rdlt feature `iceberg`);
  `benches/parity_specs.yaml:67-80` (`pipeline: parity-iceberg` destination block —
  full YAML vocabulary spelled there, incl. `oauth2_client_credentials`);
  `benches/RESULTS.md:55` (016 `iceberg-polaris-200k` evidence note, scoreboard —
  no bench cell TOMLs reference iceberg today); `benches/GOVERNANCE.md:106,115-117`.
- Interop tools: `tools/interop/{pyiceberg_readback.py, spark_readback.sh,
  requirements.txt}`.

## 9. DEPENDENCIES (crate Cargo.toml)

deps (all `workspace = true`): `rdlt-connector` (features `["schema"]`), `iceberg`,
`iceberg-catalog-rest`, `iceberg-storage-opendal`, `arrow-array`, `arrow-schema`,
`arrow-cast`, `parquet`, `async-trait`, `schemars`, `serde`, `serde_json`,
`serde_yaml`, `thiserror`, `tokio`.
dev-deps: `jsonschema`, `reqwest`, `rdlt-engine`, `rdlt-testkit`, `tempfile`.
features: `failpoints = ["rdlt-connector/failpoints"]`. Lints: workspace.
NOTE for gen 2: the 027 ONE-DEPENDENCY rule (connectors depend on rdlt-connector-sdk
alone, SPI via its `spi` re-export; sdk forwards failpoints/schema) will replace most
of this list; `serde_yaml::with::singleton_map` is a direct serde_yaml dependency the
sdk config::Document path must still accommodate (the transform spellings are frozen).

## 10. SUSPICIOUS (candidate inherited defects — NOT fixed, for the review loop)

1. **status_from_context spoofable through a preceding context entry** —
   src/dest/errors.rs:89-97. It splits on the FIRST `", context: { "` then scans
   `split(", ")` pieces for prefix `status: `. A NON-status context entry whose VALUE
   contains the literal `, status: 401 ...` (e.g. a `headers` entry rendering a header
   map) would be read as the transport status; conversely a source-chain error that
   renders its own context block first could shadow the outer one. The body-text case
   is defended; the context-value case is not.
2. **`catalog.props` silently overrides credential/security keys** —
   src/dest/catalog.rs:74-76. User props are inserted LAST so `token`, `credential`,
   `s3.secret-access-key` etc. can be replaced verbatim from plain-text config,
   bypassing the Secret discipline (a credential can enter via a non-Secret field).
   Documented as "win over generated ones" — but worth an owner decision in gen 2.
3. **Polaris image tag floats** — tests/common/mod.rs:156 `docker.io/apache/polaris:
   latest`, directly under a comment block (rustfs, line 133-135) explaining why
   floating tags fail gates. Inconsistent with the crate's own stated rule.
4. **`ice.files.write` sits at writer OPEN, not at file write/close** —
   src/dest/writer.rs:77-80. It fires before any bytes are staged and before the
   builder even builds; a crash between actual parquet writes/close and commit is
   only covered by `ice.commit`. Name overpromises placement.
5. **Empty-commit windows skip the state write only if ALL windows are empty? No —
   state is written unconditionally** — src/dest/session.rs:371-375: even a commit
   where EVERY table's window was empty (no snapshot anywhere) still writes the state
   doc. A crash after `ice.receipt.visible` but before write_state on an all-empty
   commit replays harmlessly, but state advances with no snapshot receipt backing it —
   worth re-deriving deliberately in gen 2 (probably correct: empty window = nothing
   to make exactly-once, state is the only payload).
6. **`already_committed` scans potentially large snapshot histories linearly per
   table per commit** (commit.rs:53-60) and the retry loop re-runs it every attempt —
   fine at test scale, unmeasured on long-lived tables (thousands of snapshots).
7. **List elements always created non-required** — schema.rs:78
   `NestedField::list_element(element_id, element, false)`: a `ScalarList` of a
   non-nullable item still yields an optional element in Iceberg. Engine's
   ScalarList{item} carries no item-nullability, so this is a forced choice — but the
   drift comparison compares element REQUIRED-ness (compare_column on element_field),
   so if a future catalog normalizes differently this becomes phantom drift.
8. **`IcebergDest::from_config` double-validates** (session.rs:48-51 — from_yaml
   already validated inside IcebergConfig::from_yaml, then from_config validates
   again). Harmless; noise for the sdk parse-then-validate flow.
9. **Scope hash truncation, 12 hex chars** (session.rs:34): two pipelines colliding on
   `ident_hash(.., 12)` share a state key; read_state's `filter(|s| &s.pipeline ==
   pipeline)` makes a collision read as None (state loss → full reload), and snapshot
   replay detection compares scope ONLY by the same hash — a colliding pipeline's
   identical (load-id, seq) would be mistaken for a replay. Astronomically unlikely,
   but the load-id namespace is engine-generated; record as accepted risk.
10. **`session_nonce` u128→u64 truncation + `unwrap_or(0)`** (session.rs:135-139):
    pre-epoch clock yields nonce collisions across processes in the same nanosecond —
    counter disambiguates within a process only. Recovery-session uniqueness rests on
    wall clock.
11. **Merge rejection message references `capabilities.merge = false`**
    (session.rs:160-162) — an internal-surface spelling in an operator-facing string;
    Principle V flavor question for the review loop.
12. **016 plan vs shipped drift**: the plan block in CLAUDE.md says "StateDoc in table
    property rdlt.state updated in the same atomic commit" and "Replace = overwrite
    once-per-load". Shipped reality: state in `_rdlt_state` marker-table properties
    under `rdlt.state.{scope}` in a SEPARATE commit; Replace typed-unsupported. The
    rewrite must freeze the SHIPPED behavior, not the plan text.
