# Research: Iceberg Destination

## R1 — The survey (FR-002), run at PLAN time with registry facts

**Decision: TAKE Apache iceberg-rust — `iceberg` 0.10.0,
`iceberg-catalog-rest` 0.10.0, `iceberg-storage-opendal` 0.10.0
(`opendal-s3` feature only).**

**Disqualifier (a) — arrow major: CLEARED with facts.** crates.io
dependency data for `iceberg` 0.10.0: `arrow-*` `^58`, `parquet` `^58`
— exactly the workspace major (pinned 58.3 because duckdb-rs links
it). A live probe project resolving `iceberg` + `iceberg-catalog-rest`
+ `iceberg-storage-opendal` + `arrow-array 58.3` + `parquet 58.3`
produced ONE arrow tree (58.4). Also matching existing pins: `reqwest
^0.12.12`, `tokio ^1.47`, `zstd ^0.13`, `flate2 ^1`, `futures ^0.3`,
`chrono ^0.4`, `uuid ^1`. Rust floor 1.94 ≤ toolchain 1.96.

**Disqualifier (b) — write path: PARTIALLY resolved; T001 probe.**
Append transactions (fast-append) are an established part of the
transaction API. Overwrite/replace surface in 0.10 is probed LIVE at
the environment gate; the fallback is designed (R7), not improvised.

**What the dep buys** (why the presumption was TAKE): the whole
metadata layer — Avro manifests + manifest lists (`apache-avro ^0.21`
comes with it), table-metadata JSON, field-ID assignment, column
stats, sequence numbers, snapshot lineage — plus the REST catalog
client with its commit machinery. This is the largest spec-compliance
surface the project has ever considered; hand-rolling it is a project
in itself for zero differentiation, and subtle field-ID/stats bugs
produce tables that only FOREIGN readers reveal as broken.

**Recorded transitive additions**: apache-avro, opendal ^0.57, moka,
roaring, fastnum, murmur3, ordered-float, typetag, derive_builder,
serde_with, strum (plus their trees). Heavier than any prior
connector dep except duckdb-bundled; accepted under the same
precedent. Governance: Apache, same as arrow/object_store.

**Alternatives rejected**: hand-roll (above); `iceberg-catalog-glue`
0.10 (native AWS SDK — pulls aws-sdk-glue/aws-config smithy tree;
deferred to the phase-2 Glue decision, R4); community forks
(`iceberg-*-arrow58`, `iceberg-unofficial`) — unofficial governance,
and now unnecessary since upstream is on arrow 58.

## R2 — Reuse boundaries: vocabulary yes, plumbing no

**Decision**: `rdlt-connector-rest` is NOT a dependency (the library
ships its own catalog client + OAuth2); the file crate's `location/`
is NOT extracted (iceberg-rust's FileIO is opendal — it would not
consume our object_store plumbing). What IS shared: the CONFIG
VOCABULARY — the S3 block spelling (endpoint/bucket/access_key/
secret_key/region/path_style) and the `Secret` newtype pattern are
mirrored so users learn ONE storage spelling; values translate into
FileIO props at one boundary. Rationale: reuse of words costs nothing;
reuse of plumbing would force an extraction with a single consumer on
each side. The extraction triggers recorded in 015 stand unfired.

## R3 — Exactly-once: receipts in snapshot properties; state in table properties

**Decision**: one engine commit = one Iceberg transaction. The commit
carries snapshot SUMMARY properties `rdlt.pipeline` (scope hash),
`rdlt.load-id`, `rdlt.commit-seq`. Replay detection: before building a
transaction, walk the table's snapshot history summaries for the
(load, seq) identity — present ⇒ discard staged work, return the
prior receipt (D3, readable from table history alone; no side
store). StateDoc: persisted as a TABLE PROPERTY (`rdlt.state`,
compact JSON) updated in the same atomic commit; `read_state` loads
the table and reads it back. Conflict retry: bounded (4 attempts,
jittered backoff) around refresh → rebuild → commit; exhaustion is
typed fatal naming table + competing snapshot.

**Alternatives considered**: a dedicated `_rdlt_state` TABLE per
namespace (heavier: extra table lifecycle, two-table commit is not
atomic); catalog namespace properties (not transactional with the
data commit); external state store (violates the "state rides the
destination" SPI posture).

## R4 — Auth vocabulary and the Glue deferral

**Decision**: v1 auth = `oauth2_client_credentials` {token_url?
(defaults to the catalog's oauth endpoint), client_id, client_secret:
Secret, scopes} and `bearer` {token: Secret} — both flow through
iceberg-catalog-rest's config props; our config owns the spelling and
the redaction. **Glue (SigV4) is PHASE-2**: `iceberg-catalog-rest`
0.10 exposes no SigV4 signing; the options are a signing middleware
(if its client customization permits) or `iceberg-catalog-glue`
(native SDK, own survey). The T001 gate records which door is open;
v1 ships without Glue and the parity/matrix record the deferral
loudly. Rationale: Polaris/Snowflake/UC cover the REST-native world
today; bundling the AWS SDK tree into v1 for one provider would
double the dependency surface of the whole feature.

## R5 — Credential vending

**Decision**: default storage access = catalog-vended credentials
(`X-Iceberg-Access-Delegation: vended-credentials`), which
iceberg-rust supports via catalog properties; the explicit
storage-override block (family S3 spelling) covers self-managed
buckets and the local RUSTFS leg. Session-token credentials and their
expiry are the library's concern; expiry surfacing mid-run classifies
transient. Both modes proven against Polaris (it vends against its
backing store).

## R6 — Schema mapping (closed table)

**Decision**: engine logical types → Iceberg: Bool→boolean,
Int64→long, Float64→double, Utf8→string, TimestampTz→timestamptz,
Date→date, Time→time, Uuid→uuid, Json→string (with a documented
note — Iceberg has no JSON type; the variant type is future),
Decimal(p,s)→decimal(p,s), Binary→binary; nested structs/lists map
per the engine's arrow schema. UNMAPPABLE columns are typed at
ensure-table naming the column (closed-table posture). Field IDs are
assigned by the library/catalog — never invented by us. Additive
drift → UpdateSchema add-nullable-column in the pre-commit
transaction; any other drift stays typed.

## R7 — Write modes and the designed fallback

**Decision**: Append = fast-append transaction per commit. Replace =
Iceberg overwrite ONCE per load (the durable guard read from snapshot
history: any receipt for this load ⇒ do not re-truncate — 015's rule,
snapshot-native). T001 probes the overwrite surface of iceberg-rust
0.10; if absent, v1 NARROWS: Replace becomes a typed
"replace is not supported by this release of the iceberg destination"
error at ensure_table, the narrowing is recorded in spec/parity/
README, and a follow-up lands it when upstream does. No half-measures
(delete-all hacks) — a wrong overwrite is corrupted history.

## R8 — Containers and interop harness

**Decision**: canonical leg = `apache/polaris` container (in-memory
metastore mode) + the 015 RUSTFS container as its S3 backing; fixture
health = `GET /v1/config` with a bootstrap credential; both via
testcontainers, skip-not-fail, images/env VERIFIED at T001 (the 015
RUSTFS-gate pattern — assumptions corrected from reality). UC OSS
(`unitycatalog/unitycatalog`) is the CANDIDATE bearer leg — T001
verifies its Iceberg REST surface is usable; else recorded deferred.
pyiceberg read-back: a pinned venv (the benches/competitors pattern)
+ `tools/interop/pyiceberg_readback.py` asserting counts/schema/
partitions through the same catalog — runs in the standard
container-gated suite. Spark read-back: `tools/interop/spark_readback`
in the DEEP tier only (JVM container weight); wired into
`make test TARGET=deep`.

## R9 — Crash points

**Decision**: `ice.files.write` (before data-file upload),
`ice.commit` (before the catalog commit), `ice.receipt.visible`
(between commit and receipt return) — the transaction boundaries.
Swept ×3 actions against the LIVE Polaris arm (container-gated,
skip-not-fail), asserting exactly-once totals AND a duplicate-free
snapshot history (each identity appears at most once across all
snapshots). Registry `ICE_FAIL_POINTS` pinned by the sweep.

## R10 — Bench posture

**Decision**: one scoreboard cell `iceberg-polaris-200k` (the flagship
200k dataset → Polaris+RUSTFS), declared with the Container fixture
kind from 015; never gated (the floor measures the catalog/store
containers, not rdlt). No dlt baseline pair initially (dlt's iceberg
support runs through pyiceberg — a pair is possible later; recorded).
Existing gated bars untouched.


## T001 addendum — VERIFIED verdicts (2026-07-22, all live)

**(a) Append path: GREEN, proven end-to-end.** A scratch probe against
Polaris+RUSTFS: RestCatalogBuilder (0.10 API: `CatalogBuilder::load`
with props uri/warehouse/credential/scope +
`with_storage_factory(Arc::new(OpenDalResolvingStorageFactory::new()))`)
→ create_table → arrow batch through
ParquetWriterBuilder→RollingFileWriterBuilder→DataFileWriterBuilder
(arrow schema from `iceberg::arrow::schema_to_arrow_schema` — field-id
metadata built in) → `tx.fast_append().add_data_files(..)
.set_snapshot_properties(..)` → commit. Snapshot summary carried
`rdlt.load-id`/`rdlt.commit-seq` — the R3 receipt design works as
specified.

**(b) Overwrite: RED — v1 NARROWS (the R7 fallback fires).** iceberg
0.10.0's transaction actions (source inspection): fast_append,
update_schema, update_table_properties, replace_sort_order (sort
metadata only), update_location, update_statistics, expire_snapshots,
upgrade_table_version — NO overwrite/rewrite/delete action. FR-008:
v1 ships Append; Replace = typed "not supported by this release" at
ensure_table; recorded here + parity + README; revisit on the next
iceberg-rust release.

**(c) Vending: config-level YES, per-table STS NO (local leg).**
iceberg-catalog-rest 0.10 has no `X-Iceberg-Access-Delegation`
support (source grep) and SENDS NO delegation header — storage creds
flow via /v1/config defaults, which Polaris populates from catalog
properties. VERIFIED: the probe wrote to RUSTFS with zero client-side
storage config. Two recorded facts: the catalog properties MUST
include `s3.region` (opendal requires it; "region is missing" error
otherwise) alongside s3.endpoint/s3.path-style-access/
s3.access-key-id/s3.secret-access-key; and clients that DO send the
delegation header (pyiceberg) get a 400 from an stsUnavailable
catalog — the interop harness sets
`header.X-Iceberg-Access-Delegation: ""`.

**(d) Signing hook: NONE adequate — Glue phase-2 CONFIRMED.**
RestCatalogBuilder exposes only `with_client(reqwest::Client)`; no
per-request signing seam. Doors recorded: upstream middleware
contribution or the native iceberg-catalog-glue (aws-sdk tree), each
its own survey.

**(e) UC OSS bearer leg: NOT VIABLE — READ-ONLY.** unitycatalog/
unitycatalog:latest starts (port 8080, `./bin/start-uc-server`, UC API
+ Iceberg REST at /api/2.1/unity-catalog/iceberg); its /v1/config
endpoint list contains ONLY GET/HEAD + metrics POST — no namespace/
table creation, no commit endpoint. Bearer is proven at config/
attachment level; the managed-UC write leg stays opt-in gated-live.

**(f) pyiceberg: pin 0.11.1.** 0.11.0's ConfigResponse rejects
Polaris's PUT endpoints (pydantic validation); 0.11.1 parses. Python
3.14 has no prebuilt wheel — the venv build needs python3-devel
(installed in the distrobox; recorded as an environment prerequisite).
Read-back of the probe table: 3 rows, schema [id], rdlt.* props
visible — the interop oracle works.

**(g) Polaris fixture facts (VERIFIED)**: image
`docker.io/apache/polaris:latest` — Quarkus; API port 8181, health
8182 (`/q/health`), also exposes 8080/8443; bootstrap env
`POLARIS_BOOTSTRAP_CREDENTIALS=<REALM>,<client-id>,<client-secret>` +
`polaris.realm-context.realms=<REALM>`; server-side S3 access via
standard AWS_* env (AWS_ACCESS_KEY_ID/AWS_SECRET_ACCESS_KEY/
AWS_REGION). OAuth: POST /api/catalog/v1/oauth/tokens
(client_credentials, scope PRINCIPAL_ROLE:ALL). Catalog create: POST
/api/management/v1/catalogs with storageConfigInfo {storageType: S3,
endpoint, pathStyleAccess: true, stsUnavailable: true,
allowedLocations} and properties {default-base-location, s3.endpoint,
s3.path-style-access, s3.access-key-id, s3.secret-access-key,
s3.region}. Grants: PUT catalog-roles/catalog_admin/grants
{CATALOG_MANAGE_CONTENT} + PUT principal-roles/service_admin/
catalog-roles/rdlt. Iceberg REST config:
GET /api/catalog/v1/config?warehouse=<name>. Startup to healthy
≈ 15–25 s.
