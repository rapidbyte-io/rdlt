# 029 — ICEBERG SECOND GENERATION (`rdlt-connector-iceberg-v2`)

Owner goal: "design and plan and rewrite rdlt-connector-iceberg in
rdlt-connector-iceberg-v2 (greenfield/clean layout/from scratch clean
implementation) — similarly current to postgres/rest."

Branch `029-iceberg-v2`, stacked on `028-snowflake-v2` @ 47d6e936 (which
carries the swapped-in snowflake second generation; 028 is complete but
not merged — 029 depends only on main-merged 027's sdk, and stacking
keeps the sequential merge trivial).

THE DISCIPLINE (learned across 025/026/028, binding here): TRUE
greenfield — generation 1 is the REFERENCE IMPLEMENTATION for its
CONTRACT only. Near-verbatim transcription counts as copying and is
rejected (see memory `rdlt-rewrite-means-no-copying`). Frozen spellings
are identical because they are contracts; everything else — structure,
layout, naming, tests, prose — is re-derived and improved. The review
loop afterward treats "found an inherited defect" as its success
condition, not an embarrassment.

## The authoritative contract inventory

`specs/029-iceberg-v2/contract-inventory.md` (committed, produced by an
exhaustive read of generation 1 at 47d6e936) is D3's substance: config
vocabulary with every serde spelling, the ~12 validation and ~25
operational frozen message spellings quoted exactly, the classification
rulebook, the exactly-once snapshot design, crash points, the closed
type table, partition naming, tests census, consumers, dependencies,
and 12 suspicious items reserved for the review loop.

THREE CORRECTIONS the inventory made to the 016-era summary — the
rewrite freezes SHIPPED behavior, not the old plan's prose:
- **Replace is typed-unsupported** (016's ID5 fallback was taken; the
  refusal spelling is frozen). v1 semantics = Append only.
- **State is NOT in the same atomic commit as data**: it lives in a
  marker table `_rdlt_state`'s table properties under
  `rdlt.state.{scope}`, written in a SEPARATE property commit AFTER the
  data commit; `ice.receipt.visible` sits between the two.
- **The identity scope is `ident_hash(pipeline, 12)`**, not the raw
  pipeline name, in the snapshot-summary keys
  `rdlt.pipeline`/`rdlt.load-id`/`rdlt.commit-seq`.

## Decisions

**D1 — Born on the sdk, one-dependency rule.** `DestinationConnector`
(Backend = the session type) + `destination::Shell` alias; SPI only via
`rdlt_connector_sdk::spi`. NO sqlcore (not a SQL destination — no
recorded exception applies). The iceberg library trio (iceberg,
iceberg-catalog-rest, iceberg-storage-opendal) stays at ONE boundary
module: library types never cross the public surface (gen 1's rule,
kept). sdk `test_dependency_rule` gains
`("rdlt-connector-iceberg-v2", &["rdlt-connector-sdk"])`.

**D2 — Typed ConfigError.** `Yaml(String)`/`Json(String)` variants via
`From` impls rendering parser text BARE (the 028 pattern), plus typed
validation variants rendering the inventory's 12 frozen spellings. The
partition-transform `singleton_map` spelling (`transform: day` vs
`transform: {bucket: 16}`) is preserved through the sdk Document path
(same serde_yaml machinery). `config_schema()` from the same structs.

**D3 — Frozen surfaces.** The contract inventory, in full. Notably: the
commit identity keys and scope hash; replay = snapshot-history scan for
(load-id, commit-seq); `COMMIT_ATTEMPTS = 4` with the shared
refresh→rebuild→commit retry loop and the
`"({subject} attempt {n}/4)"` context prefix over subjects
`commit`/`property commit`/`schema commit`; the conflict-exhaustion
spelling naming the table and the competing snapshot; the
`status_from_context` classification (401/403 fatal, 429 RateLimited,
other 4xx fatal, 5xx/absent transient) INCLUDING its parse-the-rendered-
error mechanism (pinned as-is; its fragility is review-loop material);
crash points `ice.files.write`/`ice.commit`/`ice.receipt.visible`, all
`crash_point!` macro form; the closed 12-row type table (Json→String);
additive drift = AddColumn::optional, id-ignoring drift comparison,
asymmetric nullability; partition names `{col}_day`/`{col}_bucket`/
`{col}_trunc`, partition field-ids from 1000, spec fixed at create;
Replace's typed refusal.

**D4 — Fresh design.** Modules by noun under `src/destination/` behind
pure-TOC mod.rs files (lib.rs = façade):
- `config.rs` — document vocabulary + validate + schema (the Document
  impl; nothing else renders config text).
- `client.rs` — THE library boundary: catalog construction (REST +
  opendal-s3 + credential vending), table load/create/refresh, the
  error-wrapping seam (classification lives here with
  `status_from_context`), nothing library-typed escapes.
- `schema.rs` — the closed type map, arrow↔iceberg conversion, drift
  detection (id-ignoring compare) and the additive UpdateSchema plan.
- `partition.rs` — transform vocabulary → PartitionSpec, the frozen
  naming rule.
- `commit.rs` — the identity properties, replay scan, the ONE bounded
  retry loop all three subjects share, snapshot-summary stamping.
- `state.rs` — the `_rdlt_state` marker table and the
  `rdlt.state.{scope}` property protocol.
- `write.rs` — parquet file writing through the library writer,
  `ice.files.write`.
- `load.rs` — the Backend: session state, ensure/write/existing_receipt/
  replay/publish choreography mapped onto snapshots (existing_receipt =
  history scan; replay = discard staged files; publish = data commit
  [`ice.commit`] → `ice.receipt.visible` → state property commit).
- `connector.rs` — `Iceberg` (DestinationConnector), capabilities
  (merge=false, structs/lists per gen-1 claims), connect = catalog
  handshake + namespace ensure + marker-table ensure, FAIL_POINTS,
  testhook.
Naming per the seven rules: `destination::Config`, `destination::
Iceberg`, `destination::Shell`; no crate-root re-export soup; booleans
as assertions. Tests = `tests/integration.rs` + `cases/test_<noun>.rs`
with sentence names; the sweep its own failpoints-gated binary; the
`iceberg-live` nextest group covers the v2 binaries during coexistence.

**D5 — Parity = coverage + needles, not ported files.** Fresh tests
answer the gen-1 census (57 default / 59 failpoints across 11 binaries)
via a coverage map in this plan; frozen spellings verified by needle
assertions; the conformance kit runs via Shell against the Polaris+
RUSTFS containers (skip-not-fail, `rdlt-test=1` labels); pyiceberg
read-back leg kept; the 12 suspicious inventory items are the review
loop's opening docket.

**D6 — Coexistence.** `publish = false`, consumed by nothing; the swap
(delete gen 1, rename, port facade `pipeline_spec` to
`destination::Config` + `Shell::new`) is the owner's decision, exactly
as 028 executed it.

## STATUS

- Branch created; contract inventory committed; this plan written.
- SRC COMPLETE (2026-08-02): all nine modules written fresh — config
  (the frozen vocabulary + 12 refusal spellings), client (the library
  boundary: classification with the status-anchor rule, catalog_props
  as the one credential-audit function), schema (closed type map,
  depth-first ids, id-ignoring drift), partition (spec building + the
  fixed-at-creation check), commit (identity keys, the ONE 4-attempt
  retry, fast-append with per-attempt replay re-check), state (the
  _rdlt_state marker protocol), write (plain/fanout writers + the
  parquet-properties seam, defaults COMPRESS), load (the Backend:
  align/window/reinstall choreography; existing_receipt deliberately
  None — the RECORDED D7 mapping decision: receipts are per-table
  snapshot properties, publish converges from partial state, no
  load-level receipt store exists), connector (capabilities, connect,
  FAIL_POINTS, testhook). 37/37 offline unit tests; clippy clean both
  feature shapes; sdk test_dependency_rule carries
  ("rdlt-connector-iceberg-v2", &["rdlt-connector-sdk"]).
- OFFLINE SUITE IN (7d672e71): document corpus vs schema (two gates
  cannot drift), Shell family through the gate, secrets grep-proof,
  UNGATED registry check (the 028 lesson), live-group membership pin;
  the nextest iceberg-live group extended over the v2 package. 43/43.
- RECORDED TOOL-REUSE DECISION: tests/fixtures/polaris_bootstrap.py is
  carried as-is from generation 1 — it is a stdlib PROTOCOL tool
  (SigV4 PUT-bucket + management-API create-catalog + grants), shared
  identically by both generations like tools/interop, not crate code;
  re-deriving hand-rolled SigV4 buys risk and nothing else. One copy
  remains at swap-in.
- NEXT: the container fixture in cases/common.rs (plain-podman
  host-network Polaris+RUSTFS, PID-derived ports, skip-not-fail), the
  live cells per the census, the sweep body, Makefile coexistence
  lines; then the review loop and gates (baseline 1011).


## REVIEW-DOCKET ADDITION (found live while authoring the suite, 2026-08-02)

A WRONG OAUTH CLIENT SECRET CLASSIFIES TRANSIENT: the library's token-
endpoint failure renders its context entry as `code: 400 Bad Request`
— not `status:` — so `status_from_context` reads nothing and the
frozen table's no-status arm classifies the deterministic credential
error TRANSIENT (an engine would retry it forever). Generation 1 runs
the identical parser over the identical flow — inherited. The
data-path 401 (bad bearer) DOES carry `status:` and classifies Fatal
with the credential advice (pinned live:
`a_rejected_token_is_fatal_with_advice`). Joins inventory item #1 in
the review loop's docket: teach the parser the `code:` spelling, or
classify the oauth `operation: auth` context — decided at review, not
mid-suite.
