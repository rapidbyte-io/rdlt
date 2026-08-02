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

**D2 — Typed ConfigError.** `Yaml(#[from] serde_yaml::Error)` /
`Json(#[from] serde_json::Error)` rendered with generation 1's FROZEN
framing (`invalid iceberg destination YAML: {0}` etc.) — D3's frozen
surface forecloses the 028 bare-text pattern here — plus the `Invalid`
variant rendering the inventory's 12 frozen spellings. (Amended at
round 3: the original prose described the 028 shape the freeze does
not permit; the code was always as-built.) The
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
- `load.rs` — the Backend: session state, ensure/write/publish
  choreography mapped onto snapshots (existing_receipt deliberately
  None — superseded by the recorded D7 decision; publish = per-table
  history-checked data commits [`ice.commit`] → `ice.receipt.visible`
  → state property commit).
- `connector.rs` — `Iceberg` (DestinationConnector), capabilities
  (merge=false, structs/lists per gen-1 claims), connect = writer
  properties (pure, FIRST — amended at round 3) + catalog handshake +
  namespace ensure (the `_rdlt_state` marker table is created LAZILY
  on first state write, gen-1 parity — the sketch originally said
  connect ensures it; it never did), FAIL_POINTS, testhook.
- `testsupport.rs` (cfg(test)) — the ConflictCatalog mock +
  memory-FileIO table builders (added to this list at round 3; present
  since the build).
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

**D7 — The receipt mapping (numbered at round 3; decided at build
time and recorded in STATUS).** `existing_receipt` deliberately
answers `None`: receipts are PER-TABLE snapshot properties, a
partially published commit is exactly the state `publish` knows how to
converge from (the per-table `already_committed` check against fresh
metadata), and no load-level receipt store exists — answering `Some`
would claim an atomicity the catalog does not offer. The sdk
choreography tolerates this: `replay` is never invoked and `publish`
carries the at-least-once burden its history scan discharges.

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


## COVERAGE MAP (fresh suite vs the gen-1 census, 2026-08-02;
REFRESHED at round 3 — the original snapshot predated the review
rounds' cells)

Crate totals AT ROUND 3: 70 default tests (41 unit + 29 integration)
+ 2 failpoints-gated sweep tests. Rows added by the review rounds:
test_evolution (mid-window retirement, cross-load ALTER, reserved
name, closed namespace, shared-table refusal, normalized-key
semantics), test_providers::open_failures, the exactly-once raw-state
readback, test_quickstart (README drift pin), and the offline
drift-refusal/retirement/backoff pins in src. Original census answers
(counts as of the pre-review snapshot):
- config_schema (9) → cases/test_document (schema-vs-parser corpus,
  unknown-field parity both gates, Shell family, secrets) +
  cases/test_gating (UNGATED registry check — the 028 lesson — and the
  live-group membership pin, now over the explicit binary list) + the
  config/client unit matrices in src.
- catalog_live (5) → test_ingestion (exact totals + identity props via
  the raw-REST oracle; empty window publishes NO snapshot) + the
  fixture smoke implicit in every cell.
- exactly_once (4) → test_exactly_once: the TRUE SPI replay (two
  sessions, same (load, seq) → one snapshot), the engine-level RESUME
  cell (republishes nothing, exact totals), narrowed-stream null-fill,
  Replace refused live with the frozen spelling.
- conflict (1) → test_concurrency::two_live_writers_lose_no_snapshot
  (two pipelines × 4 commits, 8 snapshots, 16 rows exact).
- partitioning (3) → test_partitioning (bucket spec in raw metadata:
  field-id 1000 + `id_bucket` + `bucket[4]`; spec mismatch refused
  with drop-or-align) + the unit naming/id pins in partition.rs.
- providers (4) → test_providers (override works; WRONG override
  FAILS; bearer loads; data-path 401 = Fatal with credential advice).
- nested_types (1) → test_concurrency::a_nested_stream_loads_twice…
  (+ the RED unit pins in schema.rs, which the confirmation cell backs).
- auth_probe (1) → folded into test_providers (the 401 cell).
- interop (3→1) → test_interop: pyiceberg reads back the partitioned
  load (count, partition field, identity props) — skip-not-fail
  without the venv, flag-form argv matching the shared tool. The
  plain/post-drift readback variants are covered by the same script
  path; recorded as a deliberate consolidation.
- spark_deep (1) → DEFERRED TO SWAP-IN, recorded: the deep tier is
  by-hand (RDLT_DEEP=1), its Makefile line names generation 1, and the
  shared script is unchanged — the line binds to this crate at rename.
- sweep (2) → crash_sweep binary: 3 points × 3 actions armed-twice-
  recover through the ENGINE, exact totals, duplicate-free identity
  set, fired == full matrix; registry check in the sweep AND ungated.
  Wired into `make test TARGET=sweep` beside generation 1 (the
  coexistence line dies at swap-in).
- NEW over generation 1: the sdk conformance kit CERTIFIES the shell
  live (test_conformance — gen 1 predated the kit), and the review
  docket carries the live-found oauth `code:`-context classification
  gap.


## REVIEW ROUND 1 (three parallel lenses, 2026-08-02)

CONTRACT FIDELITY: CLEAN across every area (config spellings,
classification, identity/retry/backoff, state protocol, crash points,
type map/drift, partitioning, session semantics, library boundary —
per-area confidence 97-99); D7 verified sound and tolerated by the sdk
choreography. ANTI-TRANSCRIPTION: fresh at design level (merged
boundary module, extracted partition noun, relocated Drift rendering,
re-typed comparisons, wholly re-authored integration tree — 0/208
distinctive gen-1 test comment lines reappear); five flagged passages
all addressed below.

FIXED, with pins:

1. TWO STREAMS, ONE TABLE (new find, high severity): nothing refused
   two streams resolving to one physical table — colliding data-file
   paths (same load/window/nonce prefixes) let one stream's bytes
   shadow the other's, and the shared commit identity read the second
   stream's publish as a REPLAY, discarding its files silently. Now
   refused TWICE: the config gate rejects duplicate explicit names
   (new spelling, recorded: "tables.{a} and tables.{b} both resolve to
   table `{name}` — two streams may not share one table"), and ensure
   rejects the rename-onto-default collision only visible at
   resolution time. Pinned offline (the refusal matrix) and live
   (`two_streams_sharing_one_table_are_refused`).
2. THE STATUS PARSER, REVISED (docket #1 + the live find, one
   change): the scanned context block is now TRUNCATED at its closing
   ` } => ` (the tail-scan made the "outside the block" defense a
   false claim — the old pinning test's spoof merely lacked a comma),
   and the parser accepts the `code:` entry key beside `status:` — the
   library's token-endpoint path attaches the HTTP status under
   `code`, so a wrong client secret (deterministic 400/401) classified
   TRANSIENT and retried forever, in BOTH generations; both attachment
   sites verified in the library source (the third `code` site rides
   DataInvalid and never reaches the parser). Behavior improvement
   recorded; pinned: the code-key classification, the credential
   advice on a code-carried 401, and the tail spoof.
3. POLARIS PINNED BY DIGEST (docket #3): `latest` re-resolved on every
   upstream push; the digest every recorded gate ran against is now
   explicit (no stable upstream version tag exists).
4. SESSION NONCE carries the pid (docket #10): closes the
   two-processes-in-one-nanosecond window the per-process counter
   cannot see.
5. TEST ADEQUACY (that lens's findings): the mid-window
   schema-evolution path — the 028 defect class, previously defended
   by unexercised code — is now pinned LIVE
   (`a_column_added_mid_window_keeps_every_row`: two batches one
   checkpoint, writer retired, 2 rows in 2 data files under ONE
   snapshot, the evolved column visible in raw metadata); additive
   cross-load evolution pinned live; the reserved-name and
   closed-namespace refusals pinned live with their frozen spellings;
   open failures pinned end-to-end (wrong warehouse named; dead uri
   typed — the catalog handshake is LAZY, surfacing at the first
   namespace operation, which the cell now documents); the
   wrong-credentials leg gained a context needle; the resume cell now
   also reads the RAW state protocol through the testhook; the interop
   module doc no longer overstates; the dead `connect_catalog` hook
   removed (every remaining hook is used). Crate suite 60 default + 5
   evolution/refusal cells = 65, all green live.
6. README shipped (the 028 pattern: written pre-swap for the name the
   crate will carry), including the catalog.props Secret-bypass
   warning (docket #2's docs half).
7. ANTI-TRANSCRIPTION flags: both verbatim prose sentences rewritten;
   the align/state test literals re-derived; the container fixture's
   carry is now RECORDED (below) alongside the bootstrap tool's.

RECORDED, NOT CHANGED:

- N2 (design-level, the most consequential record): WITHOUT a WAL
  workdir, a mid-publish transient failure restarts the run under a
  NEW load id, and snapshot-history convergence cannot recognise the
  prior attempt's partial commits — table A's rows can append twice.
  WAL recovery (the tested path) replays under the SPAN's original
  load id and converges. Owner options: require/document workdir for
  this destination, retry transients inside publish, or a
  retry-stable identity component. Inherent to the catalog's
  non-atomic multi-table publish; generation-1 parity.
- N3: the library caches the OAuth token forever (upstream TODO); a
  load outliving the TTL fails FATAL with credential advice for a
  credential that is correct. Library limitation, recorded.
- Docket #2 (props override Secrets): deliberate, documented, pinned;
  README warns. #4 (ice.files.write at writer open): naming nit; the
  ID is frozen and every point between open and commit is equivalent
  to recovery. #6 (linear history scan): O(already-loaded metadata);
  the real bound is snapshot expiry, already documented. #9 (12-hex
  scope): accepted risk — state collision degrades to a clean
  first-run; replay collision additionally needs equal engine-minted
  load ids. #11 ("capabilities.merge = false" in an operator string):
  the frozen spelling; recorded for the next sanctioned wording
  window. Fixture carry: tests/cases/common.rs re-derives gen 1's
  fixture with rewritten prose but carries its measured mechanics
  (the PID-derived port formula, readiness rules, image envs) — like
  the bootstrap tool, those mechanics are shared verified
  infrastructure, now recorded as such.
- REFUTED by the lens: docket #5 (all-empty-commit state write is
  deliberate and correct), #7 (list-element nullability is a forced
  choice; no known catalog rewrites it), #8 (no double-validate in
  v2 — one Document gate), #12 (shipped behavior frozen, verified).


## REVIEW ROUND 2 (verification lens on round 1, 2026-08-02) — TERMINUS

Every attack on the round-1 changes held: the shared-table refusal is
deterministic (BTreeMap order), covers BOTH ensure orders and every
naming combination (including the explicit-onto-literal and the
correctly-not-refused freed-name case), excludes self on re-ensure
(the live mid-window cell rides exactly that path); the status
parser's truncation is fail-safe under early ` } => ` values
(insertion-order rendering puts genuine keys first — verified in the
library's Display), the `code:` acceptance is safe against every
attachment site in the whole dependency tree (opendal attaches
neither key; non-numeric first tokens parse to None → transient),
and no frozen message can be shadowed by the new config refusal (the
first empty name fires inside its own iteration). Nonce, digest
(RepoDigest verified locally), and both new live cells' teeth
confirmed — the mid-window cell fails in a DIFFERENT diagnostic way
for each plausible regression.

ONE FINDING, F1 (the 024 vacuous-pin class, caught by MUTATION): the
tail-spoof pin carried no context block, so the parser bailed before
the truncation it claimed to pin — the verifier deleted the truncation
and the pin stayed green. FIXED: the spoof now rides a message tail
BEHIND a real context entry, and the mutation was re-run both ways —
RED without the truncation, green with it. Test-only; no code change.

TERMINUS: all code attacks held across rounds 1-2; the loop closes.


## GATE OF RECORD (2026-08-02, tree @ 627e2757) — FEATURE COMPLETE

`make check` TWICE CLEAN, first attempt both, untouched between runs,
`env -u RUSTUP_TOOLCHAIN`, reclaim + corpse-sweep + drain before each.
COUNT PREDICTED AND VERIFIED: 1076/1076 both runs (post-028 baseline
1011 + this crate's 65), 0 skipped. The `TARGET=sweep` leg ran BOTH
generations' iceberg sweeps (the coexistence line dies at swap-in);
semver no update required; 6 benches 0 regressed; cold start 23.1/23.6
ms (bar <= 40).

029 IS COMPLETE: contract inventoried and committed (with three
corrections to the 016-era prose — shipped behavior frozen), built
TRUE-greenfield on the sdk under the one-dependency rule (nine modules
by noun, the D7 receipt-mapping decision recorded), the fresh suite
green live end to end (65 default + 2 sweep tests: the conformance
kit certifies the shell — new over generation 1 — plus exactly-once
replay/resume, mid-window evolution, partitioning, providers,
concurrency, and the full crash sweep), the review loop closed at
round 2 terminus (the shared-table silent-corruption refusal and the
status-parser revision — a wrong client secret classified
transient-forever in BOTH generations — fixed and pinned; N2's no-WAL
retry-duplication is the standing owner record), and the gates twice
clean at the predicted count. The crate coexists UNCONSUMED as
`rdlt-connector-iceberg-v2`; the swap (delete generation 1, rename,
port the facade pipeline_spec to `destination::Config` + `Shell::new`)
is the owner's decision, per the 028 precedent.


## REVIEW ROUND 3 (four parallel lenses over the whole branch, 2026-08-02)

Compliance: CLEAN (one-dependency rule, layout, naming, error
taxonomy, rustdoc -D warnings run and passing) with two minor items.
Fresh-eyes: no gate-breaking defect; every prompted attack cleared
(writer failure paths, WAL-case publish convergence verified against
the engine's recovery driver, readiness budgets start after the pull,
guard drops on panic, filter grammar). Docs: no false behavior claims;
six documentation-drift items. Test adequacy: one defect-grade vacuity
plus cheap adjacent pins. ALL DISPOSED:

FIXED, with pins:
1. The `tables` map's doc claimed "keyed by STREAM name"; the truth
   (gen-1 frozen semantics) is the ENGINE'S NORMALIZED root-table
   name — a stream `Order-Items` reads its options under
   `order_items`, and the old doc invited a silent config miss.
   Doc corrected; pinned LIVE (`table_options_key_on_the_normalized_name`).
2. The contradictory-drift refusal's ENFORCEMENT site was
   test-invisible (the round-2 class one layer up: deleting reconcile's
   check would silently arrow-cast the mismatch with every layer-below
   pin green). Pinned offline against the mock catalog, including the
   nothing-commits-past-the-refusal count.
3. The writer-retirement choreography gained its OFFLINE pin (a
   container-less gate previously never exercised it): identical
   re-ensure keeps the writer, a changed target retires it and parks
   its files, the window counter survives — driven against a real
   memory-FileIO writer.
4. Backoff was entirely untested (a deleted sleep passed 67 tests):
   the delay computation split pure and pinned — doubling base, jitter
   inside the window, per-writer reproducibility, cross-writer
   divergence.
5. `connect` now resolves writer properties FIRST: the translation is
   pure and can fail on level ranges the config gate cannot see, and a
   refusal must not leave a freshly created namespace as its trace.
6. The README quickstart is now drift-pinned verbatim through the
   Shell (the snowflake convention); "digest-pinned images" corrected
   to name which pin is which; the Makefile coexistence comments now
   state the REAL swap mechanics (the surviving sweep and deep lines
   need their binary filters edited at rename — an unedited filter
   empty-selects, which post-024 fails loudly).
7. Plan drift amended in place: D2's prose matched to the as-built
   frozen framing; D4's connector sketch corrected (lazy marker table;
   properties-first ordering) and testsupport added to the module
   list; D7 promoted to a numbered decision; the coverage map
   refreshed with the review rounds' cells.

RECORDED, NOT CHANGED:
- RateLimited (429) is unit-pinned; no live leg exists because Polaris
  exposes no throttling knob — genuinely unprovokable live.
- The fixture's port TOCTOU residual (probe-then-bind window while a
  JVM boots) is mitigated by the PID-disjoint below-ephemeral range +
  bind probe; ~0.26% per concurrent pair worst case, visible as an 80s
  readiness panic — on the flake-log suspect list, not fixable short
  of per-fixture networks that host networking forecloses.
- All v2 binaries (including offline cells) ride the 3-thread
  iceberg-live group — a scheduling inefficiency, deliberate for
  simplicity during coexistence.


## REVIEW ROUND 4 (verification lens on round 3, 2026-08-02) — CLEAN, TERMINUS

Every round-3 change verified sound: the connect reorder breaks no
pinned precedence (the frozen surface is spellings, not incidental
failure sequencing; the improvement is recorded); the backoff pin is
deterministic with a divergence-flake probability of ~1 in 2.6×10²¹
per process and independently restates the formula (not a tautology);
the drift-refusal pin dies with the enforcement check and its
zero-commits assert is airtight; the retirement pin is genuinely
offline (memory FileIO, no network) — its identical-branch now feeds a
RECOMPUTED target so a value-eq→pointer-eq regression fails rather
than silently retiring every window (the lens's one note, taken); the
quickstart const is byte-identical to the README block; the
normalized-key cell's tooth is the table-uuid assert (a 404 parses as
an error body, loudly failing the assert — noted so nobody mistakes
the expect for the tooth); the map's 70 = 41 + 29 recounted exact.

THE LOOP'S FULL SHAPE ACROSS THE FEATURE: rounds 1 and 3 found (and
fixed, each with pins), rounds 2 and 4 verified clean — terminus at
round 4. The standing owner records: N2 (no-WAL retry duplication),
N3 (token TTL), the props-override Secret bypass, the 12-hex scope
risk, the frozen "capabilities.merge = false" wording, the fixture
port-TOCTOU residual, and the swap-mechanics edits owed to the
Makefile's surviving lines.
