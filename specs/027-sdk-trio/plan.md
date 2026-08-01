# 027 — The SDK trio: rdlt-connector, rdlt-connector-sdk, rdlt-testkit

The connector-facing foundation, rewritten as a deliberate SDK in three
layers — the same greenfield method as 025/026 (no code copied; every
identifier re-derived under the seven naming rules; operator/consumer
surfaces frozen and verified), applied to the widest surface yet: the SPI
reaches ~240 files across 10 crates, the testkit ~73.

- Layer 1 — `rdlt-connector`: the SMALL, STABLE protocol (semver-sacred).
- Layer 2 — `rdlt-connector-sdk`: NEW optional scaffolding connectors may
  use; the engine never depends on it; it may move fast.
- Layer 3 — `rdlt-testkit`: the verification half — "certified = passes
  the kits".

## STATUS — Wave 1 COMPLETE as `rdlt-connector-v2` (2026-08-01)

Branch `027-sdk-trio` off main @ cb130ee8 (the rest swap-in merge).

`crates/rdlt-connector-v2` is BUILT, from scratch under the no-copying
rule (every file re-derived from the generation-1 contract): 41 unit +
integration tests (including the object-safety pin, which matters more
here than in generation 1 because of the default async `check` methods)
plus the protocol doctest; zero warnings across every feature shape
(none, failpoints, schema, object-store, all); coexists UNCONSUMED
(publish = false, no in-tree consumer) per amended D3. What is new over
generation 1 — all sanctioned by D7/D8 and the ledger: `check()` probes,
error `context()` (single-frame rule, compiler-forced exhaustiveness),
`#[non_exhaustive]` capabilities with `with_*` declaration builders,
`OpenContext`, modules `parquet`/`store` (ledgered renames of
`output`/`objects`), `ByteSender`/`ByteReceiver` (rule-2 rename of
`ByteTx`/`ByteRx`), and a typed `parquet::OptionsError` replacing the
bare-`String` validate. The swap — porting engine + connectors +
testkit, bumping 0.3.0 (D4) — remains the owner's call; waves 2–5
(sdk crate, testkit rewrite, adoption, guide) follow separately.

GATE OF RECORD: full `make check` TWICE CLEAN on the review-complete
tree, both first attempt (reclaim + TIME_WAIT drain applied between
runs): 1027/1027 workspace tests (2 named instrument skips), all
sweeps, semver no update required, perf gate 0 regressed, cold start
23.6 / 23.9 ms vs the 40 ms bar, exit 0 both. Review round 1 (three
lenses, record below) closed with all eight findings fixed and pinned.

D9 IMPLEMENTED (owner-directed, ahead of the testkit wave): the four
gate env knobs deleted from the live tree — testkit's
`runtime_available()` probes directly, the snowflake credential gate
keeps its resolution rules and visible skip. The 8 knob-behavior tests
died with the knobs; the gate then ran TWICE CLEAN at exactly the
predicted 1019/1019 (both first attempt, cold 24.0 / 23.5 ms, exit 0) —
the count discipline verifying its own change.

## SWAP-IN — EXECUTED (2026-08-01, owner decision)

Generation 1 DELETED; `rdlt-connector-v2` renamed to `rdlt-connector`
(publish restored, docs.rs key back, "second generation" out of the
description); workspace version 0.2.0 → 0.3.0 (D4 — the 014-recorded
window finally lands; `cargo semver-checks` against the pinned baseline
registers the major change and requires no further bump). The consumer
port, ~240 files across 10 crates, all mechanical per the ledger:
`OpenCtx`→`OpenContext`, capability struct literals → `with_*` builder
chains (the functional-update sites collapse beautifully:
`self.inner.capabilities().with_merge(false)`), `ByteTx`/`ByteRx`→
`ByteSender`/`ByteReceiver` in the engine's three channel files,
`objects::is_recoverable`→`store::` in the file connector, and the typed
`parquet::OptionsError` at its two validate seams (one `.to_string()`,
one already formatting through Display). Workspace: 0 errors, 0 clippy
warnings, fmt clean; 989/989 tests (the pre-swap 1019 minus generation
1's own 30 — v2's 41 were already in the count as a member).

## Decision record

- D1. EXTRACTION-READINESS is a requirement, not a nice-to-have (owner
  direction: connectors will later move out of this repository). All
  three crates must be consumable out-of-tree: publishable, no
  workspace-internal reach-ins, no path-only contracts, kits usable
  against an external crate, feature spellings stable
  (`failpoints`, `schema`, `object-store`), and the vocabulary crate
  (`rdlt-core`) reachable ONLY through `rdlt_connector::core` from
  connector code — the measured status quo (no connector has a direct
  rdlt-core dependency; 143 path references route through the SPI) is
  hereby the RULE.
- D2. The 025 naming rules apply. Unlike 025/026 this surface's names
  are mostly already right (it is the youngest, most-reviewed code in
  the tree); rule 5 cuts both ways — rename where a name is WRONG, keep
  where it is right. The ledger below is deliberately short; churn for
  its own sake across 240 files is not cleanliness.
- D3. COEXISTING NEW CRATE `rdlt-connector-v2` (owner direction,
  superseding the first-recorded in-place plan): the 025/026
  two-generations method applies to the CRATE even though the traits
  cannot serve two masters — v2 coexists UNCONSUMED (publish = false, no
  in-tree consumer), its parity is proven by its own ported unit suite
  (the SPI's tests are self-contained: channel, secret, pem, output,
  objects, capabilities), and the consumer port is the SWAP, a separate
  owner-decided step exactly as it was for postgres and rest. An earlier
  in-place rewrite commit was made and then DROPPED (reset) in favor of
  this method; the workspace version bump (D4) moves to swap time with
  it.
- D4. The SEMVER WINDOW OPENS: workspace version 0.2.0 → 0.3.0 — the
  bump feature 014 recorded as owed at the next breaking publish. The
  gate's `cargo semver-checks --baseline-rev 34ccd379` then admits the
  renames (0.x minor is the breaking lane). Persisted DATA formats do
  NOT move (greenfield-no-compat covers Rust names only; WAL v2, StateDoc
  v1, cursor JSON, bench artifact v3 all stay).
- D5. `rdlt-connector-sdk` extracts ONLY what two or more connectors
  already implement message- and behavior-identically (config entry
  triple, error-context attachment, cursor watermarking). Anything one
  connector owns stays in that connector. The engine MUST NOT depend on
  the sdk crate — that boundary is what lets it move fast while the SPI
  stays sacred.
- D6. `rdlt-core` is OUT OF SCOPE (not in the trio; `crash_point!` and
  the vocabulary stay put). The engine's direct rdlt-core imports are
  likewise untouched except where a port forces a line.
- D7. Capability declarations become extendable: `DestinationCapabilities`
  is constructed by struct literal in every destination today, which
  makes ANY future field a breaking change for out-of-tree connectors —
  exactly what D1 forbids. It becomes `#[non_exhaustive]` with a
  conservative `Default` and `with_*` builders; the six in-tree
  destinations port to builder construction. Capability EXPANSION beyond
  what the engine consumes today is DEFERRED (trigger: engine-side
  per-mode validation) — speculative fields nobody plans from would
  violate the struct's own "truthful declaration" contract.
- D8. New SPI surface, minimal and consumer-driven:
  (a) `check()` on `Source` and `Destination` — a cheap connectivity
  probe with a default body returning Ok (documented "not probed"),
  the operationally-missed lifecycle step every peer SDK has;
  (b) `SourceError::context(...)` / `DestinationError::context(...)` —
  attach context around the INNER cause preserving classification and
  `retry_after`, making the double-framing defect (found independently
  in two connectors) inexpressible;
  (c) nothing else. RateLimited-for-destinations, richer check reports,
  capability matrices: all deferred with named triggers.

- D9. RESOURCE GATES LOSE THEIR KNOBS (owner direction, twice refined):
  after a no-assumptions audit of every env variable the workspace reads,
  the owner first replaced the `RDLT_TESTKIT_{FORCE_NO,REQUIRE}_*`
  boolean pairs with one tri-state knob per resource, then removed the
  knobs entirely. The final design, implemented at the testkit wave:
  - ONE behavior, the sane default: probe for the resource; if absent,
    print a visible `SKIP` line and pass. No env override demands the
    resource and none fakes its absence. All four old gate vars are
    DELETED, unaliased.
  - SUPERSESSION, stated not buried: this removes 024's "a resource
    probe can be DEMANDED" guarantee (GI: a skip stops reading as a
    pass). The remaining net is COUNT DISCIPLINE — nextest's run/skip
    counts, the gate-of-record convention of naming expected skips, and
    the testkit README's counts-interpretation table. A wrongly-skipping
    suite surfaces as a moved number a human compares, not a panic. The
    owner accepts that trade for the simpler surface.
  - CONNECTOR-AGNOSTICISM enforced at the seam: the testkit carries only
    what every connector shares — the container-runtime probe (probe
    order unchanged: DOCKER_HOST → podman user socket →
    /var/run/docker.sock → `podman ps`) and the reclaim label. A
    connector needing credentials (snowflake) owns its OWN probe in its
    own tests, same skip-not-fail posture; the testkit never names a
    connector's resource.
  - Unchanged: the `RDLT_SNOWFLAKE_*` credential DATA vars (they are the
    credentials, not switches); the cost-tier flags `RDLT_NET` /
    `RDLT_HEAVY` / `RDLT_DEEP` (they gate expense, not resource
    presence), unified on one read convention (set-to-`1` enables); the
    tool vars `RDLT_REPIN`, `RDLT_BENCH_FORCE`, `RDLT_INTEROP_PYTHON`.

## Frozen surfaces (the parity bar)

1. ERROR RENDERING, verbatim: the six classification frames
   `transient source error:` / `source rate limited:` /
   `fatal source error:` / `transient destination error:` /
   `destination rate limited:` / `fatal destination error:` — the first
   three are pinned EXACT-ONCE by rest's anti-double-frame tests; the
   word `fatal` is pinned by postgres. `records channel closed by host`
   kept (unpinned but public). Variant names and payload shapes kept:
   ~20 external `matches!`/match sites, snowflake destructures all three
   variants' payloads, engine's classify has (required) wildcard arms.
   Helper constructors `transient`/`rate_limited`/`fatal` kept.
2. CHANNEL SEMANTICS, verbatim (mutation-hardened): byte budget is the
   flow control and counts QUEUED bytes (permit rides with the value,
   released on drop); zero-byte checkpoints are never budget-gated;
   an oversized item degrades to drain-the-budget, never deadlocks;
   `close()` refuses further sends AND wakes a parked producer; message
   cap 64 secondary. Every existing channel test ports as written.
3. SECRET/PEM: serde-transparent, `***` from both Debug and Display
   (pinned twice), `reveal()` the sole accessor, schema feature's
   `{"type":"string"}` with the never-rendered description; PemSource
   read semantics.
4. SPI SHAPES: `ReadRequest`/`OpenCtx`-successor constructed via `new`
   at every one of the 35 sites (the hedge held — additions stay
   non-breaking); `ConnectorSpec`/`StreamSpec` serde field names
   (platform-facing, and `StreamSpec.structured` serde-defaults false);
   `StreamSpec` builder methods; `ConnectorSpec.config_schema` stays a
   settable field (6 production mutation sites).
5. TESTKIT CONTRACTS, verbatim (the 024 inheritance):
   - The two crate rules: SPI-only dependencies; connector-agnostic and
     feature-less.
   - The gate's CAPABILITIES (probe, skip-not-fail default, a way to
     DEMAND the resource so a skip cannot read as a pass, a way to force
     the absent path on a machine that has the resource) — the spelling
     is REDESIGNED under D9; the old two-boolean env pair is NOT frozen
     (owner direction, this feature).
   - `RECLAIM_LABEL = "rdlt-test"` with value "1" at start sites; the
     Makefile reclaim filter and the probe order (DOCKER_HOST → podman
     user socket → /var/run/docker.sock → `podman ps`) stay in sync.
   - THE SCANNER IS NOT SIMPLIFIED: two arming spellings
     (`crash_point!(`, `crash_at(`), union-of-registries per crate,
     vacuity guard FIRST (needle `no crash-point sites found`), two
     directions (armed⊆declared; declared appears twice — the indirect-
     arming escape), declaration blocks excluded BY SHAPE
     (`: &[&str] = &[` … `];`), text-scan-counts-comments deliberate,
     the declared can't-catch-double-deletion limitation stated, and the
     committed per-crate direct-name counts (engine 7, file 11, rest 3,
     iceberg 3, duckdb 2, snowflake 4, postgres 11) as the independent
     check. All rationale comments carry forward in substance.
   - Conformance clause IDs and their meanings (S1/S2/S4, D1–D6/D8,
     E1/E5/E6 as referenced from messages) — clause NUMBERING never
     changes; `ConformanceFailure` renders `violates clause {id}: …`;
     `assert_conformant` reports ALL failures at once.
   - `CrashDestination` fire-once/shared-across-clones semantics and
     `FaultPoint`'s three 1-based points; injected messages
     `injected crash: {what}`.
   - Memory connectors' clause semantics in full (D4-on-open staging
     wipe, D3 receipt replay, per-load Replace truncation, root-id
     subtree merge with last-wins dedup, migration coercion table,
     strict resume-after-checkpoint, fatal-on-unknown-cursor,
     Ok-on-closed-channel, E1 write-before-ensure, since-log-as-attempt-
     counter — named explicitly this time), `MemorySource::default()`
     (a facade doctest constructs it), inspection API names with their
     ~500 call sites.
   - Helper signatures `schema_for`/`batch_of`/`commit_meta_for`
     (~120 call sites).
6. FEATURE SPELLINGS: `failpoints`, `schema`, `object-store` on the SPI;
   the six connectors' forward wiring unchanged.

## Rename ledger (old → new)

| Old | New | Rule |
|---|---|---|
| `OpenCtx` | `OpenContext` | 2 (no ad-hoc truncations); 31 files, all via `::new` |
| module `output` | `parquet` | module named by its noun — the module IS the parquet-writing vocabulary; the types are root-re-exported so swap cost is near zero |
| module `objects` | `store` | module named by its noun (the object-store rule); `is_recoverable` is NOT root-re-exported, so the swap renames `objects::is_recoverable` → `store::is_recoverable` at its two file-connector call sites |
| SPI trait definitions inline in `lib.rs` | `source.rs` / `destination.rs`; `lib.rs` a TOC + re-exports | layout |
| testkit's dead direct `rdlt-core` dependency | REMOVED (vocabulary flows through the SPI — D1's rule applied to ourselves) | D1 |
| testkit README's `PgFixture`/`CdcPgFixture` reference | `fixtures::PostgresContainer` | staleness |
| iceberg's hardcoded `"rdlt-test=1"` + stale `containers` comment | derived from `RECLAIM_LABEL`; comment names `gate` | staleness |
| everything else | KEPT — D2's rule-5-cuts-both-ways | D2 |

## The crates

```
crates/rdlt-connector/src/
  lib.rs            — crate docs + TOC + re-exports (no trait bodies)
  source.rs         — Source (spec/check/streams/read) + ReadRequest
  destination.rs    — Destination (spec/check/capabilities/open) +
                      LoadSession + OpenContext
  error.rs          — taxonomy + context() attachment
  channel.rs        — byte-budget channel (semantics frozen)
  capabilities.rs   — non_exhaustive + Default + with_* builders (D7)
  spec.rs stream.rs secret.rs pem.rs parquet.rs store.rs — rewritten,
                      same public shapes (two module renames, ledgered)

crates/rdlt-connector-sdk/src/          (NEW, publishable, engine-free)
  lib.rs
  document.rs       — the config-document contract: one validate-once
                      gate behind from_yaml/from_json/from_value +
                      schema, with a per-connector SUBJECT so every
                      existing message needle ("invalid REST source
                      YAML: …") renders verbatim
  cursor.rs         — max-observed watermark (observe/merge/render,
                      string-or-number, skip-never-guess)
  (error-skeleton helpers only if Wave 4 proves two connectors
   message-identical; otherwise cursor+document ship alone)

crates/rdlt-testkit/src/
  lib.rs            — TOC + doctest (certify a memory source, no
                      network/containers/credentials)
  gate.rs fixtures.rs
  conformance/{mod,source,destination,failure}.rs
  crash/{mod,injector,registry}.rs
  memory/{mod,source,destination}.rs
```

Testkit rewrite improvements (from the dossier, contracts intact):
one logical→Arrow fixture derivation instead of two; the conformance
module docs claim EXACTLY the asserted clauses (S1/S2/S4, D1–D6/D8)
instead of ranges they don't cover — adding the missing clauses is
DEFERRED, renumbering forbidden; `verify_source`'s Arrow-payload
degradation to count-only checking stated in its docs; the scanner's
shape marker sensitivity to reformatting documented at the marker.

## Waves (each ends with a clean full gate)

1. `rdlt-connector` rewritten; consumers ported (engine, 6 connectors,
   sqlcore, testkit-as-is, facade, fuzz); version 0.3.0; semver gate
   green under the new baseline math.
2. `rdlt-connector-sdk` built with its own unit suite; no consumers yet.
3. `rdlt-testkit` rewritten; all 73 consumer files re-pointed; the
   selfcheck counts re-verified against the tree.
4. SDK adoption, one connector at a time, each gated: config triple →
   sdk::document; rest+postgres cursor tracking → sdk::cursor; rest's
   fanout context → SourceError::context. Zero-duplicate check: the
   config-triple pattern appears ONLY in the sdk when done.
5. Authoring guide (`docs/connector-authoring.md`): the seven naming
   rules, the reference architecture, frozen-surface method, kit
   contract, review-lens checklist. Then the adversarial review loop
   (025/026 protocol) until clean, and the gate TWICE CLEAN untouched.

## Verification checklist (close-out)

- [ ] Every consumer suite passes with assertions unchanged (parity).
- [ ] Pinned needles verified: six error frames, `***`, gate panics,
      scanner messages, clause renderings.
- [ ] Extraction-readiness: `cargo publish --dry-run` shape-clean for
      all three (modulo workspace-internal dev-deps), no connector
      imports `rdlt_core` directly, kits documented for out-of-tree use.
- [ ] Zero-duplicate audit: config triple, cursor tracking, context
      attachment each have ONE home.
- [ ] Naming audit; zero-warning bar; gate twice clean; review rounds
      recorded below.

## REVIEW ROUNDS (running record)

Round 1 on `rdlt-connector-v2` (three parallel lenses, 2026-08-01):

- CONTRACT PARITY vs generation 1: nine areas verified clean
  mechanically (all six error frames byte-identical, channel semantics
  item by item, Secret/PemSource, parquet options, the recoverability
  allow-list, spec/stream shapes, trait signatures, features). Two
  findings, both fixed: the `output`→`parquet` and `objects`→`store`
  module renames were real but unledgered (now ledgered, with the
  `store::is_recoverable` swap note); generation 1's object-safety pin
  had not been ported (now `tests/cases/test_object_safety.rs`, written
  fresh — the coercions are the assertion, and the defaulted `check`
  dispatches through the vtable in the proof).
- BUG SCAN: no findings at the confidence bar. Verified sound: the
  close() ordering across mpsc + semaphore, permit-rides-with-value,
  `context()` classification survival, dyn-compatibility with default
  async methods, the serde-default traps. Recorded below-bar: the u32
  saturation arm has no test (it needs a >4 GiB budget; the degradation
  is documented at the arm).
- NAMING/COMMENT AUDIT: six items, all applied — `ByteTx`/`ByteRx` →
  `ByteSender`/`ByteReceiver` (the same rule-2 class as `OpenCtx`);
  the manifest's coexistence comment reworded generation-neutrally;
  lib.rs's serde claim made exact (declaration/state vocabulary is
  serde; record payloads are wire forms); `context()`'s doc now states
  the downcast boundary (the cause re-boxes as rendered text);
  `parquet::validate` returns a typed `OptionsError` named by what
  failed (message text verbatim, so every needle holds); the semver
  sentence names the gate rather than CI. Rules 1/3/4/5/6 and the
  comment standard verified clean explicitly, including a line-by-line
  README accuracy pass.
