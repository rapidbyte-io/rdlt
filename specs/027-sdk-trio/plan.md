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

## STATUS — IN PROGRESS (started 2026-08-01)

Branch `027-sdk-trio` off main @ cb130ee8 (the rest swap-in merge).

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
- D3. IN-PLACE REPLACEMENT, not coexistence: a trait crate cannot
  usefully coexist with its successor (every implementation names ONE
  Source trait), so the 025 two-generations method does not apply.
  Instead: rewrite lands on this branch wave by wave, the full gate runs
  green at every wave boundary, and the ported consumer suites (unchanged
  assertions) are the parity proof.
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
   - Env vars `RDLT_TESTKIT_FORCE_NO_CONTAINERS` /
     `RDLT_TESTKIT_REQUIRE_CONTAINERS` (any value counts as set),
     both-set panics with needle `are both set`, demand-fail panics with
     needle `RDLT_TESTKIT_REQUIRE_CONTAINERS is set but no container
     runtime`; the `RDLT_TESTKIT_` prefix is shared vocabulary with the
     snowflake credential gate and does not move.
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
  spec.rs stream.rs secret.rs pem.rs output.rs objects.rs — rewritten,
                      same public shapes

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

(none yet)
