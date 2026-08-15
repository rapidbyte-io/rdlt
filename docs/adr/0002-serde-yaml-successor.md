# ADR 0002 — serde_yaml successor (047 L11 decision by proof)

Status: EXECUTED (2026-08-15; recorded 2026-08-14). Decision for the
security-hardening feature 047, finding L11. The owner pulled the
named trigger and the honest rename landed — see Executed, below.
Amendments recorded here.

## 2026-08-15 security amendment

The maintained-fork decision does not address YAML graph expansion: both the
deprecated crate and its continuation materialize aliases into the target
value. Production pipeline and path-form connector-config parsing therefore
rejects anchors and aliases in raw YAML before deserialization. The 8 MiB read
cap is now enforced by a bounded read rather than a stat-then-read check. A
future parser migration must preserve both gates; dependency replacement is
not a substitute for them.

## 2026-08-15 second security amendment — the graph gate rebuilt, and where it lives

The first amendment's guard was a character scanner tracking quote state
alone, and one apostrophe inside a plain scalar (`pipeline: john's orders`)
misread as quote-open blinded it to every anchor and alias after it —
restoring the quadratic alias expansion it existed to refuse. Two facts
found while fixing it are recorded here because they bind the migration:

- The pinned `serde_yaml 0.9.34` keeps its event stream private (`mod
  loader`, `mod libyaml` — items inside are `pub` but the modules are not),
  so a pre-deserialization refusal on `Anchor`/`Alias` events is not
  available from the pinned crate. The replacement guard is therefore still
  a raw-text scanner — now a token-start tracker modeled on libyaml's own
  scanner dispatch (verified against the vendored `unsafe-libyaml` source),
  refusing `&`/`*` only where a token can start and refusing outright the
  spellings it cannot decide line-locally (quoted scalars spanning lines,
  quote/tag/block-scalar indicators where a plain scalar may continue,
  verbatim tags). Adversarial and acceptance pins live with it.
- `serde_yaml` deserializes the event prefix BEFORE surfacing a document's
  parse error (`de.rs` checks `document.error` after the visitor runs), so
  aliases parsed ahead of a late syntax error still expand. Any future
  event-based guard must refuse on the first graph event, not rely on the
  document failing to parse.

The guard moved from a private function in `rdlt`'s `pipeline_spec` to
`rdlt_connector_sdk::yaml` (`reject_graph_syntax`, plus the shared
`MAX_DOCUMENT_BYTES`), because the facade depends on the sdk and the sdk's
`Document::from_yaml` was itself an unguarded, uncapped serde_yaml seat —
both parse surfaces now answer to the one scanner. A successor crate that
exposes parser events publicly would allow replacing the scanner with a
true event-stream refusal; that remains the preferred shape and rides the
same owner trigger as the migration itself.

## Context

`serde_yaml` is pinned at `0.9.34+deprecated` — the crate's terminal,
self-labelled-deprecated release, with no maintained patch stream. It
is a runtime dependency on the pipeline-YAML parse surface of `rdlt`,
`rdlt-cli`, and `rdlt-connector-sdk`, and a dev/support dependency in
five more crates. The exposure is contained (documents are capped at
8 MiB *before* the read, `pipeline_spec.rs`; hand-written config only),
so L11 is Low severity — but a dependency with no upstream is a
standing supply-chain liability, and the decision of what to move to
should be made on evidence, not taste.

The load-bearing API surface the workspace uses:
`serde_yaml::from_str`, `serde_yaml::Error` (its `Display` is asserted
byte-for-byte by the config-refusal suites), `serde_yaml::Value` with
`as_mapping`/`as_str` (rdlt-bench), and — the one that constrains a
successor most — `serde_yaml::with::singleton_map`, which the sdk uses
to spell the tagged-enum connector-config vocabulary
(`pipeline_spec.rs`). Any successor must carry `singleton_map` and
render identical parse results and error text, or the frozen
vocabulary shifts.

## The proof

Each candidate was swapped in a throwaway worktree via a Cargo
`package =` alias (zero code churn, so the test is of the library
alone) and built against the YAML-consuming crates; drop-in survivors
were run against the full YAML suites (`facade`, `desugar`,
`connector_spec`, `pipeline_spec`, `spec_model`, `build_parity`, the
sdk config suite, the commit-policy parses — 165 tests, including
every error-spelling assertion).

- **serde_yml 0.0.13** — NOT drop-in. It tightens `from_str<T>` to
  `T: 'static`, which forces a `Self: 'static` bound onto the sdk's
  public `Document` trait (`rustc E0310` at
  `rdlt-connector-sdk/src/config.rs`). That is a public-surface change
  to a frozen trait, not a mechanical swap — disqualified. (Its own
  maintenance stability is also debated upstream.)

- **saphyr / saphyr-serde** — not applicable as a drop-in. `saphyr`
  is a low-level YAML 1.2 parser, not a serde `Deserializer`; adopting
  it means writing a YAML→`serde_json::Value` front-end and
  deserialising from that (the config types already have a
  `from_value` path, so this is viable — but it is a rewrite, not a
  swap, and removes the frozen-vocabulary coupling only by taking on a
  new hand-written layer). `saphyr-serde` is nascent. Out of scope for
  a like-for-like replacement; recorded as the longer-term shape if
  the fork route ever sours.

- **serde_yaml_ng 0.10.0** — DROP-IN GREEN. The conservative
  maintained continuation of `serde_yaml`: it preserves the 0.9 API
  including `with::singleton_map` and `Value`. Aliased in, the whole
  workspace compiled with zero code changes and all 165 YAML-suite
  tests passed, error spellings included — byte-identical behaviour on
  the frozen vocabulary.

## Decision

The recommended successor is **serde_yaml_ng**: it is the only
candidate proven drop-in and byte-identical, and it removes the
deprecated dependency without touching the frozen config surface.

The swap is left as an owner trigger rather than applied inside 047,
deliberately:

- `serde_yaml_ng` was not in L11's originally-scoped candidate set
  (serde_yml / saphyr); it is this proof's own finding, and adopting a
  new dependency on the CLI's primary parse surface is an owner call.
- The honest migration renames the dependency across the eight
  consuming crates and every `serde_yaml::` path (an alias that keeps
  the misleading `serde_yaml` key is the only one-line form, and it
  trades the deprecation debt for a naming-indirection debt) — a
  cross-cutting rename best done as one deliberate act, not a
  security-wave side effect.
- L11 is Low with contained exposure; the value here is the *proven
  path*, so the trigger is cheap whenever the owner opens it.

Named trigger to pull it: the honest rename to `serde_yaml_ng` across
all consumers, or — if the fork route sours — the saphyr front-end
rewrite. Until then the deprecated dependency is a *watched* dep, not
an unexamined one: a `serde_yaml` advisory or the 0.3.0 publish window
is the escalation point.

The proof stands on its own: a wrong migration (serde_yml's silent
public-trait bound) is worse than a watched deprecated dependency, and
this record is what keeps the swap from being a guess later.

## Executed (2026-08-15)

The owner pulled the named trigger; the honest rename landed on the
migration worktree off main @ c4441a1f.

- **Resolved version:** `serde_yaml_ng 0.10.0` (workspace entry
  `serde_yaml_ng = "0.10"`, exact version pinned by both lockfiles;
  0.10.0 is the crate's current release).
- **What the rename touched:** the workspace manifest plus all eight
  consuming crates' manifests (rdlt-core, rdlt-cli, rdlt, rdlt-bench,
  rdlt-runtime, rdlt-connector-sdk, rdlt-connector-client,
  rdlt-connector-reference), every `serde_yaml::` use path including
  the string-form serde attributes
  (`with = "serde_yaml_ng::with::singleton_map"`), and the sdk
  `Document` trait's public `From<serde_yaml_ng::Error>` bound. No
  `package =` alias, per this record's own rejection of the
  naming-indirection debt. The deprecated `serde_yaml` is absent from
  both `Cargo.lock` and `fuzz/Cargo.lock` — no transitive holdout.
- **Byte-identical re-verified, not inherited:** the full workspace
  suite ran 842/842 green after the rename with zero error-spelling
  drift — every full-string config-refusal pin passed unchanged, so
  the 2026-08-14 proof holds on the release actually shipped.
- **The event-guard hope did NOT materialize:** `serde_yaml_ng`
  0.10.0's `lib.rs` keeps `mod loader;` and `mod libyaml;` private,
  unchanged from the `serde_yaml` 0.9.34 it continues; its public
  surface is only `de`/`error`/`ser`/`value`/`mapping`/`with`. A
  first-graph-event refusal is therefore still not available from the
  dependency, and `rdlt_connector_sdk::yaml::reject_graph_syntax`
  remains the token-start scanner, deliberately unchanged (a forced
  worse guard was not substituted for the absent better one). The
  saphyr front-end remains the recorded event-level door, and the
  prefix-expansion amendment above still binds it: refuse on the FIRST
  graph event, never on the document failing to parse. The scanner's
  honest over-refusals (multiline quoted scalars, opener indicators at
  plain-scalar continuation positions, verbatim tags) accordingly
  stand; the 3L12 residue closes only when an event-level parser
  arrives.
- **Seed-repo next bump (`rdlt-connectors`, adjusts when it takes the
  sdk at this commit or later):** workspace `Cargo.toml` line
  `serde_yaml = "0.9"` → `serde_yaml_ng = "0.10"`; the eight member
  manifests carrying `serde_yaml = { workspace = true }` (rest,
  duckdb, file, iceberg, oracle, postgres, snowflake, examples-gate)
  → `serde_yaml_ng`; and every `serde_yaml::` path — the rest
  vocabulary's `with::singleton_map` serialize/deserialize calls and
  `Value`/`Value::Tagged` match (`source/config/vocabulary.rs`), the
  iceberg singleton_map string attribute
  (`destination/config.rs:249`), the six `#[from] serde_yaml::Error`
  config-error arms (rest, duckdb, file source+destination, oracle,
  postgres source+destination), snowflake's hand-written
  `From<serde_yaml::Error>` impl and its parser-text probe
  (`destination/config.rs`), and the test-side `from_str`/`to_string`
  uses (file format/config/options, iceberg test_document, rest
  test_robustness, examples-gate). The sdk's `Document` trait bound is
  now `From<serde_yaml_ng::Error>`, so the bump is compile-forced, not
  optional.
