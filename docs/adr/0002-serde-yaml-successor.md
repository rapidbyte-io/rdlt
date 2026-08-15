# ADR 0002 — serde_yaml successor (047 L11 decision by proof)

Status: RECORDED (2026-08-14). Decision for the security-hardening
feature 047, finding L11. Recommends a migration target; the swap
itself is an owner trigger (see Decision). Amendments recorded here.

## 2026-08-15 security amendment

The maintained-fork decision does not address YAML graph expansion: both the
deprecated crate and its continuation materialize aliases into the target
value. Production pipeline and path-form connector-config parsing therefore
rejects anchors and aliases in raw YAML before deserialization. The 8 MiB read
cap is now enforced by a bounded read rather than a stat-then-read check. A
future parser migration must preserve both gates; dependency replacement is
not a substitute for them.

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
