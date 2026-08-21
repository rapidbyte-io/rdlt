# REVIEW_ITEMS — the ten moves that matter next

Reviewed 2026-08-21 on main @ `f27c2119` (the 076-deepening merge), full
tree. This document is a snapshot review, not a spec: each item names
the evidence, why NOW, and the shape of the work. Order is priority —
each item states what it unblocks, because that is the real ranking
criterion.

---

## The health snapshot (what the review found)

The codebase is in the strongest state of its history, and the review
says so with numbers rather than adjectives:

| Axis | State |
|---|---|
| Gate | `make check` twice clean at 076 close: **1054/1054** workspace tests + every sweep suite + e2e, 0 skipped instruments requiring containers |
| Semver | `cargo semver-checks`: no update required across all published crates after a nine-commit structural branch — the frozen surfaces held |
| Cold start | **5.5 ms** median end-to-end including two connector spawn+handshakes (bar ≤40 ms) — the reference-connector regime |
| Performance identity | Four bars holding out-of-process at their recorded values; wire overhead bounded and measured; budget metering rides slice-length footprint walks everywhere |
| Exactly-once | Proven by failpoint sweeps AND a nine-arm SIGKILL matrix at every message boundary, re-run converged, both in-process and over the wire |
| Certification | `rdlt-certify` drives any executable against 29 named clauses; S5/S6/D7 now judged by ONE law at ONE strength on both sides (076) |
| Depth | The 076 review found almost no shallow modules; the friction class it did find (prose-pinned mirrors) is now eliminated at all eight sites |

The remaining risk is not inside the code. It is that the code is still
largely invisible to the outside: five crates are `publish = false`, the
0.3.0 window has never shipped, and several recorded doors (telemetry,
network transport, third-party authorship) are waiting on owner
scheduling rather than on engineering risk. The ten items below are
ordered to convert that standing potential into compounding value.

---

## 1 · Ship the publish wave — cut 0.3.0 for real

**What.** Flip the five contract-surface crates (`rdlt-connector-client`,
`-protocol`, `-runtime`, `-certify`, `-bench`) from `publish = false` to
publishable, respell the connectors repo's git dependencies to version
requirements crate-by-crate (the 023 packaging rule makes git-without-
version REFUSE package today — deliberate, but it also means nothing
outside this org can resolve the SDK), and publish the workspace at
0.3.0.

**Why now.** Every other item on this list compounds only once the
crates are consumable. The semver gate has said "no update required"
through two structural programs; the API has been frozen-behaving for
months; ADR 0001 explicitly deferred publishing to "the publish wave,
owner-scheduled" — the wave is the item. The honest migration mechanics
are already recorded (ADR 0002's seed-repo bump list is the template:
manifest lines, `serde_yaml_ng` paths, compile-forced trait bounds).

**Cost / risk.** Mostly mechanical plus one naming decision per crate.
Risk of NOT doing it: the longer the window stays open, the more the
"unpublished" posture licenses surface churn that a published crate
would have had to answer for — the window itself is the debt.

## 2 · Execute feature 036 — the telemetry spine

**What.** The planned-not-started feature (`specs/036-cli/plan.md`),
whose D1 is the spine: extend the `PipelineEvent` feed additively
(`BatchRead`, `CommitStarted`, `PartClosed{encoded_bytes, reason}`,
`Heartbeat` — no fake ETAs), mint the canonical `Metrics` fold in
`rdlt-core` that CLI and embedders SHARE, and formalize the tracing
span/field contract. The CLI is consumer #1, not the product.

**Why now.** rdlt's pitch is embeddability, and an embedder's first
question is "what is my run doing right now" answered through a library
type, not by parsing stdout. The report JSON already pins final honesty
(final numbers always from the exactly-once RunReport); 036 extends
that honesty to live totals. The event feed is additive and
`#[non_exhaustive]`, so the wire-era rules make this safe to land
pre-publish — but item 1 and this item should sequence deliberately,
because landing telemetry BEFORE freezing the publish makes 0.3.0 the
version everyone starts on.

**Cost / risk.** One feature-sized increment under the house discipline.
The `PartClosed{encoded_bytes}` event is where output MB/s becomes REAL
(BatchLoaded.bytes is Arrow footprint and differs 5–10× from encoded) —
a measurement correction, not just new data.

## 3 · Re-certify the benchmark bars on the current tree

**What.** One recorded benchmark session, all five e2e cells + their
spawned twins, after two waves of change since the last recording: 075
bumped every dependency to newest stable, and 076 restructured the wire
seats (shared IPC encode/decode, moved render stack).

**Why now.** The bars ARE the project's performance identity ("the
remote numbers are now the benchmark identity", ADR 0001 D1). The gate
re-checks cold start, not throughput bars; those bind only in a
recorded session. 076 made the hot path marginally different (one
function call into the SPI gate per frame, sink-rendered causes) —
almost certainly neutral-to-positive, but "almost certainly" is not the
house standard, and the recorded-session floor requirement exists so
this question is answered by measurement. Cheap, high assurance.

**Cost / risk.** Half a day of machine time plus the RESULTS.md entry.
If a bar fails: stop, diagnose, do not re-record to clear it (the 022
lesson stands).

## 4 · Bind the same proto to TCP+mTLS — the network transport door

**What.** ADR 0001 D3 records network transports as "a future binding
of the SAME proto, provider-side". Build the second transport: the
client dials an address instead of a UDS path; the serve side accepts a
listener; mTLS at the provider layer. Zero proto bytes move (additive
rules already permit everything needed).

**Why now.** Three unlocks in one move: remote connector fleets (the
rapidbyte business layer's actual deployment shape), connectors on hosts
that are not the engine's host, and development on platforms without
Unix domain sockets (Windows). The codebase's own architecture review
showed the trust-boundary gates are direction-correct already — they
gate CONNECTOR-authored bytes whichever socket carries them — but a
network peer is a strictly stronger adversary than localhost, so this
item must ship with its own threat-model pass over
`rdlt_connector::gate` and a certify matrix arm over TCP.

**Cost / risk.** Medium-large. The freeze's escape hatches were designed
for exactly this; the risk is scope creep into fleet management, which
ADR 0001 places forever out of rdlt's scope — hold that line.

## 5 · Make third-party authorship REAL, not demonstrated

**What.** The polyglot claim rests on ONE deliberately-small Python
connector written once as proof (feature 040). Grow it into the
supported non-Rust entry point: a maintained template repository
(handshake line, one stream, one table, the certifier wired as CI),
a walkthrough in `docs/connector-authoring.md` driving it end-to-end,
and — closing the two owed clauses ADR 0001 names — a THIRD PARTY's
measurement of P11 (multi-batch Write refusal) and P12 (cause-text
rule) over the write direction, which are in-tree-pinned but
externally-unmeasured.

**Why now.** D8's entire purpose was third-party connectors; the
certifier-as-boundary design means the template repo IS the adoption
path (a connector enters when `rdlt-certify` passes it). Every month
the template doesn't exist, the protocol's polyglot claim ages from
"demonstrated" toward "asserted".

**Cost / risk.** Small-medium; mostly documentation and one CI wiring.
Also the cheapest possible external feedback loop on the frozen
contract while it is still cheap to learn from outsiders.

## 6 · Exercise the format-evolution escape hatch: negotiate a v2

**What.** The handshake's `state_format_versions_json` field is frozen
at its number and ships EMPTY because there is nothing to negotiate —
no second format version exists. Mint one: a v2 of the cursor or
StateDoc shape (the standing publish-time bump 020 US5 deferred is the
natural candidate), refused-or-negotiated up front per the 037
version-gate discipline, certified by a clause that drives a v1-writer
against a v2-reader and vice versa.

**Why now.** The handshake negotiation was designed but never run —
untested machinery that third parties will rely on before it has ever
fired. Doing it once, deliberately, proves the whole evolution story
(persisted-format gates, refusal spellings, resume behavior across
versions) while the ecosystem is still exactly one party deep.

**Cost / risk.** One feature-sized increment. The failure mode it
prevents is the worst kind: discovering mid-adoption that the
negotiation path has a defect nobody ever executed.

## 7 · Assemble the wire memory story into ONE document — then decide ReadCredit with data

**What.** The composition invariant — "a stalled reader bounds total
in-flight memory to the byte budget + READ_CHANNEL_BUDGET (8 MiB) + one
push in hand + one queued frame" — is provable today only by reading
four files in order (`serve/source.rs` budgets → client `wire.rs`
window sizing → runtime `local.rs` default → engine channel). Write the
one document that owns it (the protocol README is the natural home),
and add ONE adversarial stress cell: maximal batches, slowest legal
consumer, peak-RSS instrumented, both transports once item 4 lands.

**Why now.** The ReadCredit escape hatch stays documented-but-unbuilt
precisely because flow control was never SHOWN insufficient — but it
was never shown sufficient either, at batch sizes larger than the bench
matrix uses. One measured cell converts "likely fine" into either a
closed question or a scoped addition. And the document ends the last
locality gap the 076 review found in the seam.

**Cost / risk.** Days, not weeks. Risk if skipped: someone builds
ReadCredit speculatively (complexity the wire froze to avoid), or a
large-batch workload discovers the bound empirically in production.

## 8 · Operate the two-repo model like a product

**What.** Since 044 the connectors live in `rdlt-connectors`, consuming
this repo via lockfile-pinned git dependencies, entering only through
`rdlt-certify`. That boundary exists on paper; give it its operating
rituals: a pinned version-matrix document (engine rev ↔ connectors rev,
updated by the deliberate `cargo update`), a CI job in the connectors
repo that runs `rdlt-certify` on every connector PR against a CHOSEN
engine revision (not just the lockfile's), and a recorded cadence for
pulling engine changes forward.

**Why now.** Two repos drift quietly until something forces the
conversation. The seed commit proved decoupling is MEASURED (932 tests
green standalone before and after the cut); keeping that property is
routine work that compounds — and it is prerequisite hygiene for item 1,
because version-pinned consumption replaces lockfile pinning.

**Cost / risk.** Small. Mostly CI configuration and one document.

## 9 · Operator ergonomics: `rdlt watch`, doctor, reclaim

**What.** Three verbs operators keep reaching for: `rdlt watch` (the
ratatui door 036 deliberately left open — a live monitor over the event
feed for runs started elsewhere), `rdlt doctor` (probe the environment:
connector bins resolvable, workdir writable and unlocked, versions
agreeing), and surfacing staging/WAL reclaim as a user verb (the
machinery exists — `make reclaim` internally, dead-predecessor sweeps
in the sdk — but operators have no window into it).

**Why now.** After items 1–2, the population of people RUNNING rdlt
without reading its source grows sharply. These three verbs answer the
questions they will actually have ("why is my pipeline stuck" — usually
the workdir lock or a lease TTL; "is it alive"; "how do I clean up").
All three sit on existing, tested machinery; none opens new surface in
the frozen contract.

**Cost / risk.** Small-medium each, independently shippable. Sequence
after 036 so `watch` consumes the canonical Metrics fold rather than
folding events a second time.

## 10 · Robustness continuity: point the heavy suites at the NEW seams

**What.** 076 concentrated several defenses into single functions —
which is exactly what makes them cheap to attack harder:

- **Fuzz the shared decode seat directly** (`rdlt_connector::gate::
  decode_one_batch_ipc`): one target now covers BOTH wire directions'
  hostile-byte handling; the existing corpus transfers.
- **Mutation-test the new owners** (`session_exit`, `serve/wire.rs`,
  the gate seats): the weekly mutants pass should prove the fresh pins
  can fail — a test suite that survived a refactor untested is a suite
  that may not survive the NEXT one.
- **Close the YAML scanner residue when the trigger fires**: the
  saphyr front-end remains the recorded event-level door (ADR 0002);
  the scanner's honest over-refusals (multiline quoted scalars, verbatim
  tags) stand only until a parser exposes graph events. Watch the
  trigger; pull it deliberately.

**Why now.** The whole security-findings series (features 061–069)
worked because findings landed while the code was fresh. The 076 seams
are the freshest correctness-bearing code in the tree and have seen the
lightest adversarial exposure.

**Cost / risk.** Config-plus-a-target for fuzzing; the mutants pass is
already scheduled. Negligible cost, and it is how the house caught
every one of its best defects.

---

## What the review deliberately does NOT recommend

Recorded so future reviews don't re-suggest them:

- **No process pooling / daemon connectors.** Spawn-through-handshake
  measures 1.6–2.1 ms; the frozen contract needs no pooling, and none
  was added (ADR 0001). Revisit only with a measured workload.
- **No ratatui in `run`.** Rejected for the batch tool (scrollback);
  the TUI belongs to `rdlt watch` (item 9).
- **No widening the certifier's clause vocabulary speculatively.**
  Clauses exist because a law needs enforcing; P11/P12's closure shows
  the pattern — measure first, name second.
- **No second-generation rewrites anywhere.** The 025–031 playbook
  applies to accreted generation-1 surfaces; 076's review found none
  worth the swap discipline today.

## Sequencing sketch

```
now ──► 3 (re-certify bars)          # protects the identity, half a day
    ──► 8 (two-repo rituals)         # hygiene item 1 depends on
    ──► 1 (publish 0.3.0)            # the window closes
    ──► 2 (036 telemetry spine)      # lands pre-freeze-value, feeds 9
    ──► 10 (heavy suites vs new seams)  # anytime; cheapest insurance
    ──► 7 (memory story + stress cell)  # decides ReadCredit with data
    ──► 6 (negotiate a format v2)    # exercise the hatch
    ──► 5 (third-party program)      # grows once publishing exists
    ──► 4 (TCP+mTLS binding)         # largest; after the above matured
    ──► 9 (operator ergonomics)      # consumes 036's Metrics fold
```

Each step is independently shippable and gated on its own, per the
house discipline. The theme across all ten: the engineering core is
done being built — the next value comes from shipping it, measuring
it, hardening it where it is freshest, and opening the doors it was
designed to open.
