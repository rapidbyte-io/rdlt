# Contract: Benchmark Refinement (BR1–BR8) + Governance Amendments

Clauses this feature must satisfy; the close-out cites these IDs. Per
constitution Principle V they never appear in user-facing strings.

## BR1 — One matrix, by rule

After P0+P2 the benchmark consists of exactly the five e2e cells; every
other benchmark cell, fixture, seed, script, bar, and artifact is deleted
in ONE migration commit whose message (and the RESULTS.md policy log)
cites each retired cell's final recorded value and the pre-migration
commit hash. Harness self-test fixtures are exempt and stay. A
vocabulary sweep for the retired taxonomy (`class`, gated/scoreboard),
`suite`, and the retired mode names returns zero hits in harness code,
cell files, artifacts, and generated docs.

## BR2 — Amend-then-delete governance

Constitution v1.1.0 (Principle VIII reworded to the cells/bars model,
mechanism preserved: bars live in bars.toml, enforced by the gate, none
without recorded evidence + a policy entry — now explicitly citing a
recorded session floor) and the 012 contract amendment (BH1/BH2/BH3/BH6
wording) land BEFORE the vocabulary deletion merges. Amendment texts
below are applied verbatim; no tree state between merges contradicts
either document.

## BR3 — Artifacts are versioned data

Artifact `format_version` increments to 2 (`class` removed, optional
`extra` object added, fingerprints unchanged). The reader accepts v2 only
and rejects v1 with a message naming the archive commit. The
append-only history feed gains one line per cell per recorded
invocation; the report's Trends section is generated from it.

## BR4 — Same conditions, provable

Every recorded session: quiet guard first (one classless rule; forced
runs annotated), baselines measured first, same seeded source data, same
destination server/store instance with per-product databases/prefixes,
row-count verification on every arm, per-product timing boundaries
stated in the generated output. The dedup cell measures LOAD 2 only,
with its regime note rendered as the row caption.

## BR5 — Probes before machinery

No Airbyte harness code merges before the five probes (runtime,
networking, API fields, quiet-guard fit, reset fidelity) are recorded in
the spike directory with evidence and a go/no-go each. A no-go yields
absent-with-reason arms, never silent scope shrink. Any system-level
machine change (installing a second container runtime) requires explicit
owner approval recorded in the spike.

## BR6 — Driver kind without artifact divergence

The driver competitor kind consumes the existing last-line JSON
convention; artifact, gate, and report code paths are identical for both
kinds (the `extra` object is pass-through). Variants files are
discovered per-module into one flat namespace; a duplicate variant id is
a load-time error naming both files. A missing machine prerequisite is a
`Missing{reason}` loud skip.

## BR7 — Honest competitor configuration

dlt runs its fastest documented configuration per cell (connectorx for
pg extraction) as the headline variant, pyarrow retained as recorded
context, the sqlalchemy variant deleted; the policy log records the
scoping change. Airbyte headline = job wall (orchestration included,
labeled), attempt time as labeled context, versions pinned with
bump-means-re-measure.

## BR8 — Enforcement is measurement-first

bars.toml is empty from P0 until after the first recorded three-way
session; then at most one bar per cell, each below its cited session
floor, each with a policy entry, gate green against the justifying
session. Cluster-wide resource statistics are never bar material. Every
bar references an existing cell.

---

## Amendment A — Constitution Principle VIII (v1.0.0 → v1.1.0)

Replace the body of "### VIII. Benchmark Governance" with:

> Benchmarks are declarative cells — end-to-end pipeline comparisons,
> measured and reported. Enforcement exists only as bars: a bar
> references exactly one existing cell, lives in `bars.toml`, and is
> enforced by the bench gate. No bar exists without recorded measurement
> evidence — a bar is set below the floor of a recorded session and
> cites a governance (policy-log) entry. Performance claims MUST be
> backed by harness evidence, not ad-hoc timing.
>
> **Rationale**: ungoverned gates rot into flaky CI; ungoverned claims
> rot into marketing; importance taxonomies rot into labels that
> substitute for evidence.

Sync Impact Report (to embed in the constitution's header comment on
amendment): version 1.0.0 → 1.1.0 (MINOR — Principle VIII materially
reworded: gated/scoreboard vocabulary removed; the
no-enforcement-without-evidence mechanism preserved and strengthened
with the recorded-session-floor requirement). No other principles
changed. Templates: no static changes required (plan-time gates read the
constitution live).

## Amendment B — 012 bench-harness contract (recorded note)

Append to `specs/012-bench-harness/contracts/bench-harness.md`:

> **Amendment (feature 018, 2026)**: the gated/scoreboard cell
> classification and the library/hyperfine run modes are RETIRED from
> the harness vocabulary (BH1/BH2/BH3/BH6 wording amended accordingly).
> The mechanisms those clauses protect — declarative cells, honest
> recorded artifacts, loud skips, bars enforced only through
> `rdlt-bench gate` with governance entries — are unchanged and remain
> binding. Enforcement additionally requires a recorded session floor
> (constitution v1.1.0). Continuity for bars retired by the matrix
> rebuild rides the RESULTS.md policy log (BH8 spirit).
