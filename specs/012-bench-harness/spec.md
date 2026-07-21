# Feature Specification: Unified Benchmark Framework

**Feature Branch**: `012-bench-harness`

**Created**: 2026-07-21

**Status**: Draft

**Input**: User description: "Improve benches/ — currently scattered
shell scripts with no clear layout. With many connectors coming we need
to measure combinations (a source × destination matrix), always against
dlt (the reference project), with detailed metrics: throughput, MB/s,
peak CPU/mem — everything needed for further optimisation. Direction
approved: one Rust harness with declarative cells, rich metrics from
the library seams, dlt as a first-class competitor, machine-readable
artifacts, enforced gates, and migration of the existing cells with
continuity of the recorded numbers."

## User Scenarios & Testing *(mandatory)*

### User Story 1 - One harness, cells as data (Priority: P1)

A maintainer adds a new connector and wants its performance story: they
add a fixture and a handful of DECLARATIVE cell entries — no new shell
script. A cell names its source fixture, destination, workload, write
mode + options, run count, class (gated | scoreboard), and optional
competitor variants. One command runs any filtered slice of the matrix;
the harness owns container lifecycle, seeding (with recorded dataset
identity), warmups, run repetition, and median/percentile computation —
uniformly for every cell. The existing two-class governance survives
structurally: a small stable GATED set (per-change confidence) and the
fuller SCOREBOARD matrix (on-demand/nightly) so N connectors never blow
up the inner loop.

**Why this priority**: the framework is the feature — everything else
(metrics, baselines, gates) hangs off cells being data instead of
scripts.

**Independent Test**: define a trivial cell in a file, run it via the
harness with a filter, get medians over N runs with recorded dataset
identity — no per-cell code beyond the declaration.

**Acceptance Scenarios**:

1. **Given** the cell registry, **When** a maintainer lists cells,
   **Then** every cell shows its matrix coordinates (source ×
   destination × workload × mode), class, and baseline variants.
2. **Given** a filter expression, **When** the harness runs, **Then**
   only matching cells execute, each with the declared warmups/runs and
   the 004 measurement protocol (medians, quiet-machine discipline)
   applied uniformly.
3. **Given** a new connector pairing, **When** a cell entry referencing
   existing fixtures is added, **Then** it runs with zero new
   harness/script code.
4. **Given** shared fixtures, **When** multiple cells in one session use
   the same seeded dataset, **Then** seeding happens once where
   isolation allows, and dataset content identity (hashes) is recorded
   with the results.

---

### User Story 2 - Rich metrics, artifacts, and enforced gates (Priority: P2)

An engineer optimising a hot path gets, from every cell run: wall time
(median + spread), derived THROUGHPUT (rows/s and MB/s from the run
report's own row/byte counts), CPU utilisation (exact user+sys over
wall) with peak and a coarse time-series, peak memory (RSS) with
transient-spike visibility, and per-stream/per-phase time attribution
from the engine's existing event stream. Competitor (dlt) runs report
the SAME metric set via container cgroup accounting, so every ratio is
apples-to-apples. Each invocation writes a machine-readable artifact
(all runs, statistics, environment fingerprint: CPU model, kernel,
toolchain, competitor pin, dataset hashes). Gated bars live in a
checked-in bars file; a gate command exits nonzero on violation —
the same enforcement shape the instruction-count gate already has.
RESULTS.md keeps its human narrative (history, policy decisions,
honest measurement notes) while its number tables are GENERATED from
artifacts, never hand-transcribed again.

**Why this priority**: "measured, not asserted" currently ends at wall
time and eyeballed medians; optimisation work needs the full picture,
and hand-transcription is an error class this story deletes.

**Independent Test**: run one gated cell; the artifact contains every
metric listed above plus the fingerprint; tightening its bar below the
measured median makes the gate command fail; the generated table
matches the artifact.

**Acceptance Scenarios**:

1. **Given** a completed cell run, **When** the artifact is inspected,
   **Then** it contains per-run wall times, median/p95, rows/s, MB/s,
   CPU utilisation (mean + peak), peak RSS, per-stream attribution, and
   the environment fingerprint.
2. **Given** a cell with a dlt baseline variant, **When** it runs,
   **Then** the competitor reports the same metric set (cgroup-derived)
   and the ratio is computed automatically against the pinned version.
3. **Given** the bars file, **When** a gated cell's median violates its
   bar (beyond recorded tolerance), **Then** the gate command fails
   loudly naming the cell, bar, and measured value; scoreboard cells
   never gate.
4. **Given** artifacts, **When** the report command runs, **Then**
   RESULTS.md's tables regenerate from the latest artifact and the
   narrative sections are preserved untouched.
5. **Given** resource metrics (CPU/RSS), **When** gates are evaluated,
   **Then** these are RECORDED but not gated in this feature
   (bars come later, from observed distributions — the 004 rule).

---

### User Story 3 - Migration with continuity (Priority: P3)

Every existing cell (e2e jsonl/parquet/cold-start, pg source pairs +
jsonb, CDC throughput/latency, merge index/strategies/refinements)
moves into the new framework, the shell scripts retire, and the
recorded history stays trustworthy: migrated gated cells are re-measured
under the new harness and shown to agree with the recorded numbers
within the documented jitter band — any that cannot agree get an
explicit version-policy entry re-deriving the bar, never a silent
renumbering. The instruction-count layer (iai + its baselines and
compare script) and the cold-start protocol (hyperfine, absolute-ms
gate) are explicitly retained as-is — different instruments, same
governance.

**Why this priority**: a framework that orphans five features' worth of
recorded evidence would cost more trust than it adds; continuity is the
migration's acceptance bar.

**Independent Test**: after migration, the gated set runs green under
the new harness with numbers inside the jitter band vs RESULTS.md
history (or carries a recorded re-derivation), and no run-*.sh scripts
remain.

**Acceptance Scenarios**:

1. **Given** the migrated gated cells, **When** they run on the
   reference machine, **Then** each median lands within the documented
   session-jitter band of its recorded value, or a version-policy entry
   records the re-derivation with rationale.
2. **Given** the migration is complete, **When** benches/ is inspected,
   **Then** the per-feature shell scripts are gone, the layout is the
   agreed structure (cells, fixtures, competitors, results, bars,
   narrative RESULTS.md), and README explains how to run everything.
3. **Given** the retained layers, **When** the perf gate runs, **Then**
   the iai instruction-count gate and the hyperfine cold-start cell
   behave exactly as before.

---

### Edge Cases

- Gated configuration realism: the GATED measurement runs the pipeline
  as a CLI subprocess (what users experience; continuity with all
  recorded numbers); library-mode runs provide the richer scoreboard
  metrics (per-phase attribution) — both modes exist, the gate binds to
  subprocess wall time.
- Quiet-machine discipline: the harness refuses (or loudly annotates)
  gated runs on a loaded machine rather than recording garbage.
- Competitor unavailability (image not built, network-restricted): the
  cell's rdlt side still runs; the missing baseline is reported as
  MISSING, never silently skipped into a green run.
- Artifact hygiene: summary artifacts (statistics + fingerprint) are
  committed; raw per-run streams are not — the repo records evidence,
  not bulk.
- Matrix growth: cells are cheap to declare but the gated set only
  grows via an explicit governance decision (004 rules unchanged).

## Requirements *(mandatory)*

### Functional Requirements

- **FR-001**: Benchmark cells MUST be declarative data (source fixture ×
  destination × workload × write mode/options × class × runs ×
  competitor variants), listable and filterable; running any slice is
  one command with zero per-cell scripting.
- **FR-002**: The harness MUST own the full execution protocol
  uniformly: environment checks, container/fixture lifecycle, seeding
  with recorded dataset identity, warmups, N runs, medians/percentiles —
  the 004 measurement protocol as executable code, not prose.
- **FR-003**: Every cell run MUST record: wall time (per-run +
  median/p95), rows/s and MB/s derived from the run report's row/byte
  counts, CPU utilisation (exact user+sys/wall; mean + peak), peak RSS
  with spike visibility, and per-stream/per-phase attribution from the
  engine's existing event seam (library mode).
- **FR-004**: dlt MUST be a first-class competitor module: pinned
  version, variant registry (e.g. pyarrow/sqlalchemy/connectorx),
  same-metric reporting via container cgroup accounting, automatic
  ratios; MISSING baselines are loud.
- **FR-005**: Results MUST be machine-readable artifacts carrying all
  statistics plus an environment fingerprint (CPU model, kernel,
  toolchain, competitor pin, dataset content hashes); summary artifacts
  are committed.
- **FR-006**: Gated bars MUST live in a checked-in bars file with
  tolerances; a gate command MUST fail loudly on violation naming cell,
  bar, and measurement; scoreboard cells never gate; resource metrics
  (CPU/RSS) are recorded, NOT gated, in this feature.
- **FR-007**: RESULTS.md number tables MUST be generated from artifacts
  via a report command, preserving the hand-written narrative sections
  (history, policy, measurement notes) byte-for-byte.
- **FR-008**: All existing cells MUST migrate; the per-feature shell
  scripts MUST be removed; the iai instruction-count layer and the
  hyperfine cold-start protocol MUST be retained unchanged (the
  cold-start cell may be INVOKED by the harness but keeps its recorded
  protocol and absolute bar).
- **FR-009**: Migrated gated cells MUST demonstrate continuity: medians
  within the documented jitter band of recorded values on the reference
  machine, or an explicit version-policy entry re-deriving the bar.
- **FR-010**: The harness MUST be a dev-only workspace member (never
  published); zero new dependencies in RUNTIME crates; harness-crate
  dependencies chosen conservatively (planning decides; /proc + cgroup
  parsing needs none); engine/connector SPI untouched (semver-checks
  "no update required").
- **FR-011**: The gated measurement configuration is the CLI-subprocess
  run; library-mode metrics are scoreboard detail — recorded as a
  design decision with rationale.

### Key Entities

- **Cell**: declarative benchmark definition — matrix coordinates,
  class, protocol parameters, competitor variants, bars reference.
- **Fixture**: seedable dataset/service with recorded content identity.
- **Competitor variant**: a pinned external implementation of a cell's
  workload reporting the same metric set.
- **Artifact**: machine-readable result of one invocation — runs,
  statistics, metrics, fingerprint.
- **Bars file**: the gated set's thresholds + tolerances (+ pointers to
  version-policy entries).

## Success Criteria *(mandatory)*

- **SC-001**: A new source×destination cell is added by editing data
  only, and runs with medians + full metrics via a single filtered
  command.
- **SC-002**: Artifacts from a gated run contain every FR-003 metric and
  the FR-005 fingerprint; a deliberately-tightened bar makes the gate
  command fail naming the cell.
- **SC-003**: dlt twin runs produce the same metric set and automatic
  ratios; removing the competitor image yields a loud MISSING, not a
  green run.
- **SC-004**: RESULTS.md tables regenerate from artifacts; narrative
  sections unchanged; no hand-transcribed numbers remain in the tables.
- **SC-005**: The full migrated gated set runs green under the new
  harness with continuity per FR-009; `benches/` contains no
  per-feature run scripts; iai + cold-start behave exactly as before.
- **SC-006**: `make check`, doc-tests, sweeps, semver-checks green;
  no runtime-crate dependency changes.

## Assumptions

- Reference machine and jitter-band definitions carry over from 004
  unchanged; continuity is judged against RESULTS.md's recorded numbers
  on that machine.
- The engine's existing observability (RunReport rows/bytes, the
  events channel) is sufficient for FR-003's attribution — no engine
  changes; if a metric would require an SPI change it is out of scope
  and recorded as such.
- The dlt baseline container and dataset-identity discipline from
  `benches/baseline/` carry forward as the first competitor module.
- Makefile stays the human entry point (`make bench` family delegating
  to the harness), per the intent-verbs convention.

## Out of Scope

- CI pipeline changes (wiring the new gate into CI configs) beyond
  keeping today's gate commands working.
- New competitors beyond dlt (the module boundary makes them possible;
  adding them is future work).
- Gating CPU/RSS/throughput (recorded now; bars later from observed
  distributions, each with a version-policy entry).
- Distributed/multi-machine benchmarking, long-haul soak tests, and
  workspace-wide coverage goals.
- New engine metrics/seams; anything requiring rdlt-core/rdlt-connector
  changes.
