# Contract: Quality Gates (feature 003)

**No connector-SPI or embedder-API amendments in this feature.** The seam crates
(`rdlt-core`, `rdlt-connector`) are untouched except a possible internal
`RowIdBuilder` hash swap (identical API and output type, pre-release). This
document defines the CI gate semantics this feature adds — the "contract" is
between the repository and its contributors.

## G1 — Perf-regression gate (per-PR, blocking)

| # | Clause |
|---|---|
| G1.1 | CI measures instruction counts (iai-callgrind) for the hot-path benches and compares against `benches/perf-baselines.json`. |
| G1.2 | Any bench regressing >3% fails the job; the failure names the bench and both counts. |
| G1.3 | Baselines change only by editing `perf-baselines.json` in the same PR — a reviewed, deliberate act (lockfile discipline). |
| G1.4 | The gate blocks merge exactly like the semver gate: no continue-on-error, no advisory mode. |

## G2 — Crash-sweep gate (per-PR, blocking)

| # | Clause |
|---|---|
| G2.1 | The sweep runs for the WAL and all in-process destinations (memory, parquet, DuckDB) on every PR; the Postgres sweep joins when the service container is available (scheduled job always includes it). |
| G2.2 | The sweep enumerates the fault-point registry; an instrumented point absent from the sweep list fails the suite (no silently unswept boundaries). |
| G2.3 | Every sweep cell asserts exactly-once visibility of all acknowledged rows after restart. |

## G3 — Scheduled deep checks (non-PR, release-blocking)

| # | Clause |
|---|---|
| G3.1 | Mutation pass (weekly + dispatch): kill rate ≥85% of viable mutants; zero undispositioned survivors. Falling below blocks RELEASE tagging, not PRs. |
| G3.2 | Fuzzing (nightly): any crash/hang/memory finding opens a release-blocking issue and must graduate to a unit test when fixed. |
| G3.3 | Extended property runs (4096 cases) ride the scheduled job. |

## G4 — Single-source-of-truth Makefile (process)

| # | Clause |
|---|---|
| G4.1 | The Makefile exposes intent verbs (`build`, `release`, `test`, `bench`, `lint`, `check`) with `TARGET=` suite selectors on `test`/`bench`; every gate in G1–G3 maps into one invocation, and CI calls `make`, never inline command copies. Tool specifics are recipe implementation details. |
| G4.2 | A gate command changed in the Makefile changes it everywhere at once; CI-only or local-only command drift is a defect. |

## G5 — Optimization evidence rule (process)

| # | Clause |
|---|---|
| G5.1 | Every performance-motivated change states before/after numbers from the relevant bench in its PR description (FR-012). |
| G5.2 | A performance change with no measurable win does not merge. |
| G5.3 | The streaming shred path merges only after the equivalence gate (data-model §7) and the full existing suite pass against it — and it is a HARD deliverable of this feature (spec clarification 2026-07-20). |
