# Research: Benchmark Refinement — Three-Way E2E Matrix

Phase 0 decisions. The source document (BENCH_REFINMENT.md v3.1) already
resolved the design-level choices; this file records the plan-time
decisions that turn them into buildable work, the post-017 reconciliations,
and the probe designs. No NEEDS CLARIFICATION remain.

## D-01. Deletion list is re-derived, not transcribed

**Decision**: the plan's deletion list (25 cells / 10 fixtures / 8 bars,
1 exempt each side) is derived from the live tree by the three survival
tests, superseding the document's pre-017 snapshot (24/9).

**Rationale**: two features landed after the document was drafted
(015/016-era cells + 017's fixture restructure); rule-over-snapshot was
already the spec's stated method.

**Alternatives**: honoring the document's enumeration — rejected, it would
miss `file-s3-duckdb-200k`-era rows the rule clearly kills.

## D-02. Constitution amendment lands FIRST, inside P0

**Decision**: Principle VIII is amended to the cells/bars vocabulary
(v1.1.0, MINOR, Sync Impact Report) in P0 *before* the migration commit;
the 012 contract's BH1/BH2/BH3/BH6 get a recorded amendment note in the
same change. The amendment text ships in contracts/bench-refinement.md and
is applied verbatim.

**Rationale**: the constitution supersedes ad-hoc practice; deleting the
vocabulary while the constitution still mandates it would put the tree in
violation between commits. Amending first keeps every merge compliant.

**Alternatives**: amending after the migration (a violating window —
rejected); treating the wording as loose enough to ignore (rejected — 017
established the documents are enforced literally).

## D-03. Artifact format v2, delete-not-migrate

**Decision**: `format_version: 2` — `class` field removed, optional
`extra` object (driver-provided context like sync-attempt time) added,
fingerprint scheme unchanged. All v1 artifacts are deleted in the
migration commit; the reader accepts only v2 and REJECTS v1 loudly with a
message naming the archive commit.

**Rationale**: Principle IX's sanctioned path is explicit versioning;
since every v1 artifact belongs to a deleted cell, migration would produce
orphans. A loud v1 rejection beats silent tolerance (Principle V's spirit).

**Alternatives**: tolerating v1 on read — rejected (half-dead data, the
exact thing v3.1 forbids).

## D-04. Library-mode deletion and the parity pins (post-017 reconciliation)

**Decision**: delete `crates/rdlt-bench/src/library_mode.rs`, its module
wiring, the `Mode::Library` arm, and the bench-side
`shared_parity_specs_all_parse` test. KEEP `benches/parity_specs.yaml` and
the rdlt-cli parse + build pins (they guard `rdlt::pipeline_spec`, which
the CLI consumes; the fixture's header comment is updated to name the CLI
as the remaining consumer).

**Rationale**: the document's premise (the fixture pins a harness-local
parser) predates 017's extraction of the shared model; deleting the
fixture would orphan live pins on a shipped parser.

**Alternatives**: moving the fixture under crates/rdlt-cli/tests/data —
considered; declined to avoid churning the pin paths for zero behavior
(recorded as acceptable follow-up if benches/ ever bothers anyone).

## D-05. Mode collapse mechanics

**Decision**: `Mode` enum reduces to `Subprocess` (wall timing); the
`Hyperfine` arm's cold-start protocol moves to
`benches/check-cold-start.sh` (hyperfine invocation + the ≤ 40 ms absolute
check, exit non-zero on breach) wired into the Makefile's instruments
verbs (`TARGET=iai make bench` runs iai + cold-start; `make check`
inherits it via the existing perf-gate leg). The cell schema loses `mode`
entirely if only one value remains (a field with one legal value is
noise) — the loader rejects unknown keys as today.

**Rationale**: the embeddability claim stays guarded (spec FR-006) without
a matrix row; one-value enums are dead vocabulary.

**Alternatives**: keeping `mode = "subprocess"` in TOML for future modes —
rejected (speculative vocabulary; a future mode re-adds the field with its
PR).

## D-06. Quiet guard, classless

**Decision**: one rule — every measured run refuses/waits (existing
wait-up-to-5-min behavior) on a loaded machine; `RDLT_BENCH_FORCE=1` runs
annotated in the artifact (`forced: true`). The class branch dies.

**Rationale**: §4 verbatim; the annotation preserves honesty when forced.

## D-07. Same-conditions fixture shapes

**Decision**: ONE postgres container (`pg` fixture): `seed_pg.sql` source
table (1M × 12) in database `src`, plus EMPTY per-product destination
databases `dest_rdlt`, `dest_dlt`, `dest_airbyte` created at fixture
start; per-run reset drops/recreates the destination schema for the arm
being measured. ONE RUSTFS container (`rustfs` fixture, pin
1.0.0-beta.11): bucket `raw` seeded once per session with the generated
200k nested jsonl; bucket `lake` with per-product prefixes
(`lake/rdlt/…`, `lake/dlt/…`, `lake/airbyte/…`), reset by prefix-delete
per run. Ports fixed as today (fixture registry), conn strings templated
via the 017 `[defaults]` mechanism.

**Rationale**: §3's same-source/same-destination-instance rule with the
least machinery; per-product databases/prefixes prevent cross-product
interference while keeping one server per kind.

**Alternatives**: per-product containers — rejected (different instances
= not "same destination instance"; also 3× the fixture load).

## D-08. Dedup-cell semantics (the one two-load cell)

**Decision**: the cell's `generate` step seeds load 1 (1M rows) AND
prepares the 50%-changed second dataset; the measured run is LOAD 2 ONLY
(full re-delivery + dedup by `id`), with load 1 applied as unmeasured
setup per run-reset. rdlt arm: merge upsert key `id`; dlt:
`write_disposition="merge"` (delete-insert) key `id`; Airbyte: Full
Refresh Overwrite + Deduped, primary key `id`. The regime note (all three
full-redelivery; Airbyte's cheaper incremental regime deliberately not
benched — no dlt counterpart) is recorded in the cell's note field and
surfaces as the matrix caption.

**Rationale**: measuring load 2 isolates the "keep a table in sync" claim;
measuring both loads would blend full-refresh into the dedup number.

## D-09. Airbyte probes (P1) — designs and pass criteria

Probe order is risk order; each records evidence + a decision in
`specs/018-bench-refinement/spike/`.

1. **Runtime** (#1 risk): attempt `abctl local install --low-resource-mode`
   under (a) rootless podman with `KIND_EXPERIMENTAL_PROVIDER=podman`,
   then (b) if (a) fails, document the docker-install path and ASK THE
   OWNER before installing a second container runtime on the machine
   (system-level change — not made silently). Pass = a healthy `abctl
   local status` + one manual sync completing. No-go = neither path viable
   without owner-declined system changes → P3 ships absent-with-reason.
2. **Networking**: from a kind pod, reach the host's postgres and RUSTFS
   (candidate addresses: `host.docker.internal`, the kind node's gateway
   IP). Pass = both reachable with the fixture ports; the working address
   form is recorded for driver.py.
3. **API fields**: create one throwaway connection via the public API;
   trigger sync; `GET /v1/jobs/{id}` — pin the exact field names
   (status/timing/recordsSynced/bytesSynced) against the document's
   expectations; divergences recorded and adopted.
4. **Quiet-guard compatibility**: measure idle 1-min loadavg with the kind
   cluster up vs the guard's threshold. Pass = idle cluster fits under the
   threshold; else the guard gains a RECORDED allowance (policy entry) or
   the cluster is stopped between arms (slower, recorded).
5. **Reset fidelity**: run sync → reset + destination schema drop → row
   counts prove the destination equals initial state. Pass = counts match;
   else per-run teardown strategy is redesigned before P3.

## D-10. Driver kind (P3) — harness seam

**Decision**: `kind = "self_timed_container" | "driver"` on variants
(default = today's behavior); variants files discovered from
`benches/competitors/*/variants.toml` into one flat namespace (duplicate
variant id = load-time error naming both files). A driver run executes the
module's `driver.py` on the host venv and consumes the existing last-line
JSON convention (`seconds`, `rows`, optional `peak_rss_kb`, optional
`extra{}` carried verbatim into the artifact). Artifact/gate/report paths
unchanged. abctl is a machine PREREQUISITE probed via `abctl local
status`; absence = `Missing{reason}` loud skip. `setup.py` is idempotent
(install-if-needed + create the five connections; ids cached in gitignored
`state.json`). Airbyte arms run `runs = 3` (per-competitor run counts
exist since 012); headline `seconds` = job wall; `extra.sync_s` = attempt
time (labeled context); cluster cgroup CPU/RSS recorded never barred.

**Rationale**: §7 verbatim; the last-line JSON convention means zero
artifact-schema divergence between competitor kinds.

## D-11. dlt fastest-configuration switch

**Decision**: pg-source arms run `backend="connectorx"` as variant `dlt`
(the headline); `dlt-pyarrow` remains as recorded context in the same
sessions; `dlt-sqlalchemy` is deleted. The RESULTS.md policy log records
the scoping change ("fastest documented configuration, honestly chosen")
and that multiples drop accordingly (≈2.2× expected on pg cells per the
005-era comparison).

**Rationale**: §3's honesty rule — gating against a deliberately slow
competitor configuration is marketing by selection in the other direction.

## D-12. Presentation & history mechanics

**Decision**: RESULTS.md is rebuilt to header (methodology + policy log) /
GENERATED matrix (one table: cell, per-product median with spread from
runs_ms, ratios vs dlt and vs Airbyte, bar, status; caption = cell note) /
hand-written Caveats / GENERATED Trends from `benches/history.jsonl`
(runner appends one line per cell per recorded invocation:
`{ts, cell, variant, median_ms, rows}` — ts from the artifact, not from a
new clock source) / hand-written Milestones (retired claims + evidence
commits: the 13.5× flagship, the CDC catch-up, the rest-pg 6.7×, each
citing the pre-migration commit). Coverage/semver/exclusion records move
to `benches/GOVERNANCE.md`. Marker-splice mechanism unchanged (one
BEGIN/END pair).

**Rationale**: §8 with the only open mechanic (history feed shape) pinned.

## D-13. Milestones seed list (so the migration commit is self-contained)

**Decision**: the P0 migration commit seeds Milestones with the final
recorded values of: jsonl-duckdb-200k 13.5× / RSS 1/5.4 vs dlt;
shred-only 12.0×; rest-pg 6.7×; parquet-passthrough 3.5×;
pg-wide-duckdb 7.8× and pg-wide-pg 7.6× vs dlt-pyarrow; cold-start
24.2 ms (relocated live to instruments); cdc catch-up ~72k changes/s —
each with the pre-migration commit hash as evidence.

**Rationale**: §5.1's deletion semantics require the migration record to
cite final values; seeding Milestones in the same commit makes the record
one artifact instead of two.

## D-14. P4 bar shapes (pre-declared, set only with evidence)

**Decision**: candidate bars (set ONLY after the first recorded 3-way
session, each below its session floor, one policy entry each):
`ratio_vs dlt` on each of the five cells; optionally ONE
`rss_ratio_vs dlt` on pg-to-pg-1m (single-process both sides). No bar
binds an Airbyte statistic in v1 (job-wall includes orchestration; the
policy entry for any future Airbyte-bound bar must name its statistic).

**Rationale**: §4's candidate shapes, constrained to the constitution's
(amended) no-enforcement-without-evidence rule.
