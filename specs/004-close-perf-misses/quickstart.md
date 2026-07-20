# Quickstart: Close or Re-baseline the Two Benchmark Misses

**Feature**: 004-close-perf-misses

## Prerequisite: working measurement environment

```sh
command -v distrobox && distrobox list      # was MISSING at feature start — repair first
distrobox enter my-distrobox -- cargo --version
distrobox enter my-distrobox -- rustc --version   # must match perf-baselines.json's recorded toolchain
```

All cargo/valgrind/hyperfine commands below run inside the distrobox. Confirm
this is the 003 matrix machine before trusting any number (environment header
rules: research.md R7).

## US1 — Shred profile & A/B loop

```sh
# Current state (the numbers to beat: rdlt ≤ 297.5 ms for 20×)
cargo bench -p rdlt-engine --bench shred

# Attribution profile (primary lens, gate units)
cargo bench -p rdlt-engine --bench iai_hotpath    # iai-callgrind run
# then annotate the emitted callgrind output:
callgrind_annotate target/iai/*/callgrind.out* | head -50

# Secondary lens — REQUIRED for memory-shaped candidates (arena/tape layout)
perf stat -e cycles,instructions,cache-misses,branch-misses \
  cargo bench -p rdlt-engine --bench shred

# After ANY shred change — correctness nets, non-negotiable
cargo nextest run -p rdlt-engine shred_equivalence
cargo nextest run && cargo test --doc

# Gate check (part of every A/B record)
make bench TARGET=iai        # compare-iai.sh vs benches/perf-baselines.json
```

Write each profile / A/B outcome to `evidence/` as it happens (formats:
`data-model.md` §3; accept/reject rules: `contracts/measurement-protocol.md`
P4). Candidates and their ranking rule: `research.md` R2.

## US2 — Cold-start composition & absolute bar

```sh
# Wall-time protocol (the gated statistic: median)
hyperfine --warmup 3 --runs 20 '<rdlt one-row pipeline cmd from benches/run-e2e.sh cold cell>'

# Composition: temporary instrumented build (phase Instant stamps — throwaway,
# never shipped) + syscall corroboration:
strace -T -c -f <same one-row pipeline cmd>
```

Derive the bar: `N = floor × 1.5, round UP to nearest 5 ms` (protocol P3);
record the derivation in `evidence/resolution-cold-start.md`. Then split the
RESULTS.md cold row into gated-absolute + scoreboard-ratio rows
(`data-model.md` §1).

## US3 — Final matrix & traceability

```sh
benches/run-e2e.sh            # flagship, passthrough, cold cells (pin FROZEN at dlt 1.29.0)
# + the REST→PG recipe and normalize-only cell per RESULTS.md "Reproduce"
```

Update `benches/RESULTS.md`: every row gets `gated` or `scoreboard`; the two
resolved cells link their resolution records. Close-out check = the SC-006
walk: matrix row → `evidence/resolution-*.md` → evidence artifacts, no
contradictions; then `make check` green.

## Where things live

| Artifact | Path |
|---|---|
| Profiles, A/B records, resolution records | `specs/004-close-perf-misses/evidence/` |
| Matrix + version policy (project-wide) | `benches/RESULTS.md` |
| Gate baselines (re-record ONLY with an accepted change) | `benches/perf-baselines.json` |
| Decision rules binding all of the above | `contracts/measurement-protocol.md` |
