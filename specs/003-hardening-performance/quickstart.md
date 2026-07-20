# Quickstart: Hardening & Performance (feature 003)

All cargo commands run inside the build container:
`distrobox enter my-distrobox -- <cmd>`.

## Crash-point sweep

```bash
cargo nextest run -p rdlt-engine --features failpoints -E 'test(crash_sweep)'
```

Sweeps every registered fault point (see data-model §1) × error/panic × normal/
during-recovery, restarting and asserting exactly-once totals each time.

## Mutation pass (slow — scheduled CI or on demand)

```bash
cargo mutants -p rdlt-engine -p rdlt-core -p rdlt-connector
# survivors → dispositions in specs/003-hardening-performance/mutation-report.md
```

## Fuzzing (nightly toolchain)

```bash
cd fuzz
cargo +nightly fuzz run jsonl_slab -- -timeout=10 -max_total_time=3600
# targets: jsonl_slab | cursor_decode | file_config | arrow_schema_map | shred_push
```

## Property tests

```bash
cargo nextest run -p rdlt-engine -E 'test(shred_property) or test(shred_equivalence)'
PROPTEST_CASES=4096 cargo nextest run ...   # extended run
```

## Benches

```bash
cargo bench -p rdlt-engine --bench shred          # criterion micro (incl. hash candidates)
cargo bench -p rdlt-engine --bench iai_hotpath    # instruction counts (needs valgrind)
benches/run-e2e.sh                                # all e2e cells incl. cold start
```

Perf gate locally: run `iai_hotpath` and compare with
`benches/perf-baselines.json`; CI does the same and blocks on >3%.

## Order of work (spec constraint FR-006)

US1 (sweep, mutants, fuzz, property tests) must be green on CURRENT code before
any US3 hot-path change lands; US2's gate is armed with CURRENT-code baselines
so US3's wins/regressions are visible.
