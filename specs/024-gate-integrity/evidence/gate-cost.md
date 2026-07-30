Gate counts and cost — BEFORE this feature vs AFTER.

Before: two `make check` runs on main @ 15f17c65 (unmodified tree, 023's gate of
record). After: one clean run on this branch with all five stories in, gate exit 0.

                              BEFORE          AFTER      delta
  workspace tests             948 (2 skip)    961 (2 skip) +13 new gate-verification tests
  TARGET=e2e                  NOT RUN         2 passed   orphaned suite now gated
  sweep: engine               5               5          =
  sweep: postgres             13              14         +1 registry assertion
  sweep: duckdb               1               2          +1
  sweep: rest                 1               2          +1
  sweep: file                 2               3          +1
  sweep: iceberg              1               2          +1
  make semver                 DID NOT EXIST   2x clean   no update required, both crates
  snowflake failpoints lint   NEVER COMPILED  clean      the file no gate built
  perf gate                   0 regressed     0 regressed =
  cold start                  23.8 / 23.3 ms  23.9 ms    within noise, bar 40

  TARGET=prop (deep tier, not in check):  0.000s / 0 tests  ->  38.026s / 1 test
    The single clearest figure in this feature: a zero-second "pass" was the
    signature of the whole defect class, and it sat in plain sight.

  Skip count UNCHANGED at 2 across before and after. Both are the #[ignore]d
  measurement instruments (ingestion_session, scratch_reclaim) — named, not
  merely counted, because "2 skipped" is the shape a silently-disabled test
  hides in.

  COST: the gate gained 4 legs (e2e, semver, snowflake lint, 13 tests). Measured
  wall clock is dominated by the container and live-credential suites that were
  already there; the added legs are seconds each except semver (~2s per crate).
  No leg was removed, and nothing became easier to pass (FR-013).
