//! One compile root for the container-free postgres suites under `cases/`:
//! the golden-SQL pins, the COPY wire pin, and the config vocabulary.
//!
//! Suites that stay as their own roots do so for recorded reasons.
//! `crash_sweep`, `dest_crash_sweep`, `cdc_crash_sweep` and `memory_bound`
//! are selected BY BINARY NAME by the Makefile's sweep and heavy targets.
//! Every container-backed suite (conformance, dest_conformance, incremental,
//! cdc, scd2, tls_matrix, query_streams, reflect_live, dest_recovery,
//! direct_publish_guarantees, differential) keeps its own binary
//! DELIBERATELY: nextest schedules across binaries, and merging them into
//! one would raise intra-binary container concurrency — the rootlessport
//! bind flake has an intra-run mechanism that no reclaim can fix, so the
//! per-binary isolation is load-bearing. differential also owns a
//! proptest-regressions file that pairs with its source path.

mod cases;
