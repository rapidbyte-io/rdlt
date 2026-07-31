//! One compile root for the postgres case files under `cases/`. Suites that
//! stay as their own roots do so for recorded reasons: `crash_sweep`,
//! `dest_crash_sweep`, `cdc_crash_sweep` and `memory_bound` are selected BY
//! BINARY NAME by the Makefile's sweep and heavy targets; the container
//! suites keep separate binaries pending the consolidation decision.

mod cases;
