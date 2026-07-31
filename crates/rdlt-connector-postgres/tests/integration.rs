//! THE compile root for the postgres test surface: every suite lives under
//! `cases/` as a `test_<noun>` module — golden pins, config vocabulary,
//! conformance (source and destination), merge strategies and refinements,
//! incremental, CDC, TLS, recovery, and the differential decoder oracle.
//!
//! Only binaries a gate selects BY NAME keep their own roots: `crash_sweep`,
//! `dest_crash_sweep` and `cdc_crash_sweep` (the Makefile's sweep target)
//! and `memory_bound` (the heavy target).

mod cases;
