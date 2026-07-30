//! One compile root for the engine's integration suites, mirroring the
//! reference layout: topic files under `cases/`, named by what they test,
//! with shared corpus builders in `cases/common.rs`.
//!
//! Exactly two suites keep their own roots, both because a gate selects them
//! BY BINARY NAME: `crash_sweep` (feature-gated; the Makefile's sweep target
//! selects `binary(crash_sweep)`) and `shred_property` (`make test
//! TARGET=prop` selects `binary(shred_property)` — deliberately the binary,
//! not the test name, which is the selector bug feature 024 fixed).

mod cases;
