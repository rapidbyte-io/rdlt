//! One compile root for the engine's topic suites — thirteen binaries became
//! one, named by what they test rather than by the spec story that introduced
//! them, with shared corpus builders in `cases/common.rs`.
//!
//! Deliberately NOT here, each with its own root: `crash_sweep` (feature-gated,
//! selected by name in the Makefile's sweep target), `shred_property`
//! (selected by name by `make test TARGET=prop`), `shred_identity_pin` and
//! `wal_format_pin` (byte-exact format oracles with their own `RDLT_REPIN`
//! regeneration protocol), and `entry_points` (the fuzz-wrapper smoke suite
//! cited by close-outs).

mod cases;
