//! Locating (and optionally building) the real connector bin the
//! certification cases spawn — a thin wrapper over the ONE spawn
//! scaffold ([`rdlt_testkit::spawn`], 042 round-2 fix wave): the
//! mechanics — `CARGO_TARGET_DIR` resolution, the
//! `RDLT_BUILD_CONNECTOR_BINS` guard, the stale-bin note, the loud
//! missing-bin refusal — live there once for every spawn suite.

use std::path::PathBuf;

/// The path to a built connector bin — see
/// [`rdlt_testkit::spawn::built_connector_bin`] for the guard and
/// build semantics. The reference connector is the one bin these cases
/// spawn today; a second certification subject just names its crate.
pub(crate) fn built_bin(name: &str) -> PathBuf {
    rdlt_testkit::spawn::built_connector_bin(env!("CARGO_MANIFEST_DIR"), name)
}
