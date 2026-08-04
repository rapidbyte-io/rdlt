//! The destination half: files as an exactly-once target.
//!
//! Staging lives under `.rdlt-staging/{scope}/{load}/`, receipts
//! forever in `_rdlt_commits.{scope}.json`, state beside them — the
//! 4-phase commit is the `load` module's doc comment.

mod config;
mod connector;
mod inspect;
mod layout;
// 037 US2: not yet consumed by `Load` — Task 7 wires `Lease` into the
// write path (acquire on connect, `check_still_held` on every write,
// `release` on publish). Until then this module's public surface is
// exercised only by its own `#[cfg(test)]` tests (037 US2 review round
// 1, M9 — the earlier `testhook::probe_lease` bridge was DELETED as a
// dangerous unused mutation surface on a production doc; those in-file
// tests now carry the same lint-suppression role), so a plain `--lib`
// build (as `clippy --all-targets` checks separately from the test
// target) would otherwise see it as entirely dead code.
#[allow(dead_code)]
mod lease;
mod load;
mod stage;
mod truncate;

pub use config::{Config, ConfigError, DestFormat, config_schema};
#[doc(hidden)]
pub use connector::testhook;
pub use connector::{FAIL_POINTS, File, LEASE_FAIL_POINTS, S3_FAIL_POINTS};
pub(crate) use layout::STAGING_DIR;

/// The sdk shell around [`File`] — the destination's public form.
pub type Shell = rdlt_connector_sdk::destination::Shell<File>;

/// The CANONICAL local-parquet spelling — supported by name, not a
/// deprecated alias (bench, CLI, and crash-sweep tooling consume it).
pub type ParquetDir = Shell;
