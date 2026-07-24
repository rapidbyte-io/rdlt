//! The file DESTINATION side: parquet or jsonl output to a LOCAL directory or an
//! S3-compatible object store, optional partition column, commit-atomic visibility.
//!
//! Write-only, Append/Replace (no merge — `merge: false`). One directory/
//! prefix per table; staged parts live under `.rdlt-staging/<pipeline>/<load>/`
//! and publication is atomic renames (local) or COPY+DELETE (object store —
//! per-key atomic visibility: a reader can never observe a partial object
//! under a final name), plus a rewrite of the JSON state/receipt files. There is no
//! single set-atomic multi-file publish, but recovery converges because staged names
//! are deterministic per (load_id, commit_seq, table, partition, n), with `n` counted
//! PER TABLE+PARTITION so cross-table arrival order cannot change a file's final name.
//!
//! Pipeline scoping: staging, state, and the commit log are all keyed by a
//! hash of the pipeline id, so pipelines sharing one output location cannot
//! clobber each other's staged data, cursors, or receipts (same rule the
//! Postgres destination applies to its stage tables).
//!
//! Layout: this module owns the connector surface (`FileDest`, `ParquetDir`, the
//! `Destination` impl); `layout` owns the persisted-name vocabulary, `session`
//! the per-load staging/commit protocol, `truncate` the Replace ownership rule,
//! and `inspect` the row-count helpers. WHERE bytes live is the shared
//! `crate::location::Location`.

pub mod config;
mod inspect;
mod layout;
mod session;
mod truncate;

use std::collections::BTreeMap;

use async_trait::async_trait;
use rdlt_connector::{
    ConnectorSpec, DestCapabilities, DestError, Destination, LoadSession, OpenCtx,
    core::naming::IdentRules,
};

pub use config::{DestFormat, FileDestConfig, dest_config_schema};

use crate::location::Location;
use layout::pipeline_scope;
use session::FileSession;

/// Fail-point registries: every `crash_point!` site in the destination appears in
/// exactly one, pinned by the sweep that can FIRE it. `FAIL_POINTS` holds the LOCAL
/// protocol points the engine's crash sweep drives against `ParquetDir` (these
/// spellings are frozen). The object-store finalize boundaries live in
/// `S3_FAIL_POINTS`, swept by this crate's container-gated sweep (they cannot fire on
/// a local store).
#[cfg(feature = "failpoints")]
#[doc(hidden)]
pub const FAIL_POINTS: &[&str] = &[
    "pq.replace.truncate",
    "pq.staged.sync",
    "pq.part.rename",
    "pq.dir.fsync",
    "pq.state.write",
    "pq.receipt.write",
];

#[cfg(feature = "failpoints")]
#[doc(hidden)]
pub const S3_FAIL_POINTS: &[&str] = &[
    "file.stage.put",
    "file.finalize.copy",
    "file.finalize.delete",
];

/// The one fatal-error constructor for the destination side.
pub(crate) fn fatal(e: impl std::fmt::Display) -> DestError {
    DestError::fatal(e.to_string())
}

#[derive(Debug, Clone)]
pub struct FileDest {
    config: FileDestConfig,
    location: Location,
}

/// `ParquetDir::open(dir)` ≡ local-parquet output. This spelling is the CANONICAL
/// local-parquet entry point — the bench, CLI, and crash-sweep tooling consume it,
/// so it is supported by name, not a deprecated alias. It is pure delegation to
/// [`FileDest`]: new destination options land on `FileDest` (and its config),
/// which this aliases; both share one implementation.
pub type ParquetDir = FileDest;

impl FileDest {
    /// Open (creating if needed) a LOCAL output directory as a parquet
    /// destination — the plain-path constructor. The PathBuf is used AS-IS
    /// (no lossy string round-trip: non-UTF-8 paths keep their bytes); the
    /// config mirror is informational only.
    pub fn open(out: impl Into<std::path::PathBuf>) -> Result<Self, DestError> {
        let out = out.into();
        let config = FileDestConfig::new(out.to_string_lossy().into_owned());
        let location = Location::local_dir(out)?;
        Ok(Self { config, location })
    }

    /// The full configuration vocabulary: format, location, partitioning.
    pub fn from_config(config: FileDestConfig) -> Result<Self, DestError> {
        config.validate("file destination").map_err(fatal)?;
        let location = Location::for_dest(&config.path, config.location.as_ref())?;
        Ok(Self { config, location })
    }
}

#[async_trait]
impl Destination for FileDest {
    fn spec(&self) -> ConnectorSpec {
        let mut spec = ConnectorSpec::new("file", env!("CARGO_PKG_VERSION"));
        spec.config_schema = Some(dest_config_schema());
        spec
    }

    fn capabilities(&self) -> DestCapabilities {
        DestCapabilities {
            merge: false, // write-only destination; no per-row identity semantics
            structs: true,
            scalar_lists: true,
            json_type: false,
            decimal: true,
            ident_rules: IdentRules::default(),
        }
    }

    async fn open(&self, ctx: OpenCtx) -> Result<Box<dyn LoadSession>, DestError> {
        let scope = pipeline_scope(&ctx.pipeline);
        // Clause D4: staged data from THIS PIPELINE's dead sessions becomes
        // invisible/reclaimable. Scoped — another pipeline sharing this output
        // location keeps its live staged data (the same rule the Postgres
        // destination applies to its stage tables).
        self.location
            .prepare_staging(&scope, ctx.load_id.as_str())
            .await?;
        Ok(Box::new(FileSession {
            location: self.location.clone(),
            format: self.config.format,
            partition_by: self.config.partition_by.clone(),
            scope,
            load_id: ctx.load_id,
            tables: BTreeMap::new(),
            staged: Vec::new(),
        }))
    }
}
