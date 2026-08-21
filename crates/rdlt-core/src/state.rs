//! Pipeline state persisted *in the destination*.
//!
//! Written atomically with the data it covers by the destination's commit; the
//! reason correctness survives total loss of the local work directory.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::cursor::Cursor;
use crate::id::{LoadId, PipelineId, SchemaHash, StreamName, TableName};

/// Version of the state document's serialized shape.
///
/// A PERSISTED FORMAT. A document written at a newer version is refused rather
/// than reset: silently starting over would re-extract from zero and duplicate
/// every row under `Append`.
pub const STATE_FORMAT_VERSION: u32 = 1;

/// The state-format KIND under which a destination connector declares,
/// in the handshake's `state_format_versions_json` map, the newest
/// [`STATE_FORMAT_VERSION`] its build can resume. One spelling here so
/// the declaring side (the sdk's serve layer, which fills the map
/// automatically) and the enforcing side (the client's `ReadState`
/// seat, which refuses a document newer than the declaration) cannot
/// drift. A connector that declares nothing is not refused — the map's
/// empty-means-absent convention predates negotiation — but the
/// engine's own version gate still applies to what it parses.
pub const STATE_DOC_FORMAT_KIND: &str = "rdlt.state_doc";

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
/// Which commit last published successfully.
///
/// The `(load_id, commit_seq)` pair is the idempotence key: a destination that
/// sees the same pair again returns the prior receipt instead of republishing,
/// which is what makes the redelivery window after a crash safe.
pub struct LastCommit {
    /// The run that committed.
    pub load_id: LoadId,
    /// Its commit sequence number, monotonic within that run.
    pub commit_seq: u64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
/// Pipeline state, persisted IN the destination.
///
/// Written atomically with the data it covers, by the destination's commit.
/// That is why correctness survives total loss of the local work directory: the
/// cursors and the rows they describe cannot disagree, because nothing can
/// publish one without the other.
pub struct StateDoc {
    /// [`STATE_FORMAT_VERSION`] at the time of writing.
    pub format_version: u32,
    /// The pipeline this document belongs to.
    pub pipeline: PipelineId,
    /// Per-stream committed cursors; the unit of incremental resume.
    pub cursors: BTreeMap<StreamName, Cursor>,
    /// Current schema version per destination table — a DIAGNOSTIC audit trail,
    /// not an input to any engine decision.
    ///
    /// No code reads it, and that is deliberate rather than an omission. A hash
    /// can prove two schemas differ; it cannot say HOW, so it cannot produce the
    /// added-or-widened column a policy resolves on, and comparing hashes across
    /// runs would report drift for a run that merely observed a subset of the
    /// columns — the common shape for a sparse source. It is persisted so an
    /// operator or a support tool can see which schema version a table was on at
    /// each commit, alongside the `from → to` hashes each schema delta carries.
    pub schema_hashes: BTreeMap<TableName, SchemaHash>,
    /// The last successful commit, or `None` for a pipeline that has never
    /// committed.
    pub last_commit: Option<LastCommit>,
    /// Engine version that wrote this document (diagnostics; not used for logic).
    pub engine_version: String,
}

impl StateDoc {
    /// `engine_version` is stamped by the CALLER — the engine passes its own
    /// `CARGO_PKG_VERSION`, not rdlt-core's, so the recorded version identifies
    /// the engine that wrote the document (diagnostics only; never logic).
    pub fn new(pipeline: PipelineId, engine_version: impl Into<String>) -> Self {
        Self {
            format_version: STATE_FORMAT_VERSION,
            pipeline,
            cursors: BTreeMap::new(),
            schema_hashes: BTreeMap::new(),
            last_commit: None,
            engine_version: engine_version.into(),
        }
    }

    /// A newer on-disk format than this engine knows must be a typed failure, never a
    /// silent reset (a silent reset would re-extract from zero and duplicate under
    /// Append).
    pub fn check_readable(&self) -> Result<(), UnsupportedStateVersion> {
        if self.format_version > STATE_FORMAT_VERSION {
            return Err(UnsupportedStateVersion {
                found: self.format_version,
                supported: STATE_FORMAT_VERSION,
            });
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error(
    "state document format v{found} is newer than this engine supports (v{supported}); \
     upgrade rdlt instead of resetting state"
)]
/// A state document written by a NEWER engine than this one.
///
/// Typed rather than tolerated: continuing would mean reading a format this build
/// does not understand, and resetting would re-extract from zero and duplicate
/// every row under `Append`. Upgrading rdlt is the only safe resolution.
pub struct UnsupportedStateVersion {
    /// The version found on disk.
    pub found: u32,
    /// The newest version this build understands.
    pub supported: u32,
}

#[cfg(test)]
mod version_tests {
    use super::*;

    #[test]
    fn future_state_version_is_a_typed_error_current_is_fine() {
        let mut doc = StateDoc::new(PipelineId::new("p"), env!("CARGO_PKG_VERSION"));
        assert!(doc.check_readable().is_ok());
        doc.format_version = STATE_FORMAT_VERSION + 1;
        let err = doc.check_readable().expect_err("future version");
        assert_eq!(err.found, STATE_FORMAT_VERSION + 1);
    }
}
