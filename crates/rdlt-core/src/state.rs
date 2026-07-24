//! Pipeline state persisted *in the destination*.
//!
//! Written atomically with the data it covers by `LoadSession::commit`; the reason
//! correctness survives total loss of the local work directory.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::cursor::Cursor;
use crate::ids::{LoadId, PipelineId, SchemaHash, StreamName, TableName};

pub const STATE_FORMAT_VERSION: u32 = 1;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LastCommit {
    pub load_id: LoadId,
    pub commit_seq: u64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct StateDoc {
    pub format_version: u32,
    pub pipeline: PipelineId,
    /// Per-stream committed cursors; the unit of incremental resume.
    pub cursors: BTreeMap<StreamName, Cursor>,
    /// Current schema version per destination table.
    pub schema_hashes: BTreeMap<TableName, SchemaHash>,
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
pub struct UnsupportedStateVersion {
    pub found: u32,
    pub supported: u32,
}

#[cfg(test)]
mod version_tests {
    // Mutation-report closure: the future-version guard (same shape as the
    // WAL manifest guard).
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
