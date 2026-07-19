//! Pipeline state persisted *in the destination* (contracts/persisted-formats.md §1).
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
    pub fn new(pipeline: PipelineId) -> Self {
        Self {
            format_version: STATE_FORMAT_VERSION,
            pipeline,
            cursors: BTreeMap::new(),
            schema_hashes: BTreeMap::new(),
            last_commit: None,
            engine_version: env!("CARGO_PKG_VERSION").to_owned(),
        }
    }

    /// A newer on-disk format than this engine knows must be a typed failure, never a
    /// silent reset (a silent reset would re-extract from zero and duplicate under
    /// Append). Persisted-formats contract §1.
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
