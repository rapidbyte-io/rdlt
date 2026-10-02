//! Which state record an entry is, and the record key that names it.

use serde::{Deserialize, Serialize};

use super::StateError;
use crate::id::{PartitionId, StreamName, TablePath};

/// Which state record an entry is.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StateKey {
    /// The fencing epoch.
    Epoch,
    /// A stream's phase.
    Phase(StreamName),
    /// A partition's position.
    Partition(StreamName, PartitionId),
    /// A stream's full read in progress.
    Generation(StreamName),
    /// A stream's recently completed full reads.
    Completed(StreamName),
    /// The epoch of a stream's last reset.
    Reset(StreamName),
    /// A table's schema.
    Schema(TablePath),
    /// A table's name map.
    Names(TablePath),
    /// Who made a table's sequences.
    Sequences(TablePath),
    /// The last commit's receipt.
    Receipt,
}

impl StateKey {
    /// The record key.
    #[expect(
        clippy::missing_panics_doc,
        reason = "state keys always serialize to JSON"
    )]
    pub fn encode(&self) -> String {
        serde_json::to_string(self).expect("state keys serialize to JSON")
    }

    /// The key a record key names; only the exact text [`StateKey::encode`] writes is accepted, so
    /// one key cannot hide under two record keys.
    pub fn parse(key: &str) -> Result<Self, StateError> {
        let malformed = || StateError::MalformedKey {
            key: super::error::shown(key),
        };
        let parsed: Self = serde_json::from_str(key).map_err(|_| malformed())?;
        if parsed.encode() == key {
            Ok(parsed)
        } else {
            Err(malformed())
        }
    }
}
