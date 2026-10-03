//! How a staged segment records its table's merge key in the catalog.

use crate::destination::{ChangeColumns, HistoryColumns, MergeKey, RootKey};
use crate::error::{ConnectorError, Result};

/// The format of the recorded merge key this build writes and reads.
const FORMAT: u16 = 1;

/// How a staged segment records a merge key: the format it is recorded in, its key columns, and,
/// for a child table, its root, for a change stream's table, its change columns, and for a
/// history table, its history columns.
#[derive(serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct RecordedKey {
    format: Format,
    columns: Vec<String>,
    #[serde(deserialize_with = "Option::deserialize")]
    root: Option<RecordedRoot>,
    #[serde(deserialize_with = "Option::deserialize")]
    changes: Option<ChangeColumns>,
    #[serde(deserialize_with = "Option::deserialize")]
    history: Option<HistoryColumns>,
}

/// A recorded merge key's format: written as this build's, and read only as it, so a key
/// recorded by another build is refused rather than read as one it is not.
#[derive(Clone, Copy, serde::Serialize, serde::Deserialize)]
#[serde(try_from = "u16", into = "u16")]
struct Format;

/// A key recorded in another format.
#[derive(Debug, thiserror::Error)]
#[error("the key is recorded in format {0}; this build reads format {FORMAT}")]
struct OtherFormat(u16);

impl TryFrom<u16> for Format {
    type Error = OtherFormat;

    fn try_from(format: u16) -> std::result::Result<Self, OtherFormat> {
        if format == FORMAT {
            Ok(Self)
        } else {
            Err(OtherFormat(format))
        }
    }
}

impl From<Format> for u16 {
    fn from(Format: Format) -> Self {
        FORMAT
    }
}

#[derive(serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct RecordedRoot {
    table: String,
    id: String,
    seq: String,
}

/// `key`'s columns, root, change and history columns as a staged segment records them.
pub(super) fn encode_merge_key(key: &MergeKey) -> String {
    let recorded = RecordedKey {
        format: Format,
        columns: key.columns.iter().map(ToString::to_string).collect(),
        root: key.root.as_ref().map(|root| RecordedRoot {
            table: root.table.to_string(),
            id: root.id.to_string(),
            seq: root.seq.to_string(),
        }),
        changes: key.changes.clone(),
        history: key.history.clone(),
    };
    serde_json::to_string(&recorded).expect("merge keys serialize")
}

/// The merge key a [`SqlPlanner::staged`](super::SqlPlanner::staged) row records: its key
/// columns, with the root of a child table, a change stream's change columns or a history
/// table's history columns, and its sequence column.
///
/// # Errors
///
/// An `Internal` error where `columns` is not a key this build recorded.
pub fn merge_key(columns: &str, seq: &str) -> Result<MergeKey> {
    let recorded: RecordedKey = serde_json::from_str(columns).map_err(|error| {
        ConnectorError::internal(format!("a staged merge key is not recorded JSON: {error}"))
    })?;
    Ok(MergeKey {
        columns: recorded.columns.into_iter().map(Into::into).collect(),
        seq: seq.into(),
        root: recorded.root.map(|root| RootKey {
            table: root.table.into(),
            id: root.id.into(),
            seq: root.seq.into(),
        }),
        changes: recorded.changes,
        history: recorded.history,
    })
}

#[cfg(test)]
mod tests;
