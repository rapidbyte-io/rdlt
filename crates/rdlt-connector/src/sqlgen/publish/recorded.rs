//! How a staged segment records its table's merge key in the catalog.

use crate::destination::{ChangeColumns, MergeKey, RootKey};
use crate::error::{ConnectorError, Result};

/// How a staged segment records a merge key: its key columns as a JSON array, or an object of its
/// key columns and, for a child table, its root, or for a change stream's table, its change
/// columns.
#[derive(serde::Serialize, serde::Deserialize)]
#[serde(untagged)]
enum RecordedKey {
    Columns(Vec<String>),
    Keyed {
        columns: Vec<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        root: Option<RecordedRoot>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        changes: Option<ChangeColumns>,
    },
}

#[derive(serde::Serialize, serde::Deserialize)]
struct RecordedRoot {
    table: String,
    id: String,
    seq: String,
}

/// `key`'s columns, root and change columns as a staged segment records them.
pub(super) fn encode_merge_key(key: &MergeKey) -> String {
    let columns = key.columns.iter().map(ToString::to_string).collect();
    let recorded = match (&key.root, &key.changes) {
        (None, None) => RecordedKey::Columns(columns),
        (root, changes) => RecordedKey::Keyed {
            columns,
            root: root.as_ref().map(|root| RecordedRoot {
                table: root.table.to_string(),
                id: root.id.to_string(),
                seq: root.seq.to_string(),
            }),
            changes: changes.clone(),
        },
    };
    serde_json::to_string(&recorded).expect("merge keys serialize")
}

/// The merge key a [`SqlPlanner::staged`](super::SqlPlanner::staged) row records: its key
/// columns, a JSON array or an object of the columns with the root of a child table or a change
/// stream's change columns, and its sequence column.
pub fn merge_key(columns: &str, seq: &str) -> Result<MergeKey> {
    let recorded: RecordedKey = serde_json::from_str(columns).map_err(|error| {
        ConnectorError::internal(format!("a staged merge key is not recorded JSON: {error}"))
    })?;
    let (columns, root, changes) = match recorded {
        RecordedKey::Columns(columns) => (columns, None, None),
        RecordedKey::Keyed {
            columns,
            root,
            changes,
        } => {
            let root = root.map(|root| RootKey {
                table: root.table.into(),
                id: root.id.into(),
                seq: root.seq.into(),
            });
            (columns, root, changes)
        }
    };
    Ok(MergeKey {
        columns: columns.into_iter().map(Into::into).collect(),
        seq: seq.into(),
        root,
        changes,
    })
}
