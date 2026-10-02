//! Name maps: a table's columns mapped to destination identifiers.

use std::collections::BTreeMap;
use std::sync::Arc;

use serde::{Deserialize, Serialize};

use crate::schema::ColumnKey;

/// A table's columns mapped to destination identifiers.
///
/// The map is injective, so two columns never share an identifier, and append-only: a mapping,
/// once added, never changes. A recorded map is read back only as such.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(
    try_from = "Vec<(ColumnKey, String)>",
    into = "Vec<(ColumnKey, String)>"
)]
pub struct NameMap {
    names: BTreeMap<ColumnKey, Arc<str>>,
    /// The column each identifier names.
    owners: BTreeMap<Arc<str>, ColumnKey>,
}

/// A mapping a name map refuses.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
pub enum NameConflict {
    /// The column already has another identifier.
    #[error("{key} is already mapped to {existing:?}")]
    Remapped {
        /// The column.
        key: ColumnKey,
        /// Its identifier.
        existing: String,
    },
    /// The identifier already names another column.
    #[error("{name:?} already names {owner}")]
    Taken {
        /// The identifier.
        name: String,
        /// The column it names.
        owner: ColumnKey,
    },
}

impl NameMap {
    /// The identifier of `key`.
    pub fn get(&self, key: &ColumnKey) -> Option<&str> {
        self.names.get(key).map(AsRef::as_ref)
    }

    /// The column `name` identifies.
    pub fn owner(&self, name: &str) -> Option<&ColumnKey> {
        self.owners.get(name)
    }

    /// Maps `key` to `name`; mapping a column again to the same name is a no-op.
    pub fn insert(
        &mut self,
        key: impl Into<ColumnKey>,
        name: impl Into<Arc<str>>,
    ) -> Result<(), NameConflict> {
        let key = key.into();
        let name = name.into();
        if let Some(existing) = self.names.get(&key) {
            return if *existing == name {
                Ok(())
            } else {
                Err(NameConflict::Remapped {
                    key,
                    existing: existing.to_string(),
                })
            };
        }
        if let Some(owner) = self.owner(&name) {
            return Err(NameConflict::Taken {
                name: name.to_string(),
                owner: owner.clone(),
            });
        }
        self.owners.insert(Arc::clone(&name), key.clone());
        self.names.insert(key, name);
        Ok(())
    }

    /// The mappings, ordered by column.
    pub fn iter(&self) -> impl Iterator<Item = (&ColumnKey, &str)> {
        self.names.iter().map(|(key, name)| (key, name.as_ref()))
    }

    /// The number of mappings.
    pub fn len(&self) -> usize {
        self.names.len()
    }

    /// Whether there are no mappings.
    pub fn is_empty(&self) -> bool {
        self.names.is_empty()
    }
}

impl TryFrom<Vec<(ColumnKey, String)>> for NameMap {
    type Error = NameConflict;

    /// The map of `pairs`, each added in turn: a column mapped twice, or two on one identifier,
    /// is refused.
    fn try_from(pairs: Vec<(ColumnKey, String)>) -> Result<Self, NameConflict> {
        let mut names = Self::default();
        for (key, name) in pairs {
            if let Some(existing) = names.get(&key) {
                let existing = existing.to_owned();
                return Err(NameConflict::Remapped { key, existing });
            }
            names.insert(key, name)?;
        }
        Ok(names)
    }
}

impl From<NameMap> for Vec<(ColumnKey, String)> {
    fn from(names: NameMap) -> Self {
        names
            .names
            .into_iter()
            .map(|(key, name)| (key, name.to_string()))
            .collect()
    }
}
