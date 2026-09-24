//! Name maps: a table's columns mapped to destination identifiers.

use std::collections::BTreeMap;
use std::sync::Arc;

use serde::{Deserialize, Serialize};

use crate::schema::ColumnKey;

/// A table's columns mapped to destination identifiers.
///
/// The map is injective, so two columns never share an identifier, and append-only: a mapping,
/// once added, never changes.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(from = "Vec<(ColumnKey, String)>", into = "Vec<(ColumnKey, String)>")]
pub struct NameMap(BTreeMap<ColumnKey, Arc<str>>);

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
        self.0.get(key).map(AsRef::as_ref)
    }

    /// The column `name` identifies.
    pub fn owner(&self, name: &str) -> Option<&ColumnKey> {
        self.0
            .iter()
            .find_map(|(key, existing)| (existing.as_ref() == name).then_some(key))
    }

    /// Maps `key` to `name`; mapping a column again to the same name is a no-op.
    pub fn insert(
        &mut self,
        key: impl Into<ColumnKey>,
        name: impl Into<Arc<str>>,
    ) -> Result<(), NameConflict> {
        let key = key.into();
        let name = name.into();
        if let Some(existing) = self.0.get(&key) {
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
        self.0.insert(key, name);
        Ok(())
    }

    /// The mappings, ordered by column.
    pub fn iter(&self) -> impl Iterator<Item = (&ColumnKey, &str)> {
        self.0.iter().map(|(key, name)| (key, name.as_ref()))
    }

    /// The number of mappings.
    pub fn len(&self) -> usize {
        self.0.len()
    }

    /// Whether there are no mappings.
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

impl From<Vec<(ColumnKey, String)>> for NameMap {
    fn from(pairs: Vec<(ColumnKey, String)>) -> Self {
        Self(
            pairs
                .into_iter()
                .map(|(key, name)| (key, Arc::from(name)))
                .collect(),
        )
    }
}

impl From<NameMap> for Vec<(ColumnKey, String)> {
    fn from(names: NameMap) -> Self {
        names
            .0
            .into_iter()
            .map(|(key, name)| (key, name.to_string()))
            .collect()
    }
}
