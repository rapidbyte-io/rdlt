//! A table as the engine models it: its columns by identifier with their logical types, and the
//! source column each one holds.

use std::collections::BTreeMap;
use std::sync::Arc;

use rdlt_connector::{Field, NameMap, SchemaVersion, TableSchema, TableState};

use crate::error::{Error, ErrorKind, codes};

/// A table's columns and names; version 0 is a table not created yet.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct Model {
    /// The schema's version, which each change to the destination's table advances.
    pub(crate) version: u32,
    /// How many changes this attempt made to the model, which state has recorded when it
    /// records the model.
    pub(crate) revision: u32,
    /// The columns in order, metadata columns aside: identifiers, logical types and nullability.
    pub(crate) columns: Vec<Field>,
    pub(crate) names: NameMap,
}

/// Whether a batch's table takes every column, as one being created does.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Creation {
    /// As the table's model says: only until it is created.
    AsModeled,
    /// For every batch of the flush that found the table not yet created, so where the flush
    /// was cut decides no column's existence or type.
    ByFlush,
}

impl Model {
    /// The committed model of a table, or an empty one when nothing is committed.
    pub(crate) fn from_state(state: Option<&TableState>) -> Result<Self, Error> {
        let Some(state) = state else {
            return Ok(Self::default());
        };
        let names = state.names.clone();
        let Some((SchemaVersion(version), schema)) = &state.schema else {
            return Ok(Self {
                names,
                ..Self::default()
            });
        };
        // Version 0 is a table not created, which records no schema.
        if *version == 0 {
            return Err(Error::new(
                ErrorKind::Destination,
                "state records a table's schema at version 0",
            )
            .with_code(codes::STATE_INVALID));
        }
        let columns: Vec<Field> = schema.fields().iter().cloned().collect();
        if let Some(orphan) = columns
            .iter()
            .find(|field| names.owner(field.name()).is_none())
        {
            return Err(Error::new(
                ErrorKind::Destination,
                format!("state names no source column for column {}", orphan.name()),
            )
            .with_code(codes::STATE_INVALID));
        }
        Ok(Self {
            version: *version,
            revision: 0,
            columns,
            names,
        })
    }

    /// Whether the destination has the table.
    pub(crate) fn created(&self) -> bool {
        self.version > 0
    }

    /// The position of each column, by identifier, for finding many columns at once.
    pub(crate) fn positions(&self) -> BTreeMap<Arc<str>, usize> {
        self.columns
            .iter()
            .enumerate()
            .map(|(index, field)| (Arc::from(field.name()), index))
            .collect()
    }

    /// The model's columns as a schema.
    pub(crate) fn schema(&self) -> TableSchema {
        TableSchema::new(self.columns.clone())
            .expect("identifiers are distinct, since the name map is injective")
    }
}
