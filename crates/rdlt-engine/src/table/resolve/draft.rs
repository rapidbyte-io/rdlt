//! A resolution in progress, as schema resolution records it.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use rdlt_connector::{ColumnKey, ColumnPath, Field, LogicalType, TypeKind};

use super::super::model::Model;
use super::{Arriving, Change, Resolution, Route};
use crate::error::{Error, codes};
use crate::naming::Naming;

/// A resolution in progress: the model, the columns it adds and the changes decided so far.
pub(super) struct Draft {
    model: Model,
    /// The position of each of the model's columns, by identifier.
    positions: BTreeMap<Arc<str>, usize>,
    /// Added columns, before they have identifiers: key, type and nullability.
    adds: Vec<(ColumnKey, LogicalType, bool)>,
    changes: Vec<Change>,
}

impl Draft {
    pub(super) fn new(model: &Model) -> Self {
        Self {
            model: model.clone(),
            positions: model.positions(),
            adds: Vec::new(),
            changes: Vec::new(),
        }
    }

    /// The type a column at `column` takes to hold `arriving`'s values too: the lattice's join,
    /// except that floats join 64-bit integers as floats, which a variant of floats holds.
    pub(super) fn join(&self, column: usize, arriving: &Arriving<'_>) -> LogicalType {
        let current = self.column_type(column);
        let floats = matches!(
            arriving.logical,
            LogicalType::Float32 | LogicalType::Float64
        );
        if current == LogicalType::Int64 && floats {
            LogicalType::Float64
        } else {
            current.join(arriving.logical)
        }
    }

    /// The position of the table's column holding `key`.
    ///
    /// A batch holds each column once, so a column this resolution adds is never looked up again.
    pub(super) fn find(&self, key: &ColumnKey) -> Option<usize> {
        let name = self.model.names.get(key)?;
        self.positions.get(name).copied()
    }

    /// The positions of the table's variant columns of `path`, in kind order.
    pub(super) fn variants(&self, path: &ColumnPath) -> Vec<usize> {
        let mut kinds: Vec<(TypeKind, usize)> = self
            .model
            .names
            .iter()
            .filter_map(|(key, _)| match key {
                ColumnKey::Variant { column, kind } if column == path => {
                    self.find(key).map(|index| (*kind, index))
                }
                _ => None,
            })
            .collect();
        kinds.sort_unstable();
        kinds.into_iter().map(|(_, index)| index).collect()
    }

    pub(super) fn column_type(&self, column: usize) -> LogicalType {
        match self.model.columns.get(column) {
            Some(field) => field.logical_type().clone(),
            None => self.adds[column - self.model.columns.len()].1.clone(),
        }
    }

    /// Adds the column for `key`; returns its position.
    pub(super) fn add(&mut self, key: ColumnKey, logical: LogicalType, nullable: bool) -> usize {
        self.adds.push((key.clone(), logical, nullable));
        self.changes.push(Change::Add { key });
        self.model.columns.len() + self.adds.len() - 1
    }

    /// Widens the table's column at `column` to `to`.
    ///
    /// Only existing columns widen: a column this resolution adds takes its final type when added.
    pub(super) fn widen(&mut self, column: usize, to: LogicalType) {
        let field = &mut self.model.columns[column];
        let from = field.logical_type().clone();
        *field = Field::new(field.name(), to, field.is_nullable());
        self.changes.push(Change::Widen { column, from });
    }

    /// Names the added columns, in one sorted push, and appends them to the model.
    pub(super) fn finish(
        mut self,
        routes: Vec<Route>,
        naming: &Naming,
    ) -> Result<Resolution, Error> {
        let keys: BTreeSet<ColumnKey> = self.adds.iter().map(|(key, ..)| key.clone()).collect();
        naming.assign_columns(&mut self.model.names, &keys)?;
        for (key, logical, nullable) in self.adds {
            let name = self
                .model
                .names
                .get(&key)
                .expect("every added column was just named")
                .to_owned();
            self.model.columns.push(Field::new(name, logical, nullable));
        }
        if !self.changes.is_empty() {
            self.model.version = advanced(self.model.version)?;
            self.model.revision = advanced(self.model.revision)?;
        }
        Ok(Resolution {
            changes: self.changes,
            routes,
            model: self.model,
        })
    }
}

/// `count`, a table's schema version or its changes this attempt, advanced by one.
///
/// # Errors
///
/// A `Schema` error coded `schema_version_exhausted` past `u32::MAX`: a table that changed so
/// often is reset to change again.
pub(super) fn advanced(count: u32) -> Result<u32, Error> {
    count.checked_add(1).ok_or_else(|| {
        Error::schema(format!(
            "the table's schema changed {count} times, the most its version counts"
        ))
        .with_code(codes::SCHEMA_VERSION_EXHAUSTED)
    })
}
