//! The columns a stream excludes: a read may leave them out of what it sends, and every Arrow and
//! change push is projected without them before anything holds it.

#[cfg(test)]
mod tests;

use std::collections::BTreeSet;
use std::sync::{Arc, LazyLock};

use arrow_array::cast::AsArray;
use arrow_array::{ArrayRef, RecordBatch, RecordBatchOptions};
use arrow_schema::{FieldRef, Schema, SchemaRef};

use crate::change::{UNCHANGED_COLUMN, remap_unchanged};
use crate::error::{ConnectorError, Result};
use crate::sink::Push;

/// The top-level columns a stream excludes, by name: a read may leave them out of what it sends,
/// and the engine drops them from whatever it sends.
///
/// A name no column has excludes nothing.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Exclusion {
    columns: Arc<BTreeSet<String>>,
}

impl Default for Exclusion {
    /// Excluding nothing, which every exclusion of no column shares rather than allocates.
    fn default() -> Self {
        static NOTHING: LazyLock<Arc<BTreeSet<String>>> = LazyLock::new(Arc::default);
        Self {
            columns: Arc::clone(&NOTHING),
        }
    }
}

impl Exclusion {
    /// The exclusion of `columns`.
    pub fn new(columns: impl IntoIterator<Item = impl Into<String>>) -> Self {
        let columns: BTreeSet<String> = columns.into_iter().map(Into::into).collect();
        if columns.is_empty() {
            return Self::default();
        }
        Self {
            columns: Arc::new(columns),
        }
    }

    /// Whether it excludes nothing.
    pub fn is_empty(&self) -> bool {
        self.columns.is_empty()
    }

    /// Whether it excludes the column `name`.
    pub fn contains(&self, name: &str) -> bool {
        self.columns.contains(name)
    }

    /// The columns it excludes, in order.
    pub fn columns(&self) -> impl Iterator<Item = &str> {
        self.columns.iter().map(String::as_str)
    }
}

/// Projects batches without an exclusion's columns, keeping what it worked out for the schema it
/// met last for the batches that follow in it.
#[derive(Debug, Default)]
pub(crate) struct Projection {
    exclusion: Exclusion,
    last: Option<Projected>,
}

/// How the batches of one schema are projected.
#[derive(Debug)]
struct Projected {
    /// The schema they arrive in.
    from: SchemaRef,
    /// What is kept of them; none where they hold no excluded column.
    kept: Option<Kept>,
}

/// What is kept of the batches of a schema that holds excluded columns.
#[derive(Debug)]
struct Kept {
    /// The schema they are projected to.
    schema: SchemaRef,
    /// Where each kept column sits among the columns they arrive with, in order.
    columns: Vec<usize>,
    /// For each column they arrive with, where it sits among the kept ones; none where excluded.
    places: Vec<Option<usize>>,
}

impl Projection {
    /// Projects without `exclusion`'s columns.
    pub(crate) fn new(exclusion: Exclusion) -> Self {
        Self {
            exclusion,
            last: None,
        }
    }

    /// The columns it projects batches without.
    pub(crate) fn exclusion(&self) -> &Exclusion {
        &self.exclusion
    }

    /// `push` without the excluded columns: an Arrow or change batch projected, JSON as it is,
    /// for the shredder skips excluded keys.
    pub(crate) fn push(&mut self, push: Push) -> Result<Push> {
        Ok(match push {
            Push::Arrow(batch) => Push::Arrow(self.arrow(batch)?),
            Push::Changes(batch) => Push::Changes(self.changes(batch)?),
            json @ Push::Json(_) => json,
        })
    }

    /// `batch` without the excluded columns; the kept columns' arrays are shared, not copied.
    pub(crate) fn arrow(&mut self, batch: RecordBatch) -> Result<RecordBatch> {
        let Some(kept) = self.kept(batch.schema_ref()) else {
            return Ok(batch);
        };
        kept.batch(kept.columns(&batch), batch.num_rows())
    }

    /// `batch`, a change batch, without the excluded columns, its unchanged flags moved to the
    /// ordinals the kept fields take.
    ///
    /// An unchanged column that is not `Binary` is kept as it is, for the batch's validation to
    /// refuse.
    pub(crate) fn changes(&mut self, batch: RecordBatch) -> Result<RecordBatch> {
        let Some(kept) = self.kept(batch.schema_ref()) else {
            return Ok(batch);
        };
        let mut columns = kept.columns(&batch);
        for (column, field) in columns.iter_mut().zip(kept.schema.fields()) {
            if field.name() == UNCHANGED_COLUMN
                && let Some(flags) = column.as_binary_opt::<i32>()
            {
                *column = Arc::new(remap_unchanged(flags, &kept.places));
            }
        }
        kept.batch(columns, batch.num_rows())
    }

    /// What is kept of batches in `schema`, none where they hold no excluded column: worked out
    /// again only for a schema that is neither the schema met last, the same `Arc` or equal to
    /// it, nor the schema it projected that to.
    fn kept(&mut self, schema: &SchemaRef) -> Option<&Kept> {
        if self.exclusion.is_empty() {
            return None;
        }
        let met = match &self.last {
            // A batch already projected holds no excluded column.
            Some(Projected {
                kept: Some(kept), ..
            }) if Arc::ptr_eq(&kept.schema, schema) => return None,
            Some(last) => Arc::ptr_eq(&last.from, schema) || *last.from == **schema,
            None => false,
        };
        if !met {
            self.last = Some(Projected::of(schema, &self.exclusion));
        }
        self.last.as_ref().and_then(|last| last.kept.as_ref())
    }
}

impl Projected {
    /// How batches in `schema` are projected without `exclusion`'s columns.
    fn of(schema: &SchemaRef, exclusion: &Exclusion) -> Self {
        let fields = schema.fields();
        let mut columns = Vec::with_capacity(fields.len());
        let mut places = Vec::with_capacity(fields.len());
        for (place, field) in fields.iter().enumerate() {
            if exclusion.contains(field.name()) {
                places.push(None);
            } else {
                places.push(Some(columns.len()));
                columns.push(place);
            }
        }
        let kept = (columns.len() < fields.len()).then(|| {
            let kept: Vec<FieldRef> = columns
                .iter()
                .map(|place| Arc::clone(&fields[*place]))
                .collect();
            let projected = Schema::new_with_metadata(kept, schema.metadata().clone());
            Kept {
                schema: Arc::new(projected),
                columns,
                places,
            }
        });
        Self {
            from: Arc::clone(schema),
            kept,
        }
    }
}

impl Kept {
    /// The kept columns of `batch`, shared.
    fn columns(&self, batch: &RecordBatch) -> Vec<ArrayRef> {
        self.columns
            .iter()
            .map(|place| Arc::clone(batch.column(*place)))
            .collect()
    }

    /// The batch of `rows` rows of the kept `columns`.
    fn batch(&self, columns: Vec<ArrayRef>, rows: usize) -> Result<RecordBatch> {
        let options = RecordBatchOptions::new().with_row_count(Some(rows));
        RecordBatch::try_new_with_options(Arc::clone(&self.schema), columns, &options).map_err(
            |error| {
                ConnectorError::internal("projecting a push without its excluded columns")
                    .with_source(error)
            },
        )
    }
}
