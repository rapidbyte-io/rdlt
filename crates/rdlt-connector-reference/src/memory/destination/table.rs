//! A memory table: its rows, tombstones, generations and staged batches, and how they merge.

use std::collections::BTreeMap;
use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use rdlt_connector::prelude::*;
use rdlt_connector::{Deletion, Epoch, GenerationId, MergeKey, PipelineId, RootKey, SegmentId};

use crate::columns::changed;
use crate::merge::{Merged, failed, merge_children_sparse, merge_sparse};

#[derive(Debug, Default)]
pub(super) struct Table {
    /// The pipeline the table belongs to: the first to refer to it.
    pub(super) owner: Option<PipelineId>,
    pub(super) schema: Option<TableSchema>,
    /// How the table merges; `None` appends.
    pub(super) merge: Option<MergeKey>,
    pub(super) published: Vec<RecordBatch>,
    /// A change stream's tombstones: the rows it removed outright, which no earlier change
    /// brings back.
    pub(super) tombstones: Vec<RecordBatch>,
    /// Committed rows of replace generations not yet swapped in.
    pub(super) generations: BTreeMap<GenerationId, Vec<RecordBatch>>,
    /// Staged batches by the pipeline and epoch of the session that staged them, and segment.
    pub(super) staged: BTreeMap<(PipelineId, Epoch, SegmentId), Staged>,
}

/// What a commit does to one table: its name, the batches the commit staged for it, and for a
/// merge table its rows once merged.
pub(super) type Plan = (String, Staged, Option<Merged>);

/// Batches staged under one segment, each for the table itself or for a replace generation.
pub(super) type Staged = Vec<(Option<GenerationId>, RecordBatch)>;

/// Refuses `key` as the merge key of the table `name` unless it names a key column and, where the
/// table's columns are known as `schema`, each column it names is the table's: its key columns
/// (a child table's first alone, its root id), its sequence, where deletes are soft their
/// deletion time, and a history table's history columns.
///
/// A key the rows cannot be merged by is a `Data` error coded `merge_key_invalid`.
pub(super) fn holds_key(name: &str, key: &MergeKey, schema: Option<&TableSchema>) -> Result<()> {
    let invalid = |what: String| ConnectorError::data(what).with_code("merge_key_invalid");
    let Some(root_id) = key.columns.first() else {
        return Err(invalid(format!(
            "table {name} is merged by a key of no column"
        )));
    };
    let Some(schema) = schema else {
        return Ok(());
    };
    let keys = match &key.root {
        Some(_) => std::slice::from_ref(root_id),
        None => &key.columns[..],
    };
    let at = key
        .changes
        .as_ref()
        .and_then(|changes| match &changes.deletion {
            Deletion::Soft { at } => Some(at),
            Deletion::Hard => None,
        });
    let history = key.history.iter().flat_map(|history| {
        [
            &history.valid_from,
            &history.valid_to,
            &history.is_current,
            &history.row_hash,
        ]
    });
    for column in keys.iter().chain([&key.seq]).chain(at).chain(history) {
        if !schema
            .fields()
            .iter()
            .any(|field| *field.name() == **column)
        {
            return Err(invalid(format!(
                "table {name} has no column {column}, which its merge key names"
            )));
        }
    }
    Ok(())
}

/// A count of rows or bytes as a receipt carries it.
pub(super) fn counted(count: usize) -> u64 {
    u64::try_from(count).unwrap_or(u64::MAX)
}

impl Table {
    /// Applies `change` to the table's schema, where every row the table holds or has staged,
    /// and every tombstone, converts to the changed schema with its value kept.
    ///
    /// A change they do not fit, as a widen to a type one of them is beyond, is a `Data` error
    /// coded `schema_conflict`, and leaves the table as it was.
    pub(super) fn change(&mut self, change: &TableChange) -> Result<()> {
        let next = changed(self.schema.as_ref(), change)?;
        let arrow = Arc::new(next.to_arrow());
        let conflict = |error: arrow_schema::ArrowError| {
            let name = &change.table().name;
            ConnectorError::data(format!(
                "table {name} holds a row the change does not fit: {error}"
            ))
            .with_code("schema_conflict")
        };
        let staged = self.staged.values().flatten().map(|(_, batch)| batch);
        let rows = self
            .published
            .iter()
            .chain(self.generations.values().flatten())
            .chain(staged);
        crate::merge::holds(rows, &arrow).map_err(conflict)?;
        // A tombstone holds its key under the table's types and its sequence as it compares.
        if let Some(key) = self.merge.as_ref().or(change.table().merge.as_ref()) {
            let kept = crate::merge::tombstone_schema(&arrow, key)
                .map_err(|error| failed("reading tombstones", &error))?;
            crate::merge::holds(&self.tombstones, &kept).map_err(conflict)?;
        }
        self.schema = Some(next);
        Ok(())
    }

    /// Refuses `batch`, written for the table, where its rows cannot be merged into it: a merge
    /// table's rows each have a sequence, and a change stream's an op and flags it may carry.
    pub(super) fn admits(&self, batch: &RecordBatch) -> Result<()> {
        let Some(key) = &self.merge else {
            return Ok(());
        };
        let stored = self
            .schema
            .as_ref()
            .map(|schema| Arc::new(schema.to_arrow()));
        crate::merge::admitted(batch, stored.as_ref(), key)
            .map_err(|error| failed("staging rows", &error))
    }

    /// The table's published rows as a reader is given them: a merge table's with every column
    /// of the table, at the table's types, though it keeps each row under the columns the row
    /// holds; any other table's as they were written.
    pub(super) fn read(&self) -> Vec<RecordBatch> {
        let Some(schema) = self.schema.as_ref().filter(|_| self.merge.is_some()) else {
            return self.published.clone();
        };
        // A schema change is taken only where every held row converts to it, so this converts.
        crate::merge::read_back(&Arc::new(schema.to_arrow()), &self.published)
            .unwrap_or_else(|_| self.published.clone())
    }

    /// The schema rows of the table merge under: its own, or else `staged`'s.
    fn merge_schema(&self, staged: &Staged) -> Result<SchemaRef> {
        match &self.schema {
            Some(schema) => Ok(Arc::new(schema.to_arrow())),
            None => staged
                .first()
                .map(|(_, batch)| batch.schema())
                .ok_or_else(|| ConnectorError::internal("merging nothing")),
        }
    }

    /// The table's rows, and tombstones, once `staged` is merged in by `key`.
    pub(super) fn merged(&self, staged: &Staged, key: &MergeKey) -> Result<Merged> {
        let incoming: Vec<RecordBatch> = staged.iter().map(|(_, batch)| batch.clone()).collect();
        let schema = self.merge_schema(staged)?;
        merge_sparse(&schema, &self.published, &self.tombstones, &incoming, key)
            .map_err(|error| failed("merging rows", &error))
    }

    /// The child table's rows once `staged` replaces the children of the roots `roots` publish.
    pub(super) fn merged_children(
        &self,
        staged: &Staged,
        key: &MergeKey,
        root: &RootKey,
        roots: &[RecordBatch],
    ) -> Result<Vec<RecordBatch>> {
        let incoming: Vec<RecordBatch> = staged.iter().map(|(_, batch)| batch.clone()).collect();
        let schema = self.merge_schema(staged)?;
        merge_children_sparse(&schema, &self.published, &incoming, key, root, roots)
            .map_err(|error| failed("merging child rows", &error))
    }
}
