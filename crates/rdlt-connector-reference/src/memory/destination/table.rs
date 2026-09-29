//! A memory table: its rows, generations and staged batches, and how they merge.

use std::collections::BTreeMap;
use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use rdlt_connector::prelude::*;
use rdlt_connector::{Epoch, GenerationId, MergeKey, PipelineId, RootKey, SegmentId};

use crate::merge::{merge, merge_children};

#[derive(Debug, Default)]
pub(super) struct Table {
    /// The pipeline the table belongs to: the first to refer to it.
    pub(super) owner: Option<PipelineId>,
    pub(super) schema: Option<TableSchema>,
    /// How the table merges; `None` appends.
    pub(super) merge: Option<MergeKey>,
    pub(super) published: Vec<RecordBatch>,
    /// Committed rows of replace generations not yet swapped in.
    pub(super) generations: BTreeMap<GenerationId, Vec<RecordBatch>>,
    /// Staged batches by the pipeline and epoch of the session that staged them, and segment.
    pub(super) staged: BTreeMap<(PipelineId, Epoch, SegmentId), Staged>,
}

/// What a commit does to one table: its name, the batches the commit staged for it, and for a
/// merge table its rows once merged.
pub(super) type Plan = (String, Staged, Option<Vec<RecordBatch>>);

/// Batches staged under one segment, each for the table itself or for a replace generation.
pub(super) type Staged = Vec<(Option<GenerationId>, RecordBatch)>;

impl Table {
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

    /// The table's rows once `staged` is merged in by `key`.
    pub(super) fn merged(&self, staged: &Staged, key: &MergeKey) -> Result<Vec<RecordBatch>> {
        let incoming: Vec<RecordBatch> = staged.iter().map(|(_, batch)| batch.clone()).collect();
        merge(&self.merge_schema(staged)?, &self.published, &incoming, key)
            .map_err(|error| ConnectorError::data(format!("merging rows: {error}")))
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
        merge_children(&schema, &self.published, &incoming, key, root, roots)
            .map_err(|error| ConnectorError::data(format!("merging child rows: {error}")))
    }
}
