//! The canonical single-`id`-column schema, its Arrow batch, and the
//! `CommitMeta`/`StateDoc` builder — the trio the destination recovery and
//! exactly-once cells share, so a change lands in one place instead of six.

use std::collections::BTreeMap;
use std::sync::Arc;

use arrow::array::{Int64Array, RecordBatch};
use arrow::datatypes::{DataType, Field, Schema};
use rdlt_connector::core::{
    ColumnDef, ColumnType, CommitCounters, CommitMeta, LoadId, LogicalType, PipelineId, Provenance,
    StateDoc, TableName, TableSchema,
};

/// The canonical logical schema: one non-nullable `id: Int64` column.
pub fn schema_for(table: &str) -> TableSchema {
    TableSchema {
        table: TableName::new(table),
        parent: None,
        columns: vec![ColumnDef {
            name: "id".into(),
            ty: ColumnType::scalar(LogicalType::Int64),
            nullable: false,
            provenance: Provenance::Inferred,
        }],
    }
}

/// A one-column Arrow batch of the given ids. The Arrow field is derived from
/// [`schema_for`] so the logical and physical shapes cannot drift apart.
pub fn batch_of(ids: &[i64]) -> RecordBatch {
    let column = schema_for("_").columns.remove(0);
    RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new(
            column.name,
            DataType::Int64,
            column.nullable,
        )])),
        vec![Arc::new(Int64Array::from(ids.to_vec()))],
    )
    .expect("batch")
}

/// A commit envelope for `(pipeline, load, seq)` with an otherwise-empty
/// state doc — the fresh-state shape the recovery/exactly-once cells commit.
pub fn meta_for(pipeline: &PipelineId, load: &LoadId, seq: u64) -> CommitMeta {
    CommitMeta {
        load_id: load.clone(),
        commit_seq: seq,
        state: StateDoc {
            format_version: 1,
            pipeline: pipeline.clone(),
            cursors: BTreeMap::new(),
            schema_hashes: BTreeMap::new(),
            last_commit: None,
            engine_version: "test".into(),
        },
        counters: CommitCounters::default(),
    }
}
