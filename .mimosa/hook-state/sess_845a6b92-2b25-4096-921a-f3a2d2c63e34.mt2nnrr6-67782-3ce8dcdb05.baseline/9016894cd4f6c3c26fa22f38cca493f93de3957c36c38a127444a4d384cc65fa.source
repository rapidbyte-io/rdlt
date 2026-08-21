//! Canonical fixtures: the single-`id`-column schema, its Arrow batch,
//! the commit envelope, and the ONE logical→Arrow derivation every
//! fixture in this crate routes through — recovery and exactly-once
//! cells across many crates share these, so a change lands in one place.

use std::sync::Arc;

use arrow::array::{Int64Array, RecordBatch};
use arrow::datatypes::{DataType, Field, Schema};
use rdlt_connector::core::commit::{self, CommitMeta};
use rdlt_connector::core::id::{LoadId, PipelineId, TableName};
use rdlt_connector::core::schema::{Column, ColumnType, Provenance, TableSchema};
use rdlt_connector::core::state::StateDoc;
use rdlt_connector::core::types::LogicalType;

/// The canonical logical schema: one non-nullable `id: Int64` column.
pub fn schema_for(table: &str) -> TableSchema {
    TableSchema {
        table: TableName::new(table),
        parent: None,
        columns: vec![Column {
            name: "id".into(),
            column_type: ColumnType::scalar(LogicalType::Int64),
            nullable: false,
            provenance: Provenance::Inferred,
        }],
    }
}

/// A one-column Arrow batch of the given ids. Derived from [`schema_for`]
/// through the crate's one logical→Arrow derivation, so the logical and
/// physical shapes cannot drift apart.
pub fn batch_of(ids: &[i64]) -> RecordBatch {
    RecordBatch::try_new(
        Arc::new(arrow_schema(&schema_for("_"))),
        vec![Arc::new(Int64Array::from(ids.to_vec()))],
    )
    .expect("fixture batch")
}

/// A commit envelope for `(pipeline, load, seq)` with an otherwise-fresh
/// state doc — the shape the recovery/exactly-once cells commit.
pub fn commit_meta_for(pipeline: &PipelineId, load: &LoadId, seq: u64) -> CommitMeta {
    CommitMeta {
        load_id: load.clone(),
        commit_seq: seq,
        state: StateDoc::new(pipeline.clone(), "test"),
        counters: commit::Counters::default(),
    }
}

/// The ONE logical→Arrow schema derivation for fixtures. Only the scalar
/// types fixture schemas use are handled — any other type is a bug in a
/// FIXTURE, not a runtime input, and says so at the panic.
pub(crate) fn arrow_schema(logical: &TableSchema) -> Schema {
    Schema::new(
        logical
            .columns
            .iter()
            .map(arrow_field)
            .collect::<Vec<Field>>(),
    )
}

fn arrow_field(column: &Column) -> Field {
    let data_type = match &column.column_type {
        ColumnType::Scalar {
            scalar: LogicalType::Utf8,
        } => DataType::Utf8,
        ColumnType::Scalar {
            scalar: LogicalType::Int64,
        } => DataType::Int64,
        other => unreachable!("fixture schemas use only Utf8/Int64 columns, got {other:?}"),
    };
    Field::new(column.name.clone(), data_type, column.nullable)
}
