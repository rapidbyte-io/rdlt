//! Arrow passthrough — the shredder's sibling fast path (contract clause E7).
//!
//! Already-structured batches are NOT re-shredded: the batch's arrow schema maps
//! onto the logical schema, the SAME registry/policy seam governs evolution, and the
//! only new data is one appended constant `_rdlt_load_id` column. Columns whose
//! arrow type equals the table's current logical type pass through zero-copy
//! (`Arc` clone); when cross-batch widening or an arrow representation difference
//! (Large* variants, timestamp unit/zone) changed a column's type, its values are
//! cast LOSSLESSLY to the current type — never semantically coerced (clause E7).
//! Structured streams carry no per-row identity (no `_rdlt_id`) — which is why
//! Merge is rejected for them (clause B4).

use std::sync::Arc;

use arrow::array::{ArrayRef, StringArray, new_null_array};
use arrow::compute::cast;
use arrow::datatypes::{DataType, TimeUnit};
use arrow::record_batch::RecordBatch;
use rdlt_connector::DestCapabilities;
use rdlt_core::naming::UniqueNamer;
use rdlt_core::schema::system_columns;
use rdlt_core::{
    ColumnDef, ColumnType, LoadId, LogicalType, PolicyAction, Provenance, RdltError, SchemaChange,
    SchemaPolicy, TableName, TableSchema, WriteMode,
};

use crate::load::LoadItem;
use crate::schema::contracts::{change_column, violation_for};
use crate::schema::registry::SchemaRegistry;
use crate::shred::build::{arrow_column_type, arrow_schema};

/// Process one structured batch: schema mapping, policy enforcement, load-id
/// stamping. Emits the standard Delta/Discarded/Batch items.
pub(crate) fn passthrough_items(
    batch: &RecordBatch,
    table: &TableName,
    registry: &mut SchemaRegistry,
    policy: &SchemaPolicy,
    load_id: &LoadId,
    mode: &WriteMode,
    caps: DestCapabilities,
) -> Result<Vec<LoadItem>, RdltError> {
    // ---- Map the arrow schema onto the logical schema ----
    let (observed, name_map) = schema_from_arrow(batch, table, caps)?;

    // ---- Policy resolution (same rules as the shredder) ----
    let changes = registry.diff(&observed);
    let mut dropped_columns: Vec<String> = Vec::new();
    let mut kept: Vec<SchemaChange> = Vec::new();
    for change in changes {
        let action = if matches!(change, SchemaChange::CreateTable { .. }) {
            PolicyAction::Evolve // first version, not evolution
        } else {
            policy.action_for(table, change_column(&change))
        };
        match (&change, action) {
            (_, PolicyAction::Evolve) => kept.push(change),
            (_, PolicyAction::Freeze) => {
                return Err(RdltError::Schema(violation_for(table, &change)));
            }
            (SchemaChange::AddColumn { column }, _) => {
                // Discard on a structured stream: project the refused column away
                // (exact for column additions), counted below.
                dropped_columns.push(column.name.clone());
            }
            (SchemaChange::WidenColumn { name, .. }, _) => {
                return Err(RdltError::config(format!(
                    "table `{table}` column `{name}`: Discard policies cannot filter \
                     value-level type changes on structured streams (clause E7); use \
                     Evolve or Freeze"
                )));
            }
            (SchemaChange::CreateTable { .. }, _) => unreachable!("handled as Evolve"),
        }
    }

    let mut items = Vec::new();
    let observed = if dropped_columns.is_empty() {
        observed
    } else {
        items.push(LoadItem::Discarded {
            table: table.clone(),
            rows: 0,
            values: batch.num_rows() as u64 * dropped_columns.len() as u64,
        });
        let mut projected = observed;
        projected
            .columns
            .retain(|c| !dropped_columns.contains(&c.name));
        projected
    };
    let changes = if dropped_columns.is_empty() {
        kept
    } else {
        registry.diff(&observed)
    };

    if let Some(delta) = registry.apply(observed, changes) {
        let current = registry
            .get(&delta.table)
            .expect("apply() just stored this schema")
            .clone();
        items.push(LoadItem::Delta {
            schema: current,
            delta,
            mode: mode.clone(),
        });
    }

    // ---- Assemble the outgoing batch against the CURRENT registry schema ----
    let current = registry.get(table).expect("registered above").clone();
    let rows = batch.num_rows();
    let mut arrays: Vec<ArrayRef> = Vec::with_capacity(current.columns.len());
    for column in &current.columns {
        if column.name == system_columns::LOAD_ID {
            arrays.push(Arc::new(StringArray::from(vec![load_id.as_str(); rows])));
            continue;
        }
        let target_type = arrow_column_type(&column.ty);
        let array = match name_map
            .iter()
            .find(|(normalized, _)| normalized == &column.name)
        {
            Some((_, idx)) => {
                let source = batch.column(*idx);
                if source.data_type() == &target_type {
                    Arc::clone(source) // the common zero-copy path
                } else {
                    // Cross-batch widening (e.g. Int64 batch under a Float64 column).
                    cast(source.as_ref(), &target_type).map_err(|e| {
                        RdltError::config(format!(
                            "table `{table}` column `{}`: cannot cast {} to {target_type}: {e}",
                            column.name,
                            source.data_type()
                        ))
                    })?
                }
            }
            // Historical column absent from this batch: null-filled.
            None => new_null_array(&target_type, rows),
        };
        arrays.push(array);
    }
    let out = RecordBatch::try_new(Arc::new(arrow_schema(&current)), arrays)
        .map_err(|e| RdltError::config(format!("passthrough batch assembly: {e}")))?;
    items.push(LoadItem::Batch {
        table: table.clone(),
        batch: out,
    });
    Ok(items)
}

/// Logical schema for a structured batch: `_rdlt_load_id` + the batch's fields
/// (normalized names, mapped types). Also returns normalized-name → column-index.
fn schema_from_arrow(
    batch: &RecordBatch,
    table: &TableName,
    caps: DestCapabilities,
) -> Result<(TableSchema, Vec<(String, usize)>), RdltError> {
    let mut namer = UniqueNamer::new(caps.ident_rules);
    namer.reserve(system_columns::LOAD_ID); // even a literal `_rdlt_load_id` input suffixes

    let mut columns = vec![ColumnDef {
        name: system_columns::LOAD_ID.to_owned(),
        ty: ColumnType::scalar(LogicalType::Utf8),
        nullable: false,
        provenance: Provenance::System,
    }];
    let mut name_map = Vec::new();
    for (idx, field) in batch.schema().fields().iter().enumerate() {
        let ty = column_type_from_arrow(field.data_type()).map_err(|reason| {
            RdltError::config(format!(
                "table `{table}` column `{}`: unmappable arrow type {} ({reason}) — \
                 never coerced silently (clause E7)",
                field.name(),
                field.data_type()
            ))
        })?;
        let name = namer.name_for(field.name());
        name_map.push((name.clone(), idx));
        columns.push(ColumnDef {
            name,
            ty,
            nullable: true,
            provenance: Provenance::Inferred,
        });
    }
    Ok((
        TableSchema {
            table: table.clone(),
            parent: None,
            columns,
        },
        name_map,
    ))
}

fn column_type_from_arrow(dt: &DataType) -> Result<ColumnType, String> {
    use LogicalType::*;
    let scalar = |t| Ok(ColumnType::scalar(t));
    match dt {
        DataType::Boolean => scalar(Bool),
        DataType::Int8
        | DataType::Int16
        | DataType::Int32
        | DataType::Int64
        | DataType::UInt8
        | DataType::UInt16
        | DataType::UInt32 => scalar(Int64),
        DataType::UInt64 => Err("UInt64 can exceed Int64; re-encode upstream".into()),
        DataType::Float16 | DataType::Float32 | DataType::Float64 => scalar(Float64),
        DataType::Utf8 | DataType::LargeUtf8 => scalar(Utf8),
        DataType::Binary | DataType::LargeBinary => scalar(Binary),
        DataType::Timestamp(_, Some(_)) => scalar(TimestampTz),
        DataType::Timestamp(_, None) => scalar(TimestampNaive),
        DataType::Date32 | DataType::Date64 => scalar(Date),
        DataType::Time32(_) | DataType::Time64(TimeUnit::Microsecond | TimeUnit::Nanosecond) => {
            scalar(Time)
        }
        DataType::Decimal128(precision, scale) if *scale >= 0 => Ok(ColumnType::scalar(Decimal {
            precision: *precision,
            scale: *scale as u8,
        })),
        DataType::Struct(fields) => {
            let mapped: Result<Vec<ColumnDef>, String> = fields
                .iter()
                .map(|f| {
                    Ok(ColumnDef {
                        name: f.name().clone(),
                        ty: column_type_from_arrow(f.data_type())?,
                        nullable: true,
                        provenance: Provenance::Inferred,
                    })
                })
                .collect();
            Ok(ColumnType::Struct { fields: mapped? })
        }
        DataType::List(item) | DataType::LargeList(item) => {
            match column_type_from_arrow(item.data_type())? {
                ColumnType::Scalar { scalar } => Ok(ColumnType::ScalarList { item: scalar }),
                _ => Err("nested lists / lists of structs are not supported in v1".into()),
            }
        }
        other => Err(format!("no logical mapping for {other}")),
    }
}
