//! Arrow passthrough — the shredder's sibling fast path.
//!
//! Already-structured batches are NOT re-shredded: the batch's arrow schema maps
//! onto the logical schema, the SAME registry/policy seam governs evolution, and the
//! only new data is one appended constant `_rdlt_load_id` column. Columns whose
//! arrow type equals the table's current logical type pass through zero-copy
//! (`Arc` clone); when cross-batch widening or an arrow representation difference
//! (Large* variants, timestamp unit/zone) changed a column's type, its values are
//! cast LOSSLESSLY to the current type — never semantically coerced.
//! Structured streams carry no per-row identity (no `_rdlt_id`) — which is why
//! Keyless Merge is rejected for them; keyed structured merge is supported.

use std::sync::Arc;

use arrow::array::{ArrayRef, StringArray, new_null_array};
use arrow::compute::cast;
use arrow::datatypes::{DataType, TimeUnit};
use arrow::record_batch::RecordBatch;
use rdlt_connector::DestinationCapabilities;
use rdlt_core::naming::UniqueNamer;
use rdlt_core::schema::system_columns;
use rdlt_core::{
    ColumnDef, ColumnType, LogicalType, PolicyAction, Provenance, RdltError, SchemaChange,
    TableName, TableSchema,
};

use crate::load::LoadItem;
use crate::schema::contracts::{change_column, violation_for};
use crate::shred::ShredCtx;
use crate::shred::build::{arrow_column_type, arrow_schema};

/// Process one structured batch: schema mapping, policy enforcement, load-id
/// stamping. Emits the standard Delta/Discarded/Batch items.
pub(crate) fn passthrough_items(
    batch: &RecordBatch,
    table: &TableName,
    ctx: ShredCtx,
    caps: DestinationCapabilities,
) -> Result<Vec<LoadItem>, RdltError> {
    let ShredCtx {
        registry,
        load_id,
        mode,
        policy,
    } = ctx;
    // ---- Map the arrow schema onto the logical schema ----
    let (mut observed, normalized_to_index) = schema_from_arrow(batch, table, caps)?;

    // ---- Join with the registry's current types (widening lattice) ----
    // The shredder's observation states join implicitly; passthrough must do it
    // explicitly or a batch whose column NARROWED (Int64 after Utf8) would push
    // a narrowing delta into the registry (found by the cross-batch narrowing
    // test: debug builds assert, release builds would shrink the schema).
    if let Some(current) = registry.get(table) {
        for column in &mut observed.columns {
            if let Some(existing) = current.columns.iter().find(|c| c.name == column.name) {
                column.column_type = join_column_types(&existing.column_type, &column.column_type);
            }
        }
    }

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
                     value-level type changes on structured streams; use \
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

    if let Some((delta, current)) = registry.apply(observed, changes) {
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
        let target_type = arrow_column_type(&column.column_type);
        let array = match normalized_to_index
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
        .map_err(|e| RdltError::internal(format!("passthrough batch assembly: {e}")))?;
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
    caps: DestinationCapabilities,
) -> Result<(TableSchema, Vec<(String, usize)>), RdltError> {
    let mut namer = UniqueNamer::new(caps.ident_rules);
    namer.reserve(system_columns::LOAD_ID); // even a literal `_rdlt_load_id` input suffixes

    let mut columns = vec![ColumnDef {
        name: system_columns::LOAD_ID.to_owned(),
        column_type: ColumnType::scalar(LogicalType::Utf8),
        nullable: false,
        provenance: Provenance::System,
    }];
    let mut normalized_to_index = Vec::new();
    for (idx, field) in batch.schema().fields().iter().enumerate() {
        let ty = column_type_from_arrow(field.data_type()).map_err(|reason| {
            RdltError::config(format!(
                "table `{table}` column `{}`: unmappable arrow type {} ({reason}) — \
                 never coerced silently",
                field.name(),
                field.data_type()
            ))
        })?;
        let name = namer.name_for(field.name());
        normalized_to_index.push((name.clone(), idx));
        columns.push(ColumnDef {
            name,
            column_type: ty,
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
        normalized_to_index,
    ))
}

/// Least upper bound of two column types for cross-batch evolution: scalars
/// join on the widening lattice, lists join item-wise, structs join field-wise
/// (new fields append), and shape conflicts land on Json — the same outcomes
/// the shredder's observation states produce.
fn join_column_types(a: &ColumnType, b: &ColumnType) -> ColumnType {
    use rdlt_core::types::widen;
    match (a, b) {
        _ if a == b => a.clone(),
        (ColumnType::Scalar { scalar: x }, ColumnType::Scalar { scalar: y }) => {
            ColumnType::scalar(widen(*x, *y))
        }
        (ColumnType::ScalarList { item: x }, ColumnType::ScalarList { item: y }) => {
            ColumnType::ScalarList {
                item: widen(*x, *y),
            }
        }
        (ColumnType::Struct { fields: xs }, ColumnType::Struct { fields: ys }) => {
            let mut joined = xs.clone();
            for y in ys {
                match joined.iter_mut().find(|x| x.name == y.name) {
                    Some(x) => x.column_type = join_column_types(&x.column_type, &y.column_type),
                    None => joined.push(y.clone()),
                }
            }
            ColumnType::Struct { fields: joined }
        }
        // Shape conflict: preserved verbatim, never dropped (lattice top).
        _ => ColumnType::scalar(LogicalType::Json),
    }
}

pub(crate) fn column_type_from_arrow(dt: &DataType) -> Result<ColumnType, String> {
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
                        column_type: column_type_from_arrow(f.data_type())?,
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

#[cfg(test)]
mod tests {
    use super::*;

    /// Mutation-report closure: `Decimal128 { scale >= 0 }` guard — a negative
    /// scale must be a typed error, never a silent mapping.
    #[test]
    fn negative_decimal_scale_is_a_typed_error() {
        assert!(column_type_from_arrow(&DataType::Decimal128(10, 2)).is_ok());
        let err = column_type_from_arrow(&DataType::Decimal128(10, -2))
            .expect_err("negative scale must not map");
        assert!(err.contains("no logical mapping"), "got: {err}");
    }

    /// Mutation-report closure: the List arm's inner-scalar match — a list of
    /// scalars maps to ScalarList; lists of structs/lists are typed errors.
    #[test]
    fn list_mapping_accepts_scalars_rejects_nesting() {
        use arrow::datatypes::Field;
        use std::sync::Arc;
        let list_of = |dt| DataType::List(Arc::new(Field::new("item", dt, true)));

        assert_eq!(
            column_type_from_arrow(&list_of(DataType::Int64)).expect("scalar list"),
            ColumnType::ScalarList {
                item: LogicalType::Int64
            }
        );
        let err = column_type_from_arrow(&list_of(list_of(DataType::Int64)))
            .expect_err("nested lists are v1-unsupported");
        assert!(err.contains("not supported"), "got: {err}");
        let err = column_type_from_arrow(&list_of(DataType::Struct(
            vec![Field::new("f", DataType::Int64, true)].into(),
        )))
        .expect_err("lists of structs are v1-unsupported");
        assert!(err.contains("not supported"), "got: {err}");
    }
}
