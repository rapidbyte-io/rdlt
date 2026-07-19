//! Capability-driven lowering at the destination seam (design doc §5.3).
//!
//! Inside the engine, nested objects are struct columns. Destinations without native
//! structs receive a FLATTENED schema and batches: `profile.city` becomes column
//! `profile__city`, collision-safe (distinct source paths never merge — hash-suffixed
//! by `UniqueNamer`). Decimals lower to canonical text when unsupported.
//! Names are a pure function of the engine schema (first-seen order, append-only), so
//! they are stable across batches and runs.

use std::sync::Arc;

use arrow::array::{Array, ArrayRef, Decimal128Array, StringArray, StructArray};
use arrow::buffer::NullBuffer;
use arrow::record_batch::RecordBatch;
use rdlt_connector::DestCapabilities;
use rdlt_core::naming::UniqueNamer;
use rdlt_core::{ColumnDef, ColumnType, LogicalType, RdltError, TableSchema};

pub(crate) fn needs_lowering(caps: &DestCapabilities) -> bool {
    !caps.structs || !caps.decimal
}

/// Lower a schema for the destination's capabilities.
pub(crate) fn lower_schema(schema: &TableSchema, caps: &DestCapabilities) -> TableSchema {
    if !needs_lowering(caps) {
        return schema.clone();
    }
    let mut namer = UniqueNamer::new(caps.ident_rules);
    let mut columns: Vec<ColumnDef> = Vec::new();
    for column in &schema.columns {
        lower_column(column, &[], caps, &mut namer, &mut columns);
    }
    TableSchema {
        table: schema.table.clone(),
        parent: schema.parent.clone(),
        columns,
    }
}

fn lower_column(
    column: &ColumnDef,
    path: &[&str],
    caps: &DestCapabilities,
    namer: &mut UniqueNamer,
    out: &mut Vec<ColumnDef>,
) {
    let mut full_path: Vec<&str> = path.to_vec();
    full_path.push(&column.name);
    match &column.ty {
        ColumnType::Struct { fields } if !caps.structs => {
            for field in fields {
                lower_column(field, &full_path, caps, namer, out);
            }
        }
        ty => {
            let lowered_ty = match ty {
                ColumnType::Scalar {
                    scalar: LogicalType::Decimal { .. },
                } if !caps.decimal => ColumnType::scalar(LogicalType::Utf8),
                other => other.clone(),
            };
            out.push(ColumnDef {
                name: namer.name_for(&full_path.join("__")),
                ty: lowered_ty,
                nullable: column.nullable || !path.is_empty(),
                provenance: column.provenance,
            });
        }
    }
}

/// Lower a batch. Driven by the batch's OWN arrow schema (which mirrors the engine
/// schema by clause E4), so replay can lower without engine-schema bookkeeping.
/// Field names/paths are identical to `lower_schema`'s inputs → identical names out.
pub(crate) fn lower_batch(
    batch: &RecordBatch,
    caps: &DestCapabilities,
) -> Result<RecordBatch, RdltError> {
    if !needs_lowering(caps) {
        return Ok(batch.clone());
    }
    let mut namer = UniqueNamer::new(caps.ident_rules);
    let mut fields: Vec<arrow::datatypes::Field> = Vec::new();
    let mut arrays: Vec<ArrayRef> = Vec::new();
    for (idx, field) in batch.schema().fields().iter().enumerate() {
        flatten_array(
            field,
            &[],
            Arc::clone(batch.column(idx)),
            caps,
            &mut namer,
            &mut fields,
            &mut arrays,
        )?;
    }
    RecordBatch::try_new(Arc::new(arrow::datatypes::Schema::new(fields)), arrays)
        .map_err(|e| RdltError::config(format!("lowering produced invalid batch: {e}")))
}

fn flatten_array(
    field: &arrow::datatypes::Field,
    path: &[&str],
    array: ArrayRef,
    caps: &DestCapabilities,
    namer: &mut UniqueNamer,
    fields: &mut Vec<arrow::datatypes::Field>,
    out: &mut Vec<ArrayRef>,
) -> Result<(), RdltError> {
    use arrow::datatypes::DataType;
    let mut full_path: Vec<&str> = path.to_vec();
    full_path.push(field.name());
    match field.data_type() {
        DataType::Struct(children) if !caps.structs => {
            let struct_array = array
                .as_any()
                .downcast_ref::<StructArray>()
                .ok_or_else(|| RdltError::config("schema says struct, array is not"))?;
            let parent_nulls = struct_array.nulls().cloned();
            for (i, child_field) in children.iter().enumerate() {
                let child = with_merged_nulls(struct_array.column(i), parent_nulls.as_ref());
                flatten_array(child_field, &full_path, child, caps, namer, fields, out)?;
            }
            Ok(())
        }
        DataType::Decimal128(_, scale) if !caps.decimal => {
            let decimals = array
                .as_any()
                .downcast_ref::<Decimal128Array>()
                .ok_or_else(|| RdltError::config("schema says decimal, array is not"))?;
            let scale = (*scale).max(0) as u8;
            let rendered: StringArray = decimals
                .iter()
                .map(|v| v.map(|raw| render_decimal(raw, scale)))
                .collect();
            fields.push(arrow::datatypes::Field::new(
                namer.name_for(&full_path.join("__")),
                DataType::Utf8,
                true,
            ));
            out.push(Arc::new(rendered));
            Ok(())
        }
        other => {
            fields.push(arrow::datatypes::Field::new(
                namer.name_for(&full_path.join("__")),
                other.clone(),
                field.is_nullable() || !path.is_empty(),
            ));
            out.push(array);
            Ok(())
        }
    }
}

/// A struct-null row must null its flattened children too.
fn with_merged_nulls(child: &ArrayRef, parent: Option<&NullBuffer>) -> ArrayRef {
    let Some(parent) = parent else {
        return Arc::clone(child);
    };
    let merged = match child.nulls() {
        Some(own) => NullBuffer::union(Some(own), Some(parent)),
        None => Some(parent.clone()),
    };
    let data = child
        .to_data()
        .into_builder()
        .nulls(merged)
        .build()
        .expect("null-merge preserves layout");
    arrow::array::make_array(data)
}

fn render_decimal(raw: i128, scale: u8) -> String {
    if scale == 0 {
        return raw.to_string();
    }
    let negative = raw < 0;
    let digits = raw.unsigned_abs().to_string();
    let scale = scale as usize;
    let padded = format!("{digits:0>width$}", width = scale + 1);
    let (int_part, frac_part) = padded.split_at(padded.len() - scale);
    format!("{}{int_part}.{frac_part}", if negative { "-" } else { "" })
}
