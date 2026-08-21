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

use arrow::{
    array::{ArrayRef, StringArray, new_null_array},
    compute::cast,
    datatypes::{DataType, TimeUnit},
    record_batch::RecordBatch,
};
use rdlt_connector::{
    DestinationCapabilities,
    channel::{MAX_ARROW_DEPTH, MAX_RECORD_BATCH_ROWS},
};
use rdlt_core::{
    ColumnDef, ColumnType, LogicalType, PolicyAction, Provenance, RdltError, SchemaChange,
    TableName, TableSchema, naming::UniqueNamer, schema::system_columns,
};

use crate::{
    load::LoadItem,
    schema::contracts::{change_column, violation_for},
    shred::{
        MAX_SOURCE_COLUMNS_PER_TABLE, ShredContext,
        build::{arrow_column_type, arrow_schema},
    },
};

/// Process one structured batch: schema mapping, policy enforcement, load-id
/// stamping. Emits the standard Delta/Discarded/Batch items.
pub(crate) fn passthrough_items(
    batch: &RecordBatch,
    table: &TableName,
    ctx: ShredContext,
    capabilities: DestinationCapabilities,
) -> Result<Vec<LoadItem>, RdltError> {
    if batch.num_rows() > MAX_RECORD_BATCH_ROWS {
        return Err(RdltError::config(format!(
            "table `{table}`: Arrow batch carries {} rows, over the \
             {MAX_RECORD_BATCH_ROWS}-row cap — row count is bounded separately from \
             encoded bytes to prevent per-row column amplification",
            batch.num_rows()
        )));
    }
    let ShredContext {
        registry,
        load_id,
        mode,
        policy,
    } = ctx;
    // ---- Map the arrow schema onto the logical schema ----
    let (mut observed, normalized_to_index) = schema_from_arrow(batch, table, capabilities)?;

    // ---- Join with the registry's current types (widening lattice) ----
    // The shredder's observation states join implicitly; passthrough must do it
    // explicitly or a batch whose column NARROWED (Int64 after Utf8) would push
    // a narrowing delta into the registry (found by the cross-batch narrowing
    // test: debug builds assert, release builds would shrink the schema).
    if let Some(current) = registry.get(table) {
        for column in &mut observed.columns {
            if let Some(existing) = current.columns.iter().find(|c| c.name == column.name) {
                column.column_type = join_column_types(&existing.column_type, &column.column_type)
                    .map_err(|reason| {
                        RdltError::config(format!(
                            "table `{table}` column `{}`: {reason}",
                            column.name
                        ))
                    })?;
            }
        }
    }

    // ---- Re-count breadth AFTER the join ----
    // The join APPENDS fields a batch declares that the registry has not
    // seen, so per-batch caps alone would let a stream accumulate unbounded
    // struct breadth one batch at a time — and the registry retains and
    // re-clones whatever it accepts, every batch, for the stream's lifetime.
    let source_fields: usize = observed
        .columns
        .iter()
        .filter(|column| column.provenance != Provenance::System)
        .map(|column| 1 + nested_struct_fields(&column.column_type))
        .sum();
    if source_fields > MAX_SOURCE_COLUMNS_PER_TABLE {
        return Err(RdltError::config(format!(
            "table `{table}`: cross-batch schema growth reaches {source_fields} columns \
             and nested struct fields, over the {MAX_SOURCE_COLUMNS_PER_TABLE}-source-column \
             cap — struct breadth counts toward the same bound as columns"
        )));
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
    let current = registry.get(table).expect("registered above");
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
    let out = RecordBatch::try_new(Arc::new(arrow_schema(current)), arrays)
        .map_err(|e| RdltError::internal(format!("passthrough batch assembly: {e}")))?;
    items.push(LoadItem::batch(table.clone(), out));
    Ok(items)
}

/// Logical schema for a structured batch: `_rdlt_load_id` + the batch's fields
/// (normalized names, mapped types). Also returns normalized-name → column-index.
fn schema_from_arrow(
    batch: &RecordBatch,
    table: &TableName,
    capabilities: DestinationCapabilities,
) -> Result<(TableSchema, Vec<(String, usize)>), RdltError> {
    if batch.num_columns() > MAX_SOURCE_COLUMNS_PER_TABLE {
        return Err(RdltError::config(format!(
            "table `{table}` carries {} Arrow fields, over the \
             {MAX_SOURCE_COLUMNS_PER_TABLE}-source-column cap",
            batch.num_columns()
        )));
    }
    let mut namer = UniqueNamer::new(capabilities.ident_rules);
    namer.reserve(system_columns::LOAD_ID); // even a literal `_rdlt_load_id` input suffixes

    let mut columns = vec![ColumnDef {
        name: system_columns::LOAD_ID.to_owned(),
        column_type: ColumnType::scalar(LogicalType::Utf8),
        nullable: false,
        provenance: Provenance::System,
    }];
    let mut normalized_to_index = Vec::new();
    // Struct-field breadth spends from the SAME budget as top-level columns.
    // Counted BEFORE each field is mapped — a declared million-field struct
    // must refuse without first materializing a million `ColumnDef`s, and
    // without rendering the offending schema back into the error.
    let mut source_fields = batch.num_columns();
    for (idx, field) in batch.schema().fields().iter().enumerate() {
        source_fields = source_fields.saturating_add(declared_struct_fields(field.data_type(), 0));
        if source_fields > MAX_SOURCE_COLUMNS_PER_TABLE {
            return Err(RdltError::config(format!(
                "table `{table}` column `{}`: declared struct fields push the schema over \
                 the {MAX_SOURCE_COLUMNS_PER_TABLE}-source-column cap — struct breadth \
                 counts toward the same bound as columns",
                field.name()
            )));
        }
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

/// Struct fields a declared arrow type carries, recursively — a pure count,
/// so a hostile breadth refuses before any `ColumnDef` is built. Descent
/// stops at the shared depth cap (the mapping walk right behind this refuses
/// there with its own typed error); saturating, never panicking.
fn declared_struct_fields(dt: &DataType, depth: usize) -> usize {
    if depth > MAX_ARROW_DEPTH {
        return 0;
    }
    match dt {
        DataType::Struct(fields) => fields.iter().fold(fields.len(), |total, field| {
            total.saturating_add(declared_struct_fields(field.data_type(), depth + 1))
        }),
        DataType::List(item) | DataType::LargeList(item) => {
            declared_struct_fields(item.data_type(), depth + 1)
        }
        _ => 0,
    }
}

/// Struct fields a MAPPED column type carries, recursively. Bounded by
/// construction: every input passed the declared-breadth budget, so this walk
/// touches at most a cap's worth of nodes per column.
fn nested_struct_fields(ty: &ColumnType) -> usize {
    match ty {
        ColumnType::Struct { fields } => fields.iter().fold(fields.len(), |total, field| {
            total.saturating_add(nested_struct_fields(&field.column_type))
        }),
        _ => 0,
    }
}

/// Least upper bound of two column types for cross-batch evolution: scalars
/// join on the widening lattice, lists join item-wise, structs join field-wise
/// (new fields append), and shape conflicts land on Json — the same outcomes
/// the shredder's observation states produce.
///
/// Depth-capped like every walk over connector-controlled nesting (047 M1)
/// — belt to `column_type_from_arrow`'s own gate, since both join inputs
/// already passed it (or the shredder's serde depth bound): a stack
/// overflow is an abort, so the walk refuses rather than trusts.
fn join_column_types(a: &ColumnType, b: &ColumnType) -> Result<ColumnType, String> {
    join_column_types_at(a, b, 0)
}

fn join_column_types_at(
    a: &ColumnType,
    b: &ColumnType,
    depth: usize,
) -> Result<ColumnType, String> {
    use rdlt_core::types::widen;
    if depth > MAX_ARROW_DEPTH {
        return Err(format!(
            "column nesting exceeds the {MAX_ARROW_DEPTH}-level cap — refused before \
             the cross-batch join can overflow the stack"
        ));
    }
    Ok(match (a, b) {
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
                    Some(x) => {
                        x.column_type =
                            join_column_types_at(&x.column_type, &y.column_type, depth + 1)?;
                    }
                    None => joined.push(y.clone()),
                }
            }
            ColumnType::Struct { fields: joined }
        }
        // Shape conflict: preserved verbatim, never dropped (lattice top).
        _ => ColumnType::scalar(LogicalType::Json),
    })
}

/// Map one declared arrow type onto the logical lattice. Depth-capped
/// (047 M1): the nesting of a structured stream's schema is entirely
/// CONNECTOR-controlled, and an uncapped recursive map turns a deep
/// declaration into a stack overflow — an ABORT, not a catchable panic,
/// so `spawn_blocking` containment cannot absorb it.
pub(crate) fn column_type_from_arrow(dt: &DataType) -> Result<ColumnType, String> {
    column_type_from_arrow_at(dt, 0)
}

fn column_type_from_arrow_at(dt: &DataType, depth: usize) -> Result<ColumnType, String> {
    use LogicalType::*;
    if depth > MAX_ARROW_DEPTH {
        return Err(format!(
            "schema nesting exceeds the {MAX_ARROW_DEPTH}-level cap — refused before \
             the mapping walk can overflow the stack"
        ));
    }
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
        DataType::Decimal128(precision, scale)
            if (1..=38).contains(precision) && *scale >= 0 && *scale <= *precision as i8 =>
        {
            Ok(ColumnType::scalar(Decimal {
                precision: *precision,
                scale: *scale as u8,
            }))
        }
        DataType::Struct(fields) => {
            let mapped: Result<Vec<ColumnDef>, String> = fields
                .iter()
                .map(|f| {
                    Ok(ColumnDef {
                        name: f.name().clone(),
                        column_type: column_type_from_arrow_at(f.data_type(), depth + 1)?,
                        nullable: true,
                        provenance: Provenance::Inferred,
                    })
                })
                .collect();
            Ok(ColumnType::Struct { fields: mapped? })
        }
        DataType::List(item) | DataType::LargeList(item) => {
            match column_type_from_arrow_at(item.data_type(), depth + 1)? {
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

    /// 2H4: a bit-packed boolean frame can describe enormous row counts in a
    /// small byte payload. Refuse it before allocating the constant load-id
    /// column or any other per-row output.
    #[test]
    fn a_batch_over_the_row_cap_refuses_before_amplification() {
        let rows = MAX_RECORD_BATCH_ROWS + 1;
        let batch = RecordBatch::try_new(
            Arc::new(arrow::datatypes::Schema::new(vec![
                arrow::datatypes::Field::new("flag", DataType::Boolean, false),
            ])),
            vec![Arc::new(arrow::array::BooleanArray::from(vec![true; rows]))],
        )
        .expect("valid compact boolean batch");
        let mut registry = crate::schema::registry::SchemaRegistry::default();
        let load_id = rdlt_core::LoadId::new("load");
        let mode = rdlt_core::WriteMode::Append;
        let policy = rdlt_core::SchemaPolicy::default();
        let error = passthrough_items(
            &batch,
            &TableName::new("events"),
            ShredContext {
                registry: &mut registry,
                load_id: &load_id,
                mode: &mode,
                policy: &policy,
            },
            DestinationCapabilities::default(),
        )
        .expect_err("oversized row count must refuse");

        assert!(
            error.to_string().contains("row cap"),
            "the refusal names the independent bound: {error}"
        );
        assert!(registry.is_empty(), "nothing was registered before refusal");
    }

    /// Struct-field BREADTH counts toward the same source-column cap as
    /// top-level columns: one declared column carrying a struct of cap
    /// fields is retained by the registry and re-cloned per batch, so a
    /// small wire schema must not smuggle in an unbounded field count.
    #[test]
    fn declared_struct_fields_count_toward_the_source_column_cap() {
        use arrow::datatypes::Field;
        let wide = DataType::Struct(
            (0..MAX_SOURCE_COLUMNS_PER_TABLE)
                .map(|index| Field::new(format!("f{index}"), DataType::Int64, true))
                .collect::<Vec<_>>()
                .into(),
        );
        let batch =
            RecordBatch::new_empty(Arc::new(arrow::datatypes::Schema::new(vec![Field::new(
                "s", wide, true,
            )])));
        let error = schema_from_arrow(
            &batch,
            &TableName::new("events"),
            DestinationCapabilities::default(),
        )
        .expect_err("cap struct fields beside one column exceed the cap");
        let rendered = error.to_string();
        assert!(
            rendered.contains("source-column cap"),
            "the refusal names the cap: {rendered}"
        );
        assert!(
            rendered.len() < 1024,
            "the refusal must not render the offending schema back: {} chars",
            rendered.len()
        );

        // A modest struct still maps — the count is a cap, not a struct ban.
        let modest = DataType::Struct(
            (0..8)
                .map(|index| Field::new(format!("f{index}"), DataType::Int64, true))
                .collect::<Vec<_>>()
                .into(),
        );
        let batch =
            RecordBatch::new_empty(Arc::new(arrow::datatypes::Schema::new(vec![Field::new(
                "s", modest, true,
            )])));
        schema_from_arrow(
            &batch,
            &TableName::new("events"),
            DestinationCapabilities::default(),
        )
        .expect("ordinary struct breadth still maps");
    }

    /// The cross-batch join APPENDS unseen struct fields, so per-batch caps
    /// alone still let a stream accumulate unbounded breadth one batch at a
    /// time. The joined schema is re-counted before it reaches the registry.
    #[test]
    fn cross_batch_struct_growth_refuses_at_the_source_column_cap() {
        use arrow::datatypes::Field;
        let struct_of = |prefix: &str, count: usize| {
            DataType::Struct(
                (0..count)
                    .map(|index| Field::new(format!("{prefix}{index}"), DataType::Int64, true))
                    .collect::<Vec<_>>()
                    .into(),
            )
        };
        let batch_with = |dt: DataType| {
            RecordBatch::new_empty(Arc::new(arrow::datatypes::Schema::new(vec![Field::new(
                "s", dt, true,
            )])))
        };
        let mut registry = crate::schema::registry::SchemaRegistry::default();
        let load_id = rdlt_core::LoadId::new("load");
        let mode = rdlt_core::WriteMode::Append;
        let policy = rdlt_core::SchemaPolicy::default();
        let table = TableName::new("events");
        let half = MAX_SOURCE_COLUMNS_PER_TABLE / 2 + 1;
        passthrough_items(
            &batch_with(struct_of("a", half)),
            &table,
            ShredContext {
                registry: &mut registry,
                load_id: &load_id,
                mode: &mode,
                policy: &policy,
            },
            DestinationCapabilities::default(),
        )
        .expect("the first batch sits under the cap");
        let error = passthrough_items(
            &batch_with(struct_of("b", half)),
            &table,
            ShredContext {
                registry: &mut registry,
                load_id: &load_id,
                mode: &mode,
                policy: &policy,
            },
            DestinationCapabilities::default(),
        )
        .expect_err("disjoint fields join past the cap and must refuse");
        assert!(
            error.to_string().contains("source-column cap"),
            "the refusal names the cap: {error}"
        );
    }

    #[test]
    fn a_structured_schema_refuses_above_the_source_column_cap() {
        let fields = (0..=MAX_SOURCE_COLUMNS_PER_TABLE)
            .map(|index| arrow::datatypes::Field::new(format!("f{index}"), DataType::Null, true))
            .collect::<Vec<_>>();
        let batch = RecordBatch::new_empty(Arc::new(arrow::datatypes::Schema::new(fields)));
        let error = schema_from_arrow(
            &batch,
            &TableName::new("events"),
            DestinationCapabilities::default(),
        )
        .expect_err("one field beyond the cap must refuse");
        assert!(error.to_string().contains("source-column cap"));
    }

    /// Mutation-report closure: `Decimal128 { scale >= 0 }` guard — a negative
    /// scale must be a typed error, never a silent mapping.
    #[test]
    fn negative_decimal_scale_is_a_typed_error() {
        assert!(column_type_from_arrow(&DataType::Decimal128(10, 2)).is_ok());
        let err = column_type_from_arrow(&DataType::Decimal128(10, -2))
            .expect_err("negative scale must not map");
        assert!(err.contains("no logical mapping"), "got: {err}");
    }

    /// 2L6: the Arrow path must enforce the same decimal domain the JSON
    /// hint path and destination-facing logical schema require.
    #[test]
    fn invalid_decimal_precision_and_scale_are_typed_errors() {
        for invalid in [
            DataType::Decimal128(0, 0),
            DataType::Decimal128(39, 0),
            DataType::Decimal128(10, 11),
        ] {
            assert!(
                column_type_from_arrow(&invalid).is_err(),
                "{invalid} must not enter the registry"
            );
        }
        assert!(column_type_from_arrow(&DataType::Decimal128(38, 38)).is_ok());
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

    /// 047 M1: schema nesting is CONNECTOR-controlled on structured
    /// streams, and an uncapped mapping walk turns a deep declaration into
    /// a stack overflow — an abort no task containment absorbs. Past the
    /// shared cap the walk refuses with a typed error.
    #[test]
    fn a_schema_nested_past_the_depth_cap_refuses_instead_of_recursing() {
        use arrow::datatypes::Field;

        let deep = |levels: usize| -> DataType {
            let mut dt = DataType::Int64;
            for _ in 0..levels {
                dt = DataType::Struct(vec![Field::new("f", dt, true)].into());
            }
            dt
        };
        let err = column_type_from_arrow(&deep(100))
            .expect_err("a 100-deep declared schema must refuse, not walk");
        assert!(err.contains("nesting"), "the refusal names the cap: {err}");
        assert!(
            column_type_from_arrow(&deep(10)).is_ok(),
            "ordinary nesting still maps"
        );
    }

    /// The cross-batch join walks the SAME connector-controlled nesting
    /// and carries the same cap — belt to `column_type_from_arrow`'s gate,
    /// since both join inputs already passed it (or the shredder's own
    /// serde depth bound).
    #[test]
    fn a_join_past_the_depth_cap_refuses_instead_of_recursing() {
        // The two sides must DIFFER at the leaf, or the equality fast path
        // answers before any recursion happens.
        let deep = |levels: usize, leaf: LogicalType| -> ColumnType {
            let mut ty = ColumnType::scalar(leaf);
            for _ in 0..levels {
                ty = ColumnType::Struct {
                    fields: vec![ColumnDef {
                        name: "f".to_owned(),
                        column_type: ty,
                        nullable: true,
                        provenance: Provenance::Inferred,
                    }],
                };
            }
            ty
        };
        let err = join_column_types(
            &deep(100, LogicalType::Int64),
            &deep(100, LogicalType::Float64),
        )
        .expect_err("a 100-deep join must refuse, not walk");
        assert!(err.contains("nesting"), "the refusal names the cap: {err}");
        assert!(
            join_column_types(
                &deep(10, LogicalType::Int64),
                &deep(10, LogicalType::Float64)
            )
            .is_ok(),
            "ordinary nesting still joins"
        );
    }
}
