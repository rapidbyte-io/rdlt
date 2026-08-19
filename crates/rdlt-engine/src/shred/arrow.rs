//! The Arrow path — the shredder's sibling fast path for already-structured
//! batches. Nothing is re-shredded: the batch's arrow schema maps onto the
//! logical schema through `types`, the SAME registry/policy seam governs
//! evolution, and the only new data is one appended constant `_rdlt_load_id`
//! column. Columns whose arrow type equals the table's current logical type
//! pass through zero-copy (`Arc` clone); when cross-batch widening or an
//! arrow representation difference (Large* variants, timestamp unit/zone)
//! changed a column's type, its values are cast to the current type where
//! the cast is EXACT and refused typed where it is not — at every nesting
//! depth (`cast`). Values are never silently coerced. Structured streams
//! carry no per-row identity (no `_rdlt_id`), which is why Keyless Merge is
//! rejected for them; keyed structured merge is supported.

use std::sync::Arc;

use arrow::{
    array::{Array, ArrayRef, StringArray, StructArray, new_null_array},
    compute::cast,
    datatypes::{DataType, Field, Fields},
    record_batch::RecordBatch,
};
use rdlt_connector::channel::{MAX_ARROW_DEPTH, MAX_RECORD_BATCH_ROWS};
use rdlt_connector::destination::Capabilities;
use rdlt_core::error::Error;
use rdlt_core::id::TableName;
use rdlt_core::schema::{self, Column, ColumnType, Provenance, TableSchema};
use rdlt_core::types::LogicalType;

use super::limits::MAX_SOURCE_COLUMNS_PER_TABLE;
use super::resolve::{Input, ShredContext};
use super::{cast as exact, infer, limits, resolve, types};
use crate::load::LoadItem;
use crate::naming::UniqueNamer;

/// Process one structured batch: schema mapping, policy enforcement, load-id
/// stamping. Emits the standard Delta/Discarded/Batch items.
pub(crate) fn items(
    batch: &RecordBatch,
    table: &TableName,
    ctx: ShredContext,
    capabilities: Capabilities,
) -> Result<Vec<LoadItem>, Error> {
    if batch.num_rows() > MAX_RECORD_BATCH_ROWS {
        return Err(Error::config(format!(
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
        max_batch_cells,
    } = ctx;
    // ---- Map the arrow schema onto the logical schema ----
    let (mut observed, normalized_to_index) = schema_from_arrow(batch, table, capabilities)?;

    // ---- Join with the registry's current types (widening lattice) ----
    // The JSON path's observation states join implicitly; this path must do
    // it explicitly or a batch whose column NARROWED (Int64 after Utf8) would
    // push a narrowing delta into the registry (debug builds assert, release
    // builds would shrink the schema).
    if let Some(current) = registry.get(table) {
        // Map-backed lookups: the join, the merge and the assembly each walk
        // a per-column lookup — a linear find would be O(columns²) of string
        // compares per push on a wide table.
        let current_by_name: std::collections::HashMap<&str, &rdlt_core::schema::Column> = current
            .columns
            .iter()
            .map(|c| (c.name.as_str(), c))
            .collect();
        for column in &mut observed.columns {
            if let Some(existing) = current_by_name.get(column.name.as_str()) {
                column.column_type =
                    types::join_column_types(&existing.column_type, &column.column_type).map_err(
                        |reason| {
                            Error::config(format!(
                                "table `{table}` column `{}`: {reason}",
                                column.name
                            ))
                        },
                    )?;
            }
        }
        // Columns only append (the registry's promise): merge every column
        // the batch OMITTED back into the observation, mirroring the
        // struct-field join above. Without the merge, a batch carrying any
        // real change (an add or a widen) while omitting column X would make
        // `apply` REPLACE the stored schema with the observation — X silently
        // vanishing from the durable schema, null-filled by later batches,
        // then re-added as per-push AddColumn churn (delta → destination
        // ensure → WAL record). The assembly already null-fills absent
        // columns; this keeps the registry itself append-only. The JSON path
        // needs no such merge: its observation states accumulate across
        // pushes by construction.
        let missing: Vec<rdlt_core::schema::Column> = {
            let observed_names: std::collections::HashSet<&str> =
                observed.columns.iter().map(|c| c.name.as_str()).collect();
            current
                .columns
                .iter()
                .filter(|existing| !observed_names.contains(existing.name.as_str()))
                .cloned()
                .collect()
        };
        observed.columns.extend(missing);
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
        return Err(Error::config(format!(
            "table `{table}`: cross-batch schema growth reaches {source_fields} columns \
             and nested struct fields, over the {MAX_SOURCE_COLUMNS_PER_TABLE}-source-column \
             cap — struct breadth counts toward the same bound as columns"
        )));
    }

    // ---- Policy resolution (the shared loop, on the Arrow input) ----
    let changes = registry.diff(&observed);
    let mut dropped_columns: Vec<String> = Vec::new();
    let kept = resolve::resolve_policy(
        Input::Arrow,
        policy,
        registry,
        &observed,
        changes,
        |change, _| match change {
            schema::Change::AddColumn { column } => {
                // Discard on a structured stream: project the refused column away
                // (exact for column additions), counted below.
                dropped_columns.push(column.name.clone());
                Ok(())
            }
            schema::Change::WidenColumn { name, .. } => Err(Error::config(format!(
                "table `{table}` column `{name}`: Discard policies cannot filter \
                 value-level type changes on structured streams; use \
                 Evolve or Freeze"
            ))),
            schema::Change::CreateTable { .. } => unreachable!("handled as Evolve"),
        },
    )?;
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

    // The cell budget fires BEFORE the registry apply: a refused push must
    // not leave its schema mutation behind — the registry would desync from
    // the destination's DDL the moment an error path ever learned to
    // continue past it. Assembly pays columns × rows — null-filled for every
    // column this batch omits — so the product is refused before any array
    // is built, not metered after. `observed`'s width IS the post-apply
    // registry width.
    limits::refuse_over_cell_budget(
        table,
        observed.columns.len(),
        batch.num_rows(),
        max_batch_cells,
    )?;

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
    let index_by_name: std::collections::HashMap<&str, usize> = normalized_to_index
        .iter()
        .map(|(normalized, idx)| (normalized.as_str(), *idx))
        .collect();
    let mut arrays: Vec<ArrayRef> = Vec::with_capacity(current.columns.len());
    for column in &current.columns {
        if column.name == schema::system::LOAD_ID {
            arrays.push(Arc::new(StringArray::from(vec![load_id.as_str(); rows])));
            continue;
        }
        let target_type = types::arrow_column_type(&column.column_type);
        let array = match index_by_name.get(column.name.as_str()) {
            Some(idx) => {
                let source = batch.column(*idx);
                if source.data_type() == &target_type {
                    Arc::clone(source) // the common zero-copy path
                } else {
                    cast_exact(source, &target_type, table, &column.name)?
                }
            }
            // Historical column absent from this batch: null-filled.
            None => new_null_array(&target_type, rows),
        };
        arrays.push(array);
    }
    let out = RecordBatch::try_new(Arc::new(types::arrow_schema(current)), arrays)
        .map_err(|e| Error::internal(format!("arrow batch assembly: {e}")))?;
    items.push(LoadItem::batch(table.clone(), out));
    Ok(items)
}

/// THE ONE cast seat: a source column under a different current type
/// (cross-batch widening or a representation difference — an Int64 batch
/// under a Float64 column, ns under the µs canonical unit). The pre-cast
/// exactness walk refuses any cast that would silently ALTER a value — at
/// every nesting depth, not just the top level: the losslessness contract
/// belongs to the column, and arrow's cast recurses through struct fields
/// and list elements exactly like the walk does.
///
/// The walk is followed by a BELT: arrow's default (safe) cast answers a
/// value it cannot carry with a NULL, so a null count that GREW across
/// the cast is a loss the walk did not model — refused typed rather than
/// shipped. The walk makes a refusal precise; the belt makes it complete.
fn cast_exact(
    source: &ArrayRef,
    target_type: &DataType,
    table: &TableName,
    column: &str,
) -> Result<ArrayRef, Error> {
    // A struct field the batch omits null-fills — the same rule as an
    // omitted top-level column — before the walk and the cast see the
    // array: arrow's struct cast pairs children positionally and fails
    // on a missing one.
    let source = null_fill_omitted_fields(source, target_type);
    exact::refuse_inexact_cast(source.as_ref(), target_type, column)
        .map_err(|reason| Error::config(format!("table `{table}`: {reason}")))?;
    let cast = cast(source.as_ref(), target_type).map_err(|e| {
        Error::config(format!(
            "table `{table}` column `{column}`: cannot cast {} to {target_type}: {e}",
            source.data_type()
        ))
    })?;
    if let Some(grown) = grown_nulls(source.as_ref(), cast.as_ref(), column) {
        return Err(Error::config(format!(
            "table `{table}` column `{}`: casting {} to {} would null {} value(s) arrow \
             cannot carry — refused rather than lost; declare the column as text, or \
             deliver values the type can represent",
            grown.path, grown.source_type, grown.target_type, grown.count
        )));
    }
    Ok(cast)
}

/// Where a cast grew the null count: the nested position and the LEAF
/// source/target types there.
struct GrownNulls {
    path: String,
    source_type: DataType,
    target_type: DataType,
    count: usize,
}

/// Where the null count grew across a cast: the first position — the
/// array itself, or a struct field / list element under it (arrow's cast
/// recurses, so a nested safe-mode null never surfaces in the parent's
/// own count). Counts are LOGICAL nulls, so an encoding that reads a
/// value as null without a validity bit compares like one that has it.
fn grown_nulls(source: &dyn Array, cast: &dyn Array, path: &str) -> Option<GrownNulls> {
    let (before, after) = (source.logical_null_count(), cast.logical_null_count());
    if after > before {
        return Some(GrownNulls {
            path: path.to_owned(),
            source_type: source.data_type().clone(),
            target_type: cast.data_type().clone(),
            count: after - before,
        });
    }
    let (source_struct, cast_struct) = (
        source.as_any().downcast_ref::<StructArray>(),
        cast.as_any().downcast_ref::<StructArray>(),
    );
    if let (Some(source_struct), Some(cast_struct)) = (source_struct, cast_struct) {
        return cast_struct.fields().iter().find_map(|field| {
            let (Some(before), Some(after)) = (
                source_struct.column_by_name(field.name()),
                cast_struct.column_by_name(field.name()),
            ) else {
                return None;
            };
            grown_nulls(
                before.as_ref(),
                after.as_ref(),
                &format!("{path}.{}", field.name()),
            )
        });
    }
    match (exact::list_values(source), exact::list_values(cast)) {
        (Some(before), Some(after)) => {
            grown_nulls(before.as_ref(), after.as_ref(), &format!("{path}[]"))
        }
        _ => None,
    }
}

/// A struct source re-assembled against the target's field set: children
/// projected BY NAME in the target's order, an absent one null-filled at
/// the struct's length, recursively for nested structs; the struct's own
/// validity is kept and each child keeps its type (the cast that follows
/// converts it). Anything but a struct under a struct returns as is — the
/// walk and the cast own every other shape.
fn null_fill_omitted_fields(source: &ArrayRef, target_type: &DataType) -> ArrayRef {
    let (Some(source_struct), DataType::Struct(target_fields)) =
        (source.as_any().downcast_ref::<StructArray>(), target_type)
    else {
        return Arc::clone(source);
    };
    let children: Vec<ArrayRef> = target_fields
        .iter()
        .map(|field| match source_struct.column_by_name(field.name()) {
            Some(child) => null_fill_omitted_fields(child, field.data_type()),
            None => new_null_array(field.data_type(), source_struct.len()),
        })
        .collect();
    let fields: Fields = target_fields
        .iter()
        .zip(&children)
        .map(|(field, child)| Arc::new(Field::new(field.name(), child.data_type().clone(), true)))
        .collect();
    Arc::new(StructArray::new(
        fields,
        children,
        source_struct.nulls().cloned(),
    ))
}

/// Logical schema for a structured batch: `_rdlt_load_id` + the batch's fields
/// (normalized names, mapped types). Also returns normalized-name → column-index.
fn schema_from_arrow(
    batch: &RecordBatch,
    table: &TableName,
    capabilities: Capabilities,
) -> Result<(TableSchema, Vec<(String, usize)>), Error> {
    if batch.num_columns() > MAX_SOURCE_COLUMNS_PER_TABLE {
        return Err(Error::config(format!(
            "table `{table}` carries {} Arrow fields, over the \
             {MAX_SOURCE_COLUMNS_PER_TABLE}-source-column cap",
            batch.num_columns()
        )));
    }
    let mut namer = UniqueNamer::new(capabilities.ident_rules);
    namer.reserve(schema::system::LOAD_ID); // even a literal `_rdlt_load_id` input suffixes

    let mut columns = vec![Column {
        name: schema::system::LOAD_ID.to_owned(),
        column_type: ColumnType::scalar(LogicalType::Utf8),
        nullable: false,
        provenance: Provenance::System,
    }];
    let mut normalized_to_index = Vec::new();
    // Struct-field breadth spends from the SAME budget as top-level columns.
    // Counted BEFORE each field is mapped — a declared million-field struct
    // must refuse without first materializing a million `Column`s, and
    // without rendering the offending schema back into the error.
    let mut source_fields = batch.num_columns();
    for (idx, field) in batch.schema().fields().iter().enumerate() {
        source_fields = source_fields.saturating_add(declared_struct_fields(field.data_type(), 0));
        if source_fields > MAX_SOURCE_COLUMNS_PER_TABLE {
            return Err(Error::config(format!(
                "table `{table}` column `{}`: declared struct fields push the schema over \
                 the {MAX_SOURCE_COLUMNS_PER_TABLE}-source-column cap — struct breadth \
                 counts toward the same bound as columns",
                field.name()
            )));
        }
        let ty = types::column_type_from_arrow(field.data_type()).map_err(|reason| {
            Error::config(format!(
                "table `{table}` column `{}`: unmappable arrow type {} ({reason}) — \
                 never coerced silently",
                field.name(),
                field.data_type()
            ))
        })?;
        let name = namer.name_for(field.name());
        normalized_to_index.push((name.clone(), idx));
        columns.push(Column {
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
/// so a hostile breadth refuses before any `Column` is built. Descent
/// stops at the shared depth cap (the mapping walk right behind this refuses
/// there with its own typed error); saturating, never panicking.
fn declared_struct_fields(dt: &DataType, depth: usize) -> usize {
    if depth > MAX_ARROW_DEPTH {
        return 0;
    }
    match dt {
        DataType::Struct(fields) => infer::nested_field_count(fields, |field| {
            declared_struct_fields(field.data_type(), depth + 1)
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
        ColumnType::Struct { fields } => {
            infer::nested_field_count(fields, |field| nested_struct_fields(&field.column_type))
        }
        _ => 0,
    }
}

#[cfg(test)]
mod tests {
    use arrow::datatypes::TimeUnit;

    use super::*;

    /// One structured batch through the path under a throwaway run context,
    /// against the caller's registry.
    fn pass(
        registry: &mut crate::schema::registry::SchemaRegistry,
        table: &TableName,
        batch: &RecordBatch,
    ) -> Result<Vec<LoadItem>, Error> {
        let (load_id, mode, policy) = (
            rdlt_core::id::LoadId::new("load"),
            rdlt_core::commit::WriteMode::Append,
            crate::policy::SchemaPolicy::default(),
        );
        items(
            batch,
            table,
            ShredContext {
                registry,
                load_id: &load_id,
                mode: &mode,
                policy: &policy,
                max_batch_cells: crate::config::Config::DEFAULT_MAX_BATCH_CELLS,
            },
            Capabilities::default(),
        )
    }

    /// The engine-side expansion is bounded by the PRODUCT. An
    /// empty wide batch bootstraps a maximal registry schema for ~50 KB of
    /// wire; one 1M-row single-column push then assembles 4,096 null-filled
    /// columns — ~16 GiB — unless the cell budget refuses first.
    #[test]
    fn a_wide_registry_times_a_full_row_batch_refuses_at_the_cell_budget() {
        use arrow::datatypes::Field;
        let wide = arrow::datatypes::Schema::new(
            (0..MAX_SOURCE_COLUMNS_PER_TABLE)
                .map(|index| Field::new(format!("f{index}"), DataType::Boolean, true))
                .collect::<Vec<_>>(),
        );
        // Schema bootstrap is nearly free: zero rows, the full column set.
        let bootstrap = RecordBatch::new_empty(Arc::new(wide));
        let mut registry = crate::schema::registry::SchemaRegistry::default();
        let table = TableName::new("events");
        pass(&mut registry, &table, &bootstrap).expect("an empty wide bootstrap registers");
        // One 125 KB boolean column at the full row cap: the assembly would
        // null-fill the other 4,095 columns across a million rows.
        let push = RecordBatch::try_new(
            Arc::new(arrow::datatypes::Schema::new(vec![Field::new(
                "f0",
                DataType::Boolean,
                true,
            )])),
            vec![Arc::new(arrow::array::BooleanArray::from(vec![
                true;
                MAX_RECORD_BATCH_ROWS
            ]))],
        )
        .expect("batch");
        let error =
            pass(&mut registry, &table, &push).expect_err("the columns × rows product must refuse");
        assert!(
            error.to_string().contains("cell"),
            "the refusal names the cell budget: {error}"
        );
    }

    /// The v1 structured-stream contract at the mixed-shape join: a
    /// column that evolves between a struct-or-list shape and a scalar
    /// shape joins to Json, and the structured path cannot render the
    /// nested side to text — the refusal STATES that contract (re-shape
    /// the stream, or push the column as JSON) instead of surfacing
    /// arrow's cast vocabulary. Both mixed orders land on the same
    /// spelling.
    #[test]
    fn a_mixed_shape_evolution_refuses_stating_the_structured_stream_contract() {
        use arrow::array::{Int64Array, StructArray};
        use arrow::datatypes::{Field, Fields};

        let scalar_batch = RecordBatch::try_new(
            Arc::new(arrow::datatypes::Schema::new(vec![Field::new(
                "v",
                DataType::Int64,
                true,
            )])),
            vec![Arc::new(Int64Array::from(vec![1, 2]))],
        )
        .expect("batch");
        let struct_fields: Fields = vec![Field::new("x", DataType::Int64, true)].into();
        let struct_batch = RecordBatch::try_new(
            Arc::new(arrow::datatypes::Schema::new(vec![Field::new(
                "v",
                DataType::Struct(struct_fields.clone()),
                true,
            )])),
            vec![Arc::new(StructArray::new(
                struct_fields,
                vec![Arc::new(Int64Array::from(vec![3, 4]))],
                None,
            ))],
        )
        .expect("batch");

        let mut registry = crate::schema::registry::SchemaRegistry::default();
        let table = TableName::new("events");
        pass(&mut registry, &table, &scalar_batch).expect("the scalar shape registers");
        let error = pass(&mut registry, &table, &struct_batch)
            .expect_err("the struct shape under the scalar-joined column must refuse");
        let rendered = error.to_string();
        assert!(
            rendered.contains("between a struct-or-list shape and a scalar shape")
                && rendered.contains("re-shape the stream so the column keeps one shape")
                && rendered.contains("push the column as JSON text")
                && rendered.contains("column `v`"),
            "the refusal states the structured-stream contract: {rendered}"
        );
    }

    /// A structured batch that OMITS a registry column while carrying
    /// another change must not shrink the durable schema. Columns only
    /// append — the registry's own promise.
    #[test]
    fn an_omitting_batch_keeps_its_missing_columns_in_the_registry() {
        use arrow::array::{Float64Array, Int64Array};
        use arrow::datatypes::Field;

        let batch_of = |fields: Vec<Field>, columns: Vec<ArrayRef>| {
            RecordBatch::try_new(Arc::new(arrow::datatypes::Schema::new(fields)), columns)
                .expect("batch")
        };
        let mut registry = crate::schema::registry::SchemaRegistry::default();
        let table = TableName::new("events");

        // Batch 1 establishes a, b.
        pass(
            &mut registry,
            &table,
            &batch_of(
                vec![
                    Field::new("a", DataType::Int64, true),
                    Field::new("b", DataType::Int64, true),
                ],
                vec![
                    Arc::new(Int64Array::from(vec![1, 2])),
                    Arc::new(Int64Array::from(vec![3, 4])),
                ],
            ),
        )
        .expect("pass");

        // Batch 2 omits `a` and widens `b` — a real change, which a
        // replace-on-diff would have taken as the whole schema.
        let items = pass(
            &mut registry,
            &table,
            &batch_of(
                vec![Field::new("b", DataType::Float64, true)],
                vec![Arc::new(Float64Array::from(vec![1.5, 2.5, 3.5]))],
            ),
        )
        .expect("pass");
        let schema = registry.get(&table).expect("registered");
        let names: Vec<&str> = schema.columns.iter().map(|c| c.name.as_str()).collect();
        assert!(
            names.contains(&"a") && names.contains(&"b"),
            "an omitted column stays in the durable schema: {names:?}"
        );
        assert_eq!(
            schema.column("b").expect("b").column_type,
            ColumnType::scalar(LogicalType::Float64),
            "the widen still landed"
        );
        // …and the emitted delta is exactly the widen — no phantom churn.
        let delta_changes: Vec<String> = items
            .iter()
            .filter_map(|item| match item {
                LoadItem::Delta { delta, .. } => Some(
                    delta
                        .changes
                        .iter()
                        .map(|c| format!("{c:?}"))
                        .collect::<Vec<_>>(),
                ),
                _ => None,
            })
            .flatten()
            .collect();
        assert_eq!(
            delta_changes.len(),
            1,
            "one change: the widen, nothing else: {delta_changes:?}"
        );
        assert!(
            delta_changes[0].contains("WidenColumn"),
            "{delta_changes:?}"
        );
        // The outgoing batch null-fills the omitted column at batch 2's rows.
        let out = items.iter().find_map(|item| match item {
            LoadItem::Batch { batch, .. } => Some(batch),
            _ => None,
        });
        let out = out.expect("a batch is emitted");
        let a_index = out.schema().index_of("a").expect("column a in the output");
        let a_col = out.column(a_index);
        assert_eq!(a_col.null_count(), 3, "omitted ⇒ null-filled at these rows");

        // Batch 3 carries `a` again: no re-add delta — the column never left.
        let items = pass(
            &mut registry,
            &table,
            &batch_of(
                vec![Field::new("a", DataType::Int64, true)],
                vec![Arc::new(Int64Array::from(vec![9]))],
            ),
        )
        .expect("pass");
        assert!(
            !items
                .iter()
                .any(|item| matches!(item, LoadItem::Delta { .. })),
            "no AddColumn churn for a column that never left the registry"
        );
    }

    /// A bit-packed boolean frame can describe enormous row counts in a
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
        let error = pass(&mut registry, &TableName::new("events"), &batch)
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
        let error = schema_from_arrow(&batch, &TableName::new("events"), Capabilities::default())
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
        schema_from_arrow(&batch, &TableName::new("events"), Capabilities::default())
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
        let table = TableName::new("events");
        let half = MAX_SOURCE_COLUMNS_PER_TABLE / 2 + 1;
        pass(&mut registry, &table, &batch_with(struct_of("a", half)))
            .expect("the first batch sits under the cap");
        let error = pass(&mut registry, &table, &batch_with(struct_of("b", half)))
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
        let error = schema_from_arrow(&batch, &TableName::new("events"), Capabilities::default())
            .expect_err("one field beyond the cap must refuse");
        assert!(error.to_string().contains("source-column cap"));
    }

    /// The one widening the LATTICE licenses but a VALUE can refuse — a
    /// Float64 column (batch 1) widened by an Int64 batch (batch 2) whose
    /// integer sits beyond ±2^53. The cast arrow would perform rounds it;
    /// the JSON path escalates the same value to text; the structured
    /// path's registry type is already committed, so the honest answer is
    /// the typed refusal. In-range integers still widen exactly.
    #[test]
    fn an_inexact_int64_to_float64_widening_refuses_typed() {
        use arrow::array::{Float64Array, Int64Array};
        use arrow::datatypes::Field;

        let (mut registry, table) = (
            crate::schema::registry::SchemaRegistry::default(),
            TableName::new("events"),
        );

        let float_batch = RecordBatch::try_new(
            Arc::new(arrow::datatypes::Schema::new(vec![Field::new(
                "v",
                DataType::Float64,
                true,
            )])),
            vec![Arc::new(Float64Array::from(vec![0.5]))],
        )
        .expect("float batch");
        pass(&mut registry, &table, &float_batch).expect("the Float64 column registers");

        // 2^53 + 1 in an Int64 batch: the lattice joins to Float64, the
        // value cannot follow.
        let beyond = RecordBatch::try_new(
            Arc::new(arrow::datatypes::Schema::new(vec![Field::new(
                "v",
                DataType::Int64,
                true,
            )])),
            vec![Arc::new(Int64Array::from(vec![9_007_199_254_740_993]))],
        )
        .expect("int batch");
        let error =
            pass(&mut registry, &table, &beyond).expect_err("an inexact widening must refuse");
        let rendered = error.to_string();
        assert!(
            rendered.contains("silently round"),
            "the refusal names the loss: {rendered}"
        );

        // 2^53 itself (and everything in range) widens exactly.
        let exact = RecordBatch::try_new(
            Arc::new(arrow::datatypes::Schema::new(vec![Field::new(
                "v",
                DataType::Int64,
                true,
            )])),
            vec![Arc::new(Int64Array::from(vec![9_007_199_254_740_992]))],
        )
        .expect("int batch");
        pass(&mut registry, &table, &exact).expect("an exact widening passes");
    }

    /// The walk recurses — a struct FIELD widening Int64→Float64 refuses
    /// exactly as the top-level column does (arrow's cast rounds one
    /// nesting down just the same).
    #[test]
    fn a_nested_struct_int_widening_refuses_typed() {
        use arrow::array::{Float64Array, Int64Array, StructArray};
        use arrow::datatypes::{DataType, Field, Fields};

        let (mut registry, table) = (
            crate::schema::registry::SchemaRegistry::default(),
            TableName::new("events"),
        );
        let float_struct = || {
            StructArray::new(
                Fields::from(vec![Field::new("f", DataType::Float64, true)]),
                vec![Arc::new(Float64Array::from(vec![0.5])) as ArrayRef],
                None,
            )
        };
        let int_struct = || {
            StructArray::new(
                Fields::from(vec![Field::new("f", DataType::Int64, true)]),
                vec![Arc::new(Int64Array::from(vec![9_007_199_254_740_993i64])) as ArrayRef],
                None,
            )
        };
        let schema_of = |field_type: DataType| {
            Arc::new(arrow::datatypes::Schema::new(vec![Field::new(
                "s",
                DataType::Struct(Fields::from(vec![Field::new("f", field_type, true)])),
                true,
            )]))
        };
        let first = RecordBatch::try_new(
            schema_of(DataType::Float64),
            vec![Arc::new(float_struct()) as ArrayRef],
        )
        .expect("float-struct batch");
        pass(&mut registry, &table, &first).expect("the struct column registers");

        let second = RecordBatch::try_new(
            schema_of(DataType::Int64),
            vec![Arc::new(int_struct()) as ArrayRef],
        )
        .expect("int-struct batch");
        let error = pass(&mut registry, &table, &second)
            .expect_err("a nested inexact widening must refuse");
        let rendered = error.to_string();
        assert!(
            rendered.contains("silently round") && rendered.contains("`s.f`"),
            "the refusal names the nested path: {rendered}"
        );
    }

    /// The list shape: a list ELEMENT widening Int64→Float64 refuses.
    #[test]
    fn a_nested_list_int_widening_refuses_typed() {
        use arrow::array::{Float64Array, Int64Array, ListArray};
        use arrow::buffer::OffsetBuffer;
        use arrow::datatypes::Field;

        let (mut registry, table) = (
            crate::schema::registry::SchemaRegistry::default(),
            TableName::new("events"),
        );

        let item = |dt: DataType| Arc::new(Field::new("item", dt, true));
        let list_of = |dt: DataType, values: ArrayRef| {
            ListArray::new(
                item(dt.clone()),
                OffsetBuffer::new(vec![0i32, 1].into()),
                values,
                None,
            )
        };
        let schema = |dt: DataType| {
            Arc::new(arrow::datatypes::Schema::new(vec![Field::new(
                "l",
                DataType::List(item(dt)),
                true,
            )]))
        };

        let first = RecordBatch::try_new(
            schema(DataType::Float64),
            vec![Arc::new(list_of(
                DataType::Float64,
                Arc::new(Float64Array::from(vec![0.5])),
            )) as ArrayRef],
        )
        .expect("float-list batch");
        pass(&mut registry, &table, &first).expect("the list column registers");

        let second = RecordBatch::try_new(
            schema(DataType::Int64),
            vec![Arc::new(list_of(
                DataType::Int64,
                Arc::new(Int64Array::from(vec![9_007_199_254_740_993i64])),
            )) as ArrayRef],
        )
        .expect("int-list batch");
        let error = pass(&mut registry, &table, &second)
            .expect_err("a list-element inexact widening must refuse");
        assert!(
            error.to_string().contains("silently round"),
            "the refusal names the loss: {error}"
        );
    }

    /// Nanosecond timestamps under the µs canonical unit — a value
    /// not divisible by 1,000 refuses (arrow would truncate toward
    /// zero); a divisible one casts cleanly.
    #[test]
    fn a_sub_microsecond_timestamp_refuses_typed() {
        use arrow::array::TimestampMicrosecondArray;

        let (mut registry, table) = (
            crate::schema::registry::SchemaRegistry::default(),
            TableName::new("events"),
        );

        // Establish the µs canonical type with a µs batch.
        let us_schema = Arc::new(arrow::datatypes::Schema::new(vec![
            arrow::datatypes::Field::new(
                "t",
                DataType::Timestamp(TimeUnit::Microsecond, None),
                true,
            ),
        ]));
        let first = RecordBatch::try_new(
            Arc::clone(&us_schema),
            vec![Arc::new(TimestampMicrosecondArray::from(vec![1i64])) as ArrayRef],
        )
        .expect("µs batch");
        pass(&mut registry, &table, &first).expect("the µs column registers");

        let ns_schema = Arc::new(arrow::datatypes::Schema::new(vec![
            arrow::datatypes::Field::new(
                "t",
                DataType::Timestamp(TimeUnit::Nanosecond, None),
                true,
            ),
        ]));
        let mk_ns = |values: Vec<i64>| {
            Arc::new(
                arrow::array::TimestampNanosecondArray::from(values)
                    .with_timezone_opt::<&str>(None),
            ) as ArrayRef
        };
        // 1,500 ns is NOT divisible by 1,000: refuse.
        let second = RecordBatch::try_new(Arc::clone(&ns_schema), vec![mk_ns(vec![1_500i64])])
            .expect("ns batch");
        let error = pass(&mut registry, &table, &second)
            .expect_err("a sub-microsecond nanosecond value must refuse");
        assert!(
            error.to_string().contains("truncate"),
            "the refusal names the truncation: {error}"
        );

        // 2,000 ns divides cleanly: passes.
        let exact = RecordBatch::try_new(Arc::clone(&ns_schema), vec![mk_ns(vec![2_000i64])])
            .expect("ns batch");
        pass(&mut registry, &table, &exact)
            .expect("a microsecond-divisible nanosecond value casts exactly");
    }

    /// A pre-epoch intra-day Date64 mis-dates under Date32 —
    /// refuse; a positive intra-day value (truncation keeps its date)
    /// and a whole-day value pass.
    #[test]
    fn a_pre_epoch_intra_day_date64_refuses_typed() {
        use arrow::array::{Date32Array, Date64Array};

        let (mut registry, table) = (
            crate::schema::registry::SchemaRegistry::default(),
            TableName::new("events"),
        );
        let day32_schema = Arc::new(arrow::datatypes::Schema::new(vec![
            arrow::datatypes::Field::new("d", DataType::Date32, true),
        ]));
        let first = RecordBatch::try_new(
            Arc::clone(&day32_schema),
            vec![Arc::new(Date32Array::from(vec![0i32])) as ArrayRef],
        )
        .expect("date32 batch");
        pass(&mut registry, &table, &first).expect("the Date column registers");

        let day64_schema = Arc::new(arrow::datatypes::Schema::new(vec![
            arrow::datatypes::Field::new("d", DataType::Date64, true),
        ]));
        // 1969-12-31T12:00Z: pre-epoch intra-day — arrow would say
        // day 0 (1970-01-01), one day wrong.
        let hostile = RecordBatch::try_new(
            Arc::clone(&day64_schema),
            vec![Arc::new(Date64Array::from(vec![-43_200_000i64])) as ArrayRef],
        )
        .expect("date64 batch");
        let error = pass(&mut registry, &table, &hostile)
            .expect_err("a pre-epoch intra-day Date64 must refuse");
        assert!(
            error.to_string().contains("mis-date"),
            "the refusal names the mis-dating: {error}"
        );

        // Post-epoch intra-day (truncation keeps the date) and a
        // whole-day value both pass.
        for ms in [43_200_000i64, -86_400_000i64] {
            let benign = RecordBatch::try_new(
                Arc::clone(&day64_schema),
                vec![Arc::new(Date64Array::from(vec![ms])) as ArrayRef],
            )
            .expect("date64 batch");
            pass(&mut registry, &table, &benign)
                .expect("a whole-day or post-epoch intra-day Date64 casts");
        }
    }

    /// A Date64 whose day count lies outside i32 must refuse under
    /// Date32 — arrow's cast truncates the day count to i32, a silently
    /// WRONG date rather than a null or an error. The in-range
    /// neighbour casts.
    #[test]
    fn an_out_of_range_date64_refuses_typed() {
        use arrow::array::{Date32Array, Date64Array};

        let (mut registry, table) = (
            crate::schema::registry::SchemaRegistry::default(),
            TableName::new("events"),
        );
        let day32_schema = Arc::new(arrow::datatypes::Schema::new(vec![
            arrow::datatypes::Field::new("d", DataType::Date32, true),
        ]));
        let first = RecordBatch::try_new(
            Arc::clone(&day32_schema),
            vec![Arc::new(Date32Array::from(vec![0i32])) as ArrayRef],
        )
        .expect("date32 batch");
        pass(&mut registry, &table, &first).expect("the Date column registers");

        let day64_schema = Arc::new(arrow::datatypes::Schema::new(vec![
            arrow::datatypes::Field::new("d", DataType::Date64, true),
        ]));
        const MS_PER_DAY: i64 = 86_400_000;
        // Day 2^31 — one past i32::MAX; arrow's `as i32` makes it day
        // i32::MIN, a date ~11,760 years wrong.
        let wrapped = RecordBatch::try_new(
            Arc::clone(&day64_schema),
            vec![Arc::new(Date64Array::from(vec![
                (i64::from(i32::MAX) + 1) * MS_PER_DAY,
            ])) as ArrayRef],
        )
        .expect("date64 batch");
        let error = pass(&mut registry, &table, &wrapped)
            .expect_err("a Date64 beyond the 32-bit day range must refuse");
        assert!(
            error.to_string().contains("32-bit day range"),
            "the refusal names the range: {error}"
        );

        // Day i32::MAX itself and its negative twin cast exactly.
        for day in [i64::from(i32::MAX), i64::from(i32::MIN)] {
            let edge = RecordBatch::try_new(
                Arc::clone(&day64_schema),
                vec![Arc::new(Date64Array::from(vec![day * MS_PER_DAY])) as ArrayRef],
            )
            .expect("date64 batch");
            pass(&mut registry, &table, &edge).expect("an in-range whole-day Date64 casts");
        }
    }

    /// A coarse-unit timestamp whose value overflows i64 when scaled to
    /// the µs canonical unit must refuse — arrow's safe-mode upscale
    /// turns the overflowing product into NULL, silently and uncounted.
    /// The largest representable second value casts.
    #[test]
    fn a_timestamp_unit_upscale_that_overflows_refuses_typed() {
        use arrow::array::{TimestampMicrosecondArray, TimestampSecondArray};

        let (mut registry, table) = (
            crate::schema::registry::SchemaRegistry::default(),
            TableName::new("events"),
        );
        let us_schema = Arc::new(arrow::datatypes::Schema::new(vec![
            arrow::datatypes::Field::new(
                "t",
                DataType::Timestamp(TimeUnit::Microsecond, None),
                true,
            ),
        ]));
        let first = RecordBatch::try_new(
            Arc::clone(&us_schema),
            vec![Arc::new(TimestampMicrosecondArray::from(vec![1i64])) as ArrayRef],
        )
        .expect("µs batch");
        pass(&mut registry, &table, &first).expect("the µs column registers");

        let s_schema = Arc::new(arrow::datatypes::Schema::new(vec![
            arrow::datatypes::Field::new("t", DataType::Timestamp(TimeUnit::Second, None), true),
        ]));
        // 9.3e12 seconds × 1e6 > i64::MAX (~9.22e18).
        let overflowing = RecordBatch::try_new(
            Arc::clone(&s_schema),
            vec![Arc::new(TimestampSecondArray::from(vec![9_300_000_000_000i64])) as ArrayRef],
        )
        .expect("second batch");
        let error = pass(&mut registry, &table, &overflowing)
            .expect_err("a second value that overflows the µs unit must refuse");
        assert!(
            error.to_string().contains("overflow"),
            "the refusal names the overflow: {error}"
        );

        // i64::MAX / 1e6 seconds is the last value that fits: casts.
        let edge = RecordBatch::try_new(
            Arc::clone(&s_schema),
            vec![Arc::new(TimestampSecondArray::from(vec![
                i64::MAX / 1_000_000,
                i64::MIN / 1_000_000,
            ])) as ArrayRef],
        )
        .expect("second batch");
        pass(&mut registry, &table, &edge).expect("an in-range second value upscales exactly");
    }

    /// A decimal value whose magnitude exceeds the joined target's
    /// precision must refuse — arrow's cast neither validates a decoded
    /// value against its declared precision nor refuses the rescaled
    /// product (it nulls or carries an out-of-precision value). The
    /// fitting neighbour casts.
    #[test]
    fn a_decimal_beyond_the_target_precision_refuses_typed() {
        use arrow::array::Decimal128Array;

        let (mut registry, table) = (
            crate::schema::registry::SchemaRegistry::default(),
            TableName::new("events"),
        );
        let scale3 = Arc::new(arrow::datatypes::Schema::new(vec![
            arrow::datatypes::Field::new("amount", DataType::Decimal128(5, 3), true),
        ]));
        let first = RecordBatch::try_new(
            Arc::clone(&scale3),
            vec![Arc::new(
                Decimal128Array::from(vec![12_345i128])
                    .with_precision_and_scale(5, 3)
                    .expect("decimal"),
            ) as ArrayRef],
        )
        .expect("decimal batch");
        pass(&mut registry, &table, &first).expect("the Decimal(5,3) column registers");

        // A Decimal128(5,2) batch: the join is Decimal(6,3), the cast
        // rescales ×10. 12345678 (= 123456.78) carries 8 digits under a
        // declared 5 — arrow neither refuses nor nulls it here.
        let scale2 = Arc::new(arrow::datatypes::Schema::new(vec![
            arrow::datatypes::Field::new("amount", DataType::Decimal128(5, 2), true),
        ]));
        let over = RecordBatch::try_new(
            Arc::clone(&scale2),
            vec![Arc::new(
                Decimal128Array::from(vec![12_345_678i128])
                    .with_precision_and_scale(5, 2)
                    .expect("decimal"),
            ) as ArrayRef],
        )
        .expect("decimal batch");
        let error = pass(&mut registry, &table, &over)
            .expect_err("a decimal beyond the target precision must refuse");
        assert!(
            error.to_string().contains("precision"),
            "the refusal names the precision: {error}"
        );

        // 99999 (= 999.99) rescales to 999990 — exactly the six digits
        // Decimal(6,3) admits: casts.
        let fits = RecordBatch::try_new(
            Arc::clone(&scale2),
            vec![Arc::new(
                Decimal128Array::from(vec![99_999i128, -99_999i128])
                    .with_precision_and_scale(5, 2)
                    .expect("decimal"),
            ) as ArrayRef],
        )
        .expect("decimal batch");
        pass(&mut registry, &table, &fits).expect("a decimal within the target precision casts");
    }

    /// The BELT behind the walk: a cast that arrow's safe mode answers
    /// with new NULLs is refused at the seat even when no walk leaf
    /// models the shape. Utf8→Int64 is unreachable through the lattice
    /// (text absorbs integers), so the walk has no leaf for it — arrow
    /// nulls the unparsable string, and the belt refuses.
    #[test]
    fn a_cast_that_grows_the_null_count_refuses_at_the_seat() {
        let source: ArrayRef = Arc::new(StringArray::from(vec![Some("12"), Some("x"), None]));
        let error = cast_exact(&source, &DataType::Int64, &TableName::new("events"), "v")
            .expect_err("a cast that nulls a value must refuse");
        assert!(
            error.to_string().contains("would null 1 value"),
            "the refusal counts the loss: {error}"
        );
        // A clean cast passes the belt untouched.
        let clean: ArrayRef = Arc::new(StringArray::from(vec![Some("12"), None]));
        let cast = cast_exact(&clean, &DataType::Int64, &TableName::new("events"), "v")
            .expect("a null-preserving cast passes");
        assert_eq!(cast.null_count(), 1);
    }

    /// A batch omitting a NESTED struct field null-fills it — the same
    /// rule as an omitted top-level column — at every nesting depth,
    /// instead of failing the push on arrow's positional struct cast.
    #[test]
    fn an_omitting_batch_null_fills_nested_struct_fields() {
        use arrow::array::{Int64Array, StructArray};
        use arrow::datatypes::{Field, Fields};

        let (mut registry, table) = (
            crate::schema::registry::SchemaRegistry::default(),
            TableName::new("events"),
        );
        let inner_full: Fields = vec![
            Field::new("x", DataType::Int64, true),
            Field::new("y", DataType::Int64, true),
        ]
        .into();
        let outer_full: Fields = vec![
            Field::new("a", DataType::Int64, true),
            Field::new("inner", DataType::Struct(inner_full.clone()), true),
            Field::new("b", DataType::Int64, true),
        ]
        .into();
        let full = StructArray::new(
            outer_full.clone(),
            vec![
                Arc::new(Int64Array::from(vec![1])),
                Arc::new(StructArray::new(
                    inner_full,
                    vec![
                        Arc::new(Int64Array::from(vec![10])),
                        Arc::new(Int64Array::from(vec![20])),
                    ],
                    None,
                )),
                Arc::new(Int64Array::from(vec![2])),
            ],
            None,
        );
        let first = RecordBatch::try_new(
            Arc::new(arrow::datatypes::Schema::new(vec![Field::new(
                "s",
                DataType::Struct(outer_full),
                true,
            )])),
            vec![Arc::new(full) as ArrayRef],
        )
        .expect("struct batch");
        pass(&mut registry, &table, &first).expect("the struct column registers");

        // The second batch omits `b` and the nested `inner.y`.
        let inner_part: Fields = vec![Field::new("x", DataType::Int64, true)].into();
        let outer_part: Fields = vec![
            Field::new("a", DataType::Int64, true),
            Field::new("inner", DataType::Struct(inner_part.clone()), true),
        ]
        .into();
        let part = StructArray::new(
            outer_part.clone(),
            vec![
                Arc::new(Int64Array::from(vec![3, 4])),
                Arc::new(StructArray::new(
                    inner_part,
                    vec![Arc::new(Int64Array::from(vec![30, 40]))],
                    None,
                )),
            ],
            None,
        );
        let second = RecordBatch::try_new(
            Arc::new(arrow::datatypes::Schema::new(vec![Field::new(
                "s",
                DataType::Struct(outer_part),
                true,
            )])),
            vec![Arc::new(part) as ArrayRef],
        )
        .expect("struct batch");
        let items = pass(&mut registry, &table, &second)
            .expect("an omitted nested field null-fills instead of refusing the push");
        assert!(
            !items
                .iter()
                .any(|item| matches!(item, LoadItem::Delta { .. })),
            "omission is not a schema change"
        );
        let out = items
            .iter()
            .find_map(|item| match item {
                LoadItem::Batch { batch, .. } => Some(batch),
                _ => None,
            })
            .expect("a batch is emitted");
        let s = out
            .column(out.schema().index_of("s").expect("column s"))
            .as_any()
            .downcast_ref::<StructArray>()
            .expect("struct")
            .clone();
        let ints = |array: &ArrayRef| {
            array
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("int64")
                .iter()
                .collect::<Vec<_>>()
        };
        assert_eq!(
            ints(s.column_by_name("a").expect("a")),
            vec![Some(3), Some(4)]
        );
        assert_eq!(
            ints(s.column_by_name("b").expect("b")),
            vec![None, None],
            "the omitted top field null-fills"
        );
        let inner = s
            .column_by_name("inner")
            .expect("inner")
            .as_any()
            .downcast_ref::<StructArray>()
            .expect("struct")
            .clone();
        assert_eq!(
            ints(inner.column_by_name("x").expect("x")),
            vec![Some(30), Some(40)]
        );
        assert_eq!(
            ints(inner.column_by_name("y").expect("y")),
            vec![None, None],
            "the omitted nested field null-fills"
        );
    }
}
