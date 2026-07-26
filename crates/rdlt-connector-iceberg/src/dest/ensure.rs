//! Provisioning and drift reconciliation: load-or-create the namespace and
//! each table, then reconcile the live schema against the wanted one —
//! additive nullable columns are applied (via the shared bounded retry),
//! contradictory drift and partition-spec disagreement are typed.

use std::collections::HashMap;
use std::sync::Arc;

use iceberg::transaction::{ApplyTransactionAction, Transaction};
use iceberg::{Catalog, NamespaceIdent, TableIdent};
use rdlt_connector::DestinationError;

use super::commit::{CommitPlan, commit_with_retry};
use super::config::PartitionField;
use super::errors::{classify, fatal};

/// Load-or-create the namespace per the explicit config flag.
pub(crate) async fn ensure_namespace(
    catalog: &Arc<dyn Catalog>,
    namespace: &NamespaceIdent,
    create_if_missing: bool,
) -> Result<(), DestinationError> {
    let context = format!("namespace `{}`", namespace.to_url_string());
    let exists = catalog
        .namespace_exists(namespace)
        .await
        .map_err(|e| classify(&context, e))?;
    if exists {
        return Ok(());
    }
    if !create_if_missing {
        return Err(fatal(format!(
            "{context} does not exist and create_namespace is false — create it \
             (or set create_namespace: true)"
        )));
    }
    match catalog.create_namespace(namespace, HashMap::new()).await {
        Ok(_) => Ok(()),
        // A concurrent creator is fine.
        Err(e) if matches!(e.kind(), iceberg::ErrorKind::NamespaceAlreadyExists) => Ok(()),
        Err(e) => Err(classify(&context, e)),
    }
}

/// Load-or-create a table with the mapped schema; reconcile drift
/// (additive nullable columns via UpdateSchema; conflicts typed).
pub(crate) async fn ensure_table(
    catalog: &Arc<dyn Catalog>,
    namespace: &NamespaceIdent,
    name: &str,
    wanted: &iceberg::spec::Schema,
    partition: &[PartitionField],
) -> Result<iceberg::table::Table, DestinationError> {
    let ident = TableIdent::new(namespace.clone(), name.to_owned());
    let context = format!("table `{ident}`");
    let exists = catalog
        .table_exists(&ident)
        .await
        .map_err(|e| classify(&context, e))?;
    if !exists {
        let spec = super::schema::to_partition_spec(&context, wanted, partition)?;
        let creation = iceberg::TableCreation::builder()
            .name(name.to_owned())
            .schema(wanted.clone())
            .partition_spec_opt(spec)
            .build();
        return match catalog.create_table(namespace, creation).await {
            Ok(table) => Ok(table),
            // Concurrent creator: fall through to load + reconcile.
            Err(e) if matches!(e.kind(), iceberg::ErrorKind::TableAlreadyExists) => {
                reconcile(catalog, &ident, wanted, partition).await
            }
            Err(e) => Err(classify(&context, e)),
        };
    }
    reconcile(catalog, &ident, wanted, partition).await
}

/// Render a partition spec (column + transform pairs) for operators using
/// each transform's Display — never Debug tuples.
fn render_partition(fields: &[(String, iceberg::spec::Transform)]) -> String {
    fields
        .iter()
        .map(|(column, transform)| format!("{column}:{transform}"))
        .collect::<Vec<_>>()
        .join(", ")
}

/// The partition spec is fixed at table creation; a config that disagrees
/// with the live table is a typed error, never a silent re-spec (Iceberg
/// spec evolution is out of scope this release).
fn check_partition_spec(
    context: &str,
    table: &iceberg::table::Table,
    partition: &[PartitionField],
) -> Result<(), DestinationError> {
    let live = table.metadata().default_partition_spec();
    let schema = table.metadata().current_schema();
    let live_fields: Vec<(String, iceberg::spec::Transform)> = live
        .fields()
        .iter()
        .map(|f| {
            let column = schema
                .field_by_id(f.source_id)
                .map(|s| s.name.clone())
                .unwrap_or_else(|| format!("#{}", f.source_id));
            (column, f.transform)
        })
        .collect();
    let wanted_fields: Vec<(String, iceberg::spec::Transform)> = partition
        .iter()
        .map(|f| (f.column.clone(), super::schema::to_transform(f.transform)))
        .collect();
    if live_fields != wanted_fields {
        return Err(fatal(format!(
            "{context}: configured partition_by [{}] does not match the live table's \
             partition spec [{}] — partition specs are fixed at creation (drop the table \
             or align the config)",
            render_partition(&wanted_fields),
            render_partition(&live_fields)
        )));
    }
    Ok(())
}

/// Compare the wanted schema against the live table by NAME: identical types
/// pass, missing nullable columns are ADDED (additive drift), anything else is
/// typed naming the column. Schema commits conflict like any other commit; the
/// shared bounded retry reloads and RECOMPUTES the additions each attempt — a
/// competitor may have added the same column, converging to an empty addition
/// set (which settles without a commit).
async fn reconcile(
    catalog: &Arc<dyn Catalog>,
    ident: &TableIdent,
    wanted: &iceberg::spec::Schema,
    partition: &[PartitionField],
) -> Result<iceberg::table::Table, DestinationError> {
    use iceberg::transaction::AddColumn;
    let context = format!("table `{ident}`");
    let initial = catalog
        .load_table(ident)
        .await
        .map_err(|e| classify(&context, e))?;
    commit_with_retry(
        catalog,
        ident,
        &context,
        "schema commit",
        &context,
        initial,
        |current| {
            check_partition_spec(&context, current, partition)?;
            let existing = current.metadata().current_schema();
            let mut additions: Vec<AddColumn> = Vec::new();
            for field in wanted.as_struct().fields() {
                match existing
                    .as_struct()
                    .fields()
                    .iter()
                    .find(|f| f.name == field.name)
                {
                    Some(current_field) => {
                        // Compared STRUCTURALLY. Field ids belong to the
                        // catalog, which renumbers on create, so comparing the
                        // types directly reports drift for a stream that never
                        // changed — see `schema::compare_column`.
                        if let Err(drift) =
                            crate::dest::schema::compare_column(field, current_field)
                        {
                            let detail = match drift {
                                crate::dest::schema::Drift::Type { wanted, live } => format!(
                                    "stream type {wanted} conflicts with the table's {live}"
                                ),
                                crate::dest::schema::Drift::NestedFields { wanted, live } => {
                                    format!(
                                        "stream shape {wanted} conflicts with the table's {live} \
                                         — a nested field was added, removed or renamed, which \
                                         additive evolution cannot express"
                                    )
                                }
                                crate::dest::schema::Drift::Nullability => {
                                    "the table requires a value this stream may not supply"
                                        .to_owned()
                                }
                            };
                            return Err(fatal(format!(
                                "{context} column `{}`: {detail} — contradictory drift is never \
                                 applied",
                                field.name
                            )));
                        }
                    }
                    None => {
                        // New column: ADDITIVE only (nullable in the table even
                        // if the stream declares it required — existing rows
                        // have no value for it; the drift rules' documented
                        // posture).
                        additions.push(AddColumn::optional(
                            &field.name,
                            field.field_type.as_ref().clone(),
                        ));
                    }
                }
            }
            if additions.is_empty() {
                return Ok(CommitPlan::Settled);
            }
            let tx = Transaction::new(current);
            let mut action = tx.update_schema();
            for addition in additions {
                action = action.add_column(addition);
            }
            let tx = action.apply(tx).map_err(|e| classify(&context, e))?;
            Ok(CommitPlan::Commit(Box::new(tx)))
        },
    )
    .await
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::Ordering;

    use super::*;
    use crate::dest::commit::COMMIT_ATTEMPTS;
    use crate::dest::test_support::ConflictCatalog;

    /// Reconcile's schema commit rides the SAME bounded retry as data
    /// commits — one conflict must not fail the load.
    #[tokio::test]
    async fn reconcile_retries_schema_commit_conflicts() {
        use iceberg::spec::{NestedField, PrimitiveType, Type};
        let catalog = ConflictCatalog::failing(COMMIT_ATTEMPTS - 1);
        let arc: Arc<dyn Catalog> = catalog.clone();
        let wanted = iceberg::spec::Schema::builder()
            .with_fields(vec![
                Arc::new(NestedField::required(
                    1,
                    "id",
                    Type::Primitive(PrimitiveType::Long),
                )),
                Arc::new(NestedField::optional(
                    2,
                    "note",
                    Type::Primitive(PrimitiveType::String),
                )),
            ])
            .build()
            .expect("schema");
        let ident = TableIdent::new(NamespaceIdent::new("ns".into()), "events".into());
        reconcile(&arc, &ident, &wanted, &[])
            .await
            .expect("lands within the bound");
        assert_eq!(
            catalog.commits.load(Ordering::SeqCst),
            COMMIT_ATTEMPTS,
            "3 conflicts + the landing attempt"
        );
    }
}
