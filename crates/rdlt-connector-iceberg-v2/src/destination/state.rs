//! The pipeline-scoped state document, as marker-table properties.
//!
//! State lives in the properties of a dedicated `_rdlt_state` table in
//! the destination namespace. NOT namespace properties —
//! `update_namespace` is unimplemented in iceberg-catalog-rest 0.10
//! (`FeatureUnsupported`, verified live) — and NOT stream-table
//! properties, because a resume cannot enumerate stream tables from
//! the destination config alone; a fixed name is enumerable. The write
//! and read sides compose the key through ONE [`state_key`], so a
//! resume reads exactly where the commit wrote.

use std::sync::Arc;

use iceberg::transaction::{ApplyTransactionAction as _, Transaction};
use iceberg::{Catalog, NamespaceIdent, TableIdent};
use rdlt_connector_sdk::spi::DestinationError;

use super::client::classify;
use super::commit::{Plan, commit_with_retry};

/// The property-key prefix for state documents.
pub(super) const PROP_STATE_PREFIX: &str = "rdlt.state.";

/// The marker table every pipeline's state rides on.
pub(super) const STATE_TABLE: &str = "_rdlt_state";

/// The one key composition both sides call — drift here would
/// silently strand state across a resume.
fn state_key(scope: &str) -> String {
    format!("{PROP_STATE_PREFIX}{scope}")
}

/// The marker table's (forever-empty) schema: one optional column, so
/// the create payload is valid everywhere.
fn marker_schema() -> Result<iceberg::spec::Schema, iceberg::Error> {
    use iceberg::spec::{NestedField, PrimitiveType, Type};
    iceberg::spec::Schema::builder()
        .with_fields(vec![Arc::new(NestedField::optional(
            1,
            "scope",
            Type::Primitive(PrimitiveType::String),
        ))])
        .build()
}

/// Write the state doc as a marker-table property, creating the table
/// on first use (a concurrent creator's table is adopted, not an
/// error). Property commits conflict like any other commit — they
/// ride the shared bounded retry under the `property commit` subject.
pub(super) async fn write_state(
    catalog: &Arc<dyn Catalog>,
    namespace: &NamespaceIdent,
    scope: &str,
    state_json: String,
) -> Result<(), DestinationError> {
    let ident = TableIdent::new(namespace.clone(), STATE_TABLE.to_owned());
    let context = format!("state table `{ident}`");
    let table = match catalog.load_table(&ident).await {
        Ok(table) => table,
        Err(e) if matches!(e.kind(), iceberg::ErrorKind::TableNotFound) => {
            let schema = marker_schema().map_err(|e| classify(&context, e))?;
            let creation = iceberg::TableCreation::builder()
                .name(STATE_TABLE.to_owned())
                .schema(schema)
                .build();
            match catalog.create_table(namespace, creation).await {
                Ok(table) => table,
                Err(e) if matches!(e.kind(), iceberg::ErrorKind::TableAlreadyExists) => catalog
                    .load_table(&ident)
                    .await
                    .map_err(|e| classify(&context, e))?,
                Err(e) => return Err(classify(&context, e)),
            }
        }
        Err(e) => return Err(classify(&context, e)),
    };
    let key = state_key(scope);
    commit_with_retry(
        catalog,
        &ident,
        &context,
        "property commit",
        scope,
        table,
        |current| {
            let tx = Transaction::new(current);
            let action = tx
                .update_table_properties()
                .set(key.clone(), state_json.clone());
            let tx = action.apply(tx).map_err(|e| classify(&context, e))?;
            Ok(Plan::Commit(Box::new(tx)))
        },
    )
    .await
    .map(|_| ())
}

/// Read the state doc back. An absent marker table — or the whole
/// namespace — is a first run, not an error.
pub(super) async fn read_state_doc(
    catalog: &Arc<dyn Catalog>,
    namespace: &NamespaceIdent,
    scope: &str,
) -> Result<Option<String>, DestinationError> {
    let ident = TableIdent::new(namespace.clone(), STATE_TABLE.to_owned());
    match catalog.load_table(&ident).await {
        Ok(table) => Ok(table
            .metadata()
            .properties()
            .get(&state_key(scope))
            .cloned()),
        Err(e)
            if matches!(
                e.kind(),
                iceberg::ErrorKind::TableNotFound | iceberg::ErrorKind::NamespaceNotFound
            ) =>
        {
            Ok(None)
        }
        Err(e) => Err(classify(&format!("state table `{ident}`"), e)),
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::atomic::Ordering;

    use iceberg::NamespaceIdent;

    use super::super::commit::COMMIT_ATTEMPTS;
    use super::super::testsupport::ConflictCatalog;
    use super::*;

    /// One key composition, both sides: what the write stores under
    /// `state_key(scope)`, the read finds there — and scopes never
    /// collide onto one key.
    #[test]
    fn the_write_and_read_keys_are_one_composition() {
        let mut properties: HashMap<String, String> = HashMap::new();
        properties.insert(state_key("abc123"), "{\"v\":1}".to_owned());
        assert_eq!(
            properties.get(&state_key("abc123")).map(String::as_str),
            Some("{\"v\":1}")
        );
        assert_ne!(state_key("abc123"), state_key("def456"));
        assert!(state_key("abc123").starts_with(PROP_STATE_PREFIX));
    }

    /// The property-commit exhaustion is reachable, typed, and names
    /// its subject and bound — with "exhausted" said exactly once.
    #[tokio::test]
    async fn a_state_write_that_keeps_losing_exhausts_typed() {
        let catalog = ConflictCatalog::failing(u32::MAX);
        let arc: Arc<dyn Catalog> = catalog.clone();
        let err = write_state(
            &arc,
            &NamespaceIdent::new("ns".into()),
            "abc123",
            "{}".into(),
        )
        .await
        .expect_err("must exhaust");
        let text = format!("{err}");
        assert!(
            text.contains("property commit")
                && text.contains(&format!("attempt {COMMIT_ATTEMPTS}/{COMMIT_ATTEMPTS}")),
            "{text}"
        );
        assert_eq!(text.matches("exhausted").count(), 1, "{text}");
        assert_eq!(catalog.commits.load(Ordering::SeqCst), COMMIT_ATTEMPTS);
    }
}
