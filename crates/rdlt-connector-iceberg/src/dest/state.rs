//! The pipeline-scoped state document. It lives in the properties of a
//! dedicated marker table (`_rdlt_state`) in the destination namespace — NOT
//! in namespace properties (`update_namespace` is unimplemented in
//! iceberg-catalog-rest 0.10, `FeatureUnsupported`, verified live) and NOT in
//! stream-table properties (a resume cannot enumerate stream tables from dest
//! config alone). A fixed table name is enumerable. The write and read sides
//! compose the property key through ONE [`state_key`] so a resume reads back
//! from the exact key the commit wrote.

use std::sync::Arc;

use iceberg::transaction::{ApplyTransactionAction, Transaction};
use iceberg::{Catalog, NamespaceIdent, TableIdent};
use rdlt_connector::DestError;

use super::commit::{CommitPlan, commit_with_retry};
use super::errors::classify;

/// Property-key prefix for the pipeline-scoped state document.
pub(crate) const PROP_STATE_PREFIX: &str = "rdlt.state.";

/// The marker table carrying every pipeline's state document as properties.
pub(crate) const STATE_TABLE: &str = "_rdlt_state";

fn state_table_schema() -> Result<iceberg::spec::Schema, iceberg::Error> {
    use iceberg::spec::{NestedField, PrimitiveType, Type};
    iceberg::spec::Schema::builder()
        .with_fields(vec![Arc::new(NestedField::optional(
            1,
            "scope",
            Type::Primitive(PrimitiveType::String),
        ))])
        .build()
}

/// The property key a pipeline's state doc lives under on the marker table.
/// ONE spelling shared by the write and read sides: a resume must read state
/// back from the exact key the commit wrote it to, so the prefix + scope
/// composition lives in a single place both call.
fn state_key(scope: &str) -> String {
    format!("{PROP_STATE_PREFIX}{scope}")
}

/// Write the pipeline-scoped state doc as a property of the marker table,
/// creating the (forever-empty) table on first use. Property commits conflict
/// like any other commit — they ride the shared bounded retry.
pub(crate) async fn write_state(
    catalog: &Arc<dyn Catalog>,
    namespace: &NamespaceIdent,
    scope: &str,
    state_json: String,
) -> Result<(), DestError> {
    let ident = TableIdent::new(namespace.clone(), STATE_TABLE.to_string());
    let context = format!("state table `{ident}`");
    let table = match catalog.load_table(&ident).await {
        Ok(table) => table,
        Err(e) if matches!(e.kind(), iceberg::ErrorKind::TableNotFound) => {
            let schema = state_table_schema().map_err(|e| classify(&context, e))?;
            let creation = iceberg::TableCreation::builder()
                .name(STATE_TABLE.to_string())
                .schema(schema)
                .build();
            match catalog.create_table(namespace, creation).await {
                Ok(table) => table,
                // A concurrent creator is fine — load theirs.
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
            Ok(CommitPlan::Commit(Box::new(tx)))
        },
    )
    .await
    .map(|_| ())
}

/// Read the pipeline-scoped state document back from the marker table.
pub(crate) async fn read_state_doc(
    catalog: &Arc<dyn Catalog>,
    namespace: &NamespaceIdent,
    scope: &str,
) -> Result<Option<String>, DestError> {
    let ident = TableIdent::new(namespace.clone(), STATE_TABLE.to_string());
    match catalog.load_table(&ident).await {
        Ok(table) => Ok(table
            .metadata()
            .properties()
            .get(&state_key(scope))
            .cloned()),
        // No marker table (or namespace) yet = no state yet (first run).
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

    use super::*;
    use crate::dest::commit::COMMIT_ATTEMPTS;
    use crate::dest::test_support::ConflictCatalog;

    /// The state doc's write key and read key are ONE helper: a doc stored
    /// under `state_key(scope)` is found under `state_key(scope)`, so the
    /// write and read sides agree by construction (drift here would silently
    /// strand state across a resume).
    #[test]
    fn state_key_write_and_read_agree() {
        let scope = "abc123";
        let mut properties: HashMap<String, String> = HashMap::new();
        properties.insert(state_key(scope), "{\"v\":1}".to_string());
        assert_eq!(
            properties.get(&state_key(scope)).map(String::as_str),
            Some("{\"v\":1}"),
            "read side must look under the exact key the write side used"
        );
        // Distinct scopes never collide onto one key.
        assert_ne!(state_key("abc123"), state_key("def456"));
    }

    /// The state-write exhaustion diagnostic is REACHABLE and typed — the
    /// final conflict names the property-commit subject and the attempt bound
    /// (the word "exhausted" appears once, from `classify`).
    #[tokio::test]
    async fn write_state_exhaustion_is_typed() {
        let catalog = ConflictCatalog::failing(u32::MAX);
        let arc: Arc<dyn Catalog> = catalog.clone();
        let namespace = NamespaceIdent::new("ns".into());
        let err = write_state(&arc, &namespace, "abc123", "{}".into())
            .await
            .expect_err("must exhaust");
        let text = format!("{err}");
        assert!(
            text.contains("property commit")
                && text.contains(&format!("attempt {COMMIT_ATTEMPTS}/{COMMIT_ATTEMPTS}")),
            "names the property-commit subject and the exhausted bound: {text}"
        );
        assert_eq!(text.matches("exhausted").count(), 1, "said once: {text}");
        assert_eq!(catalog.commits.load(Ordering::SeqCst), COMMIT_ATTEMPTS);
    }
}
