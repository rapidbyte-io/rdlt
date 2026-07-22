//! The commit machinery (contracts ID2/ID3): catalog construction from
//! config, batch → data-file writing, snapshot-native receipts, bounded
//! optimistic-concurrency retry, and the marker-table state document.
//! Library types stay BELOW this module and `errors.rs` (ID1).

use std::collections::HashMap;
use std::sync::Arc;

use iceberg::spec::DataFileFormat;
use iceberg::transaction::{ApplyTransactionAction, Transaction};
use iceberg::writer::base_writer::data_file_writer::DataFileWriterBuilder;
use iceberg::writer::file_writer::ParquetWriterBuilder;
use iceberg::writer::file_writer::location_generator::{
    DefaultFileNameGenerator, DefaultLocationGenerator,
};
use iceberg::writer::file_writer::rolling_writer::RollingFileWriterBuilder;
use iceberg::writer::{IcebergWriter, IcebergWriterBuilder};
use iceberg::{Catalog, CatalogBuilder, NamespaceIdent, TableIdent};
use iceberg_catalog_rest::RestCatalogBuilder;
use iceberg_storage_opendal::OpenDalResolvingStorageFactory;
use rdlt_connector::DestError;
use rdlt_connector::core::crash_point;

use super::config::IcebergConfig;
use super::errors::{classify, is_commit_conflict};

/// Snapshot-summary receipt keys (ID2 — the persisted-format identity).
pub(crate) const PROP_PIPELINE: &str = "rdlt.pipeline";
pub(crate) const PROP_LOAD_ID: &str = "rdlt.load-id";
pub(crate) const PROP_COMMIT_SEQ: &str = "rdlt.commit-seq";
/// Property-key prefix for the pipeline-scoped state document (lives on
/// the `_rdlt_state` marker table — see [`STATE_TABLE`]).
pub(crate) const PROP_STATE_PREFIX: &str = "rdlt.state.";

/// Bounded conflict retry (ID3).
const COMMIT_ATTEMPTS: u32 = 4;

fn fatal(message: impl std::fmt::Display) -> DestError {
    DestError::fatal(message.to_string())
}

/// Build the REST catalog from the config document. Secrets are revealed
/// HERE only; the storage override (family S3 spelling) maps to FileIO
/// props that take precedence over the catalog's vended defaults.
pub(crate) async fn connect(config: &IcebergConfig) -> Result<Arc<dyn Catalog>, DestError> {
    let mut props: HashMap<String, String> = HashMap::from([
        ("uri".to_string(), config.catalog.uri.clone()),
        ("warehouse".to_string(), config.catalog.warehouse.clone()),
    ]);
    if let Some(oauth) = &config.catalog.auth.oauth2_client_credentials {
        props.insert(
            "credential".into(),
            format!("{}:{}", oauth.client_id, oauth.client_secret.reveal()),
        );
        if !oauth.scopes.is_empty() {
            props.insert("scope".into(), oauth.scopes.join(" "));
        }
        if let Some(url) = &oauth.token_url {
            props.insert("oauth2-server-uri".into(), url.clone());
        }
    }
    if let Some(bearer) = &config.catalog.auth.bearer {
        props.insert("token".into(), bearer.token.reveal().to_owned());
    }
    if let Some(s3) = config.storage.as_ref().and_then(|s| s.s3.as_ref()) {
        if let Some(endpoint) = &s3.endpoint {
            props.insert("s3.endpoint".into(), endpoint.clone());
        }
        if let Some(region) = &s3.region {
            props.insert("s3.region".into(), region.clone());
        }
        props.insert("s3.access-key-id".into(), s3.access_key.reveal().to_owned());
        props.insert(
            "s3.secret-access-key".into(),
            s3.secret_key.reveal().to_owned(),
        );
        props.insert("s3.path-style-access".into(), s3.path_style.to_string());
    }
    for (key, value) in &config.catalog.props {
        props.insert(key.clone(), value.clone());
    }
    let catalog = RestCatalogBuilder::default()
        .with_storage_factory(Arc::new(OpenDalResolvingStorageFactory::new()))
        .load(config.catalog.warehouse.clone(), props)
        .await
        .map_err(|e| {
            classify(
                &format!(
                    "catalog `{}` warehouse `{}`",
                    config.catalog.uri, config.catalog.warehouse
                ),
                e,
            )
        })?;
    Ok(Arc::new(catalog))
}

/// The commit identity a snapshot carries.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CommitIdentity {
    pub scope: String,
    pub load_id: String,
    pub commit_seq: u64,
}

impl CommitIdentity {
    pub(crate) fn summary_props(&self) -> HashMap<String, String> {
        HashMap::from([
            (PROP_PIPELINE.to_string(), self.scope.clone()),
            (PROP_LOAD_ID.to_string(), self.load_id.clone()),
            (PROP_COMMIT_SEQ.to_string(), self.commit_seq.to_string()),
        ])
    }

    /// Is this identity already present in the table's snapshot history?
    pub(crate) fn already_committed(&self, table: &iceberg::table::Table) -> bool {
        table.metadata().snapshots().any(|snapshot| {
            let summary = &snapshot.summary().additional_properties;
            summary.get(PROP_PIPELINE) == Some(&self.scope)
                && summary.get(PROP_LOAD_ID) == Some(&self.load_id)
                && summary.get(PROP_COMMIT_SEQ) == Some(&self.commit_seq.to_string())
        })
    }
}

/// One table's writer for the current commit window.
pub(crate) struct TableWriter {
    inner: Box<dyn IcebergWriter>,
}

impl TableWriter {
    /// Open a writer against the CURRENT table metadata. `session_nonce`
    /// makes file names unique ACROSS sessions: a recovery session
    /// re-staging the same (load, window) must never overwrite a file a
    /// prior session already committed (its own files stay orphaned and
    /// invisible when the replay check discards them).
    pub(crate) async fn open(
        table: &iceberg::table::Table,
        file_prefix: &str,
        session_nonce: &str,
    ) -> Result<Self, DestError> {
        let context = || format!("table `{}`", table.identifier());
        let location =
            DefaultLocationGenerator::new(table.metadata()).map_err(|e| classify(&context(), e))?;
        let names = DefaultFileNameGenerator::new(
            file_prefix.to_owned(),
            Some(session_nonce.to_owned()),
            DataFileFormat::Parquet,
        );
        let parquet = ParquetWriterBuilder::new(
            parquet::file::properties::WriterProperties::default(),
            table.metadata().current_schema().clone(),
        );
        let rolling = RollingFileWriterBuilder::new_with_default_file_size(
            parquet,
            table.file_io().clone(),
            location,
            names,
        );
        crash_point!(
            "ice.files.write",
            Err(DestError::fatal("injected crash at ice.files.write"))
        );
        let inner = DataFileWriterBuilder::new(rolling)
            .build(None)
            .await
            .map_err(|e| classify(&context(), e))?;
        Ok(Self {
            inner: Box::new(inner),
        })
    }

    pub(crate) async fn write(
        &mut self,
        context: &str,
        batch: arrow_array::RecordBatch,
    ) -> Result<(), DestError> {
        self.inner
            .write(batch)
            .await
            .map_err(|e| classify(context, e))
    }

    pub(crate) async fn close(
        mut self,
        context: &str,
    ) -> Result<Vec<iceberg::spec::DataFile>, DestError> {
        self.inner.close().await.map_err(|e| classify(context, e))
    }
}

/// Commit staged data files as ONE fast-append snapshot carrying the
/// identity — with the bounded conflict retry (refresh → rebuild →
/// commit). Returns the refreshed table. The caller has already checked
/// replay (a replayed identity never reaches here).
pub(crate) async fn append_commit(
    catalog: &Arc<dyn Catalog>,
    table: iceberg::table::Table,
    files: Vec<iceberg::spec::DataFile>,
    identity: &CommitIdentity,
) -> Result<iceberg::table::Table, DestError> {
    let ident = table.identifier().clone();
    let context = format!("table `{ident}`");
    let mut current = table;
    let mut attempt = 0u32;
    loop {
        attempt += 1;
        let tx = Transaction::new(&current);
        let action = tx
            .fast_append()
            .add_data_files(files.clone())
            .set_snapshot_properties(identity.summary_props());
        let tx = action.apply(tx).map_err(|e| classify(&context, e))?;
        crash_point!(
            "ice.commit",
            Err(DestError::fatal("injected crash at ice.commit"))
        );
        match tx.commit(catalog.as_ref()).await {
            Ok(table) => return Ok(table),
            Err(e) if is_commit_conflict(&e) && attempt < COMMIT_ATTEMPTS => {
                // Refresh and rebuild against the competitor's snapshot —
                // never dropping THEIR history (ID3).
                backoff(attempt).await;
                current = catalog
                    .load_table(&ident)
                    .await
                    .map_err(|e| classify(&context, e))?;
                // The competitor may have been OUR replay racing us.
                if identity.already_committed(&current) {
                    return Ok(current);
                }
            }
            Err(e) => {
                return Err(classify(
                    &format!("{context} (commit attempt {attempt}/{COMMIT_ATTEMPTS})"),
                    e,
                ));
            }
        }
    }
}

/// Jittered exponential backoff between optimistic-commit attempts.
async fn backoff(attempt: u32) {
    let millis = 50u64 * (1 << attempt.min(4)) + (attempt as u64 * 13) % 37;
    tokio::time::sleep(std::time::Duration::from_millis(millis)).await;
}

/// The state doc lives in the properties of a dedicated marker table
/// (`_rdlt_state`) in the destination namespace — NOT in namespace
/// properties: the REST client's `update_namespace` is unimplemented in
/// iceberg-catalog-rest 0.10 (`FeatureUnsupported`, verified live), and
/// NOT in stream-table properties: `read_state` cannot enumerate stream
/// tables from dest config alone. A fixed table name is enumerable.
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

/// Write the pipeline-scoped state doc as a property of the marker
/// table, creating the (forever-empty) table on first use. Property
/// commits conflict like any other commit — bounded retry.
pub(crate) async fn write_state(
    catalog: &Arc<dyn Catalog>,
    namespace: &NamespaceIdent,
    scope: &str,
    state_json: String,
) -> Result<(), DestError> {
    let ident = TableIdent::new(namespace.clone(), STATE_TABLE.to_string());
    let context = format!("state table `{ident}`");
    let mut table = match catalog.load_table(&ident).await {
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
    let key = format!("{PROP_STATE_PREFIX}{scope}");
    let mut last = None;
    for attempt in 0..COMMIT_ATTEMPTS {
        let tx = Transaction::new(&table);
        let action = tx
            .update_table_properties()
            .set(key.clone(), state_json.clone());
        let tx = action.apply(tx).map_err(|e| classify(&context, e))?;
        match tx.commit(catalog.as_ref()).await {
            Ok(_) => return Ok(()),
            Err(e) if is_commit_conflict(&e) && attempt + 1 < COMMIT_ATTEMPTS => {
                last = Some(e);
                backoff(attempt).await;
                table = catalog
                    .load_table(&ident)
                    .await
                    .map_err(|e| classify(&context, e))?;
            }
            Err(e) => return Err(classify(&context, e)),
        }
    }
    Err(classify(
        &format!("{context}: {COMMIT_ATTEMPTS} property-commit attempts exhausted"),
        last.expect("loop stores the error before retrying"),
    ))
}

/// Read the pipeline-scoped state document back from the marker table.
pub(crate) async fn read_state(
    catalog: &Arc<dyn Catalog>,
    namespace: &NamespaceIdent,
    scope: &str,
) -> Result<Option<String>, DestError> {
    let ident = TableIdent::new(namespace.clone(), STATE_TABLE.to_string());
    match catalog.load_table(&ident).await {
        Ok(table) => Ok(table
            .metadata()
            .properties()
            .get(&format!("{PROP_STATE_PREFIX}{scope}"))
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

/// Load-or-create the namespace per the explicit config flag.
pub(crate) async fn ensure_namespace(
    catalog: &Arc<dyn Catalog>,
    namespace: &NamespaceIdent,
    create_if_missing: bool,
) -> Result<(), DestError> {
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
) -> Result<iceberg::table::Table, DestError> {
    let ident = TableIdent::new(namespace.clone(), name.to_owned());
    let context = format!("table `{ident}`");
    let exists = catalog
        .table_exists(&ident)
        .await
        .map_err(|e| classify(&context, e))?;
    if !exists {
        let creation = iceberg::TableCreation::builder()
            .name(name.to_owned())
            .schema(wanted.clone())
            .build();
        return match catalog.create_table(namespace, creation).await {
            Ok(table) => Ok(table),
            // Concurrent creator: fall through to load + reconcile.
            Err(e) if matches!(e.kind(), iceberg::ErrorKind::TableAlreadyExists) => {
                reconcile(catalog, &ident, wanted).await
            }
            Err(e) => Err(classify(&context, e)),
        };
    }
    reconcile(catalog, &ident, wanted).await
}

/// Compare the wanted schema against the live table by NAME: identical
/// types pass, missing nullable columns are ADDED (additive drift),
/// anything else is typed naming the column.
async fn reconcile(
    catalog: &Arc<dyn Catalog>,
    ident: &TableIdent,
    wanted: &iceberg::spec::Schema,
) -> Result<iceberg::table::Table, DestError> {
    use iceberg::transaction::AddColumn;
    let context = format!("table `{ident}`");
    let table = catalog
        .load_table(ident)
        .await
        .map_err(|e| classify(&context, e))?;
    let existing = table.metadata().current_schema();
    let mut additions: Vec<AddColumn> = Vec::new();
    for field in wanted.as_struct().fields() {
        match existing
            .as_struct()
            .fields()
            .iter()
            .find(|f| f.name == field.name)
        {
            Some(current) => {
                if current.field_type != field.field_type {
                    return Err(fatal(format!(
                        "{context} column `{}`: stream type {} conflicts with the \
                         table's {} — contradictory drift is never applied",
                        field.name, field.field_type, current.field_type
                    )));
                }
            }
            None => {
                // New column: ADDITIVE only (nullable in the table even if
                // the stream declares it required — existing rows have no
                // value for it; the drift rules' documented posture).
                additions.push(AddColumn::optional(
                    &field.name,
                    field.field_type.as_ref().clone(),
                ));
            }
        }
    }
    if additions.is_empty() {
        return Ok(table);
    }
    let tx = Transaction::new(&table);
    let mut action = tx.update_schema();
    for addition in additions {
        action = action.add_column(addition);
    }
    let tx = action.apply(tx).map_err(|e| classify(&context, e))?;
    tx.commit(catalog.as_ref())
        .await
        .map_err(|e| classify(&context, e))
}
