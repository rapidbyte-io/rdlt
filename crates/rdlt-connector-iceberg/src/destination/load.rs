//! The session: one load's conversation with the catalog, as the
//! sdk's `Backend`.
//!
//! Append maps onto fast-append snapshots; Merge and Replace are typed
//! refusals (no overwrite transaction exists in the library, and
//! emulating one would not be atomic). Exactly-once is snapshot-native
//! and PER TABLE: publish stamps each table's snapshot with the commit
//! identity and converges on replay by scanning history — which is why
//! `existing_receipt` deliberately answers `None`: there is no
//! load-level receipt store, a partially published commit is exactly
//! the state publish knows how to converge from, and answering
//! otherwise would claim an atomicity the catalog does not offer.

use std::collections::BTreeMap;
use std::sync::Arc;

use async_trait::async_trait;
use iceberg::transaction::{ApplyTransactionAction as _, Transaction};
use iceberg::{Catalog, NamespaceIdent, TableIdent};
use rdlt_connector_sdk::destination::Backend;
use rdlt_connector_sdk::spi::core::naming::ident_hash;
use rdlt_connector_sdk::spi::core::{
    LoadId, PipelineId, StateDoc, TableName, TableSchema, WriteMode, crash_point,
};
use rdlt_connector_sdk::spi::{CommitMeta, CommitReceipt, DestinationError, RecordBatch};

use super::client::{classify, fatal};
use super::commit::{Identity, Plan, append_commit, commit_with_retry};
use super::config::Config;
use super::partition;
use super::schema::{self, compare_field};
use super::state::{STATE_TABLE, read_state_doc, write_state};
use super::write::Writer;

/// Width of the pipeline scope hash. The scope names the pipeline in
/// snapshot summaries and the state key, so the width a session opens
/// with MUST equal the width `read_state` re-derives with.
pub(super) const SCOPE_HASH_LEN: usize = 12;

/// One stream table's session state.
struct TableState {
    /// The live handle, refreshed at ensure and publish boundaries.
    table: iceberg::table::Table,
    /// The field-id-annotated arrow schema batches align to.
    arrow_target: Arc<arrow_schema::Schema>,
    /// The current window's writer, opened on first write.
    writer: Option<Writer>,
    /// Files closed early — a mid-window schema change retires the
    /// writer and parks its files here until the window publishes.
    pending_files: Vec<iceberg::spec::DataFile>,
    /// Window counter for unique file-name prefixes. Survives
    /// re-ensure: resetting it would regenerate window 1's exact path
    /// and overwrite a committed file.
    window_seq: u64,
    /// When the current writer opened — the clock `roll_after_seconds`
    /// reads. `None` while no writer is open.
    writer_opened_at: Option<std::time::Instant>,
}

/// The session the connector opens — the sdk drives it.
pub struct Load {
    pub(super) config: Config,
    pub(super) catalog: Arc<dyn Catalog>,
    pub(super) namespace: NamespaceIdent,
    /// `ident_hash(pipeline, SCOPE_HASH_LEN)` — never the raw name.
    pub(super) scope: String,
    pub(super) load_id: LoadId,
    /// Unique per session; see `Writer::open`'s nonce contract.
    pub(super) nonce: String,
    /// Resolved once at connect: the translation can fail, and a load
    /// should not discover that partway through writing.
    pub(super) writer_properties: parquet::file::properties::WriterProperties,
    /// Output file sizing. `target_bytes` reaches the library's own
    /// rolling writer; `roll_after_seconds` is applied here, since the
    /// library has no time trigger.
    pub(super) parts: rdlt_connector_sdk::spi::PartOptions,
    tables: BTreeMap<TableName, TableState>,
}

impl std::fmt::Debug for Load {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Load")
            .field("scope", &self.scope)
            .field("load_id", &self.load_id)
            .finish_non_exhaustive()
    }
}

impl Load {
    pub(super) fn new(
        config: Config,
        catalog: Arc<dyn Catalog>,
        namespace: NamespaceIdent,
        pipeline: &PipelineId,
        load_id: LoadId,
        writer_properties: parquet::file::properties::WriterProperties,
        parts: rdlt_connector_sdk::spi::PartOptions,
    ) -> Self {
        Self {
            parts,
            config,
            catalog,
            namespace,
            scope: ident_hash(pipeline.as_str(), SCOPE_HASH_LEN),
            load_id,
            nonce: session_nonce(),
            writer_properties,
            tables: BTreeMap::new(),
        }
    }

    /// Only Append maps onto a snapshot this release.
    fn check_mode(mode: &WriteMode) -> Result<(), DestinationError> {
        match mode {
            WriteMode::Append => Ok(()),
            WriteMode::Merge { .. } => Err(fatal(
                "iceberg destination does not support Merge (capabilities.merge = false)",
            )),
            WriteMode::Replace => Err(fatal(
                "iceberg destination: Replace is not supported — the underlying iceberg library \
                 exposes no overwrite transaction, which Replace requires; use Append, or a SQL \
                 destination for replace semantics",
            )),
        }
    }

    /// Fold the freshly ensured table into the session, carrying the
    /// window counter and any in-flight writer across a re-ensure. The
    /// writer survives ONLY while the write schema is unchanged: a
    /// re-ensure carrying drift RETIRES it — its closed files (valid
    /// under the prior schema; Iceberg reads absent columns as null
    /// after additive evolution) park in `pending_files` and join the
    /// window's publish, and the next writer opens against the evolved
    /// table.
    async fn reinstall(
        &mut self,
        stream: &TableName,
        name: &str,
        table: iceberg::table::Table,
        arrow_target: Arc<arrow_schema::Schema>,
    ) -> Result<(), DestinationError> {
        let (window_seq, prev_writer, prev_target, mut pending_files, prev_opened_at) =
            match self.tables.remove(stream) {
                Some(prev) => (
                    prev.window_seq,
                    prev.writer,
                    Some(prev.arrow_target),
                    prev.pending_files,
                    prev.writer_opened_at,
                ),
                None => (0, None, None, Vec::new(), None),
            };
        let (writer, writer_opened_at) = match prev_writer {
            Some(writer) if prev_target.as_deref() != Some(arrow_target.as_ref()) => {
                let context = format!("table `{name}` (schema-change writer retirement)");
                pending_files.extend(writer.close(&context).await?);
                (None, None)
            }
            other => (other, prev_opened_at),
        };
        self.tables.insert(
            stream.clone(),
            TableState {
                table,
                arrow_target,
                writer,
                pending_files,
                window_seq,
                writer_opened_at,
            },
        );
        Ok(())
    }

    /// Align an engine batch to the table's arrow target: columns
    /// matched BY NAME, cast where representations differ, null-filled
    /// where the table's column is nullable and the batch lacks it,
    /// and typed — attributed to the TABLE — where it is required.
    fn align(
        context: &str,
        target: &Arc<arrow_schema::Schema>,
        batch: &RecordBatch,
    ) -> Result<RecordBatch, DestinationError> {
        let mut columns = Vec::with_capacity(target.fields().len());
        for field in target.fields() {
            let column = match batch.schema().index_of(field.name()) {
                Ok(index) => {
                    let column = batch.column(index);
                    if column.data_type() == field.data_type() {
                        column.clone()
                    } else {
                        arrow_cast::cast(column, field.data_type()).map_err(|e| {
                            fatal(format!(
                                "{context}: column `{}` cannot cast {} -> {}: {e}",
                                field.name(),
                                column.data_type(),
                                field.data_type()
                            ))
                        })?
                    }
                }
                Err(_) if field.is_nullable() => {
                    arrow_array::new_null_array(field.data_type(), batch.num_rows())
                }
                Err(_) => {
                    return Err(fatal(format!(
                        "{context}: the live table requires column `{}` but the stream no longer \
                         provides it",
                        field.name()
                    )));
                }
            };
            columns.push(column);
        }
        RecordBatch::try_new(target.clone(), columns)
            .map_err(|e| fatal(format!("{context}: aligning batch: {e}")))
    }
}

/// A recovery session replaying (load, window) must never reuse a
/// prior session's data-file names: wall clock plus a process-wide
/// counter.
fn session_nonce() -> String {
    use std::sync::atomic::{AtomicU64, Ordering};
    static SEQ: AtomicU64 = AtomicU64::new(0);
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0);
    // The pid closes the two-processes-in-one-nanosecond window the
    // per-process counter cannot see.
    format!(
        "{:x}-{}-{}",
        nanos,
        std::process::id(),
        SEQ.fetch_add(1, Ordering::Relaxed)
    )
}

/// The table's field-id-annotated arrow schema — recomputed at ensure
/// AND publish boundaries through this one helper, because a
/// concurrent writer's additive evolution changes it.
fn arrow_target(
    context: &str,
    table: &iceberg::table::Table,
) -> Result<Arc<arrow_schema::Schema>, DestinationError> {
    iceberg::arrow::schema_to_arrow_schema(table.metadata().current_schema())
        .map(Arc::new)
        .map_err(|e| fatal(format!("{context}: arrow schema conversion: {e}")))
}

/// Load-or-create the namespace per the explicit config flag.
pub(super) async fn ensure_namespace(
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
            "{context} does not exist and create_namespace is false — create it (or set \
             create_namespace: true)"
        )));
    }
    match catalog
        .create_namespace(namespace, std::collections::HashMap::new())
        .await
    {
        Ok(_) => Ok(()),
        Err(e) if matches!(e.kind(), iceberg::ErrorKind::NamespaceAlreadyExists) => Ok(()),
        Err(e) => Err(classify(&context, e)),
    }
}

/// Load-or-create the table, then reconcile drift: additive nullable
/// columns are APPLIED through the shared retry (a competitor adding
/// the same column converges to an empty addition set, which settles);
/// contradictory drift and partition disagreement are typed.
async fn ensure_stream_table(
    catalog: &Arc<dyn Catalog>,
    namespace: &NamespaceIdent,
    name: &str,
    wanted: &iceberg::spec::Schema,
    fields: &[super::config::PartitionField],
) -> Result<iceberg::table::Table, DestinationError> {
    let ident = TableIdent::new(namespace.clone(), name.to_owned());
    let context = format!("table `{ident}`");
    let exists = catalog
        .table_exists(&ident)
        .await
        .map_err(|e| classify(&context, e))?;
    if !exists {
        let spec = partition::to_partition_spec(&context, wanted, fields)?;
        let creation = iceberg::TableCreation::builder()
            .name(name.to_owned())
            .schema(wanted.clone())
            .partition_spec_opt(spec)
            .build();
        match catalog.create_table(namespace, creation).await {
            Ok(table) => return Ok(table),
            // A concurrent creator: fall through to reconcile theirs.
            Err(e) if matches!(e.kind(), iceberg::ErrorKind::TableAlreadyExists) => {}
            Err(e) => return Err(classify(&context, e)),
        }
    }
    reconcile(catalog, &ident, wanted, fields).await
}

/// One reconcile attempt per retry round: partition check first, then
/// per wanted column — present compares structurally, absent becomes
/// an optional AddColumn (additive ONLY: the new column is nullable in
/// the table even where the stream declares it required — existing
/// rows have no value for it).
async fn reconcile(
    catalog: &Arc<dyn Catalog>,
    ident: &TableIdent,
    wanted: &iceberg::spec::Schema,
    fields: &[super::config::PartitionField],
) -> Result<iceberg::table::Table, DestinationError> {
    let context = format!("table `{ident}`");
    let initial = catalog
        .load_table(ident)
        .await
        .map_err(|e| classify(&context, e))?;
    let entropy = context.clone();
    commit_with_retry(
        catalog,
        ident,
        &context,
        "schema commit",
        &entropy,
        initial,
        |current| {
            partition::check_live_spec(&context, current, fields)?;
            let live = current.metadata().current_schema();
            let mut additions: Vec<iceberg::transaction::AddColumn> = Vec::new();
            for field in wanted.as_struct().fields() {
                match live
                    .as_struct()
                    .fields()
                    .iter()
                    .find(|f| f.name == field.name)
                {
                    Some(live_field) => {
                        if let Some(drift) = compare_field(field, live_field) {
                            return Err(fatal(format!(
                                "{context} column `{}`: {} — contradictory drift is never applied",
                                field.name,
                                drift.detail()
                            )));
                        }
                    }
                    None => additions.push(iceberg::transaction::AddColumn::optional(
                        &field.name,
                        field.field_type.as_ref().clone(),
                    )),
                }
            }
            if additions.is_empty() {
                return Ok(Plan::Settled);
            }
            let tx = Transaction::new(current);
            let mut action = tx.update_schema();
            for addition in additions {
                action = action.add_column(addition);
            }
            let tx = action.apply(tx).map_err(|e| classify(&context, e))?;
            Ok(Plan::Commit(Box::new(tx)))
        },
    )
    .await
}

#[async_trait]
impl Backend for Load {
    async fn ensure_table(
        &mut self,
        table_schema: &TableSchema,
        mode: &WriteMode,
    ) -> Result<(), DestinationError> {
        Self::check_mode(mode)?;
        let stream = table_schema.table.as_str();
        let name = self.config.table_name(stream);
        // Cheap and infallible BEFORE the fallible mapping: a
        // misconfigured name fails on its own terms.
        if name == STATE_TABLE {
            return Err(fatal(format!(
                "table name `{name}` is reserved for the rdlt state marker table"
            )));
        }
        // The config gate refuses duplicate EXPLICIT names; a rename
        // onto another stream's DEFAULT name is only visible here,
        // where both resolutions exist. Sharing one physical table
        // would interleave colliding file paths and read one stream's
        // commit as the other's replay.
        if let Some(other) = self
            .tables
            .keys()
            .find(|s| s.as_str() != stream && self.config.table_name(s.as_str()) == name)
        {
            return Err(fatal(format!(
                "streams `{other}` and `{stream}` both resolve to table `{name}` — two streams \
                 may not share one table"
            )));
        }
        let wanted = schema::to_iceberg_schema(table_schema)?;
        let fields = self.config.partition_fields(stream);
        let table =
            ensure_stream_table(&self.catalog, &self.namespace, &name, &wanted, fields).await?;
        let target = arrow_target(&format!("table `{name}`"), &table)?;
        self.reinstall(&table_schema.table, &name, table, target)
            .await
    }

    async fn write(
        &mut self,
        table: &TableName,
        batch: RecordBatch,
    ) -> Result<(), DestinationError> {
        let state = self
            .tables
            .get_mut(table)
            .ok_or_else(|| fatal(format!("write before ensure_table for `{table}`")))?;
        let context = format!("table `{}`", self.config.table_name(table.as_str()));
        let aligned = Self::align(&context, &state.arrow_target, &batch)?;
        // The TIME threshold is applied here because the library's
        // rolling writer has no clock — only a size. Retiring the whole
        // writer is the same move a mid-window schema change makes: its
        // files park in `pending_files` and join the window's publish.
        if let Some(opened_at) = state.writer_opened_at
            && self.parts.rolls_on_time(opened_at.elapsed().as_secs())
            && let Some(writer) = state.writer.take()
        {
            let retire = format!("{context} (roll_after_seconds writer retirement)");
            state.pending_files.extend(writer.close(&retire).await?);
            state.writer_opened_at = None;
        }
        if state.writer.is_none() {
            state.window_seq += 1;
            let prefix = format!("{}-{}", self.load_id, state.window_seq);
            state.writer = Some(
                Writer::open(
                    &state.table,
                    &prefix,
                    &self.nonce,
                    self.writer_properties.clone(),
                    self.parts.target_file_size(),
                )
                .await?,
            );
            state.writer_opened_at = Some(std::time::Instant::now());
        }
        state
            .writer
            .as_mut()
            .expect("writer just ensured")
            .write(&context, aligned)
            .await
    }

    async fn existing_receipt(
        &mut self,
        _load_id: &LoadId,
        _commit_seq: u64,
    ) -> Result<Option<CommitReceipt>, DestinationError> {
        // Deliberately None — see the module doc: receipts are
        // per-table snapshot properties, a partial publish is exactly
        // what publish converges from, and no load-level receipt store
        // exists to look one up in.
        Ok(None)
    }

    async fn publish(&mut self, meta: CommitMeta) -> Result<CommitReceipt, DestinationError> {
        let receipt = CommitReceipt {
            load_id: meta.load_id.clone(),
            commit_seq: meta.commit_seq,
        };
        let identity = Identity {
            scope: self.scope.clone(),
            load_id: meta.load_id.as_str().to_owned(),
            commit_seq: meta.commit_seq,
        };
        for (table_name, state) in self.tables.iter_mut() {
            let context = format!("table `{}`", self.config.table_name(table_name.as_str()));
            let mut files = std::mem::take(&mut state.pending_files);
            if let Some(writer) = state.writer.take() {
                files.extend(writer.close(&context).await?);
                state.writer_opened_at = None;
            }
            if files.is_empty() {
                // An empty window publishes no snapshot.
                continue;
            }
            // Replay detection against FRESH metadata: a replayed
            // identity discards this window's files — orphaned and
            // invisible, no snapshot names them — and publishes
            // nothing for this table.
            let fresh = self
                .catalog
                .load_table(state.table.identifier())
                .await
                .map_err(|e| classify(&context, e))?;
            if identity.already_committed(&fresh) {
                state.table = fresh;
            } else {
                state.table = append_commit(&self.catalog, fresh, files, &identity).await?;
            }
            // The refresh may carry a concurrent writer's additive
            // evolution: realign so the next window agrees with it.
            state.arrow_target = arrow_target(&context, &state.table)?;
        }
        crash_point!(
            "ice.receipt.visible",
            Err(DestinationError::fatal(
                "injected crash at ice.receipt.visible"
            ))
        );
        // State LAST, after every table's data commit: the per-table
        // snapshot receipts make replays converge even when the crash
        // lands before this write.
        let state_json =
            serde_json::to_string(&meta.state).map_err(|e| fatal(format!("state doc: {e}")))?;
        write_state(&self.catalog, &self.namespace, &self.scope, state_json).await?;
        Ok(receipt)
    }

    async fn read_state(
        &mut self,
        pipeline: &PipelineId,
    ) -> Result<Option<StateDoc>, DestinationError> {
        let scope = ident_hash(pipeline.as_str(), SCOPE_HASH_LEN);
        let Some(raw) = read_state_doc(&self.catalog, &self.namespace, &scope).await? else {
            return Ok(None);
        };
        let state: StateDoc =
            serde_json::from_str(&raw).map_err(|e| fatal(format!("state doc parse: {e}")))?;
        // The scope is a truncated hash; the pipeline filter turns an
        // astronomically unlikely collision into a clean first-run.
        Ok(Some(state).filter(|s| &s.pipeline == pipeline))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow_array::{Int64Array, RecordBatch, StringArray};
    use arrow_schema::{DataType, Field, Schema};

    use super::*;

    fn target(fields: Vec<Field>) -> Arc<Schema> {
        Arc::new(Schema::new(fields))
    }

    /// A nullable table column the batch lacks is null-filled — schema
    /// narrowing and concurrent additive evolution both land here.
    #[test]
    fn align_null_fills_a_missing_nullable_column() {
        let target = target(vec![
            Field::new("seq", DataType::Int64, false),
            Field::new("label", DataType::Utf8, true),
        ]);
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("seq", DataType::Int64, false)])),
            vec![Arc::new(Int64Array::from(vec![7, 8, 9]))],
        )
        .expect("batch");
        let aligned = Load::align("table `t`", &target, &batch).expect("aligns");
        assert_eq!(aligned.num_columns(), 2);
        assert_eq!(aligned.column(1).null_count(), 3);
    }

    /// A REQUIRED column the stream stopped providing is typed and
    /// attributed to the TABLE.
    #[test]
    fn align_types_a_missing_required_column_naming_the_table() {
        let target = target(vec![
            Field::new("seq", DataType::Int64, false),
            Field::new("mandatory", DataType::Utf8, false),
        ]);
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("seq", DataType::Int64, false)])),
            vec![Arc::new(Int64Array::from(vec![7]))],
        )
        .expect("batch");
        let err = Load::align("table `t`", &target, &batch).expect_err("typed");
        let text = format!("{err}");
        assert!(
            text.contains("live table requires column `mandatory`")
                && text.contains("stream no longer provides"),
            "{text}"
        );
    }

    /// Present columns cast where representations differ.
    #[test]
    fn align_casts_present_columns() {
        let target = target(vec![Field::new("label", DataType::LargeUtf8, true)]);
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("label", DataType::Utf8, true)])),
            vec![Arc::new(StringArray::from(vec!["wide"]))],
        )
        .expect("batch");
        let aligned = Load::align("table `t`", &target, &batch).expect("casts");
        assert_eq!(aligned.column(0).data_type(), &DataType::LargeUtf8);
    }

    /// Merge and Replace refuse with the frozen spellings; Append
    /// passes.
    #[test]
    fn merge_and_replace_refuse_with_the_frozen_spellings() {
        assert!(Load::check_mode(&WriteMode::Append).is_ok());
        let err = Load::check_mode(&WriteMode::Merge { key: vec![] }).expect_err("merge");
        assert!(
            format!("{err}").contains(
                "iceberg destination does not support Merge (capabilities.merge = false)"
            ),
            "{err}"
        );
        let err = Load::check_mode(&WriteMode::Replace).expect_err("replace");
        let text = format!("{err}");
        assert!(
            text.contains("Replace is not supported")
                && text.contains("no overwrite transaction")
                && text.contains("use Append, or a SQL destination for replace semantics"),
            "{text}"
        );
    }

    /// One CAS conflict against the schema commit must not fail the
    /// load: reconcile rides the same bounded retry as every other
    /// commit kind, and the attempt count proves it retried rather
    /// than resigned.
    #[tokio::test]
    async fn reconcile_retries_schema_commit_conflicts() {
        use std::sync::atomic::Ordering;

        use iceberg::spec::{NestedField, PrimitiveType, Type};

        use super::super::commit::COMMIT_ATTEMPTS;
        use super::super::testsupport::ConflictCatalog;

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
        let ident = TableIdent::new(iceberg::NamespaceIdent::new("ns".into()), "events".into());
        reconcile(&arc, &ident, &wanted, &[])
            .await
            .expect("lands within the bound");
        assert_eq!(catalog.commits.load(Ordering::SeqCst), COMMIT_ATTEMPTS);
    }

    /// Session nonces never repeat within a process.
    #[test]
    fn session_nonces_are_unique_within_a_process() {
        assert_ne!(session_nonce(), session_nonce());
    }

    /// The contradictory-drift REFUSAL at its enforcement site: a
    /// wanted type conflicting with the live table refuses with the
    /// full frozen wording — deleting the reconcile check would let
    /// align silently arrow-cast the mismatch, with every layer-below
    /// pin still green (the round-2 lesson, one layer up).
    #[tokio::test]
    async fn reconcile_refuses_contradictory_drift_with_the_frozen_wording() {
        use iceberg::spec::{NestedField, PrimitiveType, Type};

        use super::super::testsupport::ConflictCatalog;

        let catalog = ConflictCatalog::failing(0);
        let arc: std::sync::Arc<dyn Catalog> = catalog.clone();
        // The mock table carries `id: long` (required); wanting string
        // is contradictory drift, never applied.
        let wanted = iceberg::spec::Schema::builder()
            .with_fields(vec![std::sync::Arc::new(NestedField::required(
                1,
                "id",
                Type::Primitive(PrimitiveType::String),
            ))])
            .build()
            .expect("schema");
        let ident = TableIdent::new(iceberg::NamespaceIdent::new("ns".into()), "events".into());
        let err = reconcile(&arc, &ident, &wanted, &[])
            .await
            .expect_err("contradictory drift refuses");
        let text = format!("{err}");
        assert!(
            text.contains(
                "column `id`: stream type string conflicts with the table\'s long — \
                           contradictory drift is never applied"
            ) || (text.contains("column `id`")
                && text.contains("conflicts with the table\'s")
                && text.contains("contradictory drift is never applied")),
            "{text}"
        );
        assert_eq!(
            catalog.commits.load(std::sync::atomic::Ordering::SeqCst),
            0,
            "nothing may commit past the refusal"
        );
    }

    /// The retirement choreography OFFLINE (the live mid-window cell
    /// needs containers; a container-less gate must still catch a
    /// regression here): an identical re-ensure keeps the writer, a
    /// changed target retires it and PARKS its closed files, and the
    /// window counter survives both.
    #[tokio::test]
    async fn a_schema_change_retires_the_writer_and_parks_its_files() {
        use rdlt_connector_sdk::config::Document;

        use super::super::testsupport::{ConflictCatalog, table_with_schema};
        use super::super::write::writer_properties;

        let table = {
            use iceberg::spec::{NestedField, PrimitiveType, Schema, Type};
            table_with_schema(
                Schema::builder()
                    .with_fields(vec![std::sync::Arc::new(NestedField::required(
                        1,
                        "id",
                        Type::Primitive(PrimitiveType::Long),
                    ))])
                    .build()
                    .expect("schema"),
            )
        };
        let catalog = ConflictCatalog::failing(0);
        let arc: std::sync::Arc<dyn Catalog> = catalog.clone();
        let config = Config::from_value(serde_json::json!({
            "catalog": {
                "uri": "http://localhost:1/api/catalog",
                "warehouse": "wh",
                "auth": {"bearer": {"token": "t"}},
            },
            "namespace": "ns",
        }))
        .expect("valid");
        let mut load = Load::new(
            config,
            arc,
            iceberg::NamespaceIdent::new("ns".into()),
            &PipelineId::from("p"),
            LoadId::from("l"),
            writer_properties(&Default::default()).expect("props"),
            rdlt_connector_sdk::spi::PartOptions::default(),
        );
        let stream = TableName::from("events");
        let target = arrow_target("table `events`", &table).expect("target");
        load.reinstall(&stream, "events", table.clone(), target.clone())
            .await
            .expect("install");

        // One staged batch opens the window's writer.
        let batch = RecordBatch::try_new(
            target.clone(),
            vec![std::sync::Arc::new(arrow_array::Int64Array::from(vec![
                1, 2,
            ]))],
        )
        .expect("batch");
        load.write(&stream, batch).await.expect("write");
        let seq_before = load.tables[&stream].window_seq;
        assert!(load.tables[&stream].writer.is_some());

        // Identical target: the writer survives the re-ensure. The
        // target is RECOMPUTED, not the same Arc — production always
        // derives a fresh one, so a value-eq-to-pointer-eq regression
        // must fail here rather than silently retiring every window.
        let recomputed = arrow_target("table `events`", &table).expect("target");
        load.reinstall(&stream, "events", table.clone(), recomputed)
            .await
            .expect("re-ensure");
        assert!(
            load.tables[&stream].writer.is_some(),
            "an unchanged schema keeps the in-flight writer"
        );

        // A changed target retires it; the closed files are PARKED for
        // the window's publish, and the counter survives.
        let evolved = std::sync::Arc::new(arrow_schema::Schema::new(vec![
            arrow_schema::Field::new("id", arrow_schema::DataType::Int64, false),
            arrow_schema::Field::new("note", arrow_schema::DataType::Utf8, true),
        ]));
        load.reinstall(&stream, "events", table, evolved)
            .await
            .expect("evolving re-ensure");
        let state = &load.tables[&stream];
        assert!(state.writer.is_none(), "the writer was retired");
        assert!(
            !state.pending_files.is_empty(),
            "the retired writer\'s files joined the window"
        );
        assert_eq!(state.window_seq, seq_before, "the counter survived");
    }

    /// 034: `parts.target_bytes` reaches the LIBRARY's rolling writer
    /// rather than being reimplemented above it, and `None` means the
    /// library never rolls on size.
    ///
    /// The library takes a `usize` with no "unlimited" spelling, so
    /// absence has to become a number no file reaches. Pinning it
    /// here because the conversion is the only place that decision
    /// lives, and a silent truncation of a `u64` target would shrink
    /// files rather than fail.
    #[test]
    fn the_target_size_reaches_the_library_and_absence_never_rolls() {
        use rdlt_connector_sdk::spi::PartOptions;

        // The shipping default: 128 MiB, NOT the library's own 512.
        assert_eq!(PartOptions::default().target_file_size(), 128 * 1024 * 1024);
        assert_eq!(PartOptions::unbounded().target_file_size(), usize::MAX);
        assert_eq!(
            PartOptions {
                target_bytes: Some(64 * 1024 * 1024),
                ..PartOptions::default()
            }
            .target_file_size(),
            64 * 1024 * 1024
        );
    }

    /// 034: the TIME threshold is rdlt's to apply — the library rolls
    /// on size only. Pinned as its own predicate because asking
    /// `should_roll` here would answer the size half twice, once from
    /// each side, and the two would disagree.
    #[test]
    fn the_time_threshold_is_answered_without_the_size_one() {
        use rdlt_connector_sdk::spi::PartOptions;

        let timed = PartOptions {
            roll_after_seconds: Some(900),
            ..PartOptions::default()
        };
        assert!(!timed.rolls_on_time(899));
        assert!(timed.rolls_on_time(900));
        // The default names no time bound, so no elapsed time rolls.
        assert!(!PartOptions::default().rolls_on_time(u64::MAX));
    }

    /// 034: a writer retired ON TIME parks its files exactly as a
    /// schema change does, and the next write opens a fresh one.
    #[tokio::test]
    async fn an_elapsed_writer_is_retired_and_its_files_parked() {
        use rdlt_connector_sdk::config::Document;
        use rdlt_connector_sdk::spi::PartOptions;

        use super::super::testsupport::{ConflictCatalog, table_with_schema};
        use super::super::write::writer_properties;

        let table = {
            use iceberg::spec::{NestedField, PrimitiveType, Schema, Type};
            table_with_schema(
                Schema::builder()
                    .with_fields(vec![std::sync::Arc::new(NestedField::required(
                        1,
                        "id",
                        Type::Primitive(PrimitiveType::Long),
                    ))])
                    .build()
                    .expect("schema"),
            )
        };
        let catalog = ConflictCatalog::failing(0);
        let arc: std::sync::Arc<dyn Catalog> = catalog.clone();
        let config = Config::from_value(serde_json::json!({
            "catalog": {
                "uri": "http://localhost:1/api/catalog",
                "warehouse": "wh",
                "auth": {"bearer": {"token": "t"}},
            },
            "namespace": "ns",
        }))
        .expect("valid");
        let mut load = Load::new(
            config,
            arc,
            iceberg::NamespaceIdent::new("ns".into()),
            &PipelineId::from("p"),
            LoadId::from("load-a"),
            writer_properties(&Default::default()).expect("props"),
            // Zero seconds is not configurable — the gate refuses it —
            // but constructed directly it makes "already elapsed" the
            // condition under test without sleeping.
            PartOptions {
                roll_after_seconds: Some(0),
                ..PartOptions::default()
            },
        );
        let stream = TableName::from("events");
        // The FIELD-ID-annotated target, as production derives it — a
        // bare arrow schema writes batches the library cannot place.
        let target = arrow_target("table `events`", &table).expect("target");
        load.reinstall(&stream, "events", table, target.clone())
            .await
            .expect("ensure");

        let batch = RecordBatch::try_new(
            target.clone(),
            vec![std::sync::Arc::new(arrow_array::Int64Array::from(vec![
                1_i64,
            ]))],
        )
        .expect("batch");
        load.write(&stream, batch.clone()).await.expect("write");
        let seq_after_first = load.tables[&stream].window_seq;
        assert!(load.tables[&stream].pending_files.is_empty());

        // The second write finds the first writer already elapsed.
        load.write(&stream, batch).await.expect("write");
        let state = &load.tables[&stream];
        assert!(
            !state.pending_files.is_empty(),
            "the elapsed writer's files were parked for the window"
        );
        assert!(state.writer.is_some(), "a fresh writer took over");
        assert_eq!(
            state.window_seq,
            seq_after_first + 1,
            "the new writer got its own window prefix — reusing one \
             would overwrite the retired writer's files"
        );
    }
}
