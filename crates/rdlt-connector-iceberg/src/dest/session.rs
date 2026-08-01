//! The Destination/LoadSession implementation: Append write mode mapped onto
//! fast-append snapshots; Replace is typed unsupported because the underlying
//! iceberg library exposes no overwrite action, and there is no silent
//! degradation or emulation. Merge is rejected by capability.

use std::collections::BTreeMap;
use std::sync::Arc;

use async_trait::async_trait;
use iceberg::{Catalog, NamespaceIdent};
use rdlt_connector::core::crash_point;
use rdlt_connector::core::{
    LoadId, PipelineId, StateDoc, TableName, TableSchema, WriteMode,
    naming::{IdentRules, ident_hash},
};
use rdlt_connector::{
    CommitMeta, CommitReceipt, ConnectorSpec, Destination, DestinationCapabilities,
    DestinationError, LoadSession, OpenContext, RecordBatch,
};

use super::catalog::connect;
use super::commit::{CommitIdentity, append_commit};
use super::config::{IcebergConfig, config_schema};
use super::ensure::{ensure_namespace, ensure_table};
use super::errors::{classify, fatal};
use super::schema::to_iceberg_schema;
use super::state::{STATE_TABLE, read_state_doc, write_state};
use super::writer::TableWriter;

/// Width of the pipeline scope hash. The scope names the pipeline in
/// snapshot summaries and the state-doc property key, so the width a
/// session opens with MUST equal the width `read_state` re-derives with —
/// one constant on both sides keeps a resume looking under the same scope.
const SCOPE_HASH_LEN: usize = 12;

/// Fail-point registry: every `crash_point!` site in this crate, swept live
/// against the catalog fixture by the crash-sweep tests.
#[cfg(feature = "failpoints")]
#[doc(hidden)]
pub const ICE_FAIL_POINTS: &[&str] = &["ice.files.write", "ice.commit", "ice.receipt.visible"];

#[derive(Debug, Clone)]
pub struct IcebergDest {
    config: IcebergConfig,
}

impl IcebergDest {
    pub fn from_config(config: IcebergConfig) -> Result<Self, DestinationError> {
        config.validate().map_err(fatal)?;
        Ok(Self { config })
    }

    pub fn from_yaml(yaml: &str) -> Result<Self, DestinationError> {
        let config = IcebergConfig::from_yaml(yaml).map_err(fatal)?;
        Ok(Self { config })
    }
}

#[async_trait]
impl Destination for IcebergDest {
    fn spec(&self) -> ConnectorSpec {
        let mut spec = ConnectorSpec::new("iceberg", env!("CARGO_PKG_VERSION"));
        spec.config_schema = Some(config_schema());
        spec
    }

    fn capabilities(&self) -> DestinationCapabilities {
        DestinationCapabilities::default()
            .with_merge(false)
            // append-only lakehouse tables; merge stays SQL-side
            .with_structs(true)
            .with_scalar_lists(true)
            .with_json_type(false)
            // Json → string (documented closed-table row)
            .with_decimal(true)
            .with_ident_rules(IdentRules::default())
    }

    async fn open(&self, ctx: OpenContext) -> Result<Box<dyn LoadSession>, DestinationError> {
        let catalog = connect(&self.config).await?;
        let namespace = NamespaceIdent::from_vec(self.config.namespace_levels())
            .map_err(|e| fatal(format!("namespace `{}`: {e}", self.config.namespace)))?;
        ensure_namespace(&catalog, &namespace, self.config.create_namespace).await?;
        let writer_properties = super::writer_props::writer_properties(
            &self.config.parquet.clone().unwrap_or_default(),
        )
        .map_err(fatal)?;
        Ok(Box::new(IcebergSession {
            config: self.config.clone(),
            catalog,
            namespace,
            scope: ident_hash(ctx.pipeline.as_str(), SCOPE_HASH_LEN),
            load_id: ctx.load_id,
            nonce: session_nonce(),
            writer_properties,
            tables: BTreeMap::new(),
        }))
    }
}

struct TableState {
    /// Live table handle (refreshed at ensure/commit boundaries).
    table: iceberg::table::Table,
    /// Field-id-annotated arrow schema batches are aligned to.
    arrow_target: Arc<arrow_schema::Schema>,
    /// The writer for the current commit window (opened on first write).
    writer: Option<TableWriter>,
    /// Data files closed early (a mid-window schema change retires the
    /// writer — see ensure_table); committed with the window's files.
    pending_files: Vec<iceberg::spec::DataFile>,
    /// Sequence for unique data-file name prefixes.
    window_seq: u64,
}

struct IcebergSession {
    config: IcebergConfig,
    catalog: Arc<dyn Catalog>,
    namespace: NamespaceIdent,
    scope: String,
    load_id: LoadId,
    /// Unique per session — see [`TableWriter::open`]'s nonce contract.
    nonce: String,
    /// Resolved once at session open, reused for every data file: the
    /// translation can fail, and a load should not discover that partway
    /// through writing.
    writer_properties: parquet::file::properties::WriterProperties,
    tables: BTreeMap<TableName, TableState>,
}

/// A recovery session replaying (load, window) must never reuse a prior
/// session's data-file names: wall clock + a process-wide counter.
fn session_nonce() -> String {
    use std::sync::atomic::{AtomicU64, Ordering};
    static SEQ: AtomicU64 = AtomicU64::new(0);
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0);
    format!("{:x}-{}", nanos, SEQ.fetch_add(1, Ordering::Relaxed))
}

/// The table's field-id-annotated arrow schema, wrapped for reuse. Batches
/// are aligned to this; a concurrent writer's additive evolution changes it,
/// so it is recomputed at ensure/commit boundaries (one helper, both sites).
fn arrow_target(
    context: &str,
    table: &iceberg::table::Table,
) -> Result<Arc<arrow_schema::Schema>, DestinationError> {
    iceberg::arrow::schema_to_arrow_schema(table.metadata().current_schema())
        .map(Arc::new)
        .map_err(|e| fatal(format!("{context}: arrow schema conversion: {e}")))
}

impl IcebergSession {
    /// Only Append maps onto a snapshot this release; Merge and Replace are
    /// typed unsupported (see the module and `ensure_table` docs).
    fn check_mode(mode: &WriteMode) -> Result<(), DestinationError> {
        match mode {
            WriteMode::Append => Ok(()),
            WriteMode::Merge { .. } => Err(fatal(
                "iceberg destination does not support Merge (capabilities.merge = false)",
            )),
            // Replace needs an overwrite transaction the underlying iceberg
            // library does not expose. Rejecting is the only correct answer:
            // emulating it (delete + append) would not be atomic.
            WriteMode::Replace => Err(fatal(
                "iceberg destination: Replace is not supported — the underlying \
                 iceberg library exposes no overwrite transaction, which Replace \
                 requires; use Append, or a SQL destination for replace semantics",
            )),
        }
    }

    /// Stage the freshly ensured table into the session, carrying the window
    /// counter and any in-flight writer across a re-ensure.
    ///
    /// The window counter MUST survive: resetting it regenerates window 1's
    /// exact file path (same load, window, nonce), overwriting a committed
    /// data file. A staged writer survives ONLY while the write schema is
    /// unchanged: a re-ensure carrying drift retires it — its closed files
    /// (valid under the prior schema; Iceberg reads absent columns as null
    /// after additive evolution) join the window's commit via pending_files,
    /// so the next writer opens against the evolved table.
    async fn reinstall_state(
        &mut self,
        stream: &TableName,
        name: &str,
        table: iceberg::table::Table,
        arrow_target: Arc<arrow_schema::Schema>,
    ) -> Result<(), DestinationError> {
        let (window_seq, prev_writer, prev_target, mut pending_files) =
            match self.tables.remove(stream) {
                Some(prev) => (
                    prev.window_seq,
                    prev.writer,
                    Some(prev.arrow_target),
                    prev.pending_files,
                ),
                None => (0, None, None, Vec::new()),
            };
        let writer = match prev_writer {
            Some(writer) if prev_target.as_deref() != Some(arrow_target.as_ref()) => {
                let context = format!("table `{name}` (schema-change writer retirement)");
                pending_files.extend(writer.close(&context).await?);
                None
            }
            other => other,
        };
        self.tables.insert(
            stream.clone(),
            TableState {
                table,
                arrow_target,
                writer,
                pending_files,
                window_seq,
            },
        );
        Ok(())
    }

    /// Align an engine batch to the table's field-id-annotated arrow
    /// schema: columns matched BY NAME, cast where representations differ
    /// (e.g. timestamp units). A table column the batch lacks is
    /// NULL-FILLED when nullable (schema narrowing / concurrent additive
    /// evolution — the SQL destinations' tolerance) and typed when
    /// required, attributed to the TABLE, not the stream.
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
                        "{context}: the live table requires column `{}` but the \
                         stream no longer provides it",
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

#[async_trait]
impl LoadSession for IcebergSession {
    async fn ensure_table(
        &mut self,
        schema: &TableSchema,
        mode: &WriteMode,
    ) -> Result<(), DestinationError> {
        Self::check_mode(mode)?;
        let stream = schema.table.as_str();
        let name = self.config.table_name(stream);
        // The reserved-name check is cheap and infallible — do it BEFORE the
        // fallible schema mapping so a misconfigured name fails on its own
        // terms, not behind a type-mapping error.
        if name == STATE_TABLE {
            return Err(fatal(format!(
                "table name `{name}` is reserved for the rdlt state marker table"
            )));
        }
        let wanted = to_iceberg_schema(schema)?;
        let partition = self.config.partition_fields(stream);
        let table = ensure_table(&self.catalog, &self.namespace, &name, &wanted, partition).await?;
        let arrow_target = arrow_target(&format!("table `{name}`"), &table)?;
        self.reinstall_state(&schema.table, &name, table, arrow_target)
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
        if state.writer.is_none() {
            state.window_seq += 1;
            let prefix = format!("{}-{}", self.load_id, state.window_seq);
            state.writer = Some(
                TableWriter::open(
                    &state.table,
                    &prefix,
                    &self.nonce,
                    self.writer_properties.clone(),
                )
                .await?,
            );
        }
        state
            .writer
            .as_mut()
            .expect("writer just ensured")
            .write(&context, aligned)
            .await
    }

    async fn commit(&mut self, meta: CommitMeta) -> Result<CommitReceipt, DestinationError> {
        let receipt = CommitReceipt {
            load_id: meta.load_id.clone(),
            commit_seq: meta.commit_seq,
        };
        let identity = CommitIdentity {
            scope: self.scope.clone(),
            load_id: meta.load_id.as_str().to_owned(),
            commit_seq: meta.commit_seq,
        };
        for (table_name, state) in self.tables.iter_mut() {
            let context = format!("table `{}`", self.config.table_name(table_name.as_str()));
            let mut files = std::mem::take(&mut state.pending_files);
            if let Some(writer) = state.writer.take() {
                files.extend(writer.close(&context).await?);
            }
            if files.is_empty() {
                continue; // empty window: no snapshot
            }
            // Replay detection against FRESH metadata: a replayed
            // identity discards this window's files (orphaned, invisible —
            // no snapshot references them) and publishes nothing.
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
            // The refresh may carry a CONCURRENT writer's schema evolution:
            // realign the target so the next window's writer and batches
            // agree with the evolved table (nullable additions null-fill).
            state.arrow_target = arrow_target(&context, &state.table)?;
        }
        crash_point!(
            "ice.receipt.visible",
            Err(DestinationError::fatal(
                "injected crash at ice.receipt.visible"
            ))
        );
        // State is written LAST, after every table's data commit: the
        // per-table snapshot receipts make replays converge even if we crash
        // before this state write lands.
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
        Ok(Some(state).filter(|s| &s.pipeline == pipeline))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow_array::{Int64Array, RecordBatch, StringArray};
    use arrow_schema::{DataType, Field, Schema};

    use super::IcebergSession;

    fn target(fields: Vec<Field>) -> Arc<Schema> {
        Arc::new(Schema::new(fields))
    }

    /// A nullable table column absent from the batch is null-filled (schema
    /// narrowing / concurrent additive evolution).
    #[test]
    fn align_null_fills_missing_nullable_column() {
        let target = target(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("email", DataType::Utf8, true),
        ]);
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)])),
            vec![Arc::new(Int64Array::from(vec![1, 2]))],
        )
        .expect("batch");
        let aligned = IcebergSession::align("table `t`", &target, &batch).expect("aligns");
        assert_eq!(aligned.num_columns(), 2);
        assert_eq!(aligned.column(1).null_count(), 2, "null-filled");
    }

    /// A REQUIRED table column the stream stopped providing is typed and
    /// attributed to the TABLE, not the stream.
    #[test]
    fn align_missing_required_column_is_typed_naming_the_table() {
        let target = target(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("must", DataType::Utf8, false),
        ]);
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)])),
            vec![Arc::new(Int64Array::from(vec![1]))],
        )
        .expect("batch");
        let err = IcebergSession::align("table `t`", &target, &batch).expect_err("typed");
        let text = format!("{err}");
        assert!(
            text.contains("live table requires column `must`")
                && text.contains("stream no longer provides"),
            "{text}"
        );
    }

    /// Casting still applies for present columns.
    #[test]
    fn align_casts_present_columns() {
        let target = target(vec![Field::new("name", DataType::LargeUtf8, true)]);
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("name", DataType::Utf8, true)])),
            vec![Arc::new(StringArray::from(vec!["a"]))],
        )
        .expect("batch");
        let aligned = IcebergSession::align("table `t`", &target, &batch).expect("casts");
        assert_eq!(aligned.column(0).data_type(), &DataType::LargeUtf8);
    }
}
