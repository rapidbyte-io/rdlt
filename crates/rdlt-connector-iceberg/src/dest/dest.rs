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
    CommitMeta, CommitReceipt, ConnectorSpec, DestCapabilities, DestError, Destination,
    LoadSession, OpenCtx, RecordBatch,
};

use super::commit::{
    CommitIdentity, TableWriter, append_commit, connect, ensure_namespace, ensure_table,
    read_state as read_state_prop, write_state,
};
use super::config::{IcebergConfig, config_schema};
use super::schema::to_iceberg_schema;

fn fatal(message: impl std::fmt::Display) -> DestError {
    DestError::fatal(message.to_string())
}

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
    pub fn from_config(config: IcebergConfig) -> Result<Self, DestError> {
        config.validate().map_err(fatal)?;
        Ok(Self { config })
    }

    pub fn from_yaml(yaml: &str) -> Result<Self, DestError> {
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

    fn capabilities(&self) -> DestCapabilities {
        DestCapabilities {
            merge: false, // append-only lakehouse tables; merge stays SQL-side
            structs: true,
            scalar_lists: true,
            json_type: false, // Json → string (documented closed-table row)
            decimal: true,
            ident_rules: IdentRules::default(),
        }
    }

    async fn open(&self, ctx: OpenCtx) -> Result<Box<dyn LoadSession>, DestError> {
        let catalog = connect(&self.config).await?;
        let namespace = NamespaceIdent::from_vec(self.config.namespace_levels())
            .map_err(|e| fatal(format!("namespace `{}`: {e}", self.config.namespace)))?;
        ensure_namespace(&catalog, &namespace, self.config.create_namespace).await?;
        Ok(Box::new(IcebergSession {
            config: self.config.clone(),
            catalog,
            namespace,
            scope: ident_hash(ctx.pipeline.as_str(), SCOPE_HASH_LEN),
            load_id: ctx.load_id,
            nonce: session_nonce(),
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
    windows: u64,
}

struct IcebergSession {
    config: IcebergConfig,
    catalog: Arc<dyn Catalog>,
    namespace: NamespaceIdent,
    scope: String,
    load_id: LoadId,
    /// Unique per session — see [`TableWriter::open`]'s nonce contract.
    nonce: String,
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

impl IcebergSession {
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
    ) -> Result<RecordBatch, DestError> {
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
    ) -> Result<(), DestError> {
        match mode {
            WriteMode::Append => {}
            WriteMode::Merge { .. } => {
                return Err(fatal(
                    "iceberg destination does not support Merge (capabilities.merge = false)",
                ));
            }
            // Replace needs an overwrite transaction the underlying iceberg
            // library does not expose. Rejecting is the only correct answer:
            // emulating it (delete + append) would not be atomic.
            WriteMode::Replace => {
                return Err(fatal(
                    "iceberg destination: Replace is not supported — the underlying \
                     iceberg library exposes no overwrite transaction, which Replace \
                     requires; use Append, or a SQL destination for replace semantics",
                ));
            }
        }
        let stream = schema.table.as_str();
        let wanted = to_iceberg_schema(schema)?;
        let name = self.config.table_name(stream);
        if name == super::commit::STATE_TABLE {
            return Err(fatal(format!(
                "table name `{name}` is reserved for the rdlt state marker table"
            )));
        }
        let partition = self.config.partition_fields(stream);
        let table = ensure_table(&self.catalog, &self.namespace, &name, &wanted, partition).await?;
        let arrow_target = Arc::new(
            iceberg::arrow::schema_to_arrow_schema(table.metadata().current_schema())
                .map_err(|e| fatal(format!("table `{name}`: arrow schema conversion: {e}")))?,
        );
        // The engine may re-ensure mid-session (e.g. after a WAL replay).
        // The window counter MUST survive: resetting it regenerates window
        // 1's exact file path (same load, window, nonce), overwriting a
        // committed data file. A staged writer survives ONLY while the write
        // schema is
        // unchanged: a re-ensure carrying drift retires it — its closed
        // files (valid under the prior schema; Iceberg reads absent
        // columns as null after additive evolution) join the window's
        // commit via pending_files — so the next writer opens against
        // the evolved table.
        let (windows, prev_writer, prev_target, mut pending_files) =
            match self.tables.remove(&schema.table) {
                Some(prev) => (
                    prev.windows,
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
            schema.table.clone(),
            TableState {
                table,
                arrow_target,
                writer,
                pending_files,
                windows,
            },
        );
        Ok(())
    }

    async fn write(&mut self, table: &TableName, batch: RecordBatch) -> Result<(), DestError> {
        let state = self
            .tables
            .get_mut(table)
            .ok_or_else(|| fatal(format!("write before ensure_table for `{table}`")))?;
        let context = format!("table `{}`", self.config.table_name(table.as_str()));
        let aligned = Self::align(&context, &state.arrow_target, &batch)?;
        if state.writer.is_none() {
            state.windows += 1;
            let prefix = format!("{}-{}", self.load_id, state.windows);
            state.writer = Some(TableWriter::open(&state.table, &prefix, &self.nonce).await?);
        }
        state
            .writer
            .as_mut()
            .expect("writer just ensured")
            .write(&context, aligned)
            .await
    }

    async fn commit(&mut self, meta: CommitMeta) -> Result<CommitReceipt, DestError> {
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
                .map_err(|e| super::errors::classify(&context, e))?;
            if identity.already_committed(&fresh) {
                state.table = fresh;
            } else {
                state.table = append_commit(&self.catalog, fresh, files, &identity).await?;
            }
            // The refresh may carry a CONCURRENT writer's schema evolution:
            // realign the target so the next window's writer and batches
            // agree with the evolved table (nullable additions null-fill).
            state.arrow_target = Arc::new(
                iceberg::arrow::schema_to_arrow_schema(state.table.metadata().current_schema())
                    .map_err(|e| fatal(format!("{context}: arrow schema conversion: {e}")))?,
            );
        }
        crash_point!(
            "ice.receipt.visible",
            Err(DestError::fatal("injected crash at ice.receipt.visible"))
        );
        // State is written LAST, after every table's data commit: the
        // per-table snapshot receipts make replays converge even if we crash
        // before this state write lands.
        let state_json =
            serde_json::to_string(&meta.state).map_err(|e| fatal(format!("state doc: {e}")))?;
        write_state(&self.catalog, &self.namespace, &self.scope, state_json).await?;
        Ok(receipt)
    }

    async fn read_state(&mut self, pipeline: &PipelineId) -> Result<Option<StateDoc>, DestError> {
        let scope = ident_hash(pipeline.as_str(), SCOPE_HASH_LEN);
        let Some(raw) = read_state_prop(&self.catalog, &self.namespace, &scope).await? else {
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
