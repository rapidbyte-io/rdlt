//! The Destination/LoadSession implementation (contracts ID1–ID3, ID5):
//! Append write mode mapped onto fast-append snapshots; Replace is typed
//! unsupported (the RECORDED T001 verdict — iceberg-rust 0.10 has no
//! overwrite action; ID5: no silent degradation, no emulation). Merge is
//! rejected by capability.

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

/// Fail-point registry (gate G2.2): every `crash_point!` site in this
/// crate, swept live against the catalog fixture (ID7).
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
            scope: ident_hash(ctx.pipeline.as_str(), 12),
            load_id: ctx.load_id,
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
    /// Sequence for unique data-file name prefixes.
    windows: u64,
}

struct IcebergSession {
    config: IcebergConfig,
    catalog: Arc<dyn Catalog>,
    namespace: NamespaceIdent,
    scope: String,
    load_id: LoadId,
    tables: BTreeMap<TableName, TableState>,
}

impl IcebergSession {
    /// Align an engine batch to the table's field-id-annotated arrow
    /// schema: columns matched BY NAME, cast where representations differ
    /// (e.g. timestamp units); a missing column is typed.
    fn align(
        context: &str,
        target: &Arc<arrow_schema::Schema>,
        batch: &RecordBatch,
    ) -> Result<RecordBatch, DestError> {
        let mut columns = Vec::with_capacity(target.fields().len());
        for field in target.fields() {
            let index = batch.schema().index_of(field.name()).map_err(|_| {
                fatal(format!(
                    "{context}: batch is missing column `{}` declared by the stream schema",
                    field.name()
                ))
            })?;
            let column = batch.column(index);
            let column = if column.data_type() == field.data_type() {
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
            // The RECORDED T001 narrowing (ID5): iceberg-rust 0.10 exposes
            // no overwrite transaction — no emulation, a typed error.
            WriteMode::Replace => {
                return Err(fatal(
                    "iceberg destination: Replace is not supported by this release \
                     (iceberg-rust 0.10 has no overwrite transaction; recorded \
                     narrowing — use Append, or a SQL destination for replace \
                     semantics)",
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
        let table = ensure_table(&self.catalog, &self.namespace, &name, &wanted).await?;
        let arrow_target = Arc::new(
            iceberg::arrow::schema_to_arrow_schema(table.metadata().current_schema())
                .map_err(|e| fatal(format!("table `{name}`: arrow schema conversion: {e}")))?,
        );
        self.tables.insert(
            schema.table.clone(),
            TableState {
                table,
                arrow_target,
                writer: None,
                windows: 0,
            },
        );
        Ok(())
    }

    async fn write(&mut self, table: &TableName, batch: RecordBatch) -> Result<(), DestError> {
        let state = self.tables.get_mut(table).ok_or_else(|| {
            fatal(format!(
                "write before ensure_table for `{table}` (clause E1)"
            ))
        })?;
        let context = format!("table `{}`", self.config.table_name(table.as_str()));
        let aligned = Self::align(&context, &state.arrow_target, &batch)?;
        if state.writer.is_none() {
            state.windows += 1;
            let prefix = format!("{}-{}", self.load_id, state.windows);
            state.writer = Some(TableWriter::open(&state.table, &prefix).await?);
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
            let files = match state.writer.take() {
                Some(writer) => writer.close(&context).await?,
                None => Vec::new(),
            };
            if files.is_empty() {
                continue; // empty window: no snapshot (ID2)
            }
            // Replay detection against FRESH metadata (ID2): a replayed
            // identity discards this window's files (orphaned, invisible —
            // no snapshot references them) and publishes nothing.
            let fresh = self
                .catalog
                .load_table(state.table.identifier())
                .await
                .map_err(|e| super::errors::classify(&context, e))?;
            if identity.already_committed(&fresh) {
                state.table = fresh;
                continue;
            }
            state.table = append_commit(&self.catalog, fresh, files, &identity).await?;
        }
        crash_point!(
            "ice.receipt.visible",
            Err(DestError::fatal("injected crash at ice.receipt.visible"))
        );
        // State LAST (the parquet-dest ordering): per-table receipts make
        // replays converge even if we crash before this write lands.
        let state_json =
            serde_json::to_string(&meta.state).map_err(|e| fatal(format!("state doc: {e}")))?;
        write_state(&self.catalog, &self.namespace, &self.scope, state_json).await?;
        Ok(receipt)
    }

    async fn read_state(&mut self, pipeline: &PipelineId) -> Result<Option<StateDoc>, DestError> {
        let scope = ident_hash(pipeline.as_str(), 12);
        let Some(raw) = read_state_prop(&self.catalog, &self.namespace, &scope).await? else {
            return Ok(None);
        };
        let state: StateDoc =
            serde_json::from_str(&raw).map_err(|e| fatal(format!("state doc parse: {e}")))?;
        Ok(Some(state).filter(|s| &s.pipeline == pipeline))
    }
}
