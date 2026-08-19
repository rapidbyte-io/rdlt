//! One session's `Backend` choreography: staging in memory, publish
//! through the store. Staging is in-memory on purpose — a crashed
//! session's staging simply vanishes, which is the open contract by
//! construction — and clears only after a commit's receipt is durable,
//! so a mid-publish failure (transient by classification) leaves it
//! intact for a client retrying the SAME commit without re-writing.

use std::collections::BTreeMap;
use std::path::PathBuf;

use async_trait::async_trait;
use rdlt_connector_sdk::destination::Backend;
use rdlt_connector_sdk::spi::arrow::RecordBatch;
use rdlt_connector_sdk::spi::channel::arrow_batch_footprint;
use rdlt_connector_sdk::spi::core::commit::{CommitMeta, CommitReceipt, WriteMode};
use rdlt_connector_sdk::spi::core::id::{LoadId, PipelineId, TableName};
use rdlt_connector_sdk::spi::core::schema::TableSchema;
use rdlt_connector_sdk::spi::core::state::StateDoc;
use rdlt_connector_sdk::spi::error::DestinationError;

use super::{part, store};

/// A reference connector must model a bounded staging posture. Four
/// maximum-size wire frames leave ample room for ordinary commits while
/// preventing a client from retaining an unbounded session in memory.
const STAGING_CEILING_BYTES: usize = 256 << 20;

/// One session over the output directory: staged batches in memory,
/// published files, receipts and state on disk. Holds the session
/// lease; `close` and drop both release it.
#[derive(Debug)]
pub struct Session {
    dir: PathBuf,
    load_id: LoadId,
    staged: Vec<(TableName, RecordBatch)>,
    staged_bytes: usize,
    lease: Option<std::fs::File>,
}

impl Session {
    /// A fresh session over `dir` for `load_id`, holding `lease` until
    /// close or drop.
    pub(crate) fn new(dir: PathBuf, load_id: LoadId, lease: std::fs::File) -> Self {
        Self {
            dir,
            load_id,
            staged: Vec::new(),
            staged_bytes: 0,
            lease: Some(lease),
        }
    }

    /// Retain `batch` for the next publish, refusing typed rather than
    /// letting a session grow past its ceiling.
    fn stage(&mut self, table: &TableName, batch: RecordBatch) -> Result<(), DestinationError> {
        let next = self
            .staged_bytes
            .checked_add(arrow_batch_footprint(&batch))
            .ok_or_else(|| {
                DestinationError::fatal(
                    "reference destination: staged Arrow footprint overflowed usize".to_string(),
                )
            })?;
        if next > STAGING_CEILING_BYTES {
            return Err(DestinationError::fatal(format!(
                "reference destination: staged Arrow data would exceed the \
                 {STAGING_CEILING_BYTES}-byte session ceiling"
            )));
        }
        self.staged_bytes = next;
        self.staged.push((table.clone(), batch));
        Ok(())
    }

    /// Retire everything staged, footprint included.
    fn clear_staging(&mut self) {
        self.staged.clear();
        self.staged_bytes = 0;
    }
}

#[async_trait]
impl Backend for Session {
    async fn ensure_table(
        &mut self,
        schema: &TableSchema,
        mode: &WriteMode,
    ) -> Result<(), DestinationError> {
        match mode {
            // A jsonl part carries its own column names on every row —
            // there is no DDL to run, and re-ensuring is trivially
            // idempotent. Append is the ONE disposition this
            // destination performs.
            WriteMode::Append => Ok(()),
            // Everything else — Replace, Merge, and any future
            // disposition — is typed-unsupported, never silent.
            // Accepting Replace would append where the pipeline asked
            // for a table's contents to be replaced; accepting Merge
            // would append where the pipeline asked for upsert-by-key,
            // duplicating every redelivery — each quietly, forever. The
            // engine's validate gate refuses Merge against the declared
            // `merge = false` capability, but a host driving this
            // backend directly never passes that gate, so the refusal
            // lives here too.
            other => {
                let mode_name = match other {
                    WriteMode::Replace => "replace".to_owned(),
                    WriteMode::Merge { .. } => "merge".to_owned(),
                    other => format!("{other:?}"),
                };
                Err(DestinationError::fatal(format!(
                    "reference destination: table `{}`: write mode `{mode_name}` is not \
                     supported — jsonl parts are append-only",
                    schema.table
                )))
            }
        }
    }

    async fn write(
        &mut self,
        table: &TableName,
        batch: RecordBatch,
    ) -> Result<(), DestinationError> {
        self.stage(table, batch)
    }

    async fn existing_receipt(
        &mut self,
        load_id: &LoadId,
        commit_seq: u64,
    ) -> Result<Option<CommitReceipt>, DestinationError> {
        store::find_receipt(&self.dir, load_id, commit_seq)
    }

    async fn replay(
        &mut self,
        _meta: &CommitMeta,
        receipt: &CommitReceipt,
    ) -> Result<(), DestinationError> {
        // The receipt is verified against the store's own log BEFORE
        // staging is dropped: over the wire nothing forces a client to
        // hand back a receipt this store issued, and clearing on a
        // fabricated one would silently discard the staged rows while
        // answering `replayed`. (The sdk wrapper only replays a receipt
        // `existing_receipt` just returned, so it never reaches this
        // refusal.)
        if store::find_receipt(&self.dir, &receipt.load_id, receipt.commit_seq)?.is_none() {
            return Err(DestinationError::fatal(format!(
                "reference destination: replay of a receipt this store never issued — the \
                 receipt log holds no receipt for load `{}` commit {}; the staged rows are \
                 kept, not discarded",
                receipt.load_id, receipt.commit_seq
            )));
        }
        // The redelivered unit was already published under this receipt;
        // dropping its staging is what keeps a LATER commit from
        // publishing it a second time.
        self.clear_staging();
        Ok(())
    }

    async fn publish(&mut self, meta: CommitMeta) -> Result<CommitReceipt, DestinationError> {
        // One session serves ONE load: part names key on the session's
        // load and the receipt on the meta's, so a publish keyed on
        // another load would leave a receipt vouching for files it
        // never wrote. Fatal — no retry makes the two loads agree.
        if meta.load_id != self.load_id {
            return Err(DestinationError::fatal(format!(
                "reference destination: publish for load `{}` on a session opened for \
                 load `{}` — one session serves one load; open a session for the load \
                 being committed",
                meta.load_id, self.load_id
            )));
        }
        // A receipted commit is FINAL. A client that publishes the same
        // `(load, seq)` again — over the wire nothing forces it to ask
        // for the existing receipt first — gets the prior receipt back
        // and its restaged rows are dropped: the rows were published
        // under that receipt already, and re-persisting the restaged
        // ones would silently replace them under the same part names.
        if let Some(prior) = store::find_receipt(&self.dir, &meta.load_id, meta.commit_seq)? {
            self.clear_staging();
            return Ok(prior);
        }
        // The barrier: every part, then the state document, then the
        // receipt — and staging is read by reference and cleared only
        // after the receipt, so a retry of a transiently failed publish
        // re-persists every row over the same deterministic names
        // instead of minting a receipt for an empty publish.
        let mut tables: BTreeMap<&TableName, Vec<&RecordBatch>> = BTreeMap::new();
        for (table, batch) in &self.staged {
            tables.entry(table).or_default().push(batch);
        }
        for (table, batches) in &tables {
            let name = part::name(table, &self.load_id, meta.commit_seq)?;
            store::persist_part(&self.dir, &name, table, batches)?;
        }
        let state = serde_json::to_vec(&meta.state).map_err(|error| {
            DestinationError::fatal(format!(
                "reference destination: encode the state document: {error}"
            ))
        })?;
        store::persist(&self.dir, store::STATE_FILE, &state)?;
        let receipt = CommitReceipt {
            load_id: meta.load_id.clone(),
            commit_seq: meta.commit_seq,
        };
        store::append_receipt(&self.dir, &receipt)?;
        // The commit is fully durable — only now does its staging
        // retire, so no later commit can publish it a second time.
        self.clear_staging();
        Ok(receipt)
    }

    async fn read_state(
        &mut self,
        pipeline: &PipelineId,
    ) -> Result<Option<StateDoc>, DestinationError> {
        let Some(state) = store::read_state(&self.dir)? else {
            return Ok(None);
        };
        // ONE state slot, ONE pipeline per directory: another
        // pipeline's read REFUSES rather than answering None — `None`
        // is the SPI's "never committed", so the engine would
        // re-extract from scratch, append every already-loaded row a
        // second time (receipts are per load id and cannot help across
        // pipelines), and the next publish would overwrite the
        // occupant's cursors in the slot.
        if state.pipeline != *pipeline {
            return Err(DestinationError::fatal(format!(
                "reference destination: {} carries the state of pipeline `{}` — this \
                 session is pipeline `{pipeline}`, and one directory holds ONE pipeline's \
                 state: reading it as fresh would append every already-loaded row again, \
                 and the next publish would destroy `{}`' cursors; give each pipeline its \
                 own output directory",
                self.dir.join(store::STATE_FILE).display(),
                state.pipeline,
                state.pipeline
            )));
        }
        Ok(Some(state))
    }

    async fn close(&mut self) -> Result<(), DestinationError> {
        // The lease ends with the SESSION, not with the object: a
        // well-behaved host closes a session before opening the next,
        // and the successor must not be refused by a closed
        // predecessor whose handle is still in scope. Dropping the
        // file releases the advisory lock; an unclosed drop (crash,
        // error path) releases it the same way.
        self.clear_staging();
        self.lease = None;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use rdlt_connector_sdk::spi::channel::arrow_batch_footprint;
    use rdlt_connector_sdk::spi::core::id::{LoadId, TableName};
    use rdlt_testkit::fixtures::batch_of;

    use super::{STAGING_CEILING_BYTES, Session};

    fn session_at(staged_bytes: usize) -> Session {
        Session {
            dir: std::path::PathBuf::new(),
            load_id: LoadId::new("load"),
            staged: Vec::new(),
            staged_bytes,
            lease: None,
        }
    }

    /// The boundary is inclusive, one byte past it refuses, and an
    /// overflowing sum refuses rather than wrapping.
    #[test]
    fn staging_refuses_before_crossing_its_memory_ceiling() {
        let table = TableName::new("events");
        let footprint = arrow_batch_footprint(&batch_of(&[1]));
        let mut at_boundary = session_at(STAGING_CEILING_BYTES - footprint);
        at_boundary
            .stage(&table, batch_of(&[1]))
            .expect("the boundary passes");
        assert_eq!(at_boundary.staged_bytes, STAGING_CEILING_BYTES);
        assert!(
            session_at(STAGING_CEILING_BYTES - footprint + 1)
                .stage(&table, batch_of(&[1]))
                .is_err()
        );
        assert!(
            session_at(usize::MAX)
                .stage(&table, batch_of(&[1]))
                .is_err()
        );
    }
}
