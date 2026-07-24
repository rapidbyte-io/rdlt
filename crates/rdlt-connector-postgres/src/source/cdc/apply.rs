//! The per-stream apply state machine: pgoutput change messages buffered by
//! transaction and flushed into change batches with commit-boundary
//! checkpoint discipline.

use std::collections::HashMap;

use rdlt_connector::SourceError;

use crate::source::config::CdcConfig;
use crate::source::copy_decode::FieldPlan;
use crate::source::errors::{self, Phase};

use super::pgoutput::{self, Message, TupleData, TupleValue};
use super::read::TableCtx;
use super::runtime::TableIdentity;
use super::values::{self, Cell};

pub(super) enum Emit {
    Batch(arrow_array::RecordBatch),
    Checkpoint(u64),
}

/// The per-stream apply state machine: relation tracking, transaction
/// buffering, commit-boundary checkpoint discipline.
pub(super) struct Apply<'a> {
    cdc: &'a CdcConfig,
    schema: &'a str,
    name: &'a str,
    identity: &'a TableIdentity,
    plans: &'a [FieldPlan],
    batch_max_rows: usize,
    since: u64,
    /// rel id → plan-column → relation-column index (None = not ours).
    rel_maps: HashMap<u32, Option<Vec<usize>>>,
    /// Plan indices of the merge-key columns.
    key_plan_idx: Vec<usize>,
    /// Rows of the transaction currently being decoded.
    tx_rows: Vec<(Vec<Cell>, bool)>,
    /// Rows of committed transactions not yet pushed.
    ready_rows: Vec<(Vec<Cell>, bool)>,
    /// Commit position covering every row in `ready_rows` (and everything
    /// pushed before) — the only value checkpoints may carry.
    last_commit: Option<u64>,
    /// First unappliable record of the CURRENT transaction (unchanged
    /// TOAST without an image, TRUNCATE, keyless delete, drift). Raised at
    /// the COMMIT boundary — and only when the transaction is not already
    /// applied (commit ≤ cursor). Raising eagerly would make such records
    /// replay-fatal forever: the whole point of the fresh-snapshot recovery
    /// is that a new snapshot's cursor moves PAST them.
    tx_error: Option<SourceError>,
    in_tx: bool,
}

impl<'a> Apply<'a> {
    pub(super) fn new(ctx: &TableCtx<'a>, name: &'a str, since: u64) -> Self {
        let &TableCtx {
            config,
            cdc,
            identity,
            plans,
        } = ctx;
        let key_plan_idx = identity
            .key
            .iter()
            .filter_map(|k| plans.iter().position(|p| &p.name == k))
            .collect();
        Self {
            cdc,
            schema: config.schema.as_str(),
            name,
            identity,
            plans,
            batch_max_rows: config.batch_max_rows,
            since,
            rel_maps: HashMap::new(),
            key_plan_idx,
            tx_rows: Vec::new(),
            ready_rows: Vec::new(),
            last_commit: None,
            tx_error: None,
            in_tx: false,
        }
    }

    fn fatal(&self, detail: impl std::fmt::Display) -> SourceError {
        errors::fatal(Phase::Decode, Some(self.name), detail)
    }

    /// Record the current transaction's first unappliable record; decided
    /// at the commit boundary (see `tx_error`).
    fn defer(&mut self, error: SourceError) {
        if self.tx_error.is_none() {
            self.tx_error = Some(error);
        }
    }

    pub(super) fn on_message(
        &mut self,
        lsn: u64,
        message: Message,
    ) -> Result<Vec<Emit>, SourceError> {
        match message {
            Message::Begin => {
                self.in_tx = true;
                self.tx_rows.clear();
                Ok(Vec::new())
            }
            Message::Relation(rel) => {
                let map = if rel.namespace == self.schema && rel.name == self.name {
                    Some(self.plan_map(&rel)?)
                } else {
                    None
                };
                self.rel_maps.insert(rel.id, map);
                Ok(Vec::new())
            }
            Message::Insert { rel, new } => {
                if let Some(map) = self.our_map(rel) {
                    match self.tuple_row(&map, &new, None) {
                        Ok(row) => self.tx_rows.push((row, false)),
                        Err(e) => self.defer(e),
                    }
                }
                Ok(Vec::new())
            }
            Message::Update { rel, old, new } => {
                if let Some(map) = self.our_map(rel) {
                    // PK-changing update: delete(old key) then insert(new),
                    // in order, same transaction.
                    let built = (|| {
                        let mut rows = Vec::new();
                        let old_key = old.as_ref().map(|o| self.key_cells(&map, o)).transpose()?;
                        let new_row = self.tuple_row(&map, &new, old.as_ref())?;
                        let new_key: Vec<&Cell> =
                            self.key_plan_idx.iter().map(|&i| &new_row[i]).collect();
                        if let Some(old_key) = old_key {
                            let changed = old_key
                                .iter()
                                .zip(&new_key)
                                .any(|(o, n)| !matches!(o, Cell::Null) && &o != n);
                            if changed {
                                rows.push((self.delete_row(old_key), true));
                            }
                        }
                        rows.push((new_row, false));
                        Ok::<_, SourceError>(rows)
                    })();
                    match built {
                        Ok(rows) => self.tx_rows.extend(rows),
                        Err(e) => self.defer(e),
                    }
                }
                Ok(Vec::new())
            }
            Message::Delete { rel, old } => {
                if let Some(map) = self.our_map(rel) {
                    match self.key_cells(&map, &old) {
                        Ok(key) if key.iter().any(|c| matches!(c, Cell::Null)) => {
                            self.defer(self.fatal(
                                "delete record carries no usable key data — the \
                                 table's replica identity was weakened \
                                 mid-stream; restore it",
                            ));
                        }
                        Ok(key) => self.tx_rows.push((self.delete_row(key), true)),
                        Err(e) => self.defer(e),
                    }
                }
                Ok(Vec::new())
            }
            Message::Truncate { rels } => {
                if rels
                    .iter()
                    .any(|id| matches!(self.rel_maps.get(id), Some(Some(_))))
                {
                    self.defer(self.fatal(
                        "TRUNCATE arrived on this table — truncation does not \
                         replicate as row deletes; reset the stream's pipeline \
                         state AND re-initialize the destination table (a fresh \
                         snapshot starts PAST the truncation but cannot remove \
                         rows the truncation deleted), or stop truncating \
                         published tables",
                    ));
                }
                Ok(Vec::new())
            }
            Message::Commit => {
                self.in_tx = false;
                // Already-applied transaction (commit position ≤ cursor):
                // discard rows AND any unappliable-record error — replaying
                // past an applied fault must not re-raise it. Otherwise the
                // fault (if any) surfaces HERE, at its commit position.
                let rows = std::mem::take(&mut self.tx_rows);
                let error = self.tx_error.take();
                if lsn <= self.since {
                    return Ok(Vec::new());
                }
                if let Some(error) = error {
                    return Err(error);
                }
                self.ready_rows.extend(rows);
                self.last_commit = Some(lsn);
                if self.ready_rows.len() >= self.batch_max_rows {
                    return self.flush(true);
                }
                Ok(Vec::new())
            }
            Message::Origin | Message::Type => Ok(Vec::new()),
        }
    }

    /// End of the peeked range: flush the remainder and checkpoint at the
    /// run target (every commit ≤ target is applied for this table). A
    /// transaction left open at range end never saw its commit — dropping
    /// it is the whole-transaction discipline (the next pass replays it).
    pub(super) fn finish(&mut self, target: u64) -> Result<Vec<Emit>, SourceError> {
        self.tx_rows.clear();
        self.tx_error = None;
        let mut out = self.flush(false)?;
        out.push(Emit::Checkpoint(target.max(self.since)));
        Ok(out)
    }

    fn flush(&mut self, checkpoint: bool) -> Result<Vec<Emit>, SourceError> {
        if self.ready_rows.is_empty() {
            return Ok(Vec::new());
        }
        let rows: Vec<Vec<Cell>> = self.ready_rows.iter().map(|(r, _)| r.clone()).collect();
        let deleted: Vec<bool> = self.ready_rows.iter().map(|(_, d)| *d).collect();
        self.ready_rows.clear();
        let batch = values::rows_to_batch(self.plans, &self.cdc.flag_column, &rows, &deleted)
            .map_err(|e| self.fatal(e))?;
        let mut out = vec![Emit::Batch(batch)];
        if checkpoint && let Some(commit) = self.last_commit {
            out.push(Emit::Checkpoint(commit));
        }
        Ok(out)
    }

    fn our_map(&self, rel: u32) -> Option<Vec<usize>> {
        self.rel_maps.get(&rel).and_then(|m| m.clone())
    }

    /// plan column index → relation column index, by name. A reflected
    /// column missing from the relation = non-additive drift = typed error;
    /// EXTRA relation columns (added after this run's reflection) are
    /// deferred to the next run's reflection (additive drift applies at
    /// run boundaries).
    fn plan_map(&self, rel: &pgoutput::Relation) -> Result<Vec<usize>, SourceError> {
        self.plans
            .iter()
            .map(|plan| {
                rel.columns
                    .iter()
                    .position(|c| c.name == plan.name)
                    .ok_or_else(|| {
                        self.fatal(format!(
                            "column `{}` vanished from the replicated table \
                             (non-additive schema drift)",
                            plan.name
                        ))
                    })
            })
            .collect()
    }

    /// A full row from a tuple: plan-ordered cells; unchanged-TOAST markers
    /// substitute from the old image under REPLICA IDENTITY FULL, else are
    /// a typed error naming table + column.
    fn tuple_row(
        &self,
        map: &[usize],
        tuple: &TupleData,
        old: Option<&TupleData>,
    ) -> Result<Vec<Cell>, SourceError> {
        map.iter()
            .zip(self.plans)
            .map(|(&rel_idx, plan)| {
                let value = tuple
                    .values
                    .get(rel_idx)
                    .ok_or_else(|| self.fatal(format!("tuple lacks column `{}`", plan.name)))?;
                match value {
                    TupleValue::Null => Ok(Cell::Null),
                    TupleValue::Text(bytes) => self.text_cell(bytes, &plan.name),
                    TupleValue::UnchangedToast => {
                        let substitute = old.and_then(|o| o.values.get(rel_idx));
                        match substitute {
                            Some(TupleValue::Text(bytes)) if self.identity.full => {
                                self.text_cell(bytes, &plan.name)
                            }
                            _ => Err(self.fatal(format!(
                                "unchanged TOAST value in column `{}` and no old \
                                 image to substitute from — `ALTER TABLE {}.{} \
                                 REPLICA IDENTITY FULL` to retain TOAST values",
                                plan.name, self.schema, self.name
                            ))),
                        }
                    }
                }
            })
            .collect()
    }

    fn text_cell(&self, bytes: &[u8], column: &str) -> Result<Cell, SourceError> {
        std::str::from_utf8(bytes)
            .map(|t| Cell::Text(t.to_owned()))
            .map_err(|e| self.fatal(format!("column `{column}`: tuple text is not UTF-8: {e}")))
    }

    /// The key cells of a tuple (identity/old tuples), plan-key-ordered.
    fn key_cells(&self, map: &[usize], tuple: &TupleData) -> Result<Vec<Cell>, SourceError> {
        self.key_plan_idx
            .iter()
            .map(|&plan_idx| {
                let rel_idx = map[plan_idx];
                match tuple.values.get(rel_idx) {
                    None | Some(TupleValue::Null) => Ok(Cell::Null),
                    Some(TupleValue::Text(bytes)) => {
                        self.text_cell(bytes, &self.plans[plan_idx].name)
                    }
                    Some(TupleValue::UnchangedToast) => Err(self.fatal(
                        "key column arrived as an unchanged-TOAST marker — \
                         unusable key data",
                    )),
                }
            })
            .collect()
    }

    /// A delete row: key cells in place, every other column NULL, flag TRUE.
    fn delete_row(&self, key: Vec<Cell>) -> Vec<Cell> {
        let mut row = vec![Cell::Null; self.plans.len()];
        for (&plan_idx, cell) in self.key_plan_idx.iter().zip(key) {
            row[plan_idx] = cell;
        }
        row
    }
}
