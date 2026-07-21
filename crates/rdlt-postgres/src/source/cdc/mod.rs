//! CDC via logical replication (feature 009): pgoutput decoding, slot
//! lifecycle, and the per-table pass machinery over the SQL peek/advance
//! interface. Contracts: `specs/009-postgres-cdc/contracts/`.
//!
//! Delivery design (research R3/R4/R5): bounded catch-up pins `target_lsn`
//! once per run; every CDC stream's `read()` peeks `(its cursor, target]`
//! and filters its own table (peeking consumes NOTHING — P1). First run:
//! slot FIRST, then ONE `REPEATABLE READ` transaction snapshots every CDC
//! table; the slot-to-snapshot window applies twice and CONVERGES (P2).
//! Checkpoints land only at transaction-commit positions (P4). The slot's
//! acknowledged position advances once per run to the min DESTINATION-
//! COMMITTED position across CDC streams — each stream's `since` (E6: only
//! ever a cursor the destination durably committed) or its fresh-snapshot
//! start point — so an ack can never outrun a commit, run shapes be damned
//! (P6; the current run's own checkpoints are not yet known-committed, so
//! acking trails one run behind: hygiene, never correctness).

pub(crate) mod pgoutput;
pub mod slot;
pub(crate) mod values;

use std::collections::{BTreeMap, BTreeSet, HashMap};

use futures::TryStreamExt;
use rdlt_connector::{Cursor, ReadRequest, SourceError};
use serde::{Deserialize, Serialize};
use tokio_postgres::Client;

use crate::source::config::{AckMode, CdcConfig, PostgresConfig};
use crate::source::copy_decode::{CopyDecoder, FieldPlan};
use crate::source::errors::{self, Phase};
use crate::source::reflect::ReflectedTable;
use crate::source::{connect, sqlgen};
use pgoutput::{Message, TupleData, TupleValue};
use rdlt_connector::core::crash_point;
use values::Cell;

/// The engine cursor for a CDC stream: one LSN, distinct JSON shape from
/// the cursor-column state so misrouted state fails typed.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
struct CdcCursor {
    cdc_lsn: u64,
}

impl CdcCursor {
    fn encode(self) -> Cursor {
        Cursor::new(serde_json::to_value(self).expect("cdc cursor serializes"))
    }

    fn decode(cursor: &Cursor, table: &str) -> Result<Self, SourceError> {
        serde_json::from_value(cursor.as_value().clone()).map_err(|e| {
            errors::fatal(
                Phase::Slot,
                Some(table),
                format!("stored CDC cursor does not decode (state corruption?): {e}"),
            )
        })
    }
}

/// Per-table replica-identity preflight result (R8, contract O1).
#[derive(Debug, Clone)]
pub(crate) struct TableIdentity {
    /// The merge key: the table's replica identity columns.
    pub key: Vec<String>,
    /// REPLICA IDENTITY FULL — old tuples carry every column (TOAST
    /// substitution is possible, O3).
    pub full: bool,
}

/// Run-scoped CDC state on the source (one instance per engine run).
pub(crate) struct Runtime {
    state: tokio::sync::Mutex<RunState>,
    identities: tokio::sync::OnceCell<BTreeMap<String, TableIdentity>>,
}

impl std::fmt::Debug for Runtime {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // RunState holds live connections (no Debug); the identities map is
        // the useful part.
        f.debug_struct("cdc::Runtime")
            .field("identities", &self.identities.get())
            .finish_non_exhaustive()
    }
}

#[derive(Default)]
struct RunState {
    /// Lifecycle client, lazily opened; also carries peeks and the ack.
    control: Option<Client>,
    ensured: Option<slot::EnsureOutcome>,
    /// Open REPEATABLE READ snapshot transaction (first run) — ONE view
    /// for every CDC table (P2).
    snapshot: Option<Client>,
    /// `target_lsn`, pinned once per run at the first CDC read (P3).
    target: Option<u64>,
    /// Per-stream ack floors: destination-committed `since`, or the fresh
    /// snapshot start point (see module docs).
    ack_floor: BTreeMap<String, u64>,
    /// CDC streams that have not completed their pass this run; `None`
    /// until the first CDC read initializes it.
    pending: Option<BTreeSet<String>>,
}

impl Runtime {
    pub fn new() -> Self {
        Self {
            state: tokio::sync::Mutex::new(RunState::default()),
            identities: tokio::sync::OnceCell::new(),
        }
    }

    /// The replica-identity preflight, once per run (R8): key + FULL flag
    /// per CDC table, flag-column collisions, unusable identities — all
    /// typed BEFORE any stream declares itself.
    pub async fn identities(
        &self,
        config: &PostgresConfig,
        cdc: &CdcConfig,
        reflected: &BTreeMap<String, ReflectedTable>,
        cdc_tables: &[String],
    ) -> Result<&BTreeMap<String, TableIdentity>, SourceError> {
        self.identities
            .get_or_try_init(|| async {
                let client = connect(config).await?;
                preflight(&client, config, cdc, reflected, cdc_tables).await
            })
            .await
    }
}

async fn preflight(
    client: &Client,
    config: &PostgresConfig,
    cdc: &CdcConfig,
    reflected: &BTreeMap<String, ReflectedTable>,
    cdc_tables: &[String],
) -> Result<BTreeMap<String, TableIdentity>, SourceError> {
    let rows = client
        .query(
            "SELECT c.relname, c.relreplident::text,
                    coalesce((SELECT array_agg(a.attname ORDER BY x.ord)
                              FROM pg_index i
                              CROSS JOIN LATERAL unnest(i.indkey) WITH ORDINALITY AS x(attnum, ord)
                              JOIN pg_attribute a
                                ON a.attrelid = i.indrelid AND a.attnum = x.attnum
                              WHERE i.indrelid = c.oid AND i.indisreplident),
                             '{}') AS ident_cols
             FROM pg_class c
             JOIN pg_namespace n ON n.oid = c.relnamespace
             WHERE n.nspname = $1 AND c.relname = ANY($2)",
            &[&config.schema, &cdc_tables],
        )
        .await
        .map_err(|e| errors::classify(Phase::Slot, None, &e))?;
    let facts: HashMap<String, (String, Vec<String>)> = rows
        .into_iter()
        .map(|row| {
            (
                row.get::<_, String>(0),
                (row.get::<_, String>(1), row.get::<_, Vec<String>>(2)),
            )
        })
        .collect();

    let mut identities = BTreeMap::new();
    for table in cdc_tables {
        let reflected_table = reflected.get(table).ok_or_else(|| {
            errors::fatal(Phase::Slot, Some(table), "CDC table has no reflected shape")
        })?;
        if reflected_table.column(&cdc.flag_column).is_some() {
            return Err(errors::fatal(
                Phase::Slot,
                Some(table),
                format!(
                    "flag column `{}` collides with an existing column — set \
                     `cdc.flag_column` to an unused name (contract C2)",
                    cdc.flag_column
                ),
            ));
        }
        let (replident, ident_cols) = facts.get(table).ok_or_else(|| {
            errors::fatal(
                Phase::Slot,
                Some(table),
                "CDC table not found in the catalog",
            )
        })?;
        let pk: Vec<String> = reflected_table
            .primary_key()
            .iter()
            .map(|s| (*s).to_owned())
            .collect();
        let declared = config
            .table_config(table)
            .and_then(|t| t.primary_key.clone());
        let identity = match replident.as_str() {
            // Default: delete/update records carry the PRIMARY KEY columns.
            "d" if !pk.is_empty() => TableIdentity {
                key: pk,
                full: false,
            },
            "d" => {
                return Err(errors::fatal(
                    Phase::Slot,
                    Some(table),
                    "CDC needs a usable replica identity and the table has no \
                     primary key — add one, or `ALTER TABLE … REPLICA IDENTITY \
                     FULL` / `USING INDEX …` (contract O1)",
                ));
            }
            // Explicit index: the identity columns come from that index.
            "i" => TableIdentity {
                key: ident_cols.clone(),
                full: false,
            },
            // FULL: old tuples carry everything; the merge key is still the
            // PK (reflected or declared) — merging needs a key.
            "f" if !pk.is_empty() => TableIdentity {
                key: pk,
                full: true,
            },
            "f" => match declared.clone() {
                Some(key) => TableIdentity { key, full: true },
                None => {
                    return Err(errors::fatal(
                        Phase::Slot,
                        Some(table),
                        "REPLICA IDENTITY FULL but no key to merge by — add a \
                         primary key or declare `primary_key` on the table \
                         (contract O1)",
                    ));
                }
            },
            // NOTHING (or anything else): updates/deletes carry no key data.
            other => {
                return Err(errors::fatal(
                    Phase::Slot,
                    Some(table),
                    format!(
                        "replica identity `{other}` cannot replicate \
                         updates/deletes — `ALTER TABLE … REPLICA IDENTITY \
                         DEFAULT` (with a primary key) or `FULL` (contract O1)"
                    ),
                ));
            }
        };
        // A declared primary_key that disagrees with the identity columns
        // would leave delete records without values for the merge key —
        // typed, not silently ignored. (Under FULL any declared key works.)
        if let Some(declared) = declared
            && !identity.full
            && declared != identity.key
        {
            return Err(errors::fatal(
                Phase::Slot,
                Some(table),
                format!(
                    "declared primary_key {declared:?} differs from the replica \
                     identity columns {:?} — delete records only carry the \
                     identity columns; align them or use REPLICA IDENTITY FULL",
                    identity.key
                ),
            ));
        }
        identities.insert(table.clone(), identity);
    }
    Ok(identities)
}

/// The CDC read dispatch: snapshot pass (no cursor) or change pass.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn read_stream(
    runtime: &Runtime,
    config: &PostgresConfig,
    cdc: &CdcConfig,
    identity: &TableIdentity,
    cdc_tables: &[String],
    plans: Vec<FieldPlan>,
    columns: &[&crate::source::reflect::ReflectedColumn],
    mut req: ReadRequest,
) -> Result<(), SourceError> {
    let name = req.stream.name.as_str().to_owned();
    let mut state = runtime.state.lock().await;
    if state.pending.is_none() {
        state.pending = Some(cdc_tables.iter().cloned().collect());
    }
    if state.control.is_none() {
        state.control = Some(connect(config).await?);
    }
    if state.ensured.is_none() {
        crash_point!(
            "cdc.slot.create",
            Err(errors::fatal(
                Phase::Slot,
                Some(&name),
                "injected: before slot ensure"
            ))
        );
        let outcome = slot::ensure(
            state.control.as_ref().expect("control client"),
            cdc,
            &config.schema,
            cdc_tables,
        )
        .await?;
        state.ensured = Some(outcome);
    }

    let since = match &req.since {
        Some(cursor) => Some(CdcCursor::decode(cursor, &name)?.cdc_lsn),
        None => None,
    };
    match since {
        None => {
            // ---- snapshot pass (P2) ----
            // Start point: the consistent point when THIS run created the
            // slot; otherwise the pre-existing slot's confirmed position
            // (the feed replays from there; overlap converges).
            let start = match state.ensured.expect("ensured").consistent_point {
                Some(point) => point,
                None => {
                    slot::confirmed_flush_lsn(
                        state.control.as_ref().expect("control client"),
                        &cdc.slot,
                    )
                    .await?
                }
            };
            if state.snapshot.is_none() {
                let snap = connect(config).await?;
                snap.batch_execute("BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ")
                    .await
                    .map_err(|e| errors::classify(Phase::Copy, Some(&name), &e))?;
                state.snapshot = Some(snap);
            }
            let select = sqlgen::select_sql(&config.schema, &name, columns, "", "");
            let copy = sqlgen::copy_sql(&select);
            let mut decoder = CopyDecoder::new(
                plans.clone(),
                config.batch_target_bytes,
                config.batch_max_rows,
            );
            let mut pushed_any = false;
            {
                let stream = state
                    .snapshot
                    .as_ref()
                    .expect("snapshot client")
                    .copy_out(copy.as_str())
                    .await
                    .map_err(|e| errors::classify(Phase::Copy, Some(&name), &e))?;
                futures::pin_mut!(stream);
                loop {
                    let chunk = stream
                        .try_next()
                        .await
                        .map_err(|e| errors::classify(Phase::Copy, Some(&name), &e))?;
                    let Some(chunk) = chunk else { break };
                    crash_point!(
                        "cdc.snapshot.copy",
                        Err(errors::transient(
                            Phase::Copy,
                            Some(&name),
                            "injected: connection lost mid-snapshot"
                        ))
                    );
                    let batches = decoder
                        .feed(&chunk)
                        .map_err(|e| errors::fatal(Phase::Decode, Some(&name), e))?;
                    for batch in batches {
                        pushed_any = true;
                        if req
                            .out
                            .arrow(with_null_flag(batch, &cdc.flag_column))
                            .await
                            .is_err()
                        {
                            return Ok(()); // cancellation (S4)
                        }
                    }
                }
            }
            if let Some(tail) = decoder
                .finish()
                .map_err(|e| errors::fatal(Phase::Decode, Some(&name), e))?
            {
                pushed_any = true;
                if req
                    .out
                    .arrow(with_null_flag(tail, &cdc.flag_column))
                    .await
                    .is_err()
                {
                    return Ok(());
                }
            }
            if !pushed_any {
                // Schema-bearing empty batch: plans + flag, all nullable.
                let empty = values::rows_to_batch(&plans, &cdc.flag_column, &[], &[])
                    .map_err(|e| errors::fatal(Phase::Decode, Some(&name), e))?;
                if req.out.arrow(empty).await.is_err() {
                    return Ok(());
                }
            }
            if req
                .out
                .checkpoint(CdcCursor { cdc_lsn: start }.encode())
                .await
                .is_err()
            {
                return Ok(());
            }
            state.ack_floor.insert(name.clone(), start);
        }
        Some(since) => {
            // ---- change pass (P3/P4/P5) ----
            let target = match state.target {
                Some(target) => target,
                None => {
                    let target =
                        slot::current_wal_lsn(state.control.as_ref().expect("control client"))
                            .await?;
                    state.target = Some(target);
                    target
                }
            };
            state.ack_floor.insert(name.clone(), since);
            if target > since {
                let outcome = change_pass(
                    state.control.as_ref().expect("control client"),
                    cdc,
                    &config.schema,
                    &name,
                    identity,
                    &plans,
                    config.batch_max_rows,
                    since,
                    target,
                    &mut req,
                )
                .await?;
                if outcome == PassOutcome::Cancelled {
                    return Ok(());
                }
            }
        }
    }

    // ---- run completion + ack (P6) ----
    let drained = {
        let pending = state.pending.as_mut().expect("pending initialized");
        pending.remove(&name);
        pending.is_empty()
    };
    if drained {
        state.snapshot = None; // closes the snapshot tx connection
        if cdc.ack == AckMode::Auto
            && let Some(&floor) = state.ack_floor.values().min()
        {
            crash_point!(
                "cdc.ack.advance",
                Err(errors::fatal(
                    Phase::Slot,
                    Some(&name),
                    "injected: before slot advance"
                ))
            );
            let control = state.control.as_ref().expect("control client");
            let confirmed = slot::confirmed_flush_lsn(control, &cdc.slot).await?;
            if floor > confirmed {
                slot::advance(control, &cdc.slot, floor).await?;
            }
        }
    }
    Ok(())
}

#[derive(PartialEq)]
enum PassOutcome {
    Complete,
    Cancelled,
}

/// One bounded catch-up pass for one stream: peek `(since, target]` as a
/// server-side row stream, decode pgoutput, keep this table's changes,
/// batch, checkpoint at commit positions only.
#[allow(clippy::too_many_arguments)]
async fn change_pass(
    control: &Client,
    cdc: &CdcConfig,
    schema: &str,
    name: &str,
    identity: &TableIdentity,
    plans: &[FieldPlan],
    batch_max_rows: usize,
    since: u64,
    target: u64,
    req: &mut ReadRequest,
) -> Result<PassOutcome, SourceError> {
    crash_point!(
        "cdc.stream.peek",
        Err(errors::transient(
            Phase::Slot,
            Some(name),
            "injected: peek connection lost"
        ))
    );
    let params: Vec<&(dyn tokio_postgres::types::ToSql + Sync)> = vec![&cdc.slot, &cdc.publication];
    let upto = slot::fmt_lsn(target);
    let sql = format!(
        "SELECT lsn::text, data FROM pg_logical_slot_peek_binary_changes(\
         $1, '{upto}'::pg_lsn, NULL::int4, \
         'proto_version', '1', 'publication_names', $2)"
    );
    let rows = control
        .query_raw(sql.as_str(), params)
        .await
        .map_err(|e| errors::classify(Phase::Slot, Some(name), &e))?;
    futures::pin_mut!(rows);

    let mut apply = Apply::new(cdc, schema, name, identity, plans, batch_max_rows, since);
    while let Some(row) = rows
        .try_next()
        .await
        .map_err(|e| errors::classify(Phase::Slot, Some(name), &e))?
    {
        let lsn_text: String = row.get(0);
        let lsn = slot::parse_lsn(&lsn_text).ok_or_else(|| {
            errors::fatal(
                Phase::Slot,
                Some(name),
                format!("server returned unparseable change lsn `{lsn_text}`"),
            )
        })?;
        let data: Vec<u8> = row.get(1);
        let message =
            pgoutput::parse(&data).map_err(|e| errors::fatal(Phase::Decode, Some(name), e))?;
        for action in apply.on_message(lsn, message)? {
            match action {
                Emit::Batch(batch) => {
                    if req.out.arrow(batch).await.is_err() {
                        return Ok(PassOutcome::Cancelled);
                    }
                }
                Emit::Checkpoint(lsn) => {
                    if req
                        .out
                        .checkpoint(CdcCursor { cdc_lsn: lsn }.encode())
                        .await
                        .is_err()
                    {
                        return Ok(PassOutcome::Cancelled);
                    }
                }
            }
        }
    }
    for action in apply.finish(target)? {
        match action {
            Emit::Batch(batch) => {
                if req.out.arrow(batch).await.is_err() {
                    return Ok(PassOutcome::Cancelled);
                }
            }
            Emit::Checkpoint(lsn) => {
                if req
                    .out
                    .checkpoint(CdcCursor { cdc_lsn: lsn }.encode())
                    .await
                    .is_err()
                {
                    return Ok(PassOutcome::Cancelled);
                }
            }
        }
    }
    Ok(PassOutcome::Complete)
}

enum Emit {
    Batch(arrow_array::RecordBatch),
    Checkpoint(u64),
}

/// The per-stream apply state machine: relation tracking, transaction
/// buffering, commit-boundary checkpoint discipline.
struct Apply<'a> {
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
    /// pushed before) — the only value checkpoints may carry (P4).
    last_commit: Option<u64>,
    in_tx: bool,
}

impl<'a> Apply<'a> {
    #[allow(clippy::too_many_arguments)]
    fn new(
        cdc: &'a CdcConfig,
        schema: &'a str,
        name: &'a str,
        identity: &'a TableIdentity,
        plans: &'a [FieldPlan],
        batch_max_rows: usize,
        since: u64,
    ) -> Self {
        let key_plan_idx = identity
            .key
            .iter()
            .filter_map(|k| plans.iter().position(|p| &p.name == k))
            .collect();
        Self {
            cdc,
            schema,
            name,
            identity,
            plans,
            batch_max_rows,
            since,
            rel_maps: HashMap::new(),
            key_plan_idx,
            tx_rows: Vec::new(),
            ready_rows: Vec::new(),
            last_commit: None,
            in_tx: false,
        }
    }

    fn fatal(&self, detail: impl std::fmt::Display) -> SourceError {
        errors::fatal(Phase::Decode, Some(self.name), detail)
    }

    fn on_message(&mut self, lsn: u64, message: Message) -> Result<Vec<Emit>, SourceError> {
        match message {
            Message::Begin { .. } => {
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
                    let row = self.tuple_row(&map, &new, None)?;
                    self.tx_rows.push((row, false));
                }
                Ok(Vec::new())
            }
            Message::Update { rel, old, new } => {
                if let Some(map) = self.our_map(rel) {
                    // PK-changing update: delete(old key) then insert(new),
                    // in order, same transaction (P5).
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
                            self.tx_rows.push((self.delete_row(old_key), true));
                        }
                    }
                    self.tx_rows.push((new_row, false));
                }
                Ok(Vec::new())
            }
            Message::Delete { rel, old } => {
                if let Some(map) = self.our_map(rel) {
                    let key = self.key_cells(&map, &old)?;
                    if key.iter().any(|c| matches!(c, Cell::Null)) {
                        return Err(self.fatal(
                            "delete record carries no usable key data — the \
                             table's replica identity was weakened mid-stream; \
                             restore it (contract O4)",
                        ));
                    }
                    self.tx_rows.push((self.delete_row(key), true));
                }
                Ok(Vec::new())
            }
            Message::Truncate { rels } => {
                if rels
                    .iter()
                    .any(|id| matches!(self.rel_maps.get(id), Some(Some(_))))
                {
                    return Err(self.fatal(
                        "TRUNCATE arrived on this table — truncation does not \
                         replicate as row deletes; reset the stream (clear its \
                         cursor for a fresh snapshot) or stop truncating \
                         published tables",
                    ));
                }
                Ok(Vec::new())
            }
            Message::Commit { .. } => {
                self.in_tx = false;
                // Already-applied transaction (commit position ≤ cursor):
                // discard. Otherwise the buffered rows become ready and this
                // commit position becomes checkpointable.
                let rows = std::mem::take(&mut self.tx_rows);
                if lsn <= self.since {
                    return Ok(Vec::new());
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
    fn finish(&mut self, target: u64) -> Result<Vec<Emit>, SourceError> {
        self.tx_rows.clear();
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
    /// run boundaries, O4/D5).
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
                             (non-additive schema drift, contract O4)",
                            plan.name
                        ))
                    })
            })
            .collect()
    }

    /// A full row from a tuple: plan-ordered cells; unchanged-TOAST markers
    /// substitute from the old image under REPLICA IDENTITY FULL, else are
    /// a typed error naming table + column (O3).
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
                                 REPLICA IDENTITY FULL` to retain TOAST values \
                                 (contract O3)",
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
                         unusable key data (contract O4)",
                    )),
                }
            })
            .collect()
    }

    /// A delete row: key cells in place, every other column NULL, flag TRUE
    /// (data-model, change record).
    fn delete_row(&self, key: Vec<Cell>) -> Vec<Cell> {
        let mut row = vec![Cell::Null; self.plans.len()];
        for (&plan_idx, cell) in self.key_plan_idx.iter().zip(key) {
            row[plan_idx] = cell;
        }
        row
    }
}

/// Snapshot batches ride the binary COPY decoder; give them the SAME shape
/// as change batches: every field nullable + the trailing flag column
/// (NULL — snapshot rows are upserts).
fn with_null_flag(batch: arrow_array::RecordBatch, flag_column: &str) -> arrow_array::RecordBatch {
    use arrow_schema::{DataType, Field, Schema};
    let mut fields: Vec<Field> = batch
        .schema()
        .fields()
        .iter()
        .map(|f| f.as_ref().clone().with_nullable(true))
        .collect();
    fields.push(Field::new(flag_column, DataType::Boolean, true));
    let mut arrays = batch.columns().to_vec();
    arrays.push(std::sync::Arc::new(arrow_array::BooleanArray::from(vec![
        None::<bool>;
        batch.num_rows()
    ])));
    arrow_array::RecordBatch::try_new(std::sync::Arc::new(Schema::new(fields)), arrays)
        .expect("flag append preserves shape")
}
