//! CDC via logical replication: pgoutput decoding, slot lifecycle, and the
//! per-table pass machinery over the SQL peek/advance interface.
//!
//! Delivery design: bounded catch-up pins `target_lsn` once per run; every
//! CDC stream's `read()` peeks `(its cursor, target]` and filters its own
//! table (peeking consumes NOTHING). First run: slot FIRST, then ONE
//! `REPEATABLE READ` transaction snapshots every CDC table; the
//! slot-to-snapshot window applies twice and CONVERGES. Checkpoints land only
//! at transaction-commit positions. The slot's
//! acknowledged position advances once per run to the min DESTINATION-
//! COMMITTED position across CDC streams — each stream's `since` (only ever
//! a cursor the destination durably committed) or its fresh-snapshot start
//! point — so an ack can never outrun a commit, run shapes be damned (the
//! current run's own checkpoints are not yet known-committed, so acking
//! trails one run behind: hygiene, never correctness).

pub(crate) mod pgoutput;
pub mod slot;
pub(crate) mod values;

use std::collections::{BTreeMap, BTreeSet, HashMap};

use futures::TryStreamExt;
use rdlt_connector::{Cursor, ReadRequest, SourceError};
use serde::{Deserialize, Serialize};
use tokio_postgres::Client;

use crate::source::config::{AckMode, CdcConfig, CdcMode, PostgresConfig};
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

/// Per-table replica-identity preflight result.
#[derive(Debug, Clone)]
pub(crate) struct TableIdentity {
    /// The merge key: the table's replica identity columns.
    pub key: Vec<String>,
    /// REPLICA IDENTITY FULL — old tuples carry every column (TOAST
    /// substitution is possible).
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
    /// Arc so passes run WITHOUT the state lock held (a stream blocked on
    /// destination backpressure must never stall the other streams), and
    /// dropped on any error (the engine's transient in-run retries re-enter
    /// with this same state — a dead connection must not be reused).
    control: Option<std::sync::Arc<Client>>,
    ensured: Option<slot::EnsureOutcome>,
    /// Open REPEATABLE READ snapshot transaction (first run) — ONE view
    /// for every CDC table. Arc + drop-on-error like `control`.
    snapshot: Option<std::sync::Arc<Client>>,
    /// The shared snapshot's cursor start point for a PRE-EXISTING slot:
    /// the WAL position read BEFORE the transaction began (its visibility
    /// horizon) — every commit ≤ it is IN the snapshot; commits after it
    /// replay and converge. (Starting at confirmed_flush instead would
    /// replay a window that can contain unappliable records — TRUNCATE,
    /// TOAST without an old image — and permanently wedge recovery.)
    snapshot_start: Option<u64>,
    /// `target_lsn`, pinned once per run at the first CDC read.
    target: Option<u64>,
    /// Per-stream ack floors: destination-committed `since`, or the fresh
    /// snapshot start point (see module docs).
    ack_floor: BTreeMap<String, u64>,
    /// CDC streams that have not completed their pass this run; `None`
    /// until the first CDC read initializes it.
    pending: Option<BTreeSet<String>>,
    /// Each stream's final cursor this run — the lag report's baseline.
    final_cursor: BTreeMap<String, u64>,
}

impl Runtime {
    pub fn new() -> Self {
        Self {
            state: tokio::sync::Mutex::new(RunState::default()),
            identities: tokio::sync::OnceCell::new(),
        }
    }

    /// The replica-identity preflight, once per run: key + FULL flag
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
                     `cdc.flag_column` to an unused name",
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
                     FULL` / `USING INDEX …`",
                ));
            }
            // Explicit index: the identity columns come from that index. A
            // dropped identity index leaves relreplident = 'i' with NO
            // indisreplident row — an empty key would merge on nothing and
            // corrupt deletes, so it is a typed error, never accepted.
            "i" if !ident_cols.is_empty() => TableIdentity {
                key: ident_cols.clone(),
                full: false,
            },
            "i" => {
                return Err(errors::fatal(
                    Phase::Slot,
                    Some(table),
                    "REPLICA IDENTITY USING INDEX but no replica-identity \
                     index exists (was it dropped?) — recreate the index or \
                     `ALTER TABLE … REPLICA IDENTITY DEFAULT`/`FULL`",
                ));
            }
            // FULL: old tuples carry everything, so ANY declared key has its
            // values — a declared primary_key override wins; else the PK.
            "f" => match declared
                .clone()
                .or_else(|| (!pk.is_empty()).then(|| pk.clone()))
            {
                Some(key) => TableIdentity { key, full: true },
                None => {
                    return Err(errors::fatal(
                        Phase::Slot,
                        Some(table),
                        "REPLICA IDENTITY FULL but no key to merge by — add a \
                         primary key or declare `primary_key` on the table",
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
                         DEFAULT` (with a primary key) or `FULL`"
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
        // Defense in depth: every arm above guarantees a non-empty key; an
        // empty one would pass every downstream guard vacuously.
        assert!(!identity.key.is_empty(), "preflight resolved an empty key");
        identities.insert(table.clone(), identity);
    }
    Ok(identities)
}

/// The CDC read dispatch: snapshot pass (no cursor) or change pass. Any
/// error drops the run's cached connections — the engine's TRANSIENT
/// in-run retries re-enter with this same `Runtime`, and a dead snapshot
/// or control client must never be reused across attempts.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn read_stream(
    runtime: &Runtime,
    config: &PostgresConfig,
    cdc: &CdcConfig,
    identity: &TableIdentity,
    cdc_tables: &[String],
    plans: Vec<FieldPlan>,
    columns: &[&crate::source::reflect::ReflectedColumn],
    req: ReadRequest,
) -> Result<(), SourceError> {
    let result = read_stream_inner(
        runtime, config, cdc, identity, cdc_tables, plans, columns, req,
    )
    .await;
    if result.is_err() {
        let mut state = runtime.state.lock().await;
        state.control = None;
        state.snapshot = None;
        state.snapshot_start = None;
    }
    result
}

#[allow(clippy::too_many_arguments)]
async fn read_stream_inner(
    runtime: &Runtime,
    config: &PostgresConfig,
    cdc: &CdcConfig,
    identity: &TableIdentity,
    cdc_tables: &[String],
    plans: Vec<FieldPlan>,
    columns: &[&crate::source::reflect::ReflectedColumn],
    mut req: ReadRequest,
) -> Result<(), SourceError> {
    use std::sync::Arc;
    let name = req.stream.name.as_str().to_owned();
    let mut state = runtime.state.lock().await;
    if state.pending.is_none() {
        state.pending = Some(cdc_tables.iter().cloned().collect());
    }
    if state.control.is_none() {
        let client = connect(config).await?;
        // The logical-decoding SQL functions render tuple TEXT with the
        // calling session's GUCs; pin exactly the forms values.rs parses —
        // a database/role with DateStyle 'SQL, DMY' or bytea_output
        // 'escape' must not change what the feed looks like.
        client
            .batch_execute("SET datestyle = 'ISO'; SET bytea_output = 'hex'")
            .await
            .map_err(|e| errors::classify(Phase::Slot, Some(&name), &e))?;
        state.control = Some(Arc::new(client));
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
    let ensured = state.ensured.expect("ensured");

    let since = match &req.since {
        Some(cursor) => Some(CdcCursor::decode(cursor, &name)?.cdc_lsn),
        None => None,
    };
    // A slot created THIS run starts at its consistent point — it cannot
    // cover a resuming stream's history. Peeking would silently skip every
    // change in (since, consistent_point): typed error, never a gap.
    if let Some(since) = since
        && ensured.created_slot
        && let Some(point) = ensured.consistent_point
        && since < point
    {
        return Err(errors::fatal(
            Phase::Slot,
            Some(&name),
            format!(
                "replication slot `{}` was created THIS run at {} but this \
                 stream resumes from {} — the feed cannot cover that gap; \
                 reset the pipeline state so the stream takes a fresh \
                 snapshot instead of resuming past a recreated slot",
                cdc.slot,
                slot::fmt_lsn(point),
                slot::fmt_lsn(since)
            ),
        ));
    }

    let cursor = match since {
        None => {
            // ---- snapshot pass ----
            // Cursor start: the consistent point when THIS run created the
            // slot; otherwise the shared snapshot's visibility horizon —
            // the WAL position read BEFORE its transaction began (see
            // `RunState::snapshot_start`). Either way: no gap, and the
            // overlap window applies twice and converges.
            let start = match ensured.consistent_point {
                Some(point) => point,
                None => match state.snapshot_start {
                    Some(horizon) => horizon,
                    None => {
                        slot::current_wal_lsn(state.control.as_ref().expect("control client"))
                            .await?
                    }
                },
            };
            if state.snapshot.is_none() {
                let snap = connect(config).await?;
                snap.batch_execute("BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ")
                    .await
                    .map_err(|e| errors::classify(Phase::Copy, Some(&name), &e))?;
                state.snapshot = Some(Arc::new(snap));
                state.snapshot_start = Some(start);
            }
            let snap = Arc::clone(state.snapshot.as_ref().expect("snapshot client"));
            drop(state); // COPY + pushes run WITHOUT the state lock
            let select = sqlgen::select_sql(&config.schema, &name, columns, "", "");
            let copy = sqlgen::copy_sql(&select);
            let mut decoder = CopyDecoder::new(
                plans.clone(),
                config.batch_target_bytes,
                config.batch_max_rows,
            );
            let mut pushed_any = false;
            {
                let stream = snap
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
                            return Ok(()); // cancellation
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
            state = runtime.state.lock().await;
            state.ack_floor.insert(name.clone(), start);
            start
        }
        Some(since) => {
            // ---- change pass ----
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
            let control = Arc::clone(state.control.as_ref().expect("control client"));
            drop(state); // the pass pushes batches WITHOUT the state lock
            if target > since {
                let outcome = change_pass(
                    &control,
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
            state = runtime.state.lock().await;
            target.max(since)
        }
    };

    // ---- run completion + ack ----
    state.final_cursor.insert(name.clone(), cursor);
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
        // Replication lag: how far the live feed is
        // ahead of the least-advanced stream at run completion — LSN delta
        // in bytes on the dedicated `rdlt::cdc` target (embedders subscribe,
        // no log-scraping), plus a wall-clock delta when the server tracks
        // commit timestamps.
        let control = state.control.as_ref().expect("control client");
        let head = slot::current_wal_lsn(control).await?;
        if let Some(&committed) = state.final_cursor.values().min() {
            let lag_bytes = head.saturating_sub(committed);
            match commit_time_lag_seconds(control).await {
                Some(lag_seconds) => tracing::info!(
                    target: "rdlt::cdc",
                    lag_bytes,
                    lag_seconds,
                    "replication lag at run completion"
                ),
                None => tracing::info!(
                    target: "rdlt::cdc",
                    lag_bytes,
                    "replication lag at run completion"
                ),
            }
        }
    }
    if cdc.mode == CdcMode::Tail {
        let control = std::sync::Arc::clone(state.control.as_ref().expect("control client"));
        drop(state);
        return tail_loop(control, config, cdc, identity, &plans, &name, cursor, req).await;
    }
    Ok(())
}

/// A tail acks nothing beyond its first pass: acking is safe only up to
/// DESTINATION-COMMITTED positions, and no such feedback exists in-run
/// (the engine's own WAL is best-effort — its damage path re-extracts from
/// cursors, so acking pushed-but-uncommitted positions would turn "slower,
/// never wrong" into data loss). The server therefore retains WAL for the
/// tail's whole life; warn once past this span so operators cycle the tail
/// (the next run's ack reclaims retention). Documented in the quickstart.
const TAIL_UNACKED_WARN_BYTES: u64 = 256 << 20;

/// Continuous tail: a chunked loop of bounded catch-ups — each chunk pins ITS
/// OWN current position, checkpoints flow per chunk, and a quiet chunk idles
/// `idle_wait`. Cancellation is observed at commit boundaries: the per-chunk
/// checkpoint probe (always a commit/target position) fails the moment the
/// engine closes the
/// channel — no new SPI surface needed. Chunks never hold the run-state
/// lock: concurrent tail streams share the control connection via Arc.
#[allow(clippy::too_many_arguments)]
async fn tail_loop(
    control: std::sync::Arc<Client>,
    config: &PostgresConfig,
    cdc: &CdcConfig,
    identity: &TableIdentity,
    plans: &[FieldPlan],
    name: &str,
    mut cursor: u64,
    mut req: ReadRequest,
) -> Result<(), SourceError> {
    let confirmed = slot::confirmed_flush_lsn(&control, &cdc.slot).await?;
    let mut retention_warned = false;
    loop {
        if req
            .out
            .checkpoint(CdcCursor { cdc_lsn: cursor }.encode())
            .await
            .is_err()
        {
            return Ok(()); // cancellation, at a commit boundary
        }
        let target = slot::current_wal_lsn(&control).await?;
        let quiet = if target > cursor {
            let outcome = change_pass(
                &control,
                cdc,
                &config.schema,
                name,
                identity,
                plans,
                config.batch_max_rows,
                cursor,
                target,
                &mut req,
            )
            .await?;
            if outcome == PassOutcome::Cancelled {
                return Ok(());
            }
            cursor = target;
            false
        } else {
            true
        };
        let unacked = cursor.saturating_sub(confirmed);
        if !retention_warned && unacked > TAIL_UNACKED_WARN_BYTES {
            retention_warned = true;
            tracing::warn!(
                target: "rdlt::cdc",
                unacked_bytes = unacked,
                "tail mode: the slot's acknowledged position is fixed for the \
                 life of this run and the server retains WAL behind it — cycle \
                 the tail (restart the run) so the next run's ack reclaims \
                 retention"
            );
        }
        if quiet {
            tokio::time::sleep(std::time::Duration::from_secs(cdc.idle_wait.seconds)).await;
        }
    }
}

/// Wall-clock replication lag, only when the server exposes it
/// (`track_commit_timestamp = on`); `None` — never a guess — otherwise.
async fn commit_time_lag_seconds(client: &Client) -> Option<f64> {
    let tracked: String = client
        .query_one("SHOW track_commit_timestamp", &[])
        .await
        .ok()?
        .get(0);
    if tracked != "on" {
        return None;
    }
    client
        .query_one(
            "SELECT extract(epoch FROM clock_timestamp() - \
             (pg_last_committed_xact()).timestamp)::float8",
            &[],
        )
        .await
        .ok()?
        .get::<_, Option<f64>>(0)
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
    // ONE canonical peek: `slot::peek` owns the SQL, the parameter binding,
    // and the LSN parsing (it classifies its errors slot-scoped, so they
    // carry no table name — a peek reads every table's changes and filters
    // its own). The stream is consumed row-by-row, decoding each change as
    // it lands rather than buffering the whole range.
    let changes = slot::peek(control, cdc, target).await?;
    futures::pin_mut!(changes);

    let mut apply = Apply::new(cdc, schema, name, identity, plans, batch_max_rows, since);
    while let Some(change) = changes.try_next().await? {
        let message = pgoutput::parse(&change.data)
            .map_err(|e| errors::fatal(Phase::Decode, Some(name), e))?;
        for action in apply.on_message(change.lsn, message)? {
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

    fn on_message(&mut self, lsn: u64, message: Message) -> Result<Vec<Emit>, SourceError> {
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
    fn finish(&mut self, target: u64) -> Result<Vec<Emit>, SourceError> {
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
