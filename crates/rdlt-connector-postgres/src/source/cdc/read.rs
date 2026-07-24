//! The CDC read dispatch: the snapshot pass (first run, no cursor) and the
//! bounded change-catch-up pass, plus the run-completion ack/lag reporting.

use futures::TryStreamExt;
use tokio_postgres::Client;

use rdlt_connector::core::crash_point;
use rdlt_connector::{ReadRequest, SourceError};

use crate::source::config::{CdcConfig, CdcMode, PostgresConfig};
use crate::source::copy_decode::{CopyDecoder, FieldPlan};
use crate::source::copy_pump::{CrashSite, Push, pump_copy};
use crate::source::errors::{self, Phase};
use crate::source::sqlgen;

use super::apply::{Apply, Emit};
use super::runtime::{CdcCursor, Runtime, TableIdentity};
use super::tail::{advance_and_report, tail_loop};
use super::{pgoutput, slot, values};

/// The per-table read context: everything fixed for the whole of one stream's
/// read — the source config, the CDC block, the resolved replica identity, and
/// the decode plan. Built by `PostgresSource::read` and threaded through the
/// pass/apply signatures so they stay small (removing the last
/// `too_many_arguments` allow on the dispatch entry point).
pub(crate) struct TableCtx<'a> {
    pub(crate) config: &'a PostgresConfig,
    pub(crate) cdc: &'a CdcConfig,
    pub(crate) identity: &'a TableIdentity,
    pub(crate) plans: &'a [FieldPlan],
}

/// The CDC read dispatch: snapshot pass (no cursor) or change pass. Any
/// error drops the run's cached connections — the engine's TRANSIENT
/// in-run retries re-enter with this same `Runtime`, and a dead snapshot
/// or control client must never be reused across attempts.
pub(crate) async fn read_stream(
    runtime: &Runtime,
    ctx: &TableCtx<'_>,
    cdc_tables: &[String],
    columns: &[&crate::source::reflect::ReflectedColumn],
    req: ReadRequest,
) -> Result<(), SourceError> {
    let result = read_stream_inner(runtime, ctx, cdc_tables, columns, req).await;
    if result.is_err() {
        let mut state = runtime.state.lock().await;
        state.control = None;
        state.snapshot = None;
        state.snapshot_start = None;
    }
    result
}

async fn read_stream_inner(
    runtime: &Runtime,
    ctx: &TableCtx<'_>,
    cdc_tables: &[String],
    columns: &[&crate::source::reflect::ReflectedColumn],
    mut req: ReadRequest,
) -> Result<(), SourceError> {
    use std::sync::Arc;
    // `identity` is threaded onward through `ctx`; the inner dispatch itself
    // needs only the config, the CDC block, and the decode plan.
    let &TableCtx {
        config, cdc, plans, ..
    } = ctx;
    let name = req.stream.name.as_str().to_owned();
    let mut state = runtime.state.lock().await;
    if state.pending.is_none() {
        state.pending = Some(cdc_tables.iter().cloned().collect());
    }
    state.ensure_control(config, &name).await?;
    if state.ensured.is_none() {
        crash_point!(
            "cdc.slot.create",
            Err(errors::fatal(
                Phase::Slot,
                Some(&name),
                "injected: before slot ensure"
            ))
        );
        let outcome = slot::ensure(state.control(), cdc, &config.schema, cdc_tables).await?;
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
                    None => slot::current_wal_lsn(state.control()).await?,
                },
            };
            state.ensure_snapshot(config, &name, start).await?;
            let snap = Arc::clone(state.snapshot());
            drop(state); // COPY + pushes run WITHOUT the state lock
            let select = sqlgen::select_sql(&config.schema, &name, columns, "", "");
            let copy = sqlgen::copy_sql(&select);
            let mut decoder = CopyDecoder::new(
                plans.to_vec(),
                config.batch_target_bytes,
                config.batch_max_rows,
            )
            .map_err(|e| errors::fatal(Phase::Decode, Some(&name), e))?;
            // The COPY stream + decode loop is the shared pump; the snapshot's
            // only per-batch work is flag-wrapping (rows are upserts, flag
            // NULL) and pushing. `cdc.snapshot.copy` is the pump's mid-stream
            // crash site here.
            let mut pushed_any = false;
            let completed = pump_copy(
                &snap,
                copy.as_str(),
                &mut decoder,
                &mut req,
                &name,
                CrashSite {
                    label: "cdc.snapshot.copy",
                    msg: "injected: connection lost mid-snapshot",
                },
                |batch| {
                    pushed_any = true;
                    Ok(vec![Push::Arrow(with_null_flag(batch, &cdc.flag_column))])
                },
            )
            .await?;
            if !completed {
                return Ok(()); // cancellation
            }
            if !pushed_any {
                // Schema-bearing empty batch: plans + flag, all nullable.
                let empty = values::rows_to_batch(plans, &cdc.flag_column, &[], &[])
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
                    let target = slot::current_wal_lsn(state.control()).await?;
                    state.target = Some(target);
                    target
                }
            };
            state.ack_floor.insert(name.clone(), since);
            let control = Arc::clone(state.control());
            drop(state); // the pass pushes batches WITHOUT the state lock
            if target > since {
                let outcome = change_pass(&control, ctx, &name, since, target, &mut req).await?;
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
        // Ack advance + replication-lag reporting are the tail module's
        // concern (it also owns the continuous tail loop that keeps them
        // running past this first completion).
        advance_and_report(&state, cdc, &name).await?;
    }
    if cdc.mode == CdcMode::Tail {
        let control = std::sync::Arc::clone(state.control());
        drop(state);
        return tail_loop(control, ctx, &name, cursor, req).await;
    }
    Ok(())
}

#[derive(PartialEq)]
pub(super) enum PassOutcome {
    Complete,
    Cancelled,
}

/// One bounded catch-up pass for one stream: peek `(since, target]` as a
/// server-side row stream, decode pgoutput, keep this table's changes,
/// batch, checkpoint at commit positions only.
pub(super) async fn change_pass(
    control: &Client,
    ctx: &TableCtx<'_>,
    name: &str,
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
    let changes = slot::peek(control, ctx.cdc, target).await?;
    futures::pin_mut!(changes);

    let mut apply = Apply::new(ctx, name, since);
    while let Some(change) = changes.try_next().await? {
        let message = pgoutput::parse(&change.data)
            .map_err(|e| errors::fatal(Phase::Decode, Some(name), e))?;
        if !drive(apply.on_message(change.lsn, message)?, req).await? {
            return Ok(PassOutcome::Cancelled);
        }
    }
    if !drive(apply.finish(target)?, req).await? {
        return Ok(PassOutcome::Cancelled);
    }
    Ok(PassOutcome::Complete)
}

/// Deliver one apply step's emitted actions in order. Returns `false` at the
/// first engine cancellation (a closed batch or checkpoint channel) so the
/// caller can stop the pass cleanly.
async fn drive(emits: Vec<Emit>, req: &mut ReadRequest) -> Result<bool, SourceError> {
    for action in emits {
        let delivered = match action {
            Emit::Batch(batch) => req.out.arrow(batch).await.is_ok(),
            Emit::Checkpoint(lsn) => req
                .out
                .checkpoint(CdcCursor { cdc_lsn: lsn }.encode())
                .await
                .is_ok(),
        };
        if !delivered {
            return Ok(false);
        }
    }
    Ok(true)
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
