//! Continuous tail mode: a chunked loop of bounded catch-up passes that keeps
//! following the change feed after the initial catch-up completes.

use tokio_postgres::Client;

use rdlt_connector::core::crash_point;
use rdlt_connector::{ReadRequest, SourceError};

use crate::source::config::{AckMode, CdcConfig};

use super::read::{PassOutcome, TableCtx, change_pass};
use super::runtime::{CdcCursor, RunState};
use super::slot;

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
pub(super) async fn tail_loop(
    control: std::sync::Arc<Client>,
    ctx: &TableCtx<'_>,
    name: &str,
    mut cursor: u64,
    mut req: ReadRequest,
) -> Result<(), SourceError> {
    let confirmed = slot::confirmed_flush_lsn(&control, &ctx.cdc.slot).await?;
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
            let outcome = change_pass(&control, ctx, name, cursor, target, &mut req).await?;
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
            tokio::time::sleep(std::time::Duration::from_secs(ctx.cdc.idle_wait.seconds)).await;
        }
    }
}

/// Run-completion housekeeping once every CDC stream has drained its pass:
/// advance the slot's acknowledged position to the min destination-committed
/// floor (Auto ack only), then emit the replication-lag report. Reads run
/// state only — the caller holds the state lock. Lives here with the tail loop
/// because both own the ack/lag surface.
// `name` feeds only the crash-point error, which compiles to nothing without
// the failpoints feature.
#[cfg_attr(not(feature = "failpoints"), allow(unused_variables))]
pub(super) async fn advance_and_report(
    state: &RunState,
    cdc: &CdcConfig,
    name: &str,
) -> Result<(), SourceError> {
    if cdc.ack == AckMode::Auto
        && let Some(&floor) = state.ack_floor.values().min()
    {
        crash_point!(
            "cdc.ack.advance",
            Err(crate::source::errors::fatal(
                crate::source::errors::Phase::Slot,
                Some(name),
                "injected: before slot advance"
            ))
        );
        let control = state.control();
        let confirmed = slot::confirmed_flush_lsn(control, &cdc.slot).await?;
        if floor > confirmed {
            slot::advance(control, &cdc.slot, floor).await?;
        }
    }
    // Replication lag: how far the live feed is ahead of the least-advanced
    // stream at run completion — LSN delta in bytes on the dedicated
    // `rdlt::cdc` target (embedders subscribe, no log-scraping), plus a
    // wall-clock delta when the server tracks commit timestamps.
    let control = state.control();
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
    Ok(())
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
