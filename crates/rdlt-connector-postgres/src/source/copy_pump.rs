//! The shared binary-COPY pump: the read loop common to the plain snapshot
//! read and the CDC snapshot pass. Both open a server-side `COPY … TO STDOUT
//! BINARY`, feed every socket chunk through a [`CopyDecoder`], and push each
//! completed batch downstream with identical error mapping (connection loss
//! Transient, decode faults Fatal) and one mid-stream crash point.
//!
//! The per-batch work differs (the CDC snapshot flag-wraps and pushes; the
//! plain read runs the incremental tracker, pushes, and checkpoints), so it is
//! a caller-supplied SYNC `transform` that returns the ordered [`Push`] actions
//! the pump then awaits. Keeping the transform synchronous — rather than an
//! async closure that borrows across `.await` — is what lets the pump stay
//! `Send` in the async-trait `read()` path.

use arrow_array::RecordBatch;
use futures::TryStreamExt;
use rdlt_connector::core::crash_point;
use rdlt_connector::{Cursor, ReadRequest, SourceError};
use tokio_postgres::Client;

use crate::source::copy_decode::CopyDecoder;
use crate::source::errors::{self, Phase};

/// The mid-stream crash site a pump caller owns: the failpoint name plus the
/// injected transient detail. Bundled so the pump's own argument list stays
/// small. `pg.src.mid_copy` for the plain read, `cdc.snapshot.copy` for CDC.
// The fields are read only by `crash_point!`, which compiles to nothing
// without the failpoints feature.
#[cfg_attr(not(feature = "failpoints"), allow(dead_code))]
pub(crate) struct CrashSite {
    pub label: &'static str,
    pub msg: &'static str,
}

/// One action the pump performs for a decoded batch, in order. The transform
/// returns these; the pump awaits the pushes and fires the crash points.
pub(crate) enum Push {
    /// Hand an arrow batch downstream (engine cancellation stops the pump).
    Arrow(RecordBatch),
    /// Emit a resume checkpoint.
    Checkpoint(Cursor),
    /// Fire a named post-batch crash point (failpoints only) — the plain
    /// read's `pg.src.after_batch_push`, which lands BETWEEN a batch push and
    /// its checkpoint. `msg` is the injected transient detail.
    Crash(&'static str, &'static str),
}

/// Run `copy_sql` on `client`, decode through `decoder`, and for each completed
/// batch (including the finish tail) call `transform` and perform the [`Push`]
/// actions it returns. Returns `true` when the whole stream was consumed,
/// `false` at the first engine cancellation.
///
/// `crash` names the mid-stream failpoint the caller owns so both callers keep
/// their registered crash sites while sharing this body.
// `crash` feeds a `crash_point!` that compiles to nothing without the
// failpoints feature, leaving it unused in normal builds.
#[cfg_attr(not(feature = "failpoints"), allow(unused_variables))]
pub(crate) async fn pump_copy<F>(
    client: &Client,
    copy_sql: &str,
    decoder: &mut CopyDecoder,
    req: &mut ReadRequest,
    name: &str,
    crash: CrashSite,
    mut transform: F,
) -> Result<bool, SourceError>
where
    F: FnMut(RecordBatch) -> Result<Vec<Push>, SourceError>,
{
    let stream = client
        .copy_out(copy_sql)
        .await
        .map_err(|e| errors::classify(Phase::Copy, Some(name), &e))?;
    futures::pin_mut!(stream);
    loop {
        let chunk = stream
            .try_next()
            .await
            .map_err(|e| errors::classify(Phase::Copy, Some(name), &e))?;
        let Some(chunk) = chunk else { break };
        // Simulated mid-stream connection loss: Transient — the ENGINE
        // retries the whole read from committed state.
        crash_point!(
            crash.label,
            Err(errors::transient(Phase::Copy, Some(name), crash.msg))
        );
        let batches = decoder
            .feed(&chunk)
            .map_err(|e| errors::fatal(Phase::Decode, Some(name), e))?;
        for batch in batches {
            if !perform(transform(batch)?, req).await? {
                return Ok(false);
            }
        }
    }
    if let Some(tail) = decoder
        .finish()
        .map_err(|e| errors::fatal(Phase::Decode, Some(name), e))?
        && !perform(transform(tail)?, req).await?
    {
        return Ok(false);
    }
    Ok(true)
}

/// Await one batch's push actions in order. `false` = engine cancellation.
#[cfg_attr(not(feature = "failpoints"), allow(unused_variables))]
async fn perform(pushes: Vec<Push>, req: &mut ReadRequest) -> Result<bool, SourceError> {
    for push in pushes {
        match push {
            Push::Arrow(batch) => {
                if req.out.arrow(batch).await.is_err() {
                    return Ok(false);
                }
            }
            Push::Checkpoint(cursor) => {
                if req.out.checkpoint(cursor).await.is_err() {
                    return Ok(false);
                }
            }
            Push::Crash(label, msg) => {
                // Post-batch injection (plain read only). Matches the former
                // inline site: Transient, tableless.
                crash_point!(label, Err(errors::transient(Phase::Copy, None, msg)));
            }
        }
    }
    Ok(true)
}
