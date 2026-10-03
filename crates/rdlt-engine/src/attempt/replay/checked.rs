//! What a logged commit must be for replay to apply it: one a load of this destination wrote,
//! before the session replaying it opened, recording only what its frames back.

use rdlt_connector::{Epoch, LoadId, PipelineId, StateChange, StateEntry, StateKey};

use crate::error::Error;
use crate::limits::{WAL_FOREIGN, WAL_UNREADABLE};
use crate::wal::frame::Header;
use crate::wal::scan::Logged;

/// Checks that the log whose header is `header`, of `load`, was written for the destination
/// whose state names `origin` as the first load to commit there: the log names that load, or,
/// where no load has committed there yet, the log's own load, which would have been the first.
///
/// # Errors
///
/// `wal_foreign`, naming `pipeline`'s log, for a log written for another destination.
pub(super) fn bound(
    header: Option<&Header>,
    origin: Option<LoadId>,
    load: LoadId,
    pipeline: &PipelineId,
) -> Result<(), Error> {
    let named = header.map(|header| header.origin);
    if named == Some(origin.unwrap_or(load)) {
        return Ok(());
    }
    Err(Error::wal(format!(
        "the write-ahead log of load {load} of pipeline {pipeline} was written for another \
         destination than this one"
    ))
    .with_code(WAL_FOREIGN))
}

/// Checks `logged` is a commit logged by a session older than that of `epoch`, recording no
/// reset, its own receipt, no store but `store`, where it was logged, and no partition's
/// position but those its seals and phases set.
///
/// Whatever else its frames say a replay cannot confirm: a log is not authenticated, so what it
/// may change is held to what a load logs.
///
/// # Errors
///
/// `wal_unreadable`, naming `pipeline`'s log, for a commit that is not.
pub(super) fn checked(
    logged: &Logged,
    epoch: Epoch,
    store: Option<LoadId>,
    pipeline: &PipelineId,
) -> Result<(), Error> {
    let meta = &logged.meta;
    let seq = meta.commit_seq.get();
    let refused = |detail: String| {
        Error::wal(format!(
            "the write-ahead log of load {} of pipeline {pipeline} cannot be replayed: {detail}",
            meta.load_id
        ))
        .with_code(WAL_UNREADABLE)
    };
    if meta.epoch >= epoch {
        return Err(refused(format!(
            "commit {seq} was logged by session {}, which is not older than this one, {}",
            meta.epoch.0, epoch.0
        )));
    }
    for change in &meta.state_delta {
        if let Some(detail) = refusal(logged, store, change) {
            return Err(refused(format!("commit {seq} {detail}")));
        }
    }
    Ok(())
}

/// What `change`, of `logged`'s state, records that no load of a log in `store` records: a
/// reset, another commit's receipt, another load as the destination's first, another store, or
/// a position no seal or phase of the commit sets.
fn refusal(logged: &Logged, store: Option<LoadId>, change: &StateChange) -> Option<String> {
    let meta = &logged.meta;
    let key = match change {
        StateChange::Put(record) => &record.key,
        StateChange::Delete(key) => key,
    };
    let refused = match (StateKey::parse(key), change) {
        (Ok(StateKey::Reset(stream)), _) => {
            return Some(format!(
                "records a reset of stream {stream}, which no load logs"
            ));
        }
        (Ok(StateKey::Receipt), StateChange::Put(record)) => {
            let own = matches!(
                StateEntry::from_record(record),
                Ok(StateEntry::Receipt(receipt)) if receipt.answers(meta)
            );
            (!own).then_some("records another commit's receipt")
        }
        (Ok(StateKey::Origin), change) => (*change
            != StateChange::Put(StateEntry::Origin(meta.load_id).to_record()))
        .then_some("records another load as the destination's first"),
        (Ok(StateKey::LogStore), change) => store
            .is_none_or(|store| {
                *change != StateChange::Put(StateEntry::LogStore(store).to_record())
            })
            .then_some("records another store as the pipeline's logs'"),
        (Ok(StateKey::Partition(..)), StateChange::Put(record)) => {
            (!backed(logged, change, record))
                .then_some("records a position no seal or phase of it sets")
        }
        _ => None,
    };
    refused.map(str::to_owned)
}

/// Whether `change`, of `logged`'s state, putting `record`, a partition's position, is one a
/// seal of the commit or a phase it begins sets.
fn backed(logged: &Logged, change: &StateChange, record: &rdlt_connector::StateRecord) -> bool {
    let entry = StateEntry::from_record(record).ok();
    let sealed = logged.seals.iter().any(|seal| {
        entry.as_ref()
            == Some(&StateEntry::Partition {
                stream: seal.stream.clone(),
                partition: seal.partition.clone(),
                state: seal.state.clone(),
                load: logged.meta.load_id,
            })
    });
    sealed
        || logged
            .begun
            .iter()
            .any(|begun| begun.changes.contains(change))
}

#[cfg(test)]
mod tests;
