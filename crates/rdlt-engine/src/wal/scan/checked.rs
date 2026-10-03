//! What a logged commit must hold to be replayed: everything it names, as its load wrote it.

use std::collections::BTreeSet;

use rdlt_connector::CommitSeq;

use super::{Of, Scanned};
use crate::error::Error;
use crate::wal::frame::{BegunPhase, Committing, Seal};

/// Checks `commit` follows the seal and phase frames it counts, `sealing` and `beginning`.
pub(super) fn counted(
    of: Of<'_>,
    commit: &Committing,
    sealing: &[Seal],
    beginning: &[BegunPhase],
) -> Result<(), Error> {
    let seq = commit.meta.commit_seq;
    if usize::try_from(commit.seals).ok() != Some(sealing.len())
        || usize::try_from(commit.phases).ok() != Some(beginning.len())
    {
        return Err(of.unreadable(format!(
            "a chunk holds commit {} after {} seals and {} phases, where it counts {} and {}",
            seq.get(),
            sealing.len(),
            beginning.len(),
            commit.seals,
            commit.phases
        )));
    }
    Ok(())
}

/// Checks every commit of `scanned`, of `load`'s log, is one its load wrote: its own, in order,
/// dropping no table, a seal for each segment it publishes; and that each commit without a
/// receipt has every batch frame its seals count, once each.
pub(super) fn commits(scanned: &Scanned, of: Of<'_>) -> Result<(), Error> {
    let load = of.load;
    let header = scanned.header.as_ref();
    let mut previous: Option<CommitSeq> = None;
    for logged in &scanned.commits {
        let meta = &logged.meta;
        let seq = meta.commit_seq;
        if meta.load_id != load {
            return Err(of.unreadable(format!("commit {} is of load {}", seq.get(), meta.load_id)));
        }
        if header.map(|header| header.epoch) != Some(meta.epoch) {
            return Err(of.unreadable(format!(
                "commit {} is of another session than its log",
                seq.get()
            )));
        }
        if previous.is_some_and(|previous| previous >= seq) {
            return Err(of.unreadable(format!(
                "commit {} does not follow the commit before it",
                seq.get()
            )));
        }
        previous = Some(seq);
        if !meta.drop_tables.is_empty() {
            return Err(of.unreadable(format!(
                "commit {} drops tables, which no load logs",
                seq.get()
            )));
        }
        let mut sealed = BTreeSet::new();
        if !logged.seals.iter().all(|seal| sealed.insert(seal.segment)) {
            return Err(of.unreadable(format!("commit {} seals a segment twice", seq.get())));
        }
        if let Some(unsealed) = meta
            .segments
            .iter()
            .find(|segment| !sealed.contains(segment))
        {
            return Err(of.unreadable(format!(
                "commit {} publishes segment {}, which it does not seal",
                seq.get(),
                unsealed.0
            )));
        }
    }
    for logged in scanned.pending() {
        for seal in &logged.seals {
            whole(of, scanned, logged.meta.commit_seq, seal)?;
        }
    }
    Ok(())
}

/// Checks `scanned` holds every batch frame `seal` counts of its segment, once each.
fn whole(of: Of<'_>, scanned: &Scanned, seq: CommitSeq, seal: &Seal) -> Result<(), Error> {
    let seq = seq.get();
    let located = scanned
        .batches
        .get(&seal.segment)
        .map_or(&[][..], Vec::as_slice);
    let mut ordinals = BTreeSet::new();
    if !located
        .iter()
        .all(|located| ordinals.insert(located.ordinal))
    {
        return Err(of.unreadable(format!(
            "commit {seq} holds a batch of segment {} twice",
            seal.segment.0
        )));
    }
    let rows = located
        .iter()
        .try_fold(0_u64, |rows, located| rows.checked_add(located.rows));
    let batches = u64::try_from(located.len()).ok();
    if batches != Some(seal.batches) || rows != Some(seal.rows) {
        return Err(of.unreadable(format!(
            "commit {seq} seals segment {} of {} batches and {} rows, of which the log holds {} \
             batches",
            seal.segment.0,
            seal.batches,
            seal.rows,
            located.len()
        )));
    }
    Ok(())
}
