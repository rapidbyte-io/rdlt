//! Resetting streams: one commit that clears what a pipeline recorded of them (spec §16.1).

use std::sync::Arc;

use rdlt_connector::{
    CommitMeta, CommitSeq, Destination, DestinationSession, DroppedTable, Epoch, OpenContext,
    OpenedSession, PipelineId, PipelineState, Receipt, SegmentSet, Source, StateChange, StateEntry,
    StateKey, StreamName, TablePath,
};

use super::Engine;
use crate::deadline::Waits;
use crate::error::{Error, ErrorKind, Side};
use crate::naming::Naming;
use crate::scope::contained;

/// What a reset clears of each stream.
#[non_exhaustive]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ResetScope {
    /// Where the stream's reads stand: its phase, its partitions' positions, its full read in
    /// progress and its completed reads; the next run reads the stream from its beginning into
    /// the tables it has, and a replace stream fills a new generation.
    Positions,
    /// Its positions and its tables: each table of the stream, its child tables too, is dropped
    /// with its generations and tombstones and released to any pipeline, and the next run
    /// creates them anew.
    Tables,
}

/// What a reset did.
#[non_exhaustive]
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ResetReport {
    /// The streams reset.
    pub streams: Vec<StreamName>,
    /// The tables dropped.
    pub dropped: Vec<TablePath>,
    /// The epoch of the reset's session: what a session older than it logged of the streams
    /// never applies to them.
    pub epoch: Epoch,
}

impl Engine {
    /// Resets `streams` of `pipeline`, read from `source`, in `destination` as `scope` says, in
    /// one commit.
    ///
    /// Opening the destination fences every session of the pipeline opened before, so a run
    /// still loading fails its next commit and, retried, reads the streams from their beginning;
    /// the commit marks each stream with the reset's epoch, so rows such a run logged before it
    /// are never replayed into them. A stream the pipeline recorded nothing of is refused as
    /// `stream_not_found`, and one whose source cannot read again as `reset_unreplayable`; one
    /// reset before is not, so a reset retried is harmless.
    ///
    /// A reset of no streams, of a stream the source cannot read again, or of tables the
    /// destination cannot drop is refused before the destination is opened, fencing nothing. A
    /// stream the pipeline recorded nothing of is known only once it is: that refusal has fenced
    /// the pipeline's older sessions, as any reset does.
    ///
    /// A connector that panics fails the reset as an internal error, as it fails an attempt.
    pub async fn reset(
        &self,
        pipeline: &PipelineId,
        streams: &[StreamName],
        scope: ResetScope,
        source: Arc<dyn Source>,
        destination: Arc<dyn Destination>,
    ) -> Result<ResetReport, Error> {
        // A reset holds nothing but what decoding the connectors' answers takes: a budget of the
        // run's memory holds it.
        let budget = crate::budget::MemoryBudget::new(self.config.memory().get())
            .within(Arc::clone(&self.env), self.config.memory_wait());
        let waits =
            Waits::new(Arc::clone(&self.env), self.config.connector_wait()).charging(&budget);
        let (source, destination) = (waits.source(source), waits.destination(destination));
        let resetting = self.resetting(pipeline, streams, scope, source, destination);
        contained(resetting)
            .await
            .unwrap_or_else(|panic| Err(Error::internal(format!("the reset panicked: {panic}"))))
    }

    /// Resets as [`Engine::reset`] says, a connector's panic unwinding out of it.
    async fn resetting(
        &self,
        pipeline: &PipelineId,
        streams: &[StreamName],
        scope: ResetScope,
        source: Arc<dyn Source>,
        destination: Arc<dyn Destination>,
    ) -> Result<ResetReport, Error> {
        if streams.is_empty() {
            return Err(Error::config("a reset names no streams").with_code("no_streams"));
        }
        if scope == ResetScope::Tables && !destination.capabilities().drop_tables {
            return Err(Error::config(
                "the destination cannot drop tables, so a stream's tables cannot be reset",
            )
            .with_code("drop_unsupported"));
        }
        readable_again(source.as_ref(), streams).await?;
        self.logged_unreplayable(pipeline, streams).await?;
        let naming = Naming::checked(&destination.capabilities().identifiers)?;
        let load_id = self.env.load_id();
        let context = OpenContext {
            pipeline: pipeline.clone(),
            load_id,
        };
        let OpenedSession {
            mut session,
            epoch,
            state,
        } = destination.open(&context).await.map_err(|error| {
            Error::connector(Side::Destination, "opening the destination", error)
        })?;
        let reset = cleared(&state, &naming, streams, scope, epoch);
        let committed = match reset {
            Ok(cleared) => self.commit(&mut *session, load_id, epoch, cleared).await,
            Err(error) => Err(error),
        };
        let closed = session.close().await.map_err(|error| {
            Error::connector(Side::Destination, "closing the reset's session", error)
        });
        let dropped = committed?;
        closed?;
        Ok(ResetReport {
            streams: streams.to_vec(),
            dropped,
            epoch,
        })
    }

    /// Commits `cleared` in `session`, opened at `epoch` for `load_id`; the tables it dropped.
    async fn commit(
        &self,
        session: &mut dyn DestinationSession,
        load_id: rdlt_connector::LoadId,
        epoch: Epoch,
        cleared: Cleared,
    ) -> Result<Vec<TablePath>, Error> {
        let Cleared {
            mut state_delta,
            drop_tables,
        } = cleared;
        let receipt = Receipt {
            load_id,
            commit_seq: CommitSeq::FIRST,
            committed_at: self.env.now(),
            rows: 0,
            bytes: 0,
        };
        state_delta.push(StateChange::Put(StateEntry::Receipt(receipt).to_record()));
        let dropped = drop_tables.iter().map(|table| table.path.clone()).collect();
        let meta = CommitMeta {
            load_id,
            commit_seq: CommitSeq::FIRST,
            epoch,
            segments: SegmentSet::new(),
            state_delta,
            finish_generations: Vec::new(),
            child_tables: Vec::new(),
            drop_tables,
            horizon: None,
        };
        let receipt = session
            .commit(&meta)
            .await
            .map_err(|error| Error::connector(Side::Destination, "committing the reset", error))?;
        crate::table::answered(&meta, receipt)?;
        Ok(dropped)
    }
}

impl Engine {
    /// Refuses `streams` of `pipeline` a log holds rows of that their source was told were
    /// committed and that never landed, as `reset_unreplayable`: the reset would discard the only
    /// copy of them, whatever the source says of the stream now.
    ///
    /// A run of the pipeline replays the log, landing them, after which the reset may go ahead.
    async fn logged_unreplayable(
        &self,
        pipeline: &PipelineId,
        streams: &[StreamName],
    ) -> Result<(), Error> {
        let Some(store) = self.env.wal() else {
            return Ok(());
        };
        let frame_bytes = crate::wal::frame::limits(self.config.memory().get()).frame_bytes;
        for load in store.loads(pipeline).await.map_err(Error::from_wal)? {
            let scanned =
                crate::wal::scan::scan(store.as_ref(), pipeline, load, frame_bytes).await?;
            let unlanded = scanned
                .pending()
                .flat_map(|logged| &logged.seals)
                .find(|seal| !seal.replayable && streams.contains(&seal.stream));
            if let Some(seal) = unlanded {
                let stream = &seal.stream;
                return Err(Error::config(format!(
                    "stream {stream}: the log of load {load} holds rows its source was told \
                     were committed and that never landed; a run of the pipeline lands them"
                ))
                .with_code("reset_unreplayable")
                .with_stream(stream));
            }
        }
        Ok(())
    }
}

/// Refuses `streams` whose source cannot read again what it acknowledged, as
/// `reset_unreplayable`: read from its beginning, it would wait for rows it no longer holds.
///
/// A stream the source no longer serves is not refused; what a load logged of it before the
/// reset never lands after it, so the reset keeps its word however the stream was read.
async fn readable_again(source: &dyn Source, streams: &[StreamName]) -> Result<(), Error> {
    let catalog = source.discover().await.map_err(|error| {
        Error::connector(Side::Source, "discovering the source's streams", error)
    })?;
    let unreplayable = streams.iter().find(|stream| {
        catalog
            .get(stream)
            .is_some_and(|spec| !spec.is_replayable())
    });
    match unreplayable {
        Some(stream) => Err(Error::config(format!(
            "stream {stream}: its source cannot read again what it acknowledged, so it cannot \
             be read from its beginning"
        ))
        .with_code("reset_unreplayable")
        .with_stream(stream)),
        None => Ok(()),
    }
}

/// What a reset commit changes.
struct Cleared {
    state_delta: Vec<StateChange>,
    drop_tables: Vec<DroppedTable>,
}

/// What resetting `streams` as `scope` says changes of `records`, in a session opened at
/// `epoch`.
///
/// A reset is how a pipeline recovers, so recorded names do not hold it back: what it resets is
/// forgotten whatever its names, and a table is dropped only under a name `naming` admits as a
/// table's, never one under a prefix the destination keeps for its own tables.
fn cleared(
    records: &[rdlt_connector::StateRecord],
    naming: &Naming,
    streams: &[StreamName],
    scope: ResetScope,
    epoch: Epoch,
) -> Result<Cleared, Error> {
    let state = PipelineState::from_records(records).map_err(|error| {
        Error::new(
            ErrorKind::Destination,
            format!("reading pipeline state: {error}"),
        )
        .with_code("state_invalid")
    })?;
    let mut cleared = Cleared {
        state_delta: Vec::new(),
        drop_tables: Vec::new(),
    };
    // A table recorded under another table's identifier too is forgotten, never dropped: the
    // drop would reach the other table.
    let shared = crate::table::shared(&state);
    for stream in streams {
        recorded(&state, stream)?;
        let family = family(&state, stream);
        cleared.state_delta.extend(positions(&state, stream));
        if scope == ResetScope::Tables {
            for path in family {
                let table = &state.tables[&path];
                let owned = table
                    .physical
                    .as_ref()
                    .filter(|name| naming.admits_table(name) && !shared.contains(name.as_ref()));
                if let Some(name) = owned {
                    cleared.drop_tables.push(DroppedTable {
                        path: path.clone(),
                        name: Arc::clone(name),
                    });
                }
                for key in [
                    StateKey::Schema(path.clone()),
                    StateKey::Names(path.clone()),
                    StateKey::Sequences(path),
                ] {
                    cleared.state_delta.push(StateChange::Delete(key.encode()));
                }
            }
        }
        let marker = StateEntry::Reset {
            stream: stream.clone(),
            epoch,
        };
        cleared
            .state_delta
            .push(StateChange::Put(marker.to_record()));
    }
    Ok(cleared)
}

/// Checks `state` records `stream` under its own name, and no other stream displayed as it is:
/// a stream's tables are named by its displayed name, so a reset of one stream must not reach
/// another's.
///
/// # Errors
///
/// `stream_not_found` for a stream state records nothing of; `stream_ambiguous` for one another
/// recorded stream displays as.
fn recorded(state: &PipelineState, stream: &StreamName) -> Result<(), Error> {
    let mut known = state.streams.keys().chain(state.resets.keys());
    if !known.clone().any(|known| known == stream) {
        return Err(Error::config(format!(
            "stream {stream}: the pipeline recorded nothing of it"
        ))
        .with_code("stream_not_found")
        .with_stream(stream));
    }
    let displayed = stream.to_string();
    if known.any(|known| known != stream && known.to_string() == displayed) {
        return Err(Error::config(format!(
            "stream {stream}: another stream the pipeline recorded is displayed as it is, and \
             their tables cannot be told apart"
        ))
        .with_code("stream_ambiguous")
        .with_stream(stream));
    }
    Ok(())
}

/// The changes deleting where `stream`'s reads stand in `state`.
fn positions(state: &PipelineState, stream: &StreamName) -> Vec<StateChange> {
    let partitions = state
        .streams
        .get(stream)
        .into_iter()
        .flat_map(|recorded| recorded.partitions.keys())
        .map(|partition| StateKey::Partition(stream.clone(), partition.clone()));
    [
        StateKey::Phase(stream.clone()),
        StateKey::Generation(stream.clone()),
        StateKey::Completed(stream.clone()),
    ]
    .into_iter()
    .chain(partitions)
    .map(|key| StateChange::Delete(key.encode()))
    .collect()
}

/// The paths of `stream`'s tables `state` records: its own, and its child tables', whose paths
/// begin with its own.
fn family(state: &PipelineState, stream: &StreamName) -> Vec<TablePath> {
    let root = stream.to_string();
    state
        .tables
        .keys()
        .filter(|path| path.segments().next() == Some(root.as_str()))
        .cloned()
        .collect()
}

#[cfg(test)]
mod tests;
