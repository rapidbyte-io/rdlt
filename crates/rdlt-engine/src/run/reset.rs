//! Resetting streams: one commit that clears what a pipeline recorded of them (spec §16.1).

use std::sync::Arc;

use rdlt_connector::{
    CommitMeta, CommitSeq, Destination, DestinationSession, DroppedTable, Epoch, OpenContext,
    OpenedSession, PipelineId, PipelineState, Receipt, SegmentSet, Source, StateChange, StateEntry,
    StateKey, StreamName, TablePath,
};

use super::Engine;
use crate::error::{Error, ErrorKind, Side};
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
    /// A connector that panics fails the reset as an internal error, as it fails an attempt.
    pub async fn reset(
        &self,
        pipeline: &PipelineId,
        streams: &[StreamName],
        scope: ResetScope,
        source: Arc<dyn Source>,
        destination: Arc<dyn Destination>,
    ) -> Result<ResetReport, Error> {
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
        if scope == ResetScope::Tables && !destination.capabilities().drop_tables {
            return Err(Error::config(
                "the destination cannot drop tables, so a stream's tables cannot be reset",
            )
            .with_code("drop_unsupported"));
        }
        readable_again(source.as_ref(), streams).await?;
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
        let reset = cleared(&state, streams, scope, epoch);
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
        };
        session
            .commit(&meta)
            .await
            .map_err(|error| Error::connector(Side::Destination, "committing the reset", error))?;
        Ok(dropped)
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

/// What resetting `streams` as `scope` says changes of `records`, in a session opened at `epoch`.
fn cleared(
    records: &[rdlt_connector::StateRecord],
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
    for stream in streams {
        let family = family(&state, stream);
        let recorded = state.streams.contains_key(stream)
            || state.resets.contains_key(stream)
            || !family.is_empty();
        if !recorded {
            return Err(Error::config(format!(
                "stream {stream}: the pipeline recorded nothing of it"
            ))
            .with_code("stream_not_found")
            .with_stream(stream));
        }
        cleared.state_delta.extend(positions(&state, stream));
        if scope == ResetScope::Tables {
            for path in family {
                let table = &state.tables[&path];
                if let Some(name) = &table.physical {
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
