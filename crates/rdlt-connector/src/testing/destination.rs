//! Destination clauses.

mod changes;
mod checks;
mod children;
mod clauses;
mod discard;
mod dropped;
mod encoding;
mod evolving;
mod fence;
mod history;
mod idempotent;
mod lanes;
mod names;
mod owned;
mod read;
mod rows;
mod tables;
mod widenings;

use std::time::{SystemTime, UNIX_EPOCH};

use arrow_array::RecordBatch;
use bytes::Bytes;

pub use clauses::DESTINATION_CLAUSES;
#[cfg(test)]
pub(in crate::testing) use names::simply_folded;
pub use read::read_back_integers;

use super::{
    Clause, ClauseResult, Observed, Outcome, Report, Violation, bounded, bounded_call, outcome,
};
use crate::commit::{CommitMeta, SegmentSet};
use crate::destination::{
    Destination, DestinationConnector, DestinationFactory, DestinationSession, DestinationWriter,
    OpenContext, OpenedSession, TableChange, TableRef, destination_factory,
};
use crate::error::{ConnectorErrorKind, Result};
use crate::id::{CommitSeq, Epoch, LoadId, PipelineId, SchemaVersion, SegmentId, TablePath};
use crate::spec::{BoxFuture, ConnectContext};
use crate::state::{StateChange, StateRecord};
use rows::{STALE, expect_rows, rows, schema};

/// Reads what a destination has published, so clauses can compare it with what was committed.
pub trait Probe: Send + Sync {
    /// Every published batch of `table`.
    fn published<'a>(&'a self, table: &'a TableRef) -> BoxFuture<'a, Result<Vec<RecordBatch>>>;

    /// Whether this probe reads anything: [`Unprobed`] does not.
    fn reads(&self) -> bool {
        true
    }
}

/// The probe of a destination whose published data cannot be read, as one reached only over the
/// wire: the clauses that compare it with what was committed are not observed.
#[derive(Clone, Copy, Debug, Default)]
pub struct Unprobed;

impl Probe for Unprobed {
    fn published<'a>(&'a self, table: &'a TableRef) -> BoxFuture<'a, Result<Vec<RecordBatch>>> {
        let message = format!("the published data of {} cannot be read", table.name);
        Box::pin(async move {
            Err(crate::ConnectorError::new(
                ConnectorErrorKind::Unsupported,
                message,
            ))
        })
    }

    fn reads(&self) -> bool {
        false
    }
}

/// Certifies destination connector `C` with `config`, reading published data through `probe`.
pub async fn certify_destination<C: DestinationConnector>(
    config: serde_json::Value,
    probe: &dyn Probe,
) -> Report {
    certify_destination_factory(destination_factory::<C>().as_ref(), config, probe).await
}

/// Certifies the destination `factory` creates from `config`, reading published data through `probe`.
///
/// The factory connects twice with the same configuration, so clauses can play two workers of one
/// pipeline; every run uses its own pipelines, tables and load ids, so a store can be certified
/// again.
pub async fn certify_destination_factory(
    factory: &dyn DestinationFactory,
    config: serde_json::Value,
    probe: &dyn Probe,
) -> Report {
    certify_destination_factory_observed(factory, config, probe, &Observed::new()).await
}

/// Certifies the destination `factory` creates from `config`, as
/// [`certify_destination_factory`] does, telling `observed` each clause's result as its check
/// ends: what was found is then known of a certification that is cut.
pub async fn certify_destination_factory_observed(
    factory: &dyn DestinationFactory,
    config: serde_json::Value,
    probe: &dyn Probe,
    observed: &Observed,
) -> Report {
    let connector = factory.spec().id.to_string();
    observed.named(&connector);
    let connections = async {
        let destination = bounded_call(
            "connect",
            factory.connect(config.clone(), ConnectContext::new()),
        )
        .await?;
        let peer = bounded_call("connect", factory.connect(config, ConnectContext::new())).await?;
        Ok::<_, Violation>((destination, peer))
    };
    let results = match connections.await {
        Ok((destination, peer)) => {
            let started = started();
            let mut results = Vec::new();
            for (index, clause) in DESTINATION_CLAUSES.iter().enumerate() {
                let bench = Bench {
                    destination: destination.as_ref(),
                    peer: peer.as_ref(),
                    probe,
                    started,
                    index,
                };
                let unread = !probe.reads() && clauses::PROBED.contains(&clause.id);
                let inapplicable = evolving::inapplicable(destination.as_ref(), clause.id);
                let unobserved = evolving::unobserved(destination.as_ref(), clause.id);
                let outcome = match (inapplicable, unobserved) {
                    (Some(reason), _) => Outcome::Inapplicable(reason.into()),
                    (None, Some(reason)) => Outcome::Unobserved(reason.into()),
                    (None, None) if unread => Outcome::Unobserved(UNREAD.into()),
                    (None, None) => outcome(super::timed(bench.check(clause.id)).await),
                };
                let result = ClauseResult {
                    clause: *clause,
                    outcome,
                    note: None,
                };
                observed.tell(result.clone());
                results.push(result);
            }
            results
        }
        Err(violation) => {
            let outcome = violation.of("connect failed").outcome();
            let failed = |clause: &Clause| ClauseResult {
                clause: *clause,
                outcome: outcome.clone(),
                note: None,
            };
            let failed: Vec<ClauseResult> = DESTINATION_CLAUSES.iter().map(failed).collect();
            failed
                .iter()
                .cloned()
                .for_each(|failed| observed.tell(failed));
            failed
        }
    };
    Report { connector, results }
}

/// Why a clause that reads published data is not observed without a probe that reads it.
const UNREAD: &str = "the destination's published data cannot be read";

/// When this run started: it names the run's pipelines and tables and times its load ids.
fn started() -> SystemTime {
    SystemTime::now()
}

/// One clause's own pipeline and table, so clauses never see each other's data.
struct Bench<'a> {
    /// The connection clauses use by default.
    destination: &'a dyn Destination,
    /// A second connection to the same store, playing another worker.
    peer: &'a dyn Destination,
    probe: &'a dyn Probe,
    started: SystemTime,
    index: usize,
}

impl Bench<'_> {
    async fn check(&self, id: &str) -> Result<(), Violation> {
        match id {
            "D-CHECK" => self.check_agrees_with_open().await,
            "D-EPOCH" => self.epochs_increase().await,
            "D-STAGING" => self.staging_is_invisible().await,
            "D-COMMIT" => self.commits_publish().await,
            "D-IDEMPOTENT" => self.recommits_are_idempotent().await,
            "D-STATE" => self.state_round_trips().await,
            "D-DISCARD" => self.earlier_staging_is_discarded().await,
            "D-REPLACE" => self.generations_swap_in_atomically().await,
            "D-SCHEMA" => self.schema_changes_apply().await,
            "D-MERGE" => self.merges_keep_newest_rows().await,
            "D-DELETE" => self.deletes_remove_rows().await,
            "D-PARTIAL" => self.partial_updates_keep_columns().await,
            "D-TRUNCATE" => self.truncates_remove_earlier_rows().await,
            "D-HIST" => self.histories_chain_versions().await,
            "D-CHILDREN" => self.children_follow_their_roots().await,
            "D-ENCODING" => self.dictionaries_publish_their_values().await,
            "D-TABLES" => self.segments_span_tables().await,
            "D-NAMES" => self.names_are_kept().await,
            "D-LANES" => self.writers_stage_at_once().await,
            "D-OWNED" => self.tables_belong_to_their_pipeline().await,
            "D-DROP" => self.drops_release_tables().await,
            _ => self.stale_sessions_are_fenced().await,
        }
    }

    /// This run's start in nanoseconds, which tells runs apart.
    fn run(&self) -> u64 {
        let nanos = self
            .started
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        u64::try_from(nanos).unwrap_or(u64::MAX)
    }

    /// This run's name for this clause's pipeline and table.
    fn name(&self) -> String {
        format!("certify_{:x}_{}", self.run(), self.index)
    }

    fn pipeline(&self) -> PipelineId {
        PipelineId::parse(self.name()).expect("certification pipeline ids are valid")
    }

    fn table(&self) -> TableRef {
        let name = self.name();
        TableRef {
            path: TablePath::new([name.as_str()]).expect("table paths are valid"),
            name: name.into(),
            version: SchemaVersion(1),
            generation: None,
            merge: None,
        }
    }

    /// Load ids unique across clauses and runs, as real load ids are: the random part holds the
    /// run, the clause and `load`, since two runs can start within the millisecond a load id keeps.
    fn load_id(&self, load: u8) -> LoadId {
        let mut random = [0; 16];
        random[6..14].copy_from_slice(&self.run().to_be_bytes());
        random[14] = u8::try_from(self.index).unwrap_or(u8::MAX);
        random[15] = load;
        LoadId::from_parts(self.started, u128::from_be_bytes(random))
    }

    async fn open(
        &self,
        destination: &dyn Destination,
        load: u8,
    ) -> Result<OpenedSession, Violation> {
        self.open_as(destination, self.pipeline(), load).await
    }

    /// Opens a session of `pipeline` on `destination`, for load `load`.
    async fn open_as(
        &self,
        destination: &dyn Destination,
        pipeline: PipelineId,
        load: u8,
    ) -> Result<OpenedSession, Violation> {
        let context = OpenContext {
            pipeline,
            load_id: self.load_id(load),
        };
        bounded("open", destination.open(&context))
            .await?
            .map_err(|error| Violation::from(format!("open: {error}")))
    }

    /// Opens a session on `destination` and stages three rows in each of `segments`.
    async fn staged(
        &self,
        destination: &dyn Destination,
        load: u8,
        segments: &[u64],
    ) -> Result<OpenedSession, Violation> {
        let mut opened = self.open(destination, load).await?;
        let mut writer = self.writer(&mut opened.session).await?;
        for segment in segments {
            writer
                .write(SegmentId(*segment), rows(*segment))
                .await
                .map_err(|error| Violation::from(format!("write: {error}")))?;
        }
        writer
            .flush()
            .await
            .map_err(|error| Violation::from(format!("flush: {error}")))?;
        Ok(opened)
    }

    /// Creates the clause's table in `session` and returns a writer for it.
    async fn writer(
        &self,
        session: &mut Box<dyn DestinationSession>,
    ) -> Result<Box<dyn DestinationWriter>, Violation> {
        self.writer_of(session, &self.table()).await
    }

    /// Creates `table`, of the certification schema, in `session` and returns a writer for it.
    async fn writer_of(
        &self,
        session: &mut Box<dyn DestinationSession>,
        table: &TableRef,
    ) -> Result<Box<dyn DestinationWriter>, Violation> {
        session
            .apply_schema(&TableChange::Create {
                table: table.clone(),
                schema: schema(),
            })
            .await
            .map_err(|error| Violation::from(format!("apply_schema: {error}")))?;
        session
            .writer(table)
            .await
            .map_err(|error| Violation::from(format!("writer: {error}")))
    }

    /// Opens through both connections, so an epoch kept in one connection's memory is caught.
    async fn epochs_increase(&self) -> Result<(), Violation> {
        let first = self.open(self.destination, 1).await?.epoch;
        let second = self.open(self.peer, 2).await?.epoch;
        if second > first {
            Ok(())
        } else {
            Err(format!("epoch went from {first} to {second}").into())
        }
    }

    async fn staging_is_invisible(&self) -> Result<(), Violation> {
        let _staged = self.staged(self.destination, 1, &[1]).await?;
        expect_rows(&self.published_rows().await?, &[])
    }

    /// Stages two segments and commits one: the other stays staged.
    async fn commits_publish(&self) -> Result<(), Violation> {
        let mut opened = self.staged(self.destination, 1, &[1, 2]).await?;
        let receipt = commit(
            &mut opened.session,
            &meta(self.load_id(1), opened.epoch, &[1], Vec::new()),
        )
        .await?;
        if receipt.rows != 3 {
            return Err(format!("the receipt reports {} rows, expected 3", receipt.rows).into());
        }
        expect_rows(&self.published_rows().await?, &[1])
    }

    /// Commits through one connection and reads back through the other, so state kept in one
    /// connection's memory is caught.
    async fn state_round_trips(&self) -> Result<(), Violation> {
        let record = StateRecord {
            key: "certify".to_owned(),
            value: Bytes::from_static(b"{\"v\":1}"),
        };
        let mut opened = self.open(self.destination, 1).await?;
        commit(
            &mut opened.session,
            &meta(
                self.load_id(1),
                opened.epoch,
                &[],
                vec![StateChange::Put(record.clone())],
            ),
        )
        .await?;
        let mut reopened = self.open(self.peer, 2).await?;
        if !reopened.state.contains(&record) {
            return Err("the next open did not return the committed record".into());
        }
        commit(
            &mut reopened.session,
            &meta(
                self.load_id(2),
                reopened.epoch,
                &[],
                vec![StateChange::Delete(record.key.clone())],
            ),
        )
        .await?;
        if self
            .open(self.destination, 3)
            .await?
            .state
            .iter()
            .any(|stored| stored.key == record.key)
        {
            return Err("a deleted record was returned by the next open".into());
        }
        Ok(())
    }
}

/// The first commit of `load`, opened at `epoch`.
fn meta(load: LoadId, epoch: Epoch, segments: &[u64], state_delta: Vec<StateChange>) -> CommitMeta {
    CommitMeta {
        load_id: load,
        commit_seq: CommitSeq::FIRST,
        epoch,
        segments: segments
            .iter()
            .copied()
            .map(SegmentId)
            .collect::<SegmentSet>(),
        abandoned: SegmentSet::new(),
        state_delta,
        finish_generations: Vec::new(),
        child_tables: Vec::new(),
        drop_tables: Vec::new(),
        horizon: None,
    }
}

async fn commit(
    session: &mut Box<dyn DestinationSession>,
    meta: &CommitMeta,
) -> Result<crate::commit::Receipt, Violation> {
    bounded("commit", session.commit(meta))
        .await?
        .map_err(|error| Violation::from(format!("commit: {error}")))
}
