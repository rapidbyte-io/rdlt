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
mod landed;
mod lanes;
mod names;
mod owned;
mod read;
mod retried;
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
use crate::commit::{CommitKey, CommitMeta, Receipt, SegmentSet};
use crate::destination::{
    Destination, DestinationConnector, DestinationFactory, DestinationSession, DestinationWriter,
    OpenContext, OpenedSession, TableChange, TableRef, destination_factory,
};
use crate::error::{ConnectorError, ConnectorErrorKind, Result};
use crate::id::{Epoch, PipelineId, SchemaVersion, SegmentId, TablePath};
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

    /// Whether this probe drops a table's columns as someone outside the pipeline would: by
    /// default it does not, and `D-TABLES` then passes on what it reads alone, with a note.
    fn drops_columns(&self) -> bool {
        false
    }

    /// Drops `column` from `table`, a table rows are published into, as someone outside the
    /// pipeline would by hand; an `Unsupported` error where the probe
    /// [drops no columns](Self::drops_columns).
    fn drop_column<'a>(
        &'a self,
        table: &'a TableRef,
        column: &'a str,
    ) -> BoxFuture<'a, Result<()>> {
        let message = format!("the probe drops no column {column} of {}", table.name);
        Box::pin(async move {
            Err(ConnectorError::new(
                ConnectorErrorKind::Unsupported,
                message,
            ))
        })
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
            Err(ConnectorError::new(
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
/// pipeline; every run uses its own pipelines and tables, so a store can be certified again.
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
                    note: tables::noted(probe, clause.id, &outcome),
                    outcome,
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

/// When this run started: it names the run's pipelines and tables.
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
            "D-LANDED" => {
                self.landed_keys_are_answered_and_stale_commits_refused()
                    .await
            }
            "D-RETRY" => self.failed_commits_are_taken_again().await,
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

    async fn open(&self, destination: &dyn Destination) -> Result<OpenedSession, Violation> {
        self.open_as(destination, self.pipeline()).await
    }

    /// Opens a session of `pipeline` on `destination`.
    async fn open_as(
        &self,
        destination: &dyn Destination,
        pipeline: PipelineId,
    ) -> Result<OpenedSession, Violation> {
        let context = OpenContext { pipeline };
        bounded("open", destination.open(&context))
            .await?
            .map_err(|error| Violation::from(format!("open: {error}")))
    }

    /// Opens a session on `destination` and stages three rows in each of `segments`.
    async fn staged(
        &self,
        destination: &dyn Destination,
        segments: &[u64],
    ) -> Result<OpenedSession, Violation> {
        let mut opened = self.open(destination).await?;
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

    /// Opens three times, through both connections, so an epoch kept in one connection's memory
    /// is caught; the second session resets the pipeline, dropping its table where the destination
    /// drops tables and deleting its state, so an epoch kept with either is caught too.
    async fn epochs_increase(&self) -> Result<(), Violation> {
        let record = StateRecord {
            key: "certify".to_owned(),
            value: Bytes::from_static(b"{\"v\":1}"),
        };
        let mut first = self.staged(self.destination, &[1]).await?;
        let put = vec![StateChange::Put(record.clone())];
        commit(&mut first, meta(&[1], put)).await?;
        let mut second = self.open(self.peer).await?;
        let deleted = vec![StateChange::Delete(record.key)];
        let reset = if self.peer.capabilities().drop_tables {
            CommitMeta {
                state_delta: deleted,
                ..self.dropping()
            }
        } else {
            meta(&[], deleted)
        };
        commit(&mut second, reset).await?;
        let third = self.open(self.destination).await?.epoch;
        let epochs = [first.epoch, second.epoch, third];
        if epochs.windows(2).all(|pair| pair[0] < pair[1]) {
            Ok(())
        } else {
            Err(format!("epochs went {}, {} and {}", epochs[0], epochs[1], epochs[2]).into())
        }
    }

    async fn staging_is_invisible(&self) -> Result<(), Violation> {
        let _staged = self.staged(self.destination, &[1]).await?;
        expect_rows(&self.published_rows().await?, &[])
    }

    /// Stages two segments and commits one: the other stays staged.
    async fn commits_publish(&self) -> Result<(), Violation> {
        let mut opened = self.staged(self.destination, &[1, 2]).await?;
        let receipt = commit(&mut opened, meta(&[1], Vec::new())).await?;
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
        let mut opened = self.open(self.destination).await?;
        commit(
            &mut opened,
            meta(&[], vec![StateChange::Put(record.clone())]),
        )
        .await?;
        let mut reopened = self.open(self.peer).await?;
        if !reopened.state.contains(&record) {
            return Err("the next open did not return the committed record".into());
        }
        commit(
            &mut reopened,
            meta(&[], vec![StateChange::Delete(record.key.clone())]),
        )
        .await?;
        if self
            .open(self.destination)
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

/// A commit of `segments` with `state_delta`, which [`keyed`] keys as a session's next.
fn meta(segments: &[u64], state_delta: Vec<StateChange>) -> CommitMeta {
    CommitMeta {
        key: CommitKey {
            epoch: Epoch(0),
            seq: 0,
        },
        after: None,
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
    }
}

/// `meta` as `opened`'s next commit: keyed by the session's epoch and the place after the last
/// commit of it that landed, expecting the last commit that landed.
fn keyed(opened: &OpenedSession, meta: CommitMeta) -> CommitMeta {
    let landed = opened.landed.as_ref().map(|receipt| receipt.key);
    let seq = landed
        .filter(|key| key.epoch == opened.epoch)
        .map_or(1, |key| key.seq + 1);
    CommitMeta {
        key: CommitKey {
            epoch: opened.epoch,
            seq,
        },
        after: landed,
        ..meta
    }
}

/// Commits `meta` as `opened`'s next commit, which must land.
async fn commit(opened: &mut OpenedSession, meta: CommitMeta) -> Result<Receipt, Violation> {
    try_commit(opened, meta)
        .await?
        .map_err(|error| Violation::from(format!("commit: {error}")))
}

/// Sends `meta` as `opened`'s next commit, which may fail; one that lands is the session's last.
async fn try_commit(
    opened: &mut OpenedSession,
    meta: CommitMeta,
) -> Result<Result<Receipt>, Violation> {
    let meta = keyed(opened, meta);
    let answered = bounded("commit", opened.session.commit(&meta)).await?;
    if let Ok(receipt) = &answered {
        opened.landed = Some(receipt.clone());
    }
    Ok(answered)
}
