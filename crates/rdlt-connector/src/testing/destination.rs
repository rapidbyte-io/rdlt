//! Destination clauses.

use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use arrow_array::{Int64Array, RecordBatch, StringArray};
use bytes::Bytes;

use super::{Clause, ClauseResult, Outcome, Report, Violation, bounded, bounded_call, outcome};
use crate::commit::{CommitMeta, SegmentSet};
use crate::destination::{
    Destination, DestinationConnector, DestinationFactory, DestinationSession, DestinationWriter,
    OpenContext, OpenedSession, TableChange, TableRef, destination_factory,
};
use crate::error::{ConnectorErrorKind, Result};
use crate::id::{CommitSeq, Epoch, LoadId, PipelineId, SchemaVersion, SegmentId, TablePath};
use crate::schema::TableSchema;
use crate::spec::{BoxFuture, ConnectContext};
use crate::state::{StateChange, StateRecord};
use crate::types::{Field, LogicalType};

/// Reads what a destination has published, so clauses can compare it with what was committed.
pub trait Probe: Send + Sync {
    /// Every published batch of `table`.
    fn published<'a>(&'a self, table: &'a TableRef) -> BoxFuture<'a, Result<Vec<RecordBatch>>>;
}

/// The clauses [`certify_destination`] checks, in order.
pub const DESTINATION_CLAUSES: &[Clause] = &[
    Clause {
        id: "D-CHECK",
        statement: "check succeeds for a valid configuration",
    },
    Clause {
        id: "D-EPOCH",
        statement: "each open returns a higher epoch than the last",
    },
    Clause {
        id: "D-STAGING",
        statement: "staged segments are invisible until committed",
    },
    Clause {
        id: "D-COMMIT",
        statement: "a commit publishes exactly its segments and reports their rows",
    },
    Clause {
        id: "D-IDEMPOTENT",
        statement: "re-committing a commit returns its receipt and publishes nothing",
    },
    Clause {
        id: "D-STATE",
        statement: "committed state records are returned by the next open",
    },
    Clause {
        id: "D-DISCARD",
        statement: "segments staged by an earlier session are never published",
    },
    Clause {
        id: "D-FENCE",
        statement: "a session opened before the latest one cannot commit",
    },
];

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
    let connector = factory.spec().id.to_string();
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
                let outcome = outcome(bench.check(clause.id).await);
                results.push(ClauseResult {
                    clause: *clause,
                    outcome,
                });
            }
            results
        }
        Err(Violation(reason)) => DESTINATION_CLAUSES
            .iter()
            .map(|clause| ClauseResult {
                clause: *clause,
                outcome: Outcome::Failed(format!("connect failed: {reason}")),
            })
            .collect(),
    };
    Report { connector, results }
}

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
            "D-CHECK" => bounded_call("check", self.destination.check()).await,
            "D-EPOCH" => self.epochs_increase().await,
            "D-STAGING" => self.staging_is_invisible().await,
            "D-COMMIT" => self.commits_publish().await,
            "D-IDEMPOTENT" => self.recommits_are_idempotent().await,
            "D-STATE" => self.state_round_trips().await,
            "D-DISCARD" => self.earlier_staging_is_discarded().await,
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
        let context = OpenContext {
            pipeline: self.pipeline(),
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
                .write(SegmentId(*segment), rows())
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
        let table = self.table();
        let schema = TableSchema::new(vec![
            Field::new("id", LogicalType::Int64, false),
            Field::new("name", LogicalType::Utf8, true),
        ])
        .expect("the certification schema is valid");
        session
            .apply_schema(&TableChange::Create {
                table: table.clone(),
                schema,
            })
            .await
            .map_err(|error| Violation::from(format!("apply_schema: {error}")))?;
        session
            .writer(&table)
            .await
            .map_err(|error| Violation::from(format!("writer: {error}")))
    }

    async fn published_rows(&self) -> Result<usize, Violation> {
        let batches = self
            .probe
            .published(&self.table())
            .await
            .map_err(|error| Violation::from(format!("probe: {error}")))?;
        Ok(batches.iter().map(RecordBatch::num_rows).sum())
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
        expect_rows(self.published_rows().await?, 0)
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
        expect_rows(self.published_rows().await?, 3)
    }

    /// Replays a committed load the way recovery does: another worker opens the same load,
    /// stages its segment again and re-commits the same `(load_id, commit_seq)`.
    async fn recommits_are_idempotent(&self) -> Result<(), Violation> {
        let mut first = self.staged(self.destination, 1, &[1]).await?;
        let original = commit(
            &mut first.session,
            &meta(self.load_id(1), first.epoch, &[1], Vec::new()),
        )
        .await?;
        let mut replay = self.staged(self.peer, 1, &[1]).await?;
        let replayed = commit(
            &mut replay.session,
            &meta(self.load_id(1), replay.epoch, &[1], Vec::new()),
        )
        .await?;
        if replayed != original {
            return Err(format!(
                "the re-commit returned {replayed:?}, not the stored receipt {original:?}"
            )
            .into());
        }
        expect_rows(self.published_rows().await?, 3)
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

    /// Neither an abandoned session's staging nor a fenced worker's late write reaches the
    /// latest session's commit.
    async fn earlier_staging_is_discarded(&self) -> Result<(), Violation> {
        let abandoned = self.staged(self.destination, 1, &[2]).await?;
        drop(abandoned);
        let mut stale = self.open(self.destination, 2).await?;
        let mut stale_writer = self.writer(&mut stale.session).await?;
        let mut latest = self.open(self.peer, 3).await?;
        // A fenced worker may still be running; whether its write fails or is ignored is the
        // destination's choice, but it must never be published.
        drop(stale_writer.write(SegmentId(1), rows()).await);
        drop(stale_writer.flush().await);
        let mut writer = self.writer(&mut latest.session).await?;
        writer
            .write(SegmentId(1), rows())
            .await
            .map_err(|error| Violation::from(format!("write: {error}")))?;
        writer
            .flush()
            .await
            .map_err(|error| Violation::from(format!("flush: {error}")))?;
        commit(
            &mut latest.session,
            &meta(self.load_id(3), latest.epoch, &[1], Vec::new()),
        )
        .await?;
        // Committing the abandoned session's segment may fail or succeed, but publishes nothing.
        let orphan = CommitMeta {
            commit_seq: CommitSeq::FIRST.next(),
            ..meta(self.load_id(3), latest.epoch, &[2], Vec::new())
        };
        drop(bounded("commit", latest.session.commit(&orphan)).await?);
        expect_rows(self.published_rows().await?, 3)
    }

    /// A worker's session is fenced by an open through another connection.
    async fn stale_sessions_are_fenced(&self) -> Result<(), Violation> {
        let mut stale = self.staged(self.destination, 1, &[1]).await?;
        let _latest = self.open(self.peer, 2).await?;
        let meta = meta(self.load_id(1), stale.epoch, &[1], Vec::new());
        match bounded("commit", stale.session.commit(&meta)).await? {
            Err(error) if error.kind() == ConnectorErrorKind::Fenced => {
                expect_rows(self.published_rows().await?, 0)
            }
            Err(error) => Err(format!(
                "the stale commit failed with {:?}, not Fenced: {error}",
                error.kind()
            )
            .into()),
            Ok(_) => Err("a session opened before the latest one committed".into()),
        }
    }
}

fn rows() -> RecordBatch {
    let ids: Arc<Int64Array> = Arc::new(Int64Array::from(vec![1, 2, 3]));
    let names: Arc<StringArray> = Arc::new(StringArray::from(vec![Some("ann"), None, Some("ola")]));
    RecordBatch::try_from_iter([("id", ids as _), ("name", names as _)])
        .expect("the certification batch is valid")
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
        state_delta,
        finish_generations: Vec::new(),
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

fn expect_rows(actual: usize, expected: usize) -> Result<(), Violation> {
    if actual == expected {
        Ok(())
    } else {
        Err(format!("{actual} rows are published, expected {expected}").into())
    }
}
