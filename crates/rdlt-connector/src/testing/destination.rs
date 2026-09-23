//! Destination clauses.

use std::sync::Arc;
use std::time::{Duration, UNIX_EPOCH};

use arrow_array::{Int64Array, RecordBatch, StringArray};
use bytes::Bytes;

use super::{Clause, ClauseResult, Outcome, Report, Violation, bounded, outcome};
use crate::commit::{CommitMeta, SegmentSet};
use crate::destination::{
    Destination, DestinationConnector, DestinationFactory, DestinationSession, OpenContext,
    OpenedSession, TableChange, TableRef, destination_factory,
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
pub async fn certify_destination_factory(
    factory: &dyn DestinationFactory,
    config: serde_json::Value,
    probe: &dyn Probe,
) -> Report {
    let connector = factory.spec().id.to_string();
    let results = match factory.connect(config, ConnectContext::new()).await {
        Ok(destination) => {
            let mut results = Vec::new();
            for (index, clause) in DESTINATION_CLAUSES.iter().enumerate() {
                let bench = Bench {
                    destination: destination.as_ref(),
                    probe,
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
        Err(error) => DESTINATION_CLAUSES
            .iter()
            .map(|clause| ClauseResult {
                clause: *clause,
                outcome: Outcome::Failed(format!("connect failed: {error}")),
            })
            .collect(),
    };
    Report { connector, results }
}

/// One clause's own pipeline and table, so clauses never see each other's data.
struct Bench<'a> {
    destination: &'a dyn Destination,
    probe: &'a dyn Probe,
    index: usize,
}

impl Bench<'_> {
    async fn check(&self, id: &str) -> Result<(), Violation> {
        match id {
            "D-CHECK" => bounded("check", self.destination.check())
                .await?
                .map_err(|error| Violation::from(error.to_string())),
            "D-EPOCH" => self.epochs_increase().await,
            "D-STAGING" => self.staging_is_invisible().await,
            "D-COMMIT" => self.commits_publish().await,
            "D-IDEMPOTENT" => self.recommits_are_idempotent().await,
            "D-STATE" => self.state_round_trips().await,
            "D-DISCARD" => self.earlier_staging_is_discarded().await,
            _ => self.stale_sessions_are_fenced().await,
        }
    }

    fn pipeline(&self) -> PipelineId {
        PipelineId::parse(format!("certify-{}", self.index))
            .expect("certification pipeline ids are valid")
    }

    fn table(&self) -> TableRef {
        let name = format!("certify_{}", self.index);
        TableRef {
            path: TablePath::new([name.as_str()]).expect("table paths are valid"),
            name: name.into(),
            version: SchemaVersion(1),
        }
    }

    /// Load ids unique across clauses, as real load ids are.
    fn load_id(&self, load: u128) -> LoadId {
        let clause = Duration::from_secs(u64::try_from(self.index).unwrap_or(u64::MAX));
        LoadId::from_parts(UNIX_EPOCH + clause, load)
    }

    async fn open(&self, load: u128) -> Result<OpenedSession, Violation> {
        let context = OpenContext {
            pipeline: self.pipeline(),
            load_id: self.load_id(load),
        };
        bounded("open", self.destination.open(&context))
            .await?
            .map_err(|error| Violation::from(format!("open: {error}")))
    }

    /// Opens a session, creates the table and stages three rows as `segment`.
    async fn staged(&self, load: u128, segment: SegmentId) -> Result<OpenedSession, Violation> {
        let mut opened = self.open(load).await?;
        let table = self.table();
        let schema = TableSchema::new(vec![
            Field::new("id", LogicalType::Int64, false),
            Field::new("name", LogicalType::Utf8, true),
        ])
        .expect("the certification schema is valid");
        opened
            .session
            .apply_schema(&TableChange::Create {
                table: table.clone(),
                schema,
            })
            .await
            .map_err(|error| Violation::from(format!("apply_schema: {error}")))?;
        let mut writer = opened
            .session
            .writer(&table)
            .await
            .map_err(|error| Violation::from(format!("writer: {error}")))?;
        writer
            .write(segment, rows())
            .await
            .map_err(|error| Violation::from(format!("write: {error}")))?;
        writer
            .flush()
            .await
            .map_err(|error| Violation::from(format!("flush: {error}")))?;
        Ok(opened)
    }

    async fn published_rows(&self) -> Result<usize, Violation> {
        let batches = self
            .probe
            .published(&self.table())
            .await
            .map_err(|error| Violation::from(format!("probe: {error}")))?;
        Ok(batches.iter().map(RecordBatch::num_rows).sum())
    }

    async fn epochs_increase(&self) -> Result<(), Violation> {
        let first = self.open(1).await?.epoch;
        let second = self.open(2).await?.epoch;
        if second > first {
            Ok(())
        } else {
            Err(format!("epoch went from {first} to {second}").into())
        }
    }

    async fn staging_is_invisible(&self) -> Result<(), Violation> {
        let _staged = self.staged(1, SegmentId(1)).await?;
        expect_rows(self.published_rows().await?, 0)
    }

    async fn commits_publish(&self) -> Result<(), Violation> {
        let mut opened = self.staged(1, SegmentId(1)).await?;
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

    async fn recommits_are_idempotent(&self) -> Result<(), Violation> {
        let mut opened = self.staged(1, SegmentId(1)).await?;
        let meta = meta(self.load_id(1), opened.epoch, &[1], Vec::new());
        let first = commit(&mut opened.session, &meta).await?;
        let second = commit(&mut opened.session, &meta).await?;
        if (first.load_id, first.commit_seq) != (second.load_id, second.commit_seq) {
            return Err("the second commit returned a different receipt".into());
        }
        expect_rows(self.published_rows().await?, 3)
    }

    async fn state_round_trips(&self) -> Result<(), Violation> {
        let record = StateRecord {
            key: "certify".to_owned(),
            value: Bytes::from_static(b"{\"v\":1}"),
        };
        let mut opened = self.open(1).await?;
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
        let mut reopened = self.open(2).await?;
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
            .open(3)
            .await?
            .state
            .iter()
            .any(|stored| stored.key == record.key)
        {
            return Err("a deleted record was returned by the next open".into());
        }
        Ok(())
    }

    async fn earlier_staging_is_discarded(&self) -> Result<(), Violation> {
        let abandoned = self.staged(1, SegmentId(2)).await?;
        drop(abandoned);
        let mut opened = self.open(2).await?;
        commit(
            &mut opened.session,
            &meta(self.load_id(2), opened.epoch, &[2], Vec::new()),
        )
        .await?;
        expect_rows(self.published_rows().await?, 0)
    }

    async fn stale_sessions_are_fenced(&self) -> Result<(), Violation> {
        let mut stale = self.staged(1, SegmentId(1)).await?;
        let _latest = self.open(2).await?;
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
