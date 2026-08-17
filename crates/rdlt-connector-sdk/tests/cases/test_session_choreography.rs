//! The choreography pinned by OBSERVATION, not outcome: the black-box
//! conformance kit cannot distinguish the framework's replay
//! choreography from a coincidentally-idempotent backend, so a spy
//! backend records every call and these tests assert the exact
//! sequence the framework promises.

use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use rdlt_connector::arrow::RecordBatch;
use rdlt_connector::core::commit::{self, CommitMeta, CommitReceipt, WriteMode};
use rdlt_connector::core::id::{LoadId, PipelineId, TableName};
use rdlt_connector::core::schema::TableSchema;
use rdlt_connector::core::state::StateDoc;
use rdlt_connector::destination::{Capabilities, Destination, LoadSession, OpenContext};
use rdlt_connector::error::DestinationError;
use rdlt_connector_sdk::destination::{self, Backend, DestinationConnector};
use rdlt_testkit::{batch_of, schema_for};

use super::example::{TickerConfig, TickerError};

#[derive(Debug, PartialEq, Eq, Clone)]
enum Call {
    EnsureTable(String),
    Write(String),
    ExistingReceipt,
    Replay,
    Publish,
}

type Log = Arc<Mutex<Vec<Call>>>;

/// What the spy's `existing_receipt` answers.
#[derive(Clone)]
enum ReceiptScript {
    None,
    Found(CommitReceipt),
    /// Answer with a receipt whose IDENTITY disagrees with the lookup —
    /// the stale-cache/unkeyed-read bug shape: the token names a
    /// different commit than the one asked about.
    Mismatched(CommitReceipt),
    /// Lookup finds nothing, but `publish` RETURNS a receipt naming a
    /// different commit — the mirror bug shape.
    PublishForges(CommitReceipt),
    Errors,
}

struct Spy {
    log: Log,
    receipt: ReceiptScript,
}

struct SpyBackend {
    log: Log,
    receipt: ReceiptScript,
}

#[async_trait]
impl DestinationConnector for Spy {
    const NAME: &'static str = "spy";
    const VERSION: &'static str = "0.0.0";
    type Config = TickerConfig;
    type Backend = SpyBackend;

    fn assemble(_config: TickerConfig) -> Result<Self, TickerError> {
        unreachable!("spies are built directly by the tests")
    }

    fn capabilities(&self) -> Capabilities {
        Capabilities::default()
    }

    async fn connect(&self, _context: &OpenContext) -> Result<SpyBackend, DestinationError> {
        Ok(SpyBackend {
            log: Arc::clone(&self.log),
            receipt: self.receipt.clone(),
        })
    }
}

#[async_trait]
impl Backend for SpyBackend {
    async fn ensure_table(
        &mut self,
        schema: &TableSchema,
        _mode: &WriteMode,
    ) -> Result<(), DestinationError> {
        self.log
            .lock()
            .expect("log lock")
            .push(Call::EnsureTable(schema.table.as_str().to_owned()));
        Ok(())
    }

    async fn write(
        &mut self,
        table: &TableName,
        _batch: RecordBatch,
    ) -> Result<(), DestinationError> {
        self.log
            .lock()
            .expect("log lock")
            .push(Call::Write(table.as_str().to_owned()));
        Ok(())
    }

    async fn existing_receipt(
        &mut self,
        load_id: &LoadId,
        commit_seq: u64,
    ) -> Result<Option<CommitReceipt>, DestinationError> {
        self.log
            .lock()
            .expect("log lock")
            .push(Call::ExistingReceipt);
        match &self.receipt {
            ReceiptScript::None | ReceiptScript::PublishForges(_) => Ok(None),
            ReceiptScript::Found(receipt) => Ok(Some(receipt.clone())),
            ReceiptScript::Mismatched(receipt) => Ok(Some(receipt.clone())),
            ReceiptScript::Errors => Err(DestinationError::fatal(format!(
                "receipt lookup failed for {load_id}/{commit_seq}"
            ))),
        }
    }

    async fn replay(
        &mut self,
        _meta: &CommitMeta,
        _receipt: &CommitReceipt,
    ) -> Result<(), DestinationError> {
        self.log.lock().expect("log lock").push(Call::Replay);
        Ok(())
    }

    async fn publish(&mut self, meta: CommitMeta) -> Result<CommitReceipt, DestinationError> {
        self.log.lock().expect("log lock").push(Call::Publish);
        if let ReceiptScript::PublishForges(forged) = &self.receipt {
            return Ok(forged.clone());
        }
        Ok(CommitReceipt {
            load_id: meta.load_id.clone(),
            commit_seq: meta.commit_seq,
        })
    }

    async fn read_state(
        &mut self,
        _pipeline: &PipelineId,
    ) -> Result<Option<StateDoc>, DestinationError> {
        Ok(None)
    }
}

async fn open_spy(receipt: ReceiptScript) -> (Box<dyn LoadSession>, Log) {
    let log = Log::default();
    let shell = destination::shell(Spy {
        log: Arc::clone(&log),
        receipt,
    });
    let session = shell
        .open(OpenContext::new(PipelineId::from("p"), LoadId::from("l")))
        .await
        .expect("open");
    (session, log)
}

fn meta() -> CommitMeta {
    CommitMeta {
        load_id: LoadId::from("l"),
        commit_seq: 4,
        state: StateDoc::new(PipelineId::from("p"), env!("CARGO_PKG_VERSION")),
        counters: commit::Counters::default(),
    }
}

fn calls(log: &Log) -> Vec<Call> {
    log.lock().expect("log lock").clone()
}

/// Exactly-once as the framework's own promise: the receipt lookup
/// precedes the publish, and a first commit publishes exactly once.
#[tokio::test]
async fn a_first_commit_consults_the_receipt_then_publishes_once() {
    let (mut session, log) = open_spy(ReceiptScript::None).await;
    let receipt = session.commit(meta()).await.expect("commit");
    assert_eq!((receipt.load_id.as_str(), receipt.commit_seq), ("l", 4));
    assert_eq!(calls(&log), vec![Call::ExistingReceipt, Call::Publish]);
}

/// A replayed commit returns the STORED receipt after the backend's
/// replay housekeeping — the publish is never reached, which is the
/// choreography the black-box kit cannot see past an idempotent
/// backend.
#[tokio::test]
async fn a_replayed_commit_runs_replay_housekeeping_and_never_publishes() {
    let stored = CommitReceipt {
        load_id: LoadId::from("l"),
        commit_seq: 4,
    };
    let (mut session, log) = open_spy(ReceiptScript::Found(stored.clone())).await;
    let receipt = session.commit(meta()).await.expect("replayed commit");
    assert_eq!(receipt, stored);
    assert_eq!(calls(&log), vec![Call::ExistingReceipt, Call::Replay]);
}

/// The receipt is the idempotence TOKEN, and one naming a
/// different commit proves nothing — a backend whose lookup returns a
/// mismatched receipt (stale cache, unkeyed read) must fail the commit
/// LOUDLY, never skip the publish and let the engine mark the WAL
/// committed over rows that exist nowhere.
#[tokio::test]
async fn a_mismatched_receipt_identity_fails_the_commit_without_publishing() {
    let wrong_identity = CommitReceipt {
        load_id: LoadId::from("other-load"),
        commit_seq: 4,
    };
    let (mut session, log) = open_spy(ReceiptScript::Mismatched(wrong_identity)).await;
    let error = session
        .commit(meta())
        .await
        .expect_err("a receipt naming a different commit must refuse");
    let rendered = error.to_string();
    assert!(
        rendered.contains("other-load"),
        "the refusal names the forged identity: {rendered}"
    );
    assert!(
        rendered.contains("proves nothing"),
        "the refusal states why: {rendered}"
    );
    // Neither replay housekeeping nor publish ran: the choreography
    // stopped at the token it could not trust.
    assert_eq!(calls(&log), vec![Call::ExistingReceipt]);
}

/// The same refusal with a HOSTILE identity — control characters
/// riding the forged load id — renders inert: the message cannot carry
/// the bytes it judges.
#[tokio::test]
async fn a_mismatched_receipt_refusal_renders_the_forged_identity_inertly() {
    let hostile = CommitReceipt {
        load_id: LoadId::from("evil\u{1b}]52;c;AAAA\u{7}load"),
        commit_seq: 99,
    };
    let (mut session, log) = open_spy(ReceiptScript::Mismatched(hostile)).await;
    let error = session
        .commit(meta())
        .await
        .expect_err("a hostile mismatched receipt must refuse");
    let rendered = error.to_string();
    assert!(
        !rendered.contains('\u{1b}') && !rendered.contains('\u{7}'),
        "the refusal must not carry the bytes it judges: {rendered:?}"
    );
    assert!(
        rendered.contains("u{1b}]52;c;AAAA"),
        "the forged identity renders as its spelled-out escapes: {rendered}"
    );
    assert_eq!(calls(&log), vec![Call::ExistingReceipt]);
}

/// The lookup guard's mirror: a `publish` that RETURNS a
/// receipt naming a different commit fails the commit — the returned
/// receipt is the same idempotence token, and an embedder holding it
/// would vouch for this commit with a token naming some other one.
#[tokio::test]
async fn a_publish_returning_a_foreign_receipt_fails_the_commit() {
    let forged = CommitReceipt {
        load_id: LoadId::from("other-load"),
        commit_seq: 9,
    };
    let (mut session, log) = open_spy(ReceiptScript::PublishForges(forged)).await;
    let error = session
        .commit(meta())
        .await
        .expect_err("a foreign published receipt must refuse");
    let rendered = error.to_string();
    assert!(
        rendered.contains("other-load") && rendered.contains("proves nothing"),
        "the refusal names the forged identity and why: {rendered}"
    );
    // The publish RAN — the guard judges its answer, not its order.
    assert_eq!(calls(&log), vec![Call::ExistingReceipt, Call::Publish]);
}

/// A failed receipt lookup fails the commit without publishing — the
/// framework never guesses past an unanswerable replay question.
#[tokio::test]
async fn a_failed_receipt_lookup_fails_the_commit_without_publishing() {
    let (mut session, log) = open_spy(ReceiptScript::Errors).await;
    session
        .commit(meta())
        .await
        .expect_err("lookup error surfaces");
    assert_eq!(calls(&log), vec![Call::ExistingReceipt]);
}

/// The ensured set is PER TABLE, not merely non-empty: ensuring `a`
/// does not license a write to `b`, and the refusal precedes any
/// backend IO.
#[tokio::test]
async fn an_ensure_of_one_table_does_not_license_a_write_to_another() {
    let (mut session, log) = open_spy(ReceiptScript::None).await;
    session
        .ensure_table(&schema_for("a"), &WriteMode::Append)
        .await
        .expect("ensure a");
    let refused = session
        .write(&TableName::new("b"), batch_of(&[1]))
        .await
        .expect_err("b was never ensured");
    assert!(
        refused
            .to_string()
            .contains("write before ensure_table for `b`")
    );
    assert_eq!(
        calls(&log),
        vec![Call::EnsureTable("a".into())],
        "the refusal must precede backend IO"
    );
}

/// The ensured set is PER SESSION, as documented: a table ensured by a
/// crashed predecessor's session earns a new session nothing — it must
/// re-ensure at the current schema version.
#[tokio::test]
async fn the_ensured_set_does_not_survive_into_a_new_session() {
    let log = Log::default();
    let shell = destination::shell(Spy {
        log: Arc::clone(&log),
        receipt: ReceiptScript::None,
    });
    let context = OpenContext::new(PipelineId::from("p"), LoadId::from("l"));
    let mut first = shell.open(context.clone()).await.expect("open first");
    first
        .ensure_table(&schema_for("t"), &WriteMode::Append)
        .await
        .expect("ensure t");
    drop(first);
    let mut second = shell.open(context).await.expect("open second");
    second
        .write(&TableName::new("t"), batch_of(&[1]))
        .await
        .expect_err("the new session never ensured t");
}
