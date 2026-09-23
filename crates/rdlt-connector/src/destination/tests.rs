use std::collections::BTreeMap;
use std::sync::{Arc, LazyLock, Mutex};
use std::time::UNIX_EPOCH;

use arrow_array::{Int64Array, RecordBatch};
use bytes::Bytes;
use schemars::JsonSchema;
use serde::Deserialize;
use serde_json::json;

use super::{
    DestinationConnector, OpenContext, Opened, Session, TableChange, TableRef, TableWriter,
    WriteStats, destination_factory,
};
use crate::capabilities::Capabilities;
use crate::commit::{CommitMeta, Receipt, SegmentSet};
use crate::error::{ConnectorError, Result};
use crate::id::{CommitSeq, Epoch, LoadId, PipelineId, SchemaVersion, SegmentId, TablePath};
use crate::schema::TableSchema;
use crate::spec::{ConnectContext, Role};
use crate::state::StateRecord;
use crate::types::{Field, LogicalType};

type Journal = Arc<Mutex<Vec<String>>>;

static JOURNALS: LazyLock<Mutex<BTreeMap<String, Journal>>> = LazyLock::new(Mutex::default);

fn journal(name: &str) -> Journal {
    Arc::clone(JOURNALS.lock().unwrap().entry(name.to_owned()).or_default())
}

#[derive(Deserialize, JsonSchema)]
struct RecorderConfig {
    journal: String,
    #[serde(default)]
    duplicate_state: bool,
}

struct Recorder {
    journal: Journal,
    duplicate_state: bool,
}

struct RecorderSession(Journal);

struct RecorderWriter(Journal);

impl DestinationConnector for Recorder {
    const ID: &'static str = "io.test.recorder";
    const VERSION: &'static str = "0.1.0";
    type Config = RecorderConfig;
    type Session = RecorderSession;

    fn capabilities(&self) -> Capabilities {
        Capabilities::minimal()
    }

    async fn connect(config: RecorderConfig, _context: &ConnectContext) -> Result<Self> {
        Ok(Self {
            journal: journal(&config.journal),
            duplicate_state: config.duplicate_state,
        })
    }

    async fn check(&self) -> Result<()> {
        self.journal.lock().unwrap().push("check".to_owned());
        Ok(())
    }

    async fn open(&self, context: &OpenContext) -> Result<Opened<RecorderSession>> {
        self.journal
            .lock()
            .unwrap()
            .push(format!("open {}", context.pipeline));
        let record = StateRecord {
            key: "k".to_owned(),
            value: Bytes::from_static(b"v"),
        };
        let state = if self.duplicate_state {
            vec![record.clone(), record]
        } else {
            vec![record]
        };
        Ok(Opened {
            session: RecorderSession(Arc::clone(&self.journal)),
            epoch: Epoch(3),
            state,
        })
    }
}

impl Session for RecorderSession {
    type Writer = RecorderWriter;

    async fn apply_schema(&mut self, change: &TableChange) -> Result<()> {
        let TableChange::Create { table, .. } = change else {
            return Err(ConnectorError::internal("only creates are expected"));
        };
        self.0
            .lock()
            .unwrap()
            .push(format!("create {}", table.name));
        Ok(())
    }

    async fn writer(&mut self, table: &TableRef) -> Result<RecorderWriter> {
        self.0
            .lock()
            .unwrap()
            .push(format!("writer {}", table.name));
        Ok(RecorderWriter(Arc::clone(&self.0)))
    }

    async fn discard_staged(&mut self) -> Result<()> {
        self.0.lock().unwrap().push("discard".to_owned());
        Ok(())
    }

    async fn commit(&mut self, meta: &CommitMeta) -> Result<Receipt> {
        self.0
            .lock()
            .unwrap()
            .push(format!("commit {}", meta.commit_seq.get()));
        Ok(Receipt {
            load_id: meta.load_id,
            commit_seq: meta.commit_seq,
            committed_at: UNIX_EPOCH,
            rows: 1,
            bytes: 8,
        })
    }

    async fn close(self) -> Result<()> {
        self.0.lock().unwrap().push("close".to_owned());
        Ok(())
    }
}

impl TableWriter for RecorderWriter {
    async fn write(&mut self, segment: SegmentId, batch: RecordBatch) -> Result<()> {
        self.0
            .lock()
            .unwrap()
            .push(format!("write {segment} {}", batch.num_rows()));
        Ok(())
    }

    async fn flush(&mut self) -> Result<WriteStats> {
        self.0.lock().unwrap().push("flush".to_owned());
        Ok(WriteStats { rows: 1, bytes: 8 })
    }
}

fn context() -> OpenContext {
    OpenContext {
        pipeline: PipelineId::parse("orders").unwrap(),
        load_id: LoadId::from_parts(UNIX_EPOCH, 1),
    }
}

fn table() -> TableRef {
    TableRef {
        path: TablePath::new(["orders"]).unwrap(),
        name: "orders".into(),
        version: SchemaVersion(1),
    }
}

#[tokio::test]
async fn open_discards_staging_before_the_engine_sees_the_session() {
    let destination = destination_factory::<Recorder>()
        .connect(json!({ "journal": "open" }), ConnectContext::new())
        .await
        .unwrap();
    let opened = destination.open(&context()).await.unwrap();
    assert_eq!(
        format!("{opened:?}"),
        "OpenedSession { epoch: Epoch(3), state: 1, .. }"
    );
    assert_eq!(opened.epoch, Epoch(3));
    assert_eq!(opened.state.len(), 1);
    assert_eq!(*journal("open").lock().unwrap(), ["open orders", "discard"]);
}

#[tokio::test]
async fn sessions_forward_every_call() {
    let destination = destination_factory::<Recorder>()
        .connect(json!({ "journal": "forward" }), ConnectContext::new())
        .await
        .unwrap();
    destination.check().await.unwrap();
    let mut session = destination.open(&context()).await.unwrap().session;
    let schema = TableSchema::new(vec![Field::new("id", LogicalType::Int64, false)]).unwrap();
    session
        .apply_schema(&TableChange::Create {
            table: table(),
            schema,
        })
        .await
        .unwrap();
    let mut writer = session.writer(&table()).await.unwrap();
    let batch =
        RecordBatch::try_from_iter([("id", Arc::new(Int64Array::from(vec![7])) as _)]).unwrap();
    writer.write(SegmentId(4), batch).await.unwrap();
    assert_eq!(
        writer.flush().await.unwrap(),
        WriteStats { rows: 1, bytes: 8 }
    );
    let meta = CommitMeta {
        load_id: context().load_id,
        commit_seq: CommitSeq::FIRST,
        epoch: Epoch(3),
        segments: SegmentSet::from_iter([SegmentId(4)]),
        state_delta: Vec::new(),
        finish_generations: Vec::new(),
    };
    assert_eq!(session.commit(&meta).await.unwrap().rows, 1);
    session.close().await.unwrap();
    assert_eq!(
        *journal("forward").lock().unwrap(),
        [
            "check",
            "open orders",
            "discard",
            "create orders",
            "writer orders",
            "write 4 1",
            "flush",
            "commit 1",
            "close"
        ]
    );
}

#[tokio::test]
async fn repeated_state_keys_are_refused_at_open() {
    let destination = destination_factory::<Recorder>()
        .connect(
            json!({ "journal": "duplicate", "duplicate_state": true }),
            ConnectContext::new(),
        )
        .await
        .unwrap();
    let error = destination.open(&context()).await.unwrap_err();
    assert_eq!(error.code(), Some("state_duplicate_key"));
    assert_eq!(
        *journal("duplicate").lock().unwrap(),
        ["open orders"],
        "no discard for a refused open"
    );
}

#[tokio::test]
async fn the_factory_publishes_identity_and_capabilities() {
    let factory = destination_factory::<Recorder>();
    assert_eq!(factory.spec().role, Role::Destination);
    assert_eq!(factory.spec().id.as_str(), "io.test.recorder");
    assert_eq!(factory.spec().config_schema["required"], json!(["journal"]));
    let destination = factory
        .connect(json!({ "journal": "caps" }), ConnectContext::new())
        .await
        .unwrap();
    assert_eq!(destination.capabilities(), &Capabilities::minimal());
    let missing = factory
        .connect(json!({}), ConnectContext::new())
        .await
        .err()
        .unwrap();
    assert_eq!(missing.code(), Some("config_invalid"));
}
