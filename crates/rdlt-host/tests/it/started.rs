//! Where a served read started, which its host may report committed: only a start its source
//! accepted, and where the source says it started elsewhere, only that.

use std::sync::Arc;

use rdlt_connector::serve::{Served, serve_connection};
use rdlt_connector::wire::{error as carried, v1};
use rdlt_connector::{
    Checkpointing, ConnectContext, ConnectorError, ConnectorErrorKind, Emitter, LogLevel,
    Partition, PartitionId, ReadMode, ReadStream, SourceConnector, StreamName, StreamSpec, Streams,
    acknowledging_source_factory,
};
use rdlt_connector_reference::{ChangesSource, LogSource};
use rdlt_wire::ACKNOWLEDGED;
use rdlt_wire::v1::connector_client::ConnectorClient;
use tonic::transport::Channel;

use crate::acknowledged::{checkpoints_from, committed, handshake, read_from};
use crate::support::raw_client;

/// A client of `served`, handshaken and configured with `config`.
async fn configured(served: Served, config: &serde_json::Value) -> ConnectorClient<Channel> {
    let (host, connector) = tokio::net::UnixStream::pair().expect("a socket pair");
    let limits = rdlt_wire::Limits::default();
    tokio::spawn(serve_connection(Arc::new(served), connector, limits));
    let mut client = raw_client(host).await;
    let greeted = client.handshake(handshake(&[ACKNOWLEDGED])).await;
    greeted.expect("the handshake succeeds");
    let request = v1::ConfigureRequest {
        config_json: config.to_string(),
    };
    client
        .configure(request)
        .await
        .expect("the configuration succeeds");
    client
}

fn cursor(bytes: &str) -> v1::Cursor {
    v1::Cursor {
        version: 1,
        bytes: bytes.as_bytes().to_vec().into(),
    }
}

/// Where the source keeps `partition` of `stream`.
async fn standing(
    client: &mut ConnectorClient<Channel>,
    stream: &str,
    partition: &str,
) -> Option<v1::Cursor> {
    let asked = client.read_acknowledged(v1::ReadAcknowledgedRequest {
        stream: Some(v1::StreamName {
            namespace: None,
            name: stream.to_owned(),
        }),
        partition: partition.to_owned(),
    });
    asked.await.expect("the source tells").into_inner().cursor
}

/// Asserts `report` was refused as a position its host was neither sent nor read from.
fn unheard(report: Result<tonic::Response<v1::CommittedResponse>, tonic::Status>, what: &str) {
    let refused = report.expect_err(what);
    let error = carried(&refused);
    assert_eq!(error.code(), Some("position_unsent"), "{what}: {error}");
    assert_eq!(
        error.kind(),
        ConnectorErrorKind::Transient,
        "{what}: {error}"
    );
}

#[tokio::test]
async fn a_start_no_source_issued_is_refused_and_never_heard_and_moves_nothing() {
    let changes = serde_json::json!({ "seed": 3, "streams": [
        { "name": "orders", "keys": 40, "snapshot_partitions": 2, "changes": 40 },
    ]});
    let log = serde_json::json!({ "seed": 3, "group": "forged_start", "streams": [
        { "name": "orders", "partitions": 1, "messages": 40, "bounded": true },
    ]});
    let most = u64::MAX;
    let cases = [
        (
            true,
            &changes,
            "changes",
            format!(r#"{{"next":{most},"done":true}}"#),
        ),
        (
            true,
            &changes,
            "changes",
            r#"{"next":42,"done":false}"#.to_owned(),
        ),
        (
            true,
            &changes,
            "changes",
            r#"{"next":3,"done":true}"#.to_owned(),
        ),
        (
            true,
            &changes,
            "snapshot-0",
            format!(r#"{{"next":{most},"done":true}}"#),
        ),
        (
            true,
            &changes,
            "snapshot-0",
            r#"{"next":1,"done":true}"#.to_owned(),
        ),
        (false, &log, "p0", format!(r#"{{"next":{most}}}"#)),
        (false, &log, "p0", r#"{"next":41}"#.to_owned()),
    ];
    for (slot, config, partition, forged) in cases {
        let served = if slot {
            Served::new().with_source(acknowledging_source_factory::<ChangesSource>())
        } else {
            Served::new().with_source(acknowledging_source_factory::<LogSource>())
        };
        let client = configured(served, config).await;
        refused_and_unheard(client, partition, cursor(&forged)).await;
    }
}

/// Asserts a read of `partition` of `orders` from `forged` is refused, its host not heard for
/// it, the source unmoved, and a read from the start served as before.
async fn refused_and_unheard(
    mut client: ConnectorClient<Channel>,
    partition: &str,
    forged: v1::Cursor,
) {
    let what = format!("{partition} from {forged:?}");
    let before = standing(&mut client, "orders", partition).await;
    // The source refuses the read before it sends anything.
    let read = read_from(&mut client, "orders", partition, Some(forged.clone())).await;
    let refused = carried(&read.expect_err(&what));
    assert_eq!(
        refused.kind(),
        ConnectorErrorKind::Data,
        "{what}: {refused}"
    );
    assert_eq!(refused.code(), Some("cursor_unissued"), "{what}: {refused}");
    // So the host is not heard for it, and the source stays where it stood.
    let report = client.committed(committed("orders", partition, forged));
    unheard(report.await, &what);
    let after = standing(&mut client, "orders", partition).await;
    assert_eq!(after, before, "{what}");
    // A read from the start is served as before, and its host heard for what it is sent.
    let sent = checkpoints_from(&mut client, "orders", partition, None).await;
    let last = sent.last().expect("a checkpoint").clone();
    let report = client.committed(committed("orders", partition, last.clone()));
    report
        .await
        .expect("a checkpoint the read was sent is heard");
    let told = standing(&mut client, "orders", partition).await;
    assert_eq!(told, Some(last.clone()), "{what}");
    // And a read from where the partition ends, which the source issued, is accepted.
    let again = checkpoints_from(&mut client, "orders", partition, Some(last)).await;
    assert!(again.len() <= 1, "{what}: {again:?}");
}

/// A source that keeps its own position, 7, and reads from there whatever cursor it is given.
struct Resuming;

#[derive(serde::Deserialize, schemars::JsonSchema)]
struct NoConfig {}

impl SourceConnector for Resuming {
    const ID: &'static str = "io.test.resuming";
    const VERSION: &'static str = "0.0.1";
    const ACKNOWLEDGES: bool = true;
    type Config = NoConfig;

    async fn connect(_config: NoConfig, _context: &ConnectContext) -> rdlt_connector::Result<Self> {
        Ok(Self)
    }

    async fn check(&self) -> rdlt_connector::Result<()> {
        Ok(())
    }

    fn streams(&self) -> Streams<Self> {
        Streams::new().with(Kept)
    }
}

struct Kept;

impl ReadStream<Resuming> for Kept {
    type Cursor = u64;

    fn spec(&self) -> StreamSpec {
        StreamSpec::new(StreamName::new("kept").expect("a valid stream name"))
            .with_read_modes([ReadMode::Incremental])
            .with_checkpointing(Checkpointing::Natural)
    }

    async fn read(
        &self,
        _source: &Resuming,
        partition: &Partition,
        _cursor: u64,
        out: &mut Emitter<u64>,
    ) -> rdlt_connector::Result<()> {
        match partition.id().as_str() {
            // It says where it starts before anything else.
            "resumes" => out.checkpoint(&7).await,
            // It fails after a line of its log, having sent nothing of the partition.
            "fails" => {
                out.log(LogLevel::Info, "reading").await?;
                Err(ConnectorError::data("the partition cannot be read"))
            }
            _ => Ok(()),
        }
    }

    async fn committed(
        &self,
        _source: &Resuming,
        _cursors: &[(PartitionId, u64)],
    ) -> rdlt_connector::Result<()> {
        Ok(())
    }
}

#[tokio::test]
async fn a_host_is_heard_for_where_its_source_says_a_read_started_and_for_no_read_that_failed() {
    let served = Served::new().with_source(acknowledging_source_factory::<Resuming>());
    let mut client = configured(served, &serde_json::json!({})).await;
    let (asked, kept) = (cursor("3"), cursor("7"));
    // The source started from its own position and said so first: that is what is heard.
    let sent = checkpoints_from(&mut client, "kept", "resumes", Some(asked.clone())).await;
    assert_eq!(sent, std::slice::from_ref(&kept));
    let report = client.committed(committed("kept", "resumes", asked.clone()));
    unheard(report.await, "the cursor the source did not start from");
    let report = client.committed(committed("kept", "resumes", kept));
    report
        .await
        .expect("where the source said it started is heard");
    // A read that failed before it sent anything of its partition leaves its start unheard.
    let failed = read_from(&mut client, "kept", "fails", Some(asked.clone())).await;
    failed.expect_err("the read fails");
    let report = client.committed(committed("kept", "fails", asked.clone()));
    unheard(report.await, "the start of a read that failed");
    // A read that ends cleanly having sent nothing started where it was asked to.
    let idle = checkpoints_from(&mut client, "kept", "idle", Some(asked.clone())).await;
    assert!(idle.is_empty());
    let report = client.committed(committed("kept", "idle", asked));
    report.await.expect("where an idle read started is heard");
}
