use std::time::Duration;

use rdlt_connector::serve::Served;
use rdlt_connector::wire::{error as carried, v1};
use rdlt_connector::{
    CommitMeta, CommitSeq, ConnectorErrorKind, Destination as _, LoadId, OpenContext, Partition,
    PipelineId, ReadRequest, Role, SegmentSet, Source as _, StreamName, partition_channel,
    source_factory,
};
use rdlt_connector_reference::MemorySource;
use rdlt_host::{CONNECTOR_LOST, Connection, DEADLINE_EXCEEDED, Deadlines, Options, RemoteSource};
use rdlt_wire::{Limits, PROTOCOL_MAJOR, PROTOCOL_MINOR};
use tokio_stream::wrappers::ReceiverStream;

use crate::support::connectors::{Denied, SlowCommits};
use crate::support::{
    Fake, Fault, memory_destination, raw_client, serve_fake, served, served_within,
};

/// Options that notice a lost connector within about a tenth of a second.
fn quick() -> Options {
    Options {
        heartbeat: Duration::from_millis(20),
        missed: 3,
        ..Options::default()
    }
}

fn handshake(major: u32, role: v1::Role, config: &str) -> v1::HandshakeRequest {
    v1::HandshakeRequest {
        protocol_major: major,
        protocol_minor: PROTOCOL_MINOR,
        features: Vec::new(),
        role: role as i32,
        config_json: config.to_owned(),
        traceparent: String::new(),
        limits: None,
    }
}

fn memory() -> Served {
    Served::new().with_source(source_factory::<MemorySource>())
}

fn rows(count: usize) -> serde_json::Value {
    let rows: Vec<_> = (0..count)
        .map(|id| serde_json::json!({ "id": id }))
        .collect();
    serde_json::json!({ "streams": { "items": rows }, "page_size": 10 })
}

#[tokio::test]
async fn a_host_of_another_major_version_is_refused_at_the_handshake() {
    let mut client = raw_client(served(memory())).await;
    let status = client
        .handshake(handshake(PROTOCOL_MAJOR + 1, v1::Role::Source, "{}"))
        .await
        .unwrap_err();
    let error = carried(&status);
    assert_eq!(
        (error.kind(), error.code()),
        (ConnectorErrorKind::Unsupported, Some("protocol_version"))
    );
}

#[tokio::test]
async fn a_call_before_the_handshake_is_refused() {
    let mut client = raw_client(served(memory())).await;
    let status = client.check(v1::CheckRequest {}).await.unwrap_err();
    assert_eq!(carried(&status).code(), Some("no_handshake"));
}

#[tokio::test]
async fn a_role_the_connector_does_not_serve_is_refused() {
    let config = serde_json::json!({ "store": "unserved" });
    let error = Connection::connect(
        served(memory()),
        Role::Destination,
        &config,
        Options::default(),
    )
    .await
    .unwrap_err();
    assert_eq!(
        (error.kind(), error.code()),
        (ConnectorErrorKind::Unsupported, Some("role"))
    );
}

#[tokio::test]
async fn a_connectors_error_crosses_the_wire_with_its_kind_code_and_message() {
    let io = served(Served::new().with_source(source_factory::<Denied>()));
    let connection =
        Connection::connect(io, Role::Source, &serde_json::json!({}), Options::default())
            .await
            .unwrap();
    let error = RemoteSource::new(connection).check().await.unwrap_err();
    assert_eq!(
        (error.kind(), error.code()),
        (ConnectorErrorKind::Auth, Some("test.denied"))
    );
    assert_eq!(error.to_string(), "the token was refused");
}

#[tokio::test]
async fn a_configuration_beyond_the_connectors_limit_is_refused() {
    let limits = Limits {
        config_bytes: 64,
        ..Limits::default()
    };
    let error = Connection::connect(
        served_within(memory(), limits),
        Role::Source,
        &rows(10),
        Options::default(),
    )
    .await
    .unwrap_err();
    assert_eq!(error.code(), Some("limit_exceeded"));
    assert_eq!(
        error.limit().map(|limit| (limit.name, limit.limit)),
        Some(("config bytes", 64))
    );
}

/// Reads stream `items` of `source` to its end; the read's outcome.
async fn read_items(source: &RemoteSource) -> rdlt_connector::Result<()> {
    let (sink, mut feed) = partition_channel(std::num::NonZeroUsize::new(64).expect("not zero"));
    let drain = tokio::spawn(async move { while feed.recv().await.is_some() {} });
    let request = ReadRequest {
        stream: StreamName::new("items").expect("a valid stream name"),
        partition: Partition::single(),
        cursor: None,
    };
    let read = source.read(request, sink).await;
    drain.abort();
    read
}

#[tokio::test]
async fn a_json_push_beyond_the_hosts_limit_is_refused() {
    let options = Options {
        limits: Limits {
            json_push_bytes: 32,
            ..Limits::default()
        },
        ..Options::default()
    };
    let connection = Connection::connect(served(memory()), Role::Source, &rows(10), options)
        .await
        .unwrap();
    let error = read_items(&RemoteSource::new(connection))
        .await
        .unwrap_err();
    assert_eq!(
        error.limit().map(|limit| limit.name),
        Some("json push bytes")
    );
}

#[tokio::test]
async fn a_malformed_frame_from_the_connector_is_refused_typed() {
    let io = serve_fake(Fake(Fault::GarbageSchema));
    let connection =
        Connection::connect(io, Role::Source, &serde_json::json!({}), Options::default())
            .await
            .unwrap();
    let error = read_items(&RemoteSource::new(connection))
        .await
        .unwrap_err();
    assert_eq!(
        (error.kind(), error.code()),
        (ConnectorErrorKind::Internal, Some("malformed_frame"))
    );
}

#[tokio::test]
async fn a_connector_that_stops_answering_heartbeats_is_lost() {
    let io = serve_fake(Fake(Fault::Silent));
    let connection = Connection::connect(io, Role::Source, &serde_json::json!({}), quick())
        .await
        .unwrap();
    let check = RemoteSource::new(connection);
    let error = tokio::time::timeout(Duration::from_secs(5), check.check())
        .await
        .unwrap()
        .unwrap_err();
    assert_eq!(
        (error.kind(), error.code()),
        (ConnectorErrorKind::Transient, Some(CONNECTOR_LOST))
    );
}

#[tokio::test]
async fn a_call_beyond_its_deadline_fails_with_deadline_exceeded() {
    let options = Options {
        deadlines: Deadlines {
            commit: Duration::from_millis(50),
            ..Deadlines::default()
        },
        ..quick()
    };
    let io = served(Served::new().with_destination(SlowCommits::factory(Duration::from_secs(2))));
    let config = serde_json::json!({ "store": "deadline" });
    let connection = Connection::connect(io, Role::Destination, &config, options)
        .await
        .unwrap();
    let destination = rdlt_host::RemoteDestination::new(connection).unwrap();
    let context = OpenContext {
        pipeline: PipelineId::parse("deadline").unwrap(),
        load_id: LoadId::from_parts(std::time::UNIX_EPOCH, 1),
    };
    let mut opened = destination.open(&context).await.unwrap();
    let meta = CommitMeta {
        load_id: context.load_id,
        commit_seq: CommitSeq::FIRST,
        epoch: opened.epoch,
        segments: SegmentSet::new(),
        state_delta: Vec::new(),
        finish_generations: Vec::new(),
        child_tables: Vec::new(),
    };
    let error = opened.session.commit(&meta).await.unwrap_err();
    assert_eq!(
        (error.kind(), error.code()),
        (ConnectorErrorKind::Transient, Some(DEADLINE_EXCEEDED))
    );
}

#[tokio::test]
async fn a_served_read_sends_a_frame_only_while_it_has_credit() {
    use v1::read_control::Control;
    let mut client = raw_client(served(memory())).await;
    client
        .handshake(handshake(
            PROTOCOL_MAJOR,
            v1::Role::Source,
            &rows(50).to_string(),
        ))
        .await
        .unwrap();
    let (controls, receiver) = tokio::sync::mpsc::channel(4);
    let control = |control| v1::ReadControl {
        control: Some(control),
    };
    let start = v1::ReadStart {
        stream: Some(v1::StreamName {
            namespace: None,
            name: "items".to_owned(),
        }),
        partition: "whole".to_owned(),
        cursor: None,
    };
    controls.send(control(Control::Start(start))).await.unwrap();
    controls
        .send(control(Control::Credit(v1::Credit { bytes: 1 })))
        .await
        .unwrap();
    let mut frames = client
        .read(ReceiverStream::new(receiver))
        .await
        .unwrap()
        .into_inner();
    // One byte of credit lets one frame go, whatever its size, and then no other.
    let wait = Duration::from_millis(300);
    let first = tokio::time::timeout(Duration::from_secs(5), frames.message())
        .await
        .unwrap()
        .unwrap();
    assert!(first.is_some(), "a frame goes while credit remains");
    let early = tokio::time::timeout(wait, frames.message()).await;
    assert!(early.is_err(), "no frame goes once the credit is spent");
    controls
        .send(control(Control::Credit(v1::Credit { bytes: 1 << 20 })))
        .await
        .unwrap();
    let next = tokio::time::timeout(Duration::from_secs(5), frames.message())
        .await
        .unwrap()
        .unwrap();
    assert!(next.is_some(), "more credit lets more frames go");
}

#[tokio::test(flavor = "multi_thread")]
async fn slow_commit_within_deadline_succeeds() {
    // Liveness and work deadlines differ (§12.6): a commit much longer than the heartbeat's
    // patience still succeeds while the connector answers heartbeats.
    let source = crate::support::memory_source(rows(30), quick()).await;
    let io =
        served(Served::new().with_destination(SlowCommits::factory(Duration::from_millis(400))));
    let config = serde_json::json!({ "store": "slow_commit" });
    let connection = Connection::connect(io, Role::Destination, &config, quick())
        .await
        .unwrap();
    let destination = rdlt_host::RemoteDestination::new(connection).unwrap();
    let plan = rdlt_engine::PipelinePlan::new(
        PipelineId::parse("slow").unwrap(),
        [rdlt_engine::StreamPlan::new(
            StreamName::new("items").expect("a valid stream name"),
        )],
    )
    .unwrap();
    let outcome = crate::support::engine(100)
        .run(
            plan,
            std::sync::Arc::new(source),
            std::sync::Arc::new(destination),
        )
        .await;
    assert_eq!(
        outcome.report.status,
        rdlt_engine::RunStatus::Succeeded,
        "{:?}",
        outcome.error
    );
    let _ = memory_destination;
}

#[tokio::test]
async fn a_served_read_spends_its_credit_frame_by_frame_until_none_remains() {
    use rdlt_wire::prost::Message as _;
    use v1::read_control::Control;
    let mut client = raw_client(served(memory())).await;
    client
        .handshake(handshake(
            PROTOCOL_MAJOR,
            v1::Role::Source,
            &rows(500).to_string(),
        ))
        .await
        .unwrap();
    let (controls, receiver) = tokio::sync::mpsc::channel(4);
    let control = |control| v1::ReadControl {
        control: Some(control),
    };
    let start = v1::ReadStart {
        stream: Some(v1::StreamName {
            namespace: None,
            name: "items".to_owned(),
        }),
        partition: "whole".to_owned(),
        cursor: None,
    };
    let grant = 2000;
    controls.send(control(Control::Start(start))).await.unwrap();
    controls
        .send(control(Control::Credit(v1::Credit { bytes: grant })))
        .await
        .unwrap();
    let mut frames = client
        .read(ReceiverStream::new(receiver))
        .await
        .unwrap()
        .into_inner();
    let mut sizes = Vec::new();
    while let Ok(frame) = tokio::time::timeout(Duration::from_millis(300), frames.message()).await {
        sizes.push(u64::try_from(frame.unwrap().expect("the read goes on").encoded_len()).unwrap());
    }
    let (sent, last) = (
        sizes.iter().sum::<u64>(),
        *sizes.last().expect("a frame went"),
    );
    assert!(
        sent - last < grant && grant <= sent,
        "{sizes:?} for {grant} bytes of credit"
    );
}

#[tokio::test]
async fn a_connectors_catalog_crosses_the_wire() {
    let source = crate::support::memory_source(rows(3), Options::default()).await;
    let catalog = source.discover().await.unwrap();
    let names: Vec<String> = catalog.iter().map(|spec| spec.name().to_string()).collect();
    assert_eq!(names, ["items"]);
}

#[tokio::test]
async fn a_call_for_the_other_role_is_refused() {
    let mut client = raw_client(served(memory())).await;
    client
        .handshake(handshake(
            PROTOCOL_MAJOR,
            v1::Role::Source,
            &rows(1).to_string(),
        ))
        .await
        .unwrap();
    let open = v1::OpenRequest {
        pipeline: "p".to_owned(),
        load_id: vec![0; 16].into(),
    };
    let status = client.open(open).await.unwrap_err();
    assert_eq!(carried(&status).code(), Some("role"));
}

#[tokio::test]
async fn a_second_handshake_is_refused_before_it_connects() {
    let mut client = raw_client(served(memory())).await;
    let config = rows(1).to_string();
    client
        .handshake(handshake(PROTOCOL_MAJOR, v1::Role::Source, &config))
        .await
        .unwrap();
    // Refused as repeated before its configuration is read, let alone a second connector made.
    let status = client
        .handshake(handshake(PROTOCOL_MAJOR, v1::Role::Source, "not JSON"))
        .await
        .unwrap_err();
    assert_eq!(carried(&status).code(), Some("handshake_repeated"));
}

#[tokio::test]
async fn a_message_that_does_not_decode_is_refused() {
    let mut client = raw_client(served(memory())).await;
    client
        .handshake(handshake(
            PROTOCOL_MAJOR,
            v1::Role::Source,
            &rows(1).to_string(),
        ))
        .await
        .unwrap();
    let plan = v1::PlanRequest {
        stream: None,
        state: None,
    };
    let status = client.plan(plan).await.unwrap_err();
    assert_eq!(carried(&status).code(), Some("invalid_message"));
}
