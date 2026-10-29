use std::sync::Arc;
use std::time::Duration;

use rdlt_connector::serve::{ServeError, Served, serve_connection};
use rdlt_connector::wire::{error as carried, v1};
use rdlt_connector::{
    Destination as _, LoadId, OpenContext, PipelineId, Role, SchemaVersion, SegmentId, Source as _,
    TablePath, TableRef, destination_factory, source_factory,
};
use rdlt_connector_reference::{MemoryDestination, MemorySource};
use rdlt_host::remote::{Client, RawCall};
use rdlt_host::{Connection, Options, RemoteDestination, RemoteSource};
use rdlt_testkit::fake::{Fake, Fault, socket};
use rdlt_wire::limits::STREAM_WINDOW;
use rdlt_wire::prost::Message as _;
use rdlt_wire::stream::{Call, Kind, Outgoing, Rows};
use rdlt_wire::{Cut, Encoder, Limits, PROTOCOL_MAJOR};
use tokio::net::UnixStream;

use crate::support::connectors::{Writes, Writing};
use crate::support::served_within;

pub(crate) fn handshake(role: v1::Role) -> v1::HandshakeRequest {
    v1::HandshakeRequest {
        protocol_major: PROTOCOL_MAJOR,
        features: Vec::new(),
        role: role as i32,
        limits: None,
    }
}

pub(crate) fn table() -> TableRef {
    TableRef {
        path: TablePath::new(["items"]).expect("a valid table path"),
        name: Arc::from("items"),
        version: SchemaVersion(1),
        generation: None,
        merge: None,
    }
}

pub(crate) fn context() -> OpenContext {
    OpenContext {
        pipeline: PipelineId::parse("sessions").expect("a valid pipeline id"),
        load_id: LoadId::from_parts(std::time::UNIX_EPOCH, 1),
    }
}

fn ids(count: i64) -> arrow_array::RecordBatch {
    let values: Vec<i64> = (0..count).collect();
    arrow_array::RecordBatch::try_from_iter([(
        "id",
        Arc::new(arrow_array::Int64Array::from(values)) as _,
    )])
    .expect("a valid batch")
}

/// A raw client of a destination `served` over `store`, handshaken, and a session it opened.
pub(crate) async fn raw_session(served: Served, store: &str) -> (Client, u64) {
    raw_session_within(served, store, Limits::default()).await
}

/// A raw client of a destination `served` over `store` enforcing `limits`, handshaken, and a
/// session it opened.
pub(crate) async fn raw_session_within(
    served: Served,
    store: &str,
    limits: Limits,
) -> (Client, u64) {
    let io = served_within(served, limits);
    let client = rdlt_host::remote::client(io, Options::default()).await;
    // It sends whatever the size: what refuses a message is the connector.
    let mut client = client
        .expect("the client connects")
        .max_encoding_message_size(usize::MAX);
    let config = serde_json::json!({ "store": store });
    client
        .rpc
        .handshake(handshake(v1::Role::Destination))
        .await
        .expect("the destination handshakes");
    client
        .rpc
        .configure(v1::ConfigureRequest {
            config_json: config.to_string(),
        })
        .await
        .expect("the destination is configured");
    let context = context();
    let open = v1::OpenRequest {
        pipeline: context.pipeline.as_str().to_owned(),
        load_id: context.load_id.as_bytes().to_vec().into(),
    };
    let session = client
        .rpc
        .open(open)
        .await
        .expect("the session opens")
        .into_inner()
        .session;
    (client, session)
}

#[tokio::test]
async fn a_closed_session_refuses_later_calls() {
    let served = Served::new().with_destination(destination_factory::<MemoryDestination>());
    let (mut client, session) = raw_session(served, "closed").await;
    client
        .rpc
        .close(v1::CloseRequest { session })
        .await
        .unwrap();
    let create = rdlt_connector::TableChange::Create {
        table: table(),
        schema: rdlt_connector::TableSchema::new(vec![rdlt_connector::Field::new(
            "id",
            rdlt_connector::LogicalType::Int64,
            false,
        )])
        .unwrap(),
    };
    let request = v1::ApplySchemaRequest {
        session,
        change: Some(v1::TableChange::from(&create)),
    };
    let status = client.rpc.apply_schema(request).await.unwrap_err();
    assert_eq!(carried(&status).code(), Some("no_session"));
}

#[tokio::test]
async fn a_raw_client_sends_no_message_beyond_the_size_it_is_held_to() {
    let served = Served::new().with_destination(Writes::factory(Writing::Fails));
    let (client, _) = raw_session(served, "held").await;
    let mut client = client.max_encoding_message_size(16);
    let context = context();
    let open = v1::OpenRequest {
        pipeline: context.pipeline.as_str().to_owned(),
        load_id: context.load_id.as_bytes().to_vec().into(),
    };
    assert!(open.encoded_len() > 16);
    client
        .rpc
        .open(open)
        .await
        .expect_err("an open beyond the size is not sent");
}

#[tokio::test]
async fn a_failed_write_answers_with_its_error_and_ends_the_write() {
    use v1::write_frame::Frame;
    let served = Served::new().with_destination(Writes::factory(Writing::Fails));
    let (client, session) = raw_session(served, "refused").await;
    let start = v1::WriteStart {
        session,
        table: Some(v1::TableRef::from(&table())),
    };
    let mut write = RawCall::open(&client, Call::Write, &start, &Limits::default())
        .await
        .expect("the stream opens")
        .expect("the write opens");
    let batch = ids(3);
    let mut encoder = Encoder::default();
    let schema = v1::WriteFrame {
        frame: Some(Frame::Schema(v1::WriteSchema {
            version: 1,
            ipc_schema: encoder.schema(&batch.schema()).expect("the schema encodes"),
        })),
    };
    let mut frames = vec![Outgoing::message(None, &schema).expect("a schema fits a head")];
    let mut cut = Cut::new(batch, Limits::default());
    while let Some(pieces) = encoder.piece(&mut cut).expect("the batch encodes") {
        for piece in pieces {
            let kind = Kind::Batch {
                rows: Rows::Arrow,
                dictionary: piece.is_dictionary(),
                tag: 1,
            };
            frames.push(Outgoing::new(kind, piece.into_pieces()).expect("a piece fits a head"));
        }
    }
    for frame in frames {
        assert!(write.send(frame).await, "the write takes its frames");
    }
    let refused = tokio::time::timeout(Duration::from_secs(5), write.refusal())
        .await
        .expect("the write answers")
        .expect("the write's answers arrive")
        .expect("the write answers its error");
    assert_eq!(refused.to_string(), "the write was refused");
    let ended = tokio::time::timeout(Duration::from_secs(5), write.next())
        .await
        .expect("the write ends");
    assert!(matches!(ended, Ok(None)), "{ended:?}");
}

#[tokio::test]
async fn a_writer_waits_once_the_write_s_stages_and_its_stream_are_full() {
    let limits = Limits {
        frame_bytes: rdlt_wire::limits::MIN_FRAME_BYTES,
        ..Limits::default()
    };
    let io = served_within(
        Served::new().with_destination(Writes::factory(Writing::Stalls)),
        limits,
    );
    let config = serde_json::json!({ "store": "stalled" });
    let connection = Connection::connect(io, Role::Destination, &config, Options::default())
        .await
        .unwrap();
    let destination = RemoteDestination::new(connection).unwrap();
    let mut opened = destination.open(&context()).await.unwrap();
    let mut writer = opened.session.writer(&table()).await.unwrap();
    // The destination's writer takes the first batch and stalls: the batches after it wait in
    // the write's stages, one decoded, one decoding and one read, and in the stream, whose
    // window holds a frame of the least frame limit; and the next write waits.
    let mut written = 0;
    for segment in 1..=40 {
        let wrote = tokio::time::timeout(
            Duration::from_millis(300),
            writer.write(SegmentId(segment), ids(ROWS)),
        )
        .await;
        if wrote.is_err() {
            break;
        }
        wrote.unwrap().unwrap();
        written += 1;
    }
    // About 800 KB a frame: the window holds five, and each end of the stream a frame more.
    let window = usize::try_from(STREAM_WINDOW / 800_000).expect("a count");
    assert!(
        (4..=4 + window + 4).contains(&written),
        "{written} writes went before one waited"
    );
}

/// Rows in each batch written until the stream is full: about 800 KB a frame.
const ROWS: i64 = 100_000;

/// A socket whose shutdown fails with its error kind: `NotConnected` as macOS's does once the peer
/// has closed.
struct ShutdownFails(UnixStream, std::io::ErrorKind);

impl tokio::io::AsyncRead for ShutdownFails {
    fn poll_read(
        mut self: std::pin::Pin<&mut Self>,
        context: &mut std::task::Context<'_>,
        buffer: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        std::pin::Pin::new(&mut self.0).poll_read(context, buffer)
    }
}

impl tokio::io::AsyncWrite for ShutdownFails {
    fn poll_write(
        mut self: std::pin::Pin<&mut Self>,
        context: &mut std::task::Context<'_>,
        bytes: &[u8],
    ) -> std::task::Poll<std::io::Result<usize>> {
        std::pin::Pin::new(&mut self.0).poll_write(context, bytes)
    }

    fn poll_flush(
        mut self: std::pin::Pin<&mut Self>,
        context: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        std::pin::Pin::new(&mut self.0).poll_flush(context)
    }

    fn poll_shutdown(
        self: std::pin::Pin<&mut Self>,
        _: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        std::task::Poll::Ready(Err(self.1.into()))
    }
}

/// How serving ends when the socket's shutdown fails with `kind`, after a host's check.
async fn served_until_shutdown_fails(kind: std::io::ErrorKind) -> Result<(), ServeError> {
    let (host, connector) = UnixStream::pair().expect("a socket pair");
    let served = Arc::new(Served::new().with_source(source_factory::<MemorySource>()));
    let io = ShutdownFails(connector, kind);
    let serving = tokio::spawn(serve_connection(served, io, Limits::default()));
    let config = serde_json::json!({ "streams": { "items": [] } });
    let connection = Connection::connect(host, Role::Source, &config, Options::default());
    RemoteSource::new(connection.await.expect("the source handshakes"))
        .check()
        .await
        .expect("the check passes");
    tokio::time::timeout(Duration::from_secs(5), serving)
        .await
        .expect("the served connection ends")
        .expect("serving does not panic")
}

#[tokio::test]
async fn a_host_that_closes_first_ends_the_served_connection_cleanly() {
    served_until_shutdown_fails(std::io::ErrorKind::NotConnected)
        .await
        .unwrap();
}

#[tokio::test]
async fn a_transport_that_fails_otherwise_fails_the_served_connection() {
    served_until_shutdown_fails(std::io::ErrorKind::PermissionDenied)
        .await
        .unwrap_err();
}

#[tokio::test]
async fn dropping_the_connection_ends_the_served_one() {
    let (host, connector) = UnixStream::pair().unwrap();
    let served = Arc::new(Served::new().with_source(source_factory::<MemorySource>()));
    assert!(format!("{served:?}").contains("io.rapidbyte.memory"));
    let serving = tokio::spawn(serve_connection(served, connector, Limits::default()));
    let config = serde_json::json!({ "streams": { "items": [] } });
    let source = RemoteSource::new(
        Connection::connect(host, Role::Source, &config, Options::default())
            .await
            .unwrap(),
    );
    source.check().await.unwrap();
    drop(source);
    tokio::time::timeout(Duration::from_secs(5), serving)
        .await
        .expect("the served connection ends")
        .unwrap()
        .unwrap();
}

#[tokio::test]
async fn a_schema_epoch_that_does_not_grow_is_refused() {
    let connection = Connection::connect(
        socket(Fake(Fault::StaleEpoch)),
        Role::Source,
        &serde_json::json!({}),
        Options::default(),
    )
    .await
    .unwrap();
    let (sink, mut feed) =
        rdlt_connector::partition_channel(std::num::NonZeroUsize::new(8).unwrap());
    let drain = tokio::spawn(async move { while feed.recv().await.is_some() {} });
    let request = rdlt_connector::ReadRequest::new(
        rdlt_connector::StreamName::new("items").expect("a valid stream name"),
        rdlt_connector::Partition::single(),
        None,
    );
    let error = RemoteSource::new(connection)
        .read(request, sink)
        .await
        .unwrap_err();
    drain.abort();
    assert_eq!(error.code(), Some("malformed_frame"));
}

/// What a read of the fake source answering `fault` ends with, within `limits`, once its frames
/// are all sent: its error, or none while it waits for more.
async fn read_of(fault: Fault, limits: Limits) -> Option<rdlt_connector::ConnectorError> {
    let options = Options {
        limits,
        ..Options::default()
    };
    let connection = Connection::connect(
        socket(Fake(fault)),
        Role::Source,
        &serde_json::json!({}),
        options,
    )
    .await
    .expect("the fake connects");
    let eight = std::num::NonZeroUsize::new(8).expect("not zero");
    let (sink, mut feed) = rdlt_connector::partition_channel(eight);
    let drain = tokio::spawn(async move { while feed.recv().await.is_some() {} });
    let request = rdlt_connector::ReadRequest::new(
        rdlt_connector::StreamName::new("items").expect("a valid stream name"),
        rdlt_connector::Partition::single(),
        None,
    );
    let source = RemoteSource::new(connection);
    let read = tokio::time::timeout(Duration::from_secs(2), source.read(request, sink)).await;
    drain.abort();
    read.ok().and_then(Result::err)
}

#[tokio::test]
async fn frames_that_carry_no_event_are_bounded_between_events() {
    // A schema of each column's dictionary, and one more, may come between events.
    let limits = Limits {
        schema_columns: 4,
        ..Limits::default()
    };
    assert!(read_of(Fault::Schemas(5), limits).await.is_none());
    // A log line costs the engine as little: it ends no run of frames that carry no event.
    let error = read_of(Fault::Chatters(6), limits)
        .await
        .expect("the read fails");
    assert_eq!(error.code(), Some("invalid_message"), "{error}");
    let error = read_of(Fault::Schemas(6), limits)
        .await
        .expect("the read fails");
    assert_eq!(error.code(), Some("invalid_message"), "{error}");
}

#[tokio::test]
async fn log_lines_and_metrics_between_events_are_bounded_on_their_own() {
    let limit = rdlt_host::MAX_FREE_FRAMES;
    assert!(
        read_of(Fault::Logs(limit), Limits::default())
            .await
            .is_none()
    );
    let error = read_of(Fault::Logs(limit + 1), Limits::default())
        .await
        .expect("the read fails");
    assert_eq!(error.code(), Some("invalid_message"), "{error}");
}

#[tokio::test]
async fn a_connection_holds_a_few_sessions_open_and_a_further_closes_its_oldest() {
    let served = Served::new().with_destination(destination_factory::<MemoryDestination>());
    let (mut client, first) = raw_session(served, "few_sessions").await;
    // As many as a connection's descriptors allow are open at once.
    let holds = rdlt_connector::limits::ListenLimits::default().connection_sessions();
    assert_eq!(holds, 3);
    let mut sessions = vec![first];
    for pipeline in 0..10 {
        let open = v1::OpenRequest {
            pipeline: format!("pipeline-{pipeline}"),
            load_id: context().load_id.as_bytes().to_vec().into(),
        };
        let opened = client.rpc.open(open).await.expect("the session opens");
        sessions.push(opened.into_inner().session);
    }
    // The newest are served; each older was closed for one of them.
    let (closed, open) = sessions.split_at(sessions.len() - holds);
    for session in closed {
        let refused = client
            .rpc
            .close(v1::CloseRequest { session: *session })
            .await;
        let refused = carried(&refused.expect_err("the session is closed"));
        assert_eq!(refused.code(), Some("no_session"), "{session}");
    }
    for session in open {
        let closing = client.rpc.close(v1::CloseRequest { session: *session });
        closing.await.expect("the session is open");
    }
}
