use std::sync::Arc;
use std::time::Duration;

use rdlt_connector::serve::{ServeError, Served, serve_connection};
use rdlt_connector::wire::{error as carried, v1};
use rdlt_connector::{
    Destination as _, LoadId, OpenContext, PipelineId, Role, SchemaVersion, SegmentId, Source as _,
    TablePath, TableRef, destination_factory, source_factory,
};
use rdlt_connector_reference::{MemoryDestination, MemorySource};
use rdlt_host::remote::Client;
use rdlt_host::{Connection, Options, RemoteDestination, RemoteSource};
use rdlt_wire::{Encoder, Limits, PROTOCOL_MAJOR, PROTOCOL_MINOR};
use tokio::net::UnixStream;
use tokio_stream::wrappers::ReceiverStream;

use crate::support::connectors::{Writes, Writing};
use crate::support::{Fake, Fault, serve_fake, served_within};

pub(crate) fn handshake(role: v1::Role) -> v1::HandshakeRequest {
    v1::HandshakeRequest {
        protocol_major: PROTOCOL_MAJOR,
        protocol_minor: PROTOCOL_MINOR,
        features: Vec::new(),
        role: role as i32,
        traceparent: String::new(),
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
async fn a_failed_write_answers_with_its_error_and_ends_the_write() {
    use v1::write_frame::Frame;
    let served = Served::new().with_destination(Writes::factory(Writing::Fails));
    let (mut client, session) = raw_session(served, "refused").await;
    let (frames, receiver) = tokio::sync::mpsc::channel(8);
    let frame = |frame| v1::WriteFrame { frame: Some(frame) };
    frames
        .send(frame(Frame::Start(v1::WriteStart {
            session,
            table: Some(v1::TableRef::from(&table())),
        })))
        .await
        .unwrap();
    let batch = ids(3);
    let mut encoder = Encoder::default();
    frames
        .send(frame(Frame::Schema(v1::WriteSchema {
            version: 1,
            ipc_schema: encoder.schema(&batch.schema()).expect("the schema encodes"),
        })))
        .await
        .unwrap();
    for ipc in encoder.batch(&batch).expect("the batch encodes") {
        frames
            .send(frame(Frame::Batch(v1::WriteBatch {
                segment: 1,
                data_header: ipc.header,
                data_body: ipc.body,
            })))
            .await
            .unwrap();
    }
    let mut acks = client.write(ReceiverStream::new(receiver)).await.unwrap();
    let mut error = None;
    while let Some(ack) = tokio::time::timeout(Duration::from_secs(5), acks.message())
        .await
        .unwrap()
        .unwrap()
    {
        if let Some(v1::write_ack::Ack::Error(refused)) = ack.ack {
            error = Some(refused);
        }
    }
    let error =
        rdlt_connector::ConnectorError::try_from(error.expect("the write answers its error"))
            .unwrap();
    assert_eq!(error.to_string(), "the write was refused");
}

#[tokio::test]
async fn a_writer_waits_once_its_credit_is_spent() {
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
    // The destination stages nothing, so no credit returns: the writes go while the window of
    // credit lasts, each frame spending its size, and the next waits.
    let mut written = 0;
    for segment in 1..=20 {
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
    assert_eq!(
        written,
        writes_within(limits.frame_bytes),
        "writes before one waited"
    );
}

/// Rows in each batch written until the credit is spent: about 800 KB a frame.
const ROWS: i64 = 100_000;

/// How many writes of `ids(ROWS)` go before one waits, with `window` bytes of credit and none
/// returned: each frame goes while credit remains and spends its encoded size.
fn writes_within(window: u64) -> usize {
    use rdlt_wire::prost::Message as _;
    use v1::write_frame::Frame;
    let batch = ids(ROWS);
    let mut encoder = Encoder::default();
    let size = |frame: Frame| {
        i64::try_from(v1::WriteFrame { frame: Some(frame) }.encoded_len())
            .expect("a frame fits an i64")
    };
    let schema = size(Frame::Schema(v1::WriteSchema {
        version: 1,
        ipc_schema: encoder.schema(&batch.schema()).expect("the schema encodes"),
    }));
    let batches: Vec<i64> = encoder
        .batch(&batch)
        .expect("the batch encodes")
        .into_iter()
        .map(|ipc| {
            size(Frame::Batch(v1::WriteBatch {
                segment: 1,
                data_header: ipc.header,
                data_body: ipc.body,
            }))
        })
        .collect();
    let mut credit = i64::try_from(window).expect("the window fits an i64");
    for written in 0.. {
        let first: Vec<i64> = std::iter::once(schema)
            .chain(batches.iter().copied())
            .collect();
        let frames = if written == 0 { first } else { batches.clone() };
        for frame in frames {
            if credit <= 0 {
                return written;
            }
            credit -= frame;
        }
    }
    unreachable!("the credit runs out")
}

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
        serve_fake(Fake(Fault::StaleEpoch)),
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
        serve_fake(Fake(fault)),
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
