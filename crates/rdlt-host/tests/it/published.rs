//! Reading back what a served destination published: the handshake's `published` feature is
//! accepted only by a destination that reads back, and only when offered; then, and only then,
//! it serves `ReadPublished`.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use arrow_array::{Int64Array, RecordBatch};
use rdlt_connector::serve::Served;
use rdlt_connector::wire::{error as carried, v1};
use rdlt_connector::{
    BoxFuture, ConnectContext, ConnectorError, ConnectorErrorKind, ConnectorSpec,
    DestinationFactory, PublishedReader, PublishedRows, Reading, TableRef, destination_factory,
    readable_destination_factory,
};
use rdlt_connector_reference::MemoryDestination;
use rdlt_wire::{PROTOCOL_MAJOR, PROTOCOL_MINOR, PUBLISHED};

use crate::support::{raw_client, served};

fn handshake(features: &[&str]) -> v1::HandshakeRequest {
    v1::HandshakeRequest {
        protocol_major: PROTOCOL_MAJOR,
        protocol_minor: PROTOCOL_MINOR,
        features: features
            .iter()
            .map(|feature| (*feature).to_owned())
            .collect(),
        role: v1::Role::Destination as i32,
        traceparent: String::new(),
        limits: None,
    }
}

fn configure() -> v1::ConfigureRequest {
    v1::ConfigureRequest {
        config_json: r#"{"store": "host_published"}"#.to_owned(),
    }
}

fn table() -> v1::ReadPublishedRequest {
    v1::ReadPublishedRequest {
        table: Some(v1::TableRef {
            path: Some(v1::TablePath {
                segments: vec!["events".to_owned()],
            }),
            name: "events".to_owned(),
            version: 1,
            ..v1::TableRef::default()
        }),
    }
}

#[tokio::test]
async fn a_destination_that_reads_back_accepts_the_feature_when_offered_and_serves_it() {
    let readable =
        || Served::new().with_destination(readable_destination_factory::<MemoryDestination>());
    let mut client = raw_client(served(readable())).await;
    let answer = client
        .handshake(handshake(&[PUBLISHED, "unknown"]))
        .await
        .expect("the handshake succeeds")
        .into_inner();
    assert_eq!(answer.accepted_features, [PUBLISHED]);
    client
        .configure(configure())
        .await
        .expect("the configuration succeeds");
    let mut frames = client
        .read_published(table())
        .await
        .expect("the read-back starts")
        .into_inner();
    let first = frames.message().await.expect("a frame");
    assert!(
        matches!(
            first.and_then(|frame| frame.frame),
            Some(v1::read_frame::Frame::Done(_))
        ),
        "nothing published reads back as done alone"
    );
}

#[tokio::test]
async fn a_read_back_the_handshake_did_not_accept_is_refused_as_unsupported() {
    let cases = [
        (
            Served::new().with_destination(readable_destination_factory::<MemoryDestination>()),
            &[][..],
        ),
        (
            Served::new().with_destination(readable_destination_factory::<MemoryDestination>()),
            &["another"][..],
        ),
        (
            Served::new().with_destination(destination_factory::<MemoryDestination>()),
            &[PUBLISHED][..],
        ),
    ];
    for (served_by, offered) in cases {
        let mut client = raw_client(served(served_by)).await;
        let answer = client
            .handshake(handshake(offered))
            .await
            .expect("the handshake succeeds")
            .into_inner();
        assert!(answer.accepted_features.is_empty(), "{offered:?}");
        client
            .configure(configure())
            .await
            .expect("the configuration succeeds");
        let refused = client
            .read_published(table())
            .await
            .expect_err("the read-back is refused");
        let error = carried(&refused);
        assert_eq!(error.kind(), ConnectorErrorKind::Unsupported, "{error}");
        assert_eq!(error.code(), Some("published"), "{error}");
    }
}

/// Rows in each batch [`Large`] reads back: 64 KiB of them.
const ROWS: usize = 8192;

/// A memory destination that reads back `batches` batches, however many its host takes, counting
/// those sent; where `fails`, the read-back fails once they are.
struct Large {
    inner: Box<dyn DestinationFactory>,
    batches: usize,
    fails: bool,
    sent: Arc<AtomicUsize>,
    ended: Arc<AtomicUsize>,
}

impl Large {
    fn new(batches: usize, fails: bool) -> Self {
        Self {
            inner: destination_factory::<MemoryDestination>(),
            batches,
            fails,
            sent: Arc::new(AtomicUsize::new(0)),
            ended: Arc::new(AtomicUsize::new(0)),
        }
    }
}

struct LargeReader {
    batches: usize,
    fails: bool,
    sent: Arc<AtomicUsize>,
    ended: Arc<AtomicUsize>,
}

impl PublishedReader for LargeReader {
    fn published<'a>(
        &'a self,
        _: &'a TableRef,
        rows: PublishedRows,
    ) -> BoxFuture<'a, rdlt_connector::Result<()>> {
        Box::pin(async move {
            let ids = Arc::new(Int64Array::from(vec![7; ROWS]));
            let batch = RecordBatch::try_from_iter([("id", ids as _)]).expect("a valid batch");
            for _ in 0..self.batches {
                if let Err(left) = rows.send(batch.clone()).await {
                    self.ended.fetch_add(1, Ordering::SeqCst);
                    return Err(left);
                }
                self.sent.fetch_add(1, Ordering::SeqCst);
            }
            if self.fails {
                let message = "the table went away";
                return Err(ConnectorError::new(ConnectorErrorKind::Transient, message));
            }
            Ok(())
        })
    }
}

impl DestinationFactory for Large {
    fn spec(&self) -> &ConnectorSpec {
        self.inner.spec()
    }

    fn connect(
        &self,
        config: serde_json::Value,
        context: ConnectContext,
    ) -> BoxFuture<'_, rdlt_connector::Result<Box<dyn rdlt_connector::Destination>>> {
        self.inner.connect(config, context)
    }

    fn reads_back(&self) -> bool {
        true
    }

    fn connect_reading(
        &self,
        config: serde_json::Value,
        context: ConnectContext,
    ) -> BoxFuture<'_, rdlt_connector::Result<Reading>> {
        Box::pin(async move {
            let destination = self.inner.connect(config, context).await?;
            let reader = LargeReader {
                batches: self.batches,
                fails: self.fails,
                sent: Arc::clone(&self.sent),
                ended: Arc::clone(&self.ended),
            };
            Ok((
                Arc::from(destination),
                Arc::new(reader) as Arc<dyn PublishedReader>,
            ))
        })
    }
}

/// A client that reads `factory`'s table back: its frames.
async fn reading_back(factory: Large) -> tonic::Streaming<v1::ReadFrame> {
    reading_back_within(factory, None).await
}

/// As [`reading_back`], a host that takes frames within `limits`.
async fn reading_back_within(
    factory: Large,
    limits: Option<v1::Limits>,
) -> tonic::Streaming<v1::ReadFrame> {
    let mut client = raw_client(served(Served::new().with_destination(Box::new(factory)))).await;
    let offered = v1::HandshakeRequest {
        limits,
        ..handshake(&[PUBLISHED])
    };
    client
        .handshake(offered)
        .await
        .expect("the handshake succeeds");
    client
        .configure(configure())
        .await
        .expect("the configuration succeeds");
    client
        .read_published(table())
        .await
        .expect("the read-back starts")
        .into_inner()
}

#[tokio::test]
async fn a_table_is_read_back_no_further_than_its_host_takes() {
    // More than six gigabytes, were it read whole.
    let factory = Large::new(100_000, false);
    let (sent, ended) = (Arc::clone(&factory.sent), Arc::clone(&factory.ended));
    let mut frames = reading_back(factory).await;
    let first = frames.message().await.expect("a frame");
    assert!(matches!(
        first.and_then(|frame| frame.frame),
        Some(v1::read_frame::Frame::Schema(_))
    ));
    // The host takes no more: the connector reads a few batches ahead, and waits.
    tokio::time::sleep(std::time::Duration::from_millis(300)).await;
    let ahead = sent.load(Ordering::SeqCst);
    assert!(ahead < 256, "{ahead} batches were read back");
    tokio::time::sleep(std::time::Duration::from_millis(300)).await;
    assert_eq!(sent.load(Ordering::SeqCst), ahead);
    // The host leaves: the read-back ends, rather than run on for no one.
    drop(frames);
    for _ in 0..100 {
        if ended.load(Ordering::SeqCst) == 1 {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
    assert_eq!(ended.load(Ordering::SeqCst), 1);
    assert!(sent.load(Ordering::SeqCst) < 256);
}

#[tokio::test]
async fn a_table_read_back_arrives_whole_a_batch_a_frame_and_then_done() {
    use v1::read_frame::Frame;
    let mut frames = reading_back(Large::new(300, false)).await;
    let (mut schemas, mut rows, mut done) = (0, 0, 0);
    let mut decoder = rdlt_wire::Decoder::new(rdlt_wire::Limits::default());
    while let Some(frame) = frames.message().await.expect("a frame") {
        assert_eq!(done, 0, "a frame followed the done frame");
        match frame.frame {
            Some(Frame::Schema(schema)) => {
                decoder.schema(&schema.ipc_schema).expect("a schema");
                schemas += 1;
            }
            Some(Frame::Batch(batch)) => {
                let frame = rdlt_wire::IpcFrame {
                    header: batch.data_header,
                    body: batch.data_body,
                };
                let batch = decoder.frame(&frame).expect("a batch decodes");
                rows += batch.map_or(0, |batch| batch.num_rows());
            }
            Some(Frame::Done(_)) => done += 1,
            other => panic!("{other:?}"),
        }
    }
    assert_eq!((schemas, rows, done), (1, 300 * ROWS, 1));
}

#[tokio::test]
async fn a_read_back_that_fails_ends_with_its_error_after_the_rows_it_read() {
    use v1::read_frame::Frame;
    let mut frames = reading_back(Large::new(3, true)).await;
    let mut batches = 0;
    let failed = loop {
        match frames.message().await {
            Ok(Some(frame)) => match frame.frame {
                Some(Frame::Batch(_)) => batches += 1,
                Some(Frame::Schema(_)) => {}
                other => panic!("{other:?}"),
            },
            Ok(None) => panic!("the read-back ended without its error"),
            Err(status) => break carried(&status),
        }
    };
    assert_eq!(batches, 3);
    assert_eq!(failed.kind(), ConnectorErrorKind::Transient, "{failed}");
}

#[tokio::test]
async fn a_batch_read_back_beyond_its_hosts_frames_ends_the_read_back_as_exceeding_them() {
    let factory = Large::new(100_000, false);
    let (sent, ended) = (Arc::clone(&factory.sent), Arc::clone(&factory.ended));
    let small = v1::Limits {
        frame_bytes: 1024,
        ..v1::Limits::from(rdlt_wire::Limits::default())
    };
    let mut frames = reading_back_within(factory, Some(small)).await;
    let refused = loop {
        match frames.message().await {
            Ok(Some(frame)) => assert!(
                matches!(frame.frame, Some(v1::read_frame::Frame::Schema(_))),
                "a batch beyond the host's frames was sent"
            ),
            Ok(None) => panic!("the read-back ended without its error"),
            Err(status) => break carried(&status),
        }
    };
    assert!(refused.limit().is_some(), "{refused}");
    // The table is read no further.
    for _ in 0..100 {
        if ended.load(Ordering::SeqCst) == 1 {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
    assert_eq!(ended.load(Ordering::SeqCst), 1);
    assert!(sent.load(Ordering::SeqCst) < 8);
}
