//! Asking a served source where it stands: the handshake's `acknowledged` feature is accepted only
//! by a source that tells, and only when offered; then, and only then, it serves
//! `ReadAcknowledged`, which moves only with what the source is told is committed.

use std::sync::Arc;

use rdlt_connector::serve::{Served, serve_connection};
use rdlt_connector::wire::{error as carried, v1};
use rdlt_connector::{
    BoxFuture, ConnectContext, ConnectorErrorKind, ConnectorSpec, Source, SourceFactory,
    acknowledging_source_factory, source_factory,
};
use rdlt_connector_reference::{ChangesSource, GeneratorSource};
use rdlt_wire::{ACKNOWLEDGED, PROTOCOL_MAJOR, PROTOCOL_MINOR};

use rdlt_wire::v1::connector_client::ConnectorClient;
use tokio_stream::StreamExt as _;
use tokio_stream::wrappers::ReceiverStream;
use tonic::transport::Channel;

use crate::support::{raw_client, served};

/// A factory written by hand, which says nothing of where its source stands.
struct Plain(Box<dyn SourceFactory>);

impl SourceFactory for Plain {
    fn spec(&self) -> &ConnectorSpec {
        self.0.spec()
    }

    fn connect(
        &self,
        config: serde_json::Value,
        context: ConnectContext,
    ) -> BoxFuture<'_, rdlt_connector::Result<Box<dyn Source>>> {
        self.0.connect(config, context)
    }
}

pub(crate) fn handshake(features: &[&str]) -> v1::HandshakeRequest {
    v1::HandshakeRequest {
        protocol_major: PROTOCOL_MAJOR,
        protocol_minor: PROTOCOL_MINOR,
        features: features
            .iter()
            .map(|feature| (*feature).to_owned())
            .collect(),
        role: v1::Role::Source as i32,
        traceparent: String::new(),
        limits: None,
    }
}

/// A change source of one stream, `accounts`, as `served` configures it.
fn changes() -> v1::ConfigureRequest {
    v1::ConfigureRequest {
        config_json: r#"{"seed": 3, "streams": [{"name": "accounts", "keys": 4, "changes": 4}]}"#
            .to_owned(),
    }
}

fn stream() -> v1::StreamName {
    v1::StreamName {
        namespace: None,
        name: "accounts".to_owned(),
    }
}

fn asked() -> v1::ReadAcknowledgedRequest {
    v1::ReadAcknowledgedRequest {
        stream: Some(stream()),
        partition: "changes".to_owned(),
    }
}

#[tokio::test]
async fn a_source_that_tells_accepts_the_feature_when_offered_and_tells_what_it_was_told() {
    let mut client = raw_client(served(
        Served::new().with_source(acknowledging_source_factory::<ChangesSource>()),
    ))
    .await;
    let answer = client
        .handshake(handshake(&[ACKNOWLEDGED, "unknown"]))
        .await
        .expect("the handshake succeeds")
        .into_inner();
    assert_eq!(answer.accepted_features, [ACKNOWLEDGED]);
    client
        .configure(changes())
        .await
        .expect("the configuration succeeds");
    let before = client
        .read_acknowledged(asked())
        .await
        .expect("the source tells")
        .into_inner();
    assert_eq!(before.cursor, None, "told nothing yet");
    let cursor = checkpoints(&mut client, "accounts", "changes")
        .await
        .remove(0);
    client
        .committed(committed("accounts", "changes", cursor.clone()))
        .await
        .expect("the commit is heard");
    let after = client
        .read_acknowledged(asked())
        .await
        .expect("the source tells")
        .into_inner();
    assert_eq!(after.cursor, Some(cursor));
}

/// The checkpoints a read of `partition` of `stream` from its start sends, to its end.
async fn checkpoints(
    client: &mut ConnectorClient<Channel>,
    stream: &str,
    partition: &str,
) -> Vec<v1::Cursor> {
    checkpoints_from(client, stream, partition, None).await
}

/// The checkpoints a read of `partition` of `stream` from `cursor` sends, to its end.
pub(crate) async fn checkpoints_from(
    client: &mut ConnectorClient<Channel>,
    stream: &str,
    partition: &str,
    cursor: Option<v1::Cursor>,
) -> Vec<v1::Cursor> {
    let read = read_from(client, stream, partition, cursor).await;
    read.expect("the read ends cleanly")
}

/// The checkpoints a read of `partition` of `stream` from `cursor` sends, to its end, or what
/// the read failed with.
pub(crate) async fn read_from(
    client: &mut ConnectorClient<Channel>,
    stream: &str,
    partition: &str,
    cursor: Option<v1::Cursor>,
) -> Result<Vec<v1::Cursor>, tonic::Status> {
    use v1::read_control::Control;
    use v1::read_frame::Frame;
    let start = v1::ReadStart {
        stream: Some(v1::StreamName {
            namespace: None,
            name: stream.to_owned(),
        }),
        partition: partition.to_owned(),
        cursor,
        barrier: 0,
        unbounded: false,
        follow: false,
    };
    let controls = [
        Control::Start(start),
        Control::Credit(v1::Credit { bytes: 1 << 30 }),
    ];
    let controls = controls.map(|control| v1::ReadControl {
        control: Some(control),
    });
    let (open, pending) = tokio::sync::mpsc::channel(1);
    let controls = tokio_stream::iter(controls).chain(ReceiverStream::new(pending));
    let mut frames = client.read(controls).await?.into_inner();
    let mut checkpoints = Vec::new();
    while let Some(frame) = frames.message().await? {
        match frame.frame {
            Some(Frame::Checkpoint(checkpoint)) => {
                checkpoints.push(checkpoint.cursor.expect("a checkpoint has its cursor"));
            }
            Some(Frame::Done(_)) => break,
            _ => {}
        }
    }
    drop(open);
    Ok(checkpoints)
}

pub(crate) fn committed(stream: &str, partition: &str, cursor: v1::Cursor) -> v1::CommittedRequest {
    v1::CommittedRequest {
        stream: Some(v1::StreamName {
            namespace: None,
            name: stream.to_owned(),
        }),
        cursors: vec![v1::CommittedCursor {
            partition: partition.to_owned(),
            cursor: Some(cursor),
        }],
    }
}

/// Two change streams, each snapshotted in two partitions.
fn two_streams() -> v1::ConfigureRequest {
    let stream = |name: &str| serde_json::json!({ "name": name, "keys": 40, "snapshot_partitions": 2, "changes": 40 });
    let config = serde_json::json!({
        "seed": 3,
        "slot": "bound",
        "streams": [stream("accounts"), stream("orders")],
    });
    v1::ConfigureRequest {
        config_json: config.to_string(),
    }
}

#[tokio::test]
async fn a_host_acknowledges_only_the_checkpoints_its_reads_were_sent() {
    let changes = Served::new().with_source(acknowledging_source_factory::<ChangesSource>());
    let mut client = connected(&Arc::new(changes)).await;
    let refused = |refused: Result<_, tonic::Status>, what: &str| {
        let Err(refused): Result<tonic::Response<v1::CommittedResponse>, _> = refused else {
            panic!("{what} was acknowledged");
        };
        // Transient: a host that reads again is sent checkpoints it can report.
        let error = carried(&refused);
        let (kind, code) = (error.kind(), error.code());
        assert_eq!(kind, ConnectorErrorKind::Transient, "{what}: {error}");
        assert_eq!(code, Some("position_unsent"), "{what}: {error}");
    };
    // A position past every change, which no read was sent.
    let beyond = v1::Cursor {
        version: 1,
        bytes: br#"{"next":18446744073709551615,"done":true}"#.to_vec().into(),
    };
    let unread = client.committed(committed("accounts", "changes", beyond.clone()));
    refused(unread.await, "a position no read was sent");
    let sent = checkpoints(&mut client, "accounts", "changes").await;
    assert!(sent.len() > 1, "{sent:?}");
    // Still refused, though the partition has been read: it is none of what the read was sent.
    let unsent = client.committed(committed("accounts", "changes", beyond));
    refused(unsent.await, "a position the read was not sent");
    // A checkpoint is its stream's and its partition's, in the format it was sent in.
    let checkpoint = sent[0].clone();
    let elsewhere = client.committed(committed("accounts", "snapshot-0", checkpoint.clone()));
    refused(elsewhere.await, "another partition's checkpoint");
    let other = client.committed(committed("orders", "changes", checkpoint.clone()));
    refused(other.await, "another stream's checkpoint");
    let reformatted = v1::Cursor {
        version: checkpoint.version + 1,
        ..checkpoint.clone()
    };
    let another = client.committed(committed("accounts", "changes", reformatted));
    refused(another.await, "a checkpoint in another format");
    // One refused position refuses the report whole: nothing of it is told to the source.
    let mut mixed = committed("accounts", "changes", checkpoint.clone());
    mixed.cursors.push(v1::CommittedCursor {
        partition: "snapshot-0".to_owned(),
        cursor: Some(checkpoint.clone()),
    });
    refused(
        client.committed(mixed).await,
        "a report with a position unread",
    );
    let standing = client.read_acknowledged(asked()).await;
    assert_eq!(
        standing.expect("the source tells").into_inner().cursor,
        None
    );
    // A report that names no cursor is no report: a message the source cannot read.
    let mut blank = committed("accounts", "changes", checkpoint.clone());
    blank.cursors[0].cursor = None;
    let unreadable = client.committed(blank).await.expect_err("refused");
    assert_eq!(carried(&unreadable).code(), Some("invalid_message"));
    // Every checkpoint the read was sent is acknowledged, more than once too.
    for cursor in sent.iter().chain(&sent) {
        let told = client.committed(committed("accounts", "changes", cursor.clone()));
        told.await.expect("a checkpoint the read was sent is heard");
    }
}

/// A client of `served`, over a connection of its own, handshaken and configured with two
/// streams.
async fn connected(served: &Arc<Served>) -> ConnectorClient<Channel> {
    let (host, connector) = tokio::net::UnixStream::pair().expect("a socket pair");
    let limits = rdlt_wire::Limits::default();
    tokio::spawn(serve_connection(Arc::clone(served), connector, limits));
    let mut client = raw_client(host).await;
    client
        .handshake(handshake(&[ACKNOWLEDGED]))
        .await
        .expect("the handshake succeeds");
    client
        .configure(two_streams())
        .await
        .expect("the configuration succeeds");
    client
}

#[tokio::test]
async fn a_host_that_dials_again_acknowledges_what_it_was_sent_and_a_connector_started_again_refuses()
 {
    let changes = || Served::new().with_source(acknowledging_source_factory::<ChangesSource>());
    let served = Arc::new(changes());
    let mut first = connected(&served).await;
    let sent = checkpoints(&mut first, "orders", "changes").await;
    drop(first);
    // Another connection to the connector: what the first was sent is still its host's to report.
    let mut again = connected(&served).await;
    let told = again.committed(committed("orders", "changes", sent[0].clone()));
    told.await.expect("the report is heard");
    // The connector started again remembers nothing it sent: the report is refused as transient.
    let restarted = Arc::new(changes());
    let mut fresh = connected(&restarted).await;
    let unknown = fresh.committed(committed("orders", "changes", sent[1].clone()));
    let refused = unknown.await.expect_err("the report is refused");
    let error = carried(&refused);
    assert_eq!(error.kind(), ConnectorErrorKind::Transient, "{error}");
    assert_eq!(error.code(), Some("position_unsent"), "{error}");
    // A host that reads on from a position has said everything before it is committed: the
    // connector hears it for where its read starts, though it sent no such checkpoint.
    let last = sent.last().expect("a checkpoint").clone();
    let resent = checkpoints_from(&mut fresh, "orders", "changes", Some(last.clone())).await;
    assert!(!resent.contains(&last), "{resent:?}");
    let told = fresh.committed(committed("orders", "changes", last.clone()));
    told.await
        .expect("the report of where the read started is heard");
    let standing = fresh.read_acknowledged(v1::ReadAcknowledgedRequest {
        stream: Some(v1::StreamName {
            namespace: None,
            name: "orders".to_owned(),
        }),
        partition: "changes".to_owned(),
    });
    let standing = standing.await.expect("the source tells").into_inner();
    assert_eq!(standing.cursor, Some(last.clone()));
    // Where a read of another partition, or of another stream, started is not this one's.
    for (stream, partition) in [("orders", "snapshot-0"), ("accounts", "changes")] {
        let elsewhere = fresh.committed(committed(stream, partition, last.clone()));
        let refused = carried(&elsewhere.await.expect_err("refused"));
        assert_eq!(
            refused.code(),
            Some("position_unsent"),
            "{stream} {partition}"
        );
    }
    // Nor is a position the host makes up beyond where it reads from.
    let beyond = v1::Cursor {
        version: last.version,
        bytes: br#"{"next":18446744073709551615,"done":true}"#.to_vec().into(),
    };
    let forged = fresh.committed(committed("orders", "changes", beyond));
    let refused = carried(&forged.await.expect_err("refused"));
    assert_eq!(refused.code(), Some("position_unsent"));
    assert_eq!(refused.kind(), ConnectorErrorKind::Transient);
}

#[tokio::test]
async fn asking_a_source_the_handshake_did_not_accept_is_refused_as_unsupported() {
    let cases = [
        (acknowledging_source_factory::<ChangesSource>(), &[][..]),
        (
            acknowledging_source_factory::<ChangesSource>(),
            &["another"][..],
        ),
        // A source that tells, served by the factory a connector's own binary serves.
        (source_factory::<ChangesSource>(), &[ACKNOWLEDGED][..]),
        (
            acknowledging_source_factory::<GeneratorSource>(),
            &[ACKNOWLEDGED][..],
        ),
        (source_factory::<GeneratorSource>(), &[ACKNOWLEDGED][..]),
        (
            Box::new(Plain(source_factory::<GeneratorSource>())) as Box<dyn SourceFactory>,
            &[ACKNOWLEDGED][..],
        ),
    ];
    for (factory, offered) in cases {
        let generator = factory.spec().id.as_str() == "io.rapidbyte.generator";
        let mut client = raw_client(served(Served::new().with_source(factory))).await;
        let answer = client
            .handshake(handshake(offered))
            .await
            .expect("the handshake succeeds")
            .into_inner();
        assert!(answer.accepted_features.is_empty(), "{offered:?}");
        let config = if generator {
            v1::ConfigureRequest {
                config_json: r#"{"seed": 1, "streams": [{"name": "accounts", "rows": 3}]}"#
                    .to_owned(),
            }
        } else {
            changes()
        };
        client
            .configure(config)
            .await
            .expect("the configuration succeeds");
        let refused = client
            .read_acknowledged(asked())
            .await
            .expect_err("asking is refused");
        let error = carried(&refused);
        assert_eq!(error.kind(), ConnectorErrorKind::Unsupported, "{error}");
        assert_eq!(error.code(), Some("acknowledged"), "{error}");
    }
}
