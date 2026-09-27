//! Reading back what a served destination published: the handshake's `published` feature is
//! accepted only by a destination that reads back, and only when offered; then, and only then,
//! it serves `ReadPublished`.

use rdlt_connector::serve::Served;
use rdlt_connector::wire::{error as carried, v1};
use rdlt_connector::{ConnectorErrorKind, destination_factory, readable_destination_factory};
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
        config_json: r#"{"store": "host_published"}"#.to_owned(),
        traceparent: String::new(),
        limits: None,
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
        let refused = client
            .read_published(table())
            .await
            .expect_err("the read-back is refused");
        let error = carried(&refused);
        assert_eq!(error.kind(), ConnectorErrorKind::Unsupported, "{error}");
        assert_eq!(error.code(), Some("published"), "{error}");
    }
}
