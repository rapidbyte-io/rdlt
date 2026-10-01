//! Asking a served source where it stands: the handshake's `acknowledged` feature is accepted only
//! by a source that tells, and only when offered; then, and only then, it serves
//! `ReadAcknowledged`, which moves only with what the source is told is committed.

use rdlt_connector::serve::Served;
use rdlt_connector::wire::{error as carried, v1};
use rdlt_connector::{
    BoxFuture, ConnectContext, ConnectorErrorKind, ConnectorSpec, Source, SourceFactory,
    acknowledging_source_factory, source_factory,
};
use rdlt_connector_reference::{ChangesSource, GeneratorSource};
use rdlt_wire::{ACKNOWLEDGED, PROTOCOL_MAJOR, PROTOCOL_MINOR};

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

fn handshake(features: &[&str]) -> v1::HandshakeRequest {
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
        partition: "p0".to_owned(),
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
    let cursor = v1::Cursor {
        version: 1,
        bytes: br#"{"next":3,"done":false}"#.to_vec().into(),
    };
    client
        .committed(v1::CommittedRequest {
            stream: Some(stream()),
            cursors: vec![v1::CommittedCursor {
                partition: "p0".to_owned(),
                cursor: Some(cursor.clone()),
            }],
        })
        .await
        .expect("the commit is heard");
    let after = client
        .read_acknowledged(asked())
        .await
        .expect("the source tells")
        .into_inner();
    assert_eq!(after.cursor, Some(cursor));
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
