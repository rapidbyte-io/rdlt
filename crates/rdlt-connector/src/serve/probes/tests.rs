//! The probes of a build without the `certify` feature: a build with every feature compiles none
//! of these, so they run only where the feature is left out.

use rdlt_wire::Limits;
use rdlt_wire::tonic::Code;

use super::Probes;
use crate::wire::{error, v1};
use crate::{ConnectorErrorKind, destination::DestinationFactory, source::SourceFactory};

fn offering(feature: &str) -> v1::HandshakeRequest {
    v1::HandshakeRequest {
        features: vec![feature.to_owned()],
        ..v1::HandshakeRequest::default()
    }
}

fn spec(role: crate::spec::Role) -> crate::spec::ConnectorSpec {
    crate::spec::ConnectorSpec {
        id: crate::ConnectorId::parse("io.test.probed").expect("a valid id"),
        version: "0.0.1".to_owned(),
        role,
        config_schema: serde_json::Value::Null,
    }
}

struct NoSource(crate::spec::ConnectorSpec);

impl SourceFactory for NoSource {
    fn spec(&self) -> &crate::spec::ConnectorSpec {
        &self.0
    }

    fn connect(
        &self,
        _: serde_json::Value,
        _: crate::spec::ConnectContext,
    ) -> crate::spec::BoxFuture<'_, crate::Result<Box<dyn crate::source::Source>>> {
        unreachable!("nothing connects")
    }
}

struct NoDestination(crate::spec::ConnectorSpec);

impl DestinationFactory for NoDestination {
    fn spec(&self) -> &crate::spec::ConnectorSpec {
        &self.0
    }

    fn connect(
        &self,
        _: serde_json::Value,
        _: crate::spec::ConnectContext,
    ) -> crate::spec::BoxFuture<'_, crate::Result<Box<dyn crate::destination::Destination>>> {
        unreachable!("nothing connects")
    }
}

#[test]
fn a_build_without_the_probes_accepts_neither_whatever_is_offered() {
    let source = NoSource(spec(crate::spec::Role::Source));
    assert!(!Probes::of_source(
        &offering(rdlt_wire::ACKNOWLEDGED),
        &source
    ));
    let destination = NoDestination(spec(crate::spec::Role::Destination));
    assert!(!Probes::of_destination(
        &offering(rdlt_wire::PUBLISHED),
        &destination
    ));
}

#[tokio::test]
async fn a_build_without_the_probes_answers_both_calls_as_unsupported() {
    let probes = Probes::default();
    let table = v1::TableRef {
        path: Some(v1::TablePath {
            segments: vec!["rows".to_owned()],
        }),
        name: "rows".to_owned(),
        version: 1,
        ..v1::TableRef::default()
    };
    let request = v1::ReadPublishedRequest { table: Some(table) };
    let Err(read_back) = probes.read_published(request, Limits::default()) else {
        panic!("a table was read back");
    };
    assert_eq!(read_back.code(), Code::Unimplemented);
    let refused = error(&read_back);
    assert_eq!(refused.kind(), ConnectorErrorKind::Unsupported);
    assert_eq!(refused.code(), Some("published"));
    let asked = v1::ReadAcknowledgedRequest {
        stream: Some(v1::StreamName {
            namespace: None,
            name: "rows".to_owned(),
        }),
        partition: "p0".to_owned(),
    };
    let standing = probes.read_acknowledged(asked).await;
    let standing = standing.expect_err("the source told where it stands");
    assert_eq!(standing.code(), Code::Unimplemented);
    let refused = error(&standing);
    assert_eq!(refused.kind(), ConnectorErrorKind::Unsupported);
    assert_eq!(refused.code(), Some("acknowledged"));
}
