use std::sync::Arc;

use rdlt_wire::Limits;
use rdlt_wire::limits::MIN_FRAME_BYTES;
use tokio_util::sync::CancellationToken;

use crate::ConnectorId;
use crate::destination::{Destination, DestinationFactory};
use crate::error::Result;
use crate::serve::Served;
use crate::serve::service::Service;
use crate::spec::{BoxFuture, ConnectContext, ConnectorSpec};
use crate::wire::v1;

#[cfg(feature = "certify")]
mod raced;

/// A destination's factory that is only handshaken with.
struct Plain(ConnectorSpec);

impl DestinationFactory for Plain {
    fn spec(&self) -> &ConnectorSpec {
        &self.0
    }

    fn connect(
        &self,
        _: serde_json::Value,
        _: ConnectContext,
    ) -> BoxFuture<'_, Result<Box<dyn Destination>>> {
        unreachable!("nothing connects")
    }
}

#[test]
fn what_a_connection_sends_keeps_within_the_limits_its_handshake_agreed() {
    let own = Limits::default();
    let host = Limits {
        frame_bytes: MIN_FRAME_BYTES,
        ..Limits::default()
    };
    for offered in [Some(host), None] {
        let spec = ConnectorSpec {
            id: ConnectorId::parse("io.test.agreed").expect("a valid id"),
            version: "0.0.1".to_owned(),
            config_schema: serde_json::Value::Null,
        };
        let served = Served::new().with_destination(Box::new(Plain(spec)));
        let service = Service::new(Arc::new(served), own, None, 3, CancellationToken::new());
        let handshake = v1::HandshakeRequest {
            role: v1::Role::Destination as i32,
            protocol_major: rdlt_wire::PROTOCOL_MAJOR,
            limits: offered.map(Into::into),
            ..v1::HandshakeRequest::default()
        };
        service.agree(&handshake).expect("the handshake agrees");
        let expected = offered.unwrap_or_default().lesser(&own);
        assert_eq!(service.host(), expected, "{offered:?}");
    }
    assert!(
        MIN_FRAME_BYTES < own.frame_bytes,
        "the host's limit is the lesser"
    );
}
