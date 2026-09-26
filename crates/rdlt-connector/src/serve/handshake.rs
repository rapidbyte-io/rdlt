//! The handshake: it connects the connector for the host's role, with the host's configuration,
//! once per connection.

use std::sync::Arc;

use rdlt_wire::{Limits, PROTOCOL_MAJOR};

use super::service::{Connected, Service};
use crate::error::{ConnectorError, ConnectorErrorKind, LimitExceeded};
use crate::spec::ConnectContext;
use crate::wire::v1;

impl Service {
    /// Connects the connector for `request`'s role, once.
    pub(super) async fn connect(
        &self,
        request: v1::HandshakeRequest,
    ) -> Result<v1::ConnectorSpec, ConnectorError> {
        // A repeated handshake is refused before it does any work, let alone connects again.
        if self.connected.initialized() {
            return Err(repeated());
        }
        if request.protocol_major != PROTOCOL_MAJOR {
            let message = format!(
                "protocol version {} is not this connector's {PROTOCOL_MAJOR}",
                request.protocol_major
            );
            return Err(unsupported(message, "protocol_version"));
        }
        if let Err(refusal) = self.limits.admit_config(request.config_json.len()) {
            return Err(ConnectorError::exceeds(LimitExceeded {
                name: "config bytes",
                limit: refusal.limit,
                actual: refusal.actual,
            }));
        }
        let config: serde_json::Value =
            serde_json::from_str(&request.config_json).map_err(|error| {
                ConnectorError::config(format!("the configuration is not JSON: {error}"))
            })?;
        let _ = self
            .host
            .set(Limits::from(request.limits.unwrap_or_default()));
        let context = ConnectContext::new();
        let (spec, connected) = match v1::Role::try_from(request.role) {
            Ok(v1::Role::Source) => {
                let factory = self
                    .served
                    .source
                    .as_ref()
                    .ok_or_else(|| unserved("source"))?;
                let source = factory.connect(config, context).await?;
                (factory.spec(), Connected::Source(Arc::from(source)))
            }
            Ok(v1::Role::Destination) => {
                let factory = self
                    .served
                    .destination
                    .as_ref()
                    .ok_or_else(|| unserved("destination"))?;
                let destination = factory.connect(config, context).await?;
                (
                    factory.spec(),
                    Connected::Destination(Arc::from(destination)),
                )
            }
            Ok(v1::Role::Unspecified) | Err(_) => return Err(unsupported("no role named", "role")),
        };
        let spec = self.spec(spec, &connected);
        // A handshake that ran beside this one connected first.
        if self.connected.set(connected).is_err() {
            return Err(repeated());
        }
        Ok(spec)
    }

    /// The spec the handshake answers with: the connector's, the roles the binary serves, and
    /// what the connected role declares.
    fn spec(&self, spec: &crate::spec::ConnectorSpec, connected: &Connected) -> v1::ConnectorSpec {
        let mut roles = Vec::new();
        if self.served.source.is_some() {
            roles.push(v1::Role::Source as i32);
        }
        if self.served.destination.is_some() {
            roles.push(v1::Role::Destination as i32);
        }
        let (source_capabilities, destination_capabilities) = match connected {
            Connected::Source(_) => (Some(v1::SourceCapabilities {}), None),
            Connected::Destination(destination) => (
                None,
                Some(v1::Capabilities::from(destination.capabilities())),
            ),
        };
        v1::ConnectorSpec {
            id: spec.id.to_string(),
            version: spec.version.clone(),
            roles,
            config_schema_json: spec.config_schema.to_string(),
            source_capabilities,
            destination_capabilities,
        }
    }
}

/// An error for something the connector does not support, with `code`.
pub(super) fn unsupported(message: impl Into<String>, code: &'static str) -> ConnectorError {
    ConnectorError::new(ConnectorErrorKind::Unsupported, message).with_code(code)
}

fn unserved(role: &str) -> ConnectorError {
    unsupported(
        format!("this connector does not serve the {role} role"),
        "role",
    )
}

/// The error of a handshake on a connection that already had its handshake.
fn repeated() -> ConnectorError {
    ConnectorError::new(
        ConnectorErrorKind::Internal,
        "the connection already had its handshake",
    )
    .with_code("handshake_repeated")
}
