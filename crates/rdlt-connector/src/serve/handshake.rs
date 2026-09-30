//! The handshake and the configuration: the handshake agrees the role and answers who the
//! connector is, and only then, once the host has checked that, does the configuration connect it.

use std::sync::Arc;

use rdlt_wire::{Limits, PROTOCOL_MAJOR};

use super::service::{Connected, Service};
use crate::destination::{Destination, DestinationFactory};
use crate::error::{ConnectorError, ConnectorErrorKind, LimitExceeded};
use crate::source::{Source, SourceFactory};
use crate::spec::ConnectContext;
use crate::wire::v1;

/// What a handshake agreed: the role to configure, and whether it accepted the role's probe for
/// certification (a destination reading back what it published, a source telling where it
/// stands).
#[derive(Clone, Copy, Debug)]
pub(super) struct Agreed {
    role: v1::Role,
    probed: bool,
}

impl Agreed {
    /// The feature the handshake accepted, where it accepted the role's probe.
    pub(super) fn feature(self) -> Option<&'static str> {
        match (self.probed, self.role) {
            (true, v1::Role::Source) => Some(rdlt_wire::ACKNOWLEDGED),
            (true, v1::Role::Destination) => Some(rdlt_wire::PUBLISHED),
            _ => None,
        }
    }
}

/// Whether `request` offers `feature`.
fn offers(request: &v1::HandshakeRequest, feature: &str) -> bool {
    request.features.iter().any(|offered| offered == feature)
}

impl Service {
    /// Agrees `request`'s protocol version and role, once, and answers the connector's spec
    /// without what its configuration decides; nothing is connected yet.
    pub(super) fn agree(
        &self,
        request: &v1::HandshakeRequest,
    ) -> Result<v1::ConnectorSpec, ConnectorError> {
        // A repeated handshake is refused before it does any work.
        if self.agreed.initialized() {
            return Err(repeated());
        }
        if request.protocol_major != PROTOCOL_MAJOR {
            let message = format!(
                "protocol version {} is not this connector's {PROTOCOL_MAJOR}",
                request.protocol_major
            );
            return Err(unsupported(message, "protocol_version"));
        }
        let (role, spec, probed) = match v1::Role::try_from(request.role) {
            Ok(v1::Role::Source) => {
                let factory = self
                    .served
                    .source
                    .as_ref()
                    .ok_or_else(|| unserved("source"))?;
                let probed = offers(request, rdlt_wire::ACKNOWLEDGED) && factory.acknowledges();
                (v1::Role::Source, factory.spec(), probed)
            }
            Ok(v1::Role::Destination) => {
                let factory = self
                    .served
                    .destination
                    .as_ref()
                    .ok_or_else(|| unserved("destination"))?;
                let probed = offers(request, rdlt_wire::PUBLISHED) && factory.reads_back();
                (v1::Role::Destination, factory.spec(), probed)
            }
            Ok(v1::Role::Unspecified) | Err(_) => return Err(unsupported("no role named", "role")),
        };
        let spec = self.spec(spec, None);
        // A handshake that ran beside this one agreed first.
        if self.agreed.set(Agreed { role, probed }).is_err() {
            return Err(repeated());
        }
        let _ = self
            .host
            .set(Limits::from(request.limits.unwrap_or_default()));
        Ok(spec)
    }

    /// Connects the connector for the role the handshake agreed, with `request`'s configuration,
    /// once; answers its spec with what the connected role declares.
    pub(super) async fn connect(
        &self,
        request: v1::ConfigureRequest,
    ) -> Result<v1::ConnectorSpec, ConnectorError> {
        let Some(agreed) = self.agreed.get().copied() else {
            return Err(no_handshake());
        };
        // A repeated configuration is refused before it connects again.
        if self.connected.initialized() {
            return Err(configured());
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
        let (spec, connected) = match (agreed.role, &self.served.source, &self.served.destination) {
            (v1::Role::Source, Some(factory), _) => {
                let source = self
                    .connect_source(factory.as_ref(), agreed.probed, config)
                    .await?;
                (factory.spec(), Connected::Source(source))
            }
            (v1::Role::Destination, _, Some(factory)) => {
                let destination = self
                    .connect_destination(factory.as_ref(), agreed.probed, config)
                    .await?;
                (factory.spec(), Connected::Destination(destination))
            }
            _ => return Err(no_handshake()),
        };
        let spec = self.spec(spec, Some(&connected));
        // A configuration that ran beside this one connected first.
        if self.connected.set(connected).is_err() {
            return Err(configured());
        }
        Ok(spec)
    }

    /// Connects `factory`'s source with `config`, telling where it stands when the handshake
    /// accepted that.
    async fn connect_source(
        &self,
        factory: &dyn SourceFactory,
        accepted: bool,
        config: serde_json::Value,
    ) -> Result<Arc<dyn Source>, ConnectorError> {
        let context = ConnectContext::new();
        if !accepted {
            return Ok(Arc::from(factory.connect(config, context).await?));
        }
        let (source, acknowledger) = factory.connect_acknowledging(config, context).await?;
        // Set once: a handshake that ran beside this one is refused once connected.
        self.acknowledger.set(acknowledger).ok();
        Ok(source)
    }

    /// Connects `factory`'s destination with `config`, reading back what it published when the
    /// host `offered` that and it can.
    async fn connect_destination(
        &self,
        factory: &dyn DestinationFactory,
        offered: bool,
        config: serde_json::Value,
    ) -> Result<Arc<dyn Destination>, ConnectorError> {
        let context = ConnectContext::new();
        if !(offered && factory.reads_back()) {
            return Ok(Arc::from(factory.connect(config, context).await?));
        }
        let (destination, reader) = factory.connect_reading(config, context).await?;
        // Set once: a handshake that ran beside this one is refused once connected.
        self.reader.set(reader).ok();
        Ok(destination)
    }

    /// The spec the handshake and the configuration answer with: the connector's, the roles the
    /// binary serves, and, once connected, what the connected role declares.
    fn spec(
        &self,
        spec: &crate::spec::ConnectorSpec,
        connected: Option<&Connected>,
    ) -> v1::ConnectorSpec {
        let mut roles = Vec::new();
        if self.served.source.is_some() {
            roles.push(v1::Role::Source as i32);
        }
        if self.served.destination.is_some() {
            roles.push(v1::Role::Destination as i32);
        }
        let (source_capabilities, destination_capabilities) = match connected {
            None => (None, None),
            Some(Connected::Source(_)) => (Some(v1::SourceCapabilities {}), None),
            Some(Connected::Destination(destination)) => (
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

/// The error of a call before the handshake.
pub(super) fn no_handshake() -> ConnectorError {
    ConnectorError::new(
        ConnectorErrorKind::Internal,
        "the connection has had no handshake",
    )
    .with_code("no_handshake")
}

/// The error of a call before the connector is configured.
pub(super) fn not_configured() -> ConnectorError {
    ConnectorError::new(
        ConnectorErrorKind::Internal,
        "the connector has not been configured",
    )
    .with_code("not_configured")
}

/// The error of a configuration on a connection already configured.
fn configured() -> ConnectorError {
    ConnectorError::new(
        ConnectorErrorKind::Internal,
        "the connection was already configured",
    )
    .with_code("configure_repeated")
}
