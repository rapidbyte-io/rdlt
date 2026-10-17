//! The handshake and the configuration: the handshake agrees the role and answers who the
//! connector is, and only then, once the host has checked that, does the configuration connect it.

#[cfg(test)]
mod tests;

use rdlt_wire::{Limits, PROTOCOL_MAJOR};

use std::sync::Arc;

use super::probes::Probes;
use super::service::{Configured, Connected, Service};
use crate::error::{ConnectorError, ConnectorErrorKind};
use crate::spec::ConnectContext;
use crate::wire::v1;

/// What a handshake agreed: the role to configure, whether it accepted the role's probe for
/// certification (a destination reading back what it published, a source telling where it
/// stands), and the limits what this end sends keeps within.
#[derive(Clone, Copy, Debug)]
pub(super) struct Agreed {
    role: v1::Role,
    probed: bool,
    /// The lesser of the host's limits and this end's own.
    host: Limits,
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

    /// The limits what this end sends keeps within: the lesser of the host's and its own.
    pub(super) fn host(self) -> Limits {
        self.host
    }
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
                let probed = Probes::of_source(request, factory.as_ref());
                (v1::Role::Source, factory.spec(), probed)
            }
            Ok(v1::Role::Destination) => {
                let factory = self
                    .served
                    .destination
                    .as_ref()
                    .ok_or_else(|| unserved("destination"))?;
                let probed = Probes::of_destination(request, factory.as_ref());
                (v1::Role::Destination, factory.spec(), probed)
            }
            Ok(v1::Role::Unspecified) | Err(_) => return Err(unsupported("no role named", "role")),
        };
        // A host may not make this connector send frames smaller than the protocol's least; one
        // that says no limits keeps the protocol's defaults.
        let host = request.limits.map(Limits::from).unwrap_or_default();
        host.admit_peer()
            .map_err(|shortfall| crate::wire::shortfall_error(&shortfall))?;
        let spec = self.spec(spec, None);
        // What this end sends keeps within the host's limits and its own, agreed with the role.
        let agreed = Agreed {
            role,
            probed,
            host: host.lesser(&self.limits),
        };
        // A handshake that ran beside this one agreed first.
        if self.agreed.set(agreed).is_err() {
            return Err(repeated());
        }
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
        // Configurations connect one at a time, and the connector and its probes are set as one:
        // a configuration beside or after one that connected runs nothing and is refused.
        let mut answer = None;
        self.connected
            .get_or_try_init(|| self.connecting(agreed, request, &mut answer))
            .await?;
        answer.ok_or_else(configured)
    }

    /// Connects the role `agreed` names with `request`'s configuration, and its probes;
    /// `answer` is set to the spec the configuration answers with.
    async fn connecting(
        &self,
        agreed: Agreed,
        request: v1::ConfigureRequest,
        answer: &mut Option<v1::ConnectorSpec>,
    ) -> Result<Configured, ConnectorError> {
        self.limits.admit_config(request.config_json.len())?;
        let config: serde_json::Value =
            serde_json::from_str(&request.config_json).map_err(|error| {
                ConnectorError::config(format!("the configuration is not JSON: {error}"))
            })?;
        let context = match &self.host_name {
            Some(host) => ConnectContext::serving(Arc::clone(host)),
            None => ConnectContext::new(),
        };
        let (spec, configured) = match (agreed.role, &self.served.source, &self.served.destination)
        {
            (v1::Role::Source, Some(factory), _) => {
                let (source, probes) =
                    Probes::connect_source(factory.as_ref(), agreed.probed, config, context)
                        .await?;
                let connected = Connected::Source(source);
                (factory.spec(), Configured { connected, probes })
            }
            (v1::Role::Destination, _, Some(factory)) => {
                let (destination, probes) =
                    Probes::connect_destination(factory.as_ref(), agreed.probed, config, context)
                        .await?;
                let connected = Connected::Destination(destination);
                (factory.spec(), Configured { connected, probes })
            }
            _ => return Err(no_handshake()),
        };
        *answer = Some(self.spec(spec, Some(&configured.connected)));
        Ok(configured)
    }

    /// The spec the handshake and the configuration answer with: the connector's, the roles the
    /// binary serves, and, once configured as a destination, what it can store.
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
        let destination_capabilities = match connected {
            Some(Connected::Destination(destination)) => {
                Some(v1::Capabilities::from(destination.capabilities()))
            }
            Some(Connected::Source(_)) | None => None,
        };
        v1::ConnectorSpec {
            id: spec.id.to_string(),
            version: spec.version.clone(),
            roles,
            config_schema_json: spec.config_schema.to_string(),
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
