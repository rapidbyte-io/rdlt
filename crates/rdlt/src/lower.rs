//! How a pipeline's document becomes what the engine runs: its plan, its configuration, and its
//! two connectors' references, by this path alone.

#[cfg(test)]
mod tests;

use rdlt_connector::{ConnectorId, Role};
use rdlt_host::{ConnectorRef, Digest};

use crate::error::noun;
use crate::{Connector, Error, Side, codes};

/// The reference `side`, the pipeline's `role`, names: its connector, what its placement must
/// honour, and what it may reach.
pub(crate) fn reference(side: &Side, role: Role) -> Result<ConnectorRef, Error> {
    let noun = noun(role);
    let id = ConnectorId::parse(side.connector.id()).map_err(|error| {
        let message = format!("the {noun} connector's id is not one: {error}");
        Error::config_caused(codes::REFERENCE_INVALID, message, error)
    })?;
    let mut connector = ConnectorRef::new(id);
    if let Connector::Reference(reference) = &side.connector {
        if let Some(version) = &reference.version {
            let required = semver::VersionReq::parse(version).map_err(|error| {
                let message = format!(
                    "the {noun} connector's version is no semver requirement, as ^1.2 is: {error}"
                );
                Error::config_caused(codes::REFERENCE_INVALID, message, error)
            })?;
            connector = connector.version(required);
        }
        if let Some(digest) = &reference.digest {
            let digest = digest_of(digest).ok_or_else(|| {
                let message =
                    format!("the {noun} connector's digest is not a SHA-256: 64 hex digits");
                Error::config(codes::REFERENCE_INVALID, message)
            })?;
            connector = connector.digest(digest);
        }
        if let Some(endpoint) = &reference.endpoint {
            connector = connector.endpoint(endpoint.clone());
        }
    }
    for path in &side.access.read {
        connector = connector.read(path);
    }
    for path in &side.access.write {
        connector = connector.write(path);
    }
    if side.access.network {
        connector = connector.network();
    }
    Ok(connector)
}

/// The SHA-256 `hex` writes as 64 hex digits.
fn digest_of(hex: &str) -> Option<Digest> {
    let digits: Vec<u8> = hex
        .chars()
        .map(|digit| {
            digit
                .to_digit(16)
                .and_then(|value| u8::try_from(value).ok())
        })
        .collect::<Option<_>>()?;
    if digits.len() != 64 {
        return None;
    }
    let mut digest = [0_u8; 32];
    for (byte, [high, low]) in digest.iter_mut().zip(digits.as_chunks::<2>().0) {
        *byte = (high << 4) | low;
    }
    Some(Digest(digest))
}
