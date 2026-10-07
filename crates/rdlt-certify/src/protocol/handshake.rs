//! The handshake's clauses: the version it answers and refuses, the calls it and the
//! configuration must come before and not after, and the roles it refuses.

#[cfg(test)]
mod tests;

use rdlt_connector::Role;
use rdlt_connector::wire::v1;
use rdlt_wire::PROTOCOL_MAJOR;

use super::{
    Found, Violation, configure_request, error, handshaken, refused_with, request, unsupported,
    wire_role,
};
use crate::target::Target;

/// The code of a handshake at another major version.
const PROTOCOL_VERSION: &str = "protocol_version";

/// Major versions no connector speaks: none before the first, and none so far ahead.
const OTHER_MAJORS: [u32; 2] = [0, u32::MAX];

/// Checks `P-HANDSHAKE`.
pub(super) async fn answered(target: &Target, role: Role, config: &str) -> Found {
    let checked = async {
        let mut client = target.client().await.map_err(Violation::of)?;
        // A connector takes the features it knows and ignores the rest: this handshake alone
        // offers one no host defines, named anew each time.
        let unknown = unknown();
        let mut offered = request(role, PROTOCOL_MAJOR);
        offered.features.push(unknown.clone());
        let answer = client
            .rpc
            .handshake(offered)
            .await
            .map_err(|status| format!("the handshake failed: {}", error(&status)))?
            .into_inner();
        let (Some(spec), Some(_)) = (&answer.spec, &answer.limits) else {
            return Err(Violation::from(
                "the handshake answered without the connector's spec or limits",
            ));
        };
        if answer.protocol_major != PROTOCOL_MAJOR {
            return Err(format!(
                "the handshake answered protocol {}, not {PROTOCOL_MAJOR}",
                answer.protocol_major
            )
            .into());
        }
        let configured = client
            .rpc
            .configure(configure_request(config))
            .await
            .map_err(|status| format!("the configuration failed: {}", error(&status)))?
            .into_inner()
            .spec
            .unwrap_or_default();
        if (&configured.id, &configured.version) != (&spec.id, &spec.version) {
            return Err(format!(
                "the handshake answered `{}` {} and the configuration `{}` {}",
                spec.id, spec.version, configured.id, configured.version
            )
            .into());
        }
        if answer.accepted_features.contains(&unknown) {
            return Err(Violation::from(
                "the handshake accepted a feature no host defines",
            ));
        }
        for major in OTHER_MAJORS {
            let mut client = target.client().await.map_err(Violation::of)?;
            let other = client.rpc.handshake(request(role, major)).await;
            let what = format!("a handshake at major version {major}");
            unsupported(&refused_with(other, PROTOCOL_VERSION, &what)?, &what)?;
        }
        Ok(())
    };
    checked.await.into()
}

/// A feature no host defines, named after the time, so no connector knows it by name.
fn unknown() -> String {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    format!("x.{nanos:x}")
}

/// Checks `P-ORDER`.
pub(super) async fn ordered(target: &Target, role: Role, config: &str) -> Found {
    let checked = async {
        let mut early = target.client().await.map_err(Violation::of)?;
        refused_with(
            early.rpc.check(v1::CheckRequest {}).await,
            "no_handshake",
            "a check before the handshake",
        )?;
        refused_with(
            early.rpc.configure(configure_request(config)).await,
            "no_handshake",
            "a configuration before the handshake",
        )?;
        let mut agreed = target.client().await.map_err(Violation::of)?;
        agreed
            .rpc
            .handshake(request(role, PROTOCOL_MAJOR))
            .await
            .map_err(|status| format!("the handshake failed: {}", error(&status)))?;
        refused_with(
            agreed.rpc.check(v1::CheckRequest {}).await,
            "not_configured",
            "a check before the configuration",
        )?;
        let (mut client, _) = handshaken(target, role, config).await?;
        let again = client.rpc.handshake(request(role, PROTOCOL_MAJOR)).await;
        refused_with(again, "handshake_repeated", "a second handshake")?;
        let reconfigured = client.rpc.configure(configure_request(config)).await;
        refused_with(reconfigured, "configure_repeated", "a second configuration")?;
        Ok(())
    };
    checked.await.into()
}

/// Checks `P-ROLE`.
pub(super) async fn roles(target: &Target, role: Role, config: &str) -> Found {
    let (other, named) = match role {
        Role::Source => (Role::Destination, "destination"),
        Role::Destination => (Role::Source, "source"),
    };
    let checked = async {
        let (_, answer) = handshaken(target, role, config).await?;
        let served = answer.spec.map(|spec| spec.roles).unwrap_or_default();
        if served.contains(&(wire_role(other) as i32)) {
            return Ok(Found::Inapplicable(format!(
                "the connector serves the {named} role too"
            )));
        }
        let mut client = target.client().await.map_err(Violation::of)?;
        let refused = client.rpc.handshake(request(other, PROTOCOL_MAJOR)).await;
        let what = "a handshake as an unserved role";
        unsupported(
            &refused_with(refused, crate::connect::UNSERVED, what)?,
            what,
        )?;
        Ok(Found::Kept)
    };
    checked.await.into()
}
