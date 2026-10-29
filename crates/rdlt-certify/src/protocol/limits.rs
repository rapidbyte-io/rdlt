//! The limits' clause: what goes beyond a limit the connector declares is refused with
//! `limit_exceeded`: a configuration, a source's cursor, and a destination's batch frame.
//!
//! A host never sends beyond its own limits, so a connector's limit beyond the host's is never
//! met; such a limit is not exceeded, since exceeding it would only cost this process the memory.

#[cfg(test)]
mod tests;

use rdlt_connector::Role;
use rdlt_connector::wire::v1;
use rdlt_wire::limits::LIMIT_EXCEEDED;
use rdlt_wire::prost::bytes::Bytes;
use rdlt_wire::stream::Call;
use rdlt_wire::{Limits, PROTOCOL_MAJOR};

use super::writing::Writing;
use super::{
    Found, Violation, configure_request, error, first_partition, handshaken, refused_with, request,
};
use crate::target::Target;
use rdlt_host::remote::RawCall;

/// Checks `P-LIMITS`.
pub(super) async fn kept(target: &Target, role: Role, config: &str) -> Found {
    let checked = async {
        let (_, answer) = handshaken(target, role, config).await?;
        let Some(declared) = answer.limits.map(Limits::from) else {
            let reason = "the connector declares no limits";
            return Ok(Found::Inapplicable(reason.to_owned()));
        };
        let host = target.limits();
        let configured =
            configuration(target, role, declared.config_bytes, host.config_bytes).await?;
        let sent = match role {
            Role::Source => {
                cursor(target, config, declared.cursor_bytes, host.cursor_bytes).await?
            }
            Role::Destination => {
                frame(target, config, declared.frame_bytes, host.frame_bytes).await?
            }
        };
        Ok(found(configured, sent))
    };
    checked.await.into()
}

/// How one of the connector's limits fared.
#[derive(Debug, PartialEq, Eq)]
pub(super) enum Limit {
    /// What went beyond it was refused as it must be.
    Kept,
    /// The connector declares none this host can exceed, for the stated reason.
    Unexceeded(String),
    /// Nothing could be sent beyond it, for the stated reason.
    Unsent(String),
}

/// What the clause found of the two limits it checks: kept when one was, as before; else not
/// observed when one could not be sent beyond, and inapplicable when neither can be exceeded.
pub(super) fn found(first: Limit, second: Limit) -> Found {
    match (first, second) {
        (Limit::Kept, _) | (_, Limit::Kept) => Found::Kept,
        (Limit::Unsent(reason), _) | (_, Limit::Unsent(reason)) => Found::Unobserved(reason),
        (Limit::Unexceeded(first), Limit::Unexceeded(second)) => {
            Found::Inapplicable(format!("{first}; {second}"))
        }
    }
}

/// One byte beyond `declared`, when a host keeping `host` can send that much.
fn beyond(declared: u64, host: u64) -> Option<usize> {
    Some(declared)
        .filter(|declared| (1..=host).contains(declared))
        .and_then(|declared| usize::try_from(declared).ok())
        .and_then(|declared| declared.checked_add(1))
}

/// Why the connector's `what` limit, `declared`, is not exceeded by a host keeping `host`.
fn unexceeded(what: &str, declared: u64, host: u64) -> Limit {
    Limit::Unexceeded(format!(
        "the connector declares no {what} limit this host can exceed ({declared} bytes; this host \
         sends at most {host})"
    ))
}

/// A configuration beyond the connector's limit.
async fn configuration(
    target: &Target,
    role: Role,
    declared: u64,
    host: u64,
) -> Result<Limit, Violation> {
    let Some(over) = beyond(declared, host) else {
        return Ok(unexceeded("configuration", declared, host));
    };
    let padded = format!("{{\"padding\":\"{}\"}}", "x".repeat(over));
    // This end sends whatever the size: what refuses it is the connector's.
    let mut client = target
        .client()
        .await
        .map_err(Violation::of)?
        .max_encoding_message_size(usize::MAX);
    client
        .rpc
        .handshake(request(role, PROTOCOL_MAJOR))
        .await
        .map_err(|status| format!("the handshake failed: {}", error(&status)))?;
    let refused = client.rpc.configure(configure_request(&padded)).await;
    refused_with(
        refused,
        LIMIT_EXCEEDED,
        "a configuration beyond the configuration limit",
    )?;
    Ok(Limit::Kept)
}

/// A read of the source's first partition from a cursor beyond the connector's limit.
async fn cursor(
    target: &Target,
    config: &str,
    declared: u64,
    host: u64,
) -> Result<Limit, Violation> {
    let Some(over) = beyond(declared, host) else {
        return Ok(unexceeded("cursor", declared, host));
    };
    let (mut client, _) = handshaken(target, Role::Source, config).await?;
    let Some((stream, partition)) = first_partition(&mut client).await? else {
        return Ok(Limit::Unsent(
            "the source has no partition to read".to_owned(),
        ));
    };
    let start = v1::ReadStart {
        stream: Some(stream),
        partition,
        cursor: Some(v1::Cursor {
            version: 1,
            bytes: Bytes::from(vec![b'x'; over]),
        }),
        barrier: 0,
        unbounded: false,
        follow: false,
    };
    let opened = RawCall::open(&client, Call::Read, &start, &target.limits()).await;
    let what = "a read from a cursor beyond the cursor limit";
    match opened.map_err(Violation::of)? {
        Ok(_) => Err(Violation::from(format!("{what} was opened, not refused"))),
        Err(refused) if refused.code() == Some(LIMIT_EXCEEDED) => Ok(Limit::Kept),
        Err(refused) => Err(Violation::from(format!(
            "{what} was refused with `{refused}`, not with `{LIMIT_EXCEEDED}`"
        ))),
    }
}

/// A batch frame beyond the connector's frame limit, in a write of a table of its own.
async fn frame(
    target: &Target,
    config: &str,
    declared: u64,
    host: u64,
) -> Result<Limit, Violation> {
    let Some(over) = beyond(declared, host) else {
        return Ok(unexceeded("frame", declared, host));
    };
    let (mut client, _) = handshaken(target, Role::Destination, config).await?;
    // A frame's body holds its message's prefix and metadata beside its frame: so much more, and
    // the frame is beyond the limit wherever its bytes lie.
    let room = usize::try_from(rdlt_wire::limits::MESSAGE_ROOM).unwrap_or(usize::MAX);
    let batch = Bytes::from(vec![0; over.saturating_add(room)]);
    let limits = target.limits();
    let refusal = Writing::start(&mut client, &limits)
        .await?
        .refusal(batch)
        .await?;
    if refusal.code() == Some(LIMIT_EXCEEDED) {
        Ok(Limit::Kept)
    } else {
        Err(Violation::from(format!(
            "a batch beyond the frame limit was refused with `{refusal}`, not with \
             `{LIMIT_EXCEEDED}`"
        )))
    }
}
