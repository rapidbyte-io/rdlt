//! The malformed calls' clause: a read or a write whose opening is no start of its call, a batch a
//! destination cannot decode, and one whose buffers lie off their alignment where it arrives, are
//! refused with typed errors, and the connection serves on.

use rdlt_connector::Role;
use rdlt_connector::wire::{INVALID_MESSAGE, MALFORMED_FRAME, v1};
use rdlt_host::remote::Client;
use rdlt_wire::prost::bytes::Bytes;
use rdlt_wire::stream::Call;

use super::writing::{Writing, misaligned};
use super::{Found, Violation, handshaken};
use crate::target::Target;
use rdlt_host::remote::RawCall;

/// Bytes that are no encoding of any message: a field of a wire type protocol buffers lacks.
const GARBLED: &[u8] = &[0xff, 0xff];

/// Checks `P-MALFORMED`.
pub(super) async fn refused(target: &Target, role: Role, config: &str) -> Found {
    let checked = async {
        let (mut client, _) = handshaken(target, role, config).await?;
        let limits = target.limits();
        match role {
            Role::Source => {
                let what = "a read whose opening is no read's start";
                refused_opening(&client, Call::Read, &limits, what).await?;
            }
            Role::Destination => refused_writes(&mut client, &limits).await?,
        }
        client
            .rpc
            .check(v1::CheckRequest {})
            .await
            .map_err(|status| {
                format!(
                    "the connection served no check after the refusal: {}",
                    super::error(&status)
                )
            })?;
        Ok(())
    };
    checked.await.into()
}

/// Checks that `call` opened with an opening that is no start of it is refused with
/// `invalid_message`; `what` names the call.
async fn refused_opening(
    client: &Client,
    call: Call,
    limits: &rdlt_wire::Limits,
    what: &str,
) -> Result<(), Violation> {
    let opened = RawCall::opened(client, call, Bytes::from_static(GARBLED), limits).await;
    match opened.map_err(Violation::of)? {
        Ok(_) => Err(Violation::from(format!("{what} was opened, not refused"))),
        Err(refused) if refused.code() == Some(INVALID_MESSAGE) => Ok(()),
        Err(refused) => Err(Violation::from(format!(
            "{what} was refused with `{refused}`, not with `{INVALID_MESSAGE}`"
        ))),
    }
}

/// Checks that a destination refuses a write whose opening is no write's start, a batch that is
/// no IPC message and a batch whose values lie off their alignment, each with its typed error.
async fn refused_writes(client: &mut Client, limits: &rdlt_wire::Limits) -> Result<(), Violation> {
    let what = "a write whose opening is no write's start";
    refused_opening(client, Call::Write, limits, what).await?;
    let garbage = Bytes::from_static(b"not an encapsulated IPC message");
    let refusal = Writing::start(client, limits)
        .await?
        .refusal(garbage)
        .await?;
    if refusal.code() != Some(MALFORMED_FRAME) {
        return Err(Violation::from(format!(
            "a batch that is no IPC message was refused with `{refusal}`, not with \
             `{MALFORMED_FRAME}`"
        )));
    }
    // A batch whose eight-byte values lie four bytes off their alignment where it arrives: no
    // sender lays one out so, and none may be read in place.
    let off = misaligned()?;
    let refusal = Writing::start(client, limits).await?.refusal(off).await?;
    if refusal.code() != Some(MALFORMED_FRAME) {
        return Err(Violation::from(format!(
            "a batch whose values lie off their alignment was refused with `{refusal}`, not \
             with `{MALFORMED_FRAME}`"
        )));
    }
    Ok(())
}
