//! The malformed calls' clause: a read or a write that does not begin with its start, a batch a
//! destination cannot decode, and one whose buffers lie off their alignment where it arrives, are
//! refused with typed errors, and the connection serves on.

use rdlt_connector::Role;
use rdlt_connector::wire::{INVALID_MESSAGE, MALFORMED_FRAME, v1};
use rdlt_host::remote::Client;
use rdlt_wire::prost::bytes::Bytes;

use super::writing::{Writing, misaligned};
use super::{Found, Violation, first_refusal, handshaken, refused_with};
use crate::target::Target;

/// Checks `P-MALFORMED`.
pub(super) async fn refused(target: &Target, role: Role, config: &str) -> Found {
    let checked = async {
        let (mut client, _) = handshaken(target, role, config).await?;
        match role {
            Role::Source => {
                let controls = tokio_stream::iter([v1::ReadControl {
                    control: Some(v1::read_control::Control::Credit(v1::Credit {
                        bytes: 1024,
                    })),
                }]);
                let what = "a read that begins with credit";
                refused_with(
                    first_refusal(client.read(controls).await).await,
                    INVALID_MESSAGE,
                    what,
                )?;
            }
            Role::Destination => refused_writes(&mut client).await?,
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

/// Checks that a destination refuses a write that does not begin with its start, a batch that
/// is no IPC message and a batch whose values lie off their alignment, each with its typed
/// error.
async fn refused_writes(client: &mut Client) -> Result<(), Violation> {
    let frames = tokio_stream::iter([v1::WriteFrame {
        frame: Some(v1::write_frame::Frame::Flush(v1::Unit {})),
    }]);
    let what = "a write that begins with a flush";
    let refusal = first_refusal(client.write(frames).await).await;
    refused_with(refusal, INVALID_MESSAGE, what)?;
    let garbage = v1::WriteBatch {
        segment: 1,
        data_header: Bytes::from_static(b"not an IPC message"),
        padding: Bytes::new(),
        data_body: Bytes::new(),
    };
    let refusal = Writing::start(client).await?.refusal(garbage).await?;
    if refusal.code() != Some(MALFORMED_FRAME) {
        return Err(Violation::from(format!(
            "a batch that is no IPC message was refused with `{refusal}`, not with \
                 `{MALFORMED_FRAME}`"
        )));
    }
    // A batch whose eight-byte values lie four bytes off their alignment where it
    // arrives: no sender lays one out so, and none may be read in place.
    let off = misaligned(1)?;
    let refusal = Writing::start(client).await?.refusal(off).await?;
    if refusal.code() != Some(MALFORMED_FRAME) {
        return Err(Violation::from(format!(
            "a batch whose values lie off their alignment was refused with \
                 `{refusal}`, not with `{MALFORMED_FRAME}`"
        )));
    }
    Ok(())
}
