//! The malformed calls' clause: a read or a write that does not begin with its start, and a batch
//! a destination cannot decode, are refused with typed errors, and the connection serves on.

use rdlt_connector::Role;
use rdlt_connector::wire::{MALFORMED_FRAME, v1};
use rdlt_wire::prost::bytes::Bytes;

use super::writing::Writing;
use super::{Found, Violation, first_refusal, handshaken, refused_with};
use crate::target::Target;

/// The code of a message the connector cannot read.
const INVALID_MESSAGE: &str = "invalid_message";

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
                    first_refusal(client.rpc.read(controls).await).await,
                    INVALID_MESSAGE,
                    what,
                )?;
            }
            Role::Destination => {
                let frames = tokio_stream::iter([v1::WriteFrame {
                    frame: Some(v1::write_frame::Frame::Flush(v1::Unit {})),
                }]);
                let what = "a write that begins with a flush";
                let refusal = async { client.write(frames).await?.message().await.map(drop) };
                refused_with(refusal.await, INVALID_MESSAGE, what)?;
                let garbage = v1::WriteBatch {
                    segment: 1,
                    data_header: Bytes::from_static(b"not an IPC message"),
                    data_body: Bytes::new(),
                };
                let refusal = Writing::start(&mut client).await?.refusal(garbage).await?;
                if refusal.code() != Some(MALFORMED_FRAME) {
                    return Err(Violation::from(format!(
                        "a batch that is no IPC message was refused with `{refusal}`, not with \
                         `{MALFORMED_FRAME}`"
                    )));
                }
            }
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
