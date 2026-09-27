//! The malformed calls' clause: a read or a write that does not begin with its start is refused
//! with a typed error, and the connection serves on.

use rdlt_connector::Role;
use rdlt_connector::wire::v1;

use super::{Found, handshaken, refused_with};
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
                refused_with(client.read(controls).await, INVALID_MESSAGE, what)?;
            }
            Role::Destination => {
                let frames = tokio_stream::iter([v1::WriteFrame {
                    frame: Some(v1::write_frame::Frame::Flush(v1::Unit {})),
                }]);
                let what = "a write that begins with a flush";
                refused_with(client.write(frames).await, INVALID_MESSAGE, what)?;
            }
        }
        client.check(v1::CheckRequest {}).await.map_err(|status| {
            format!(
                "the connection served no check after the refusal: {}",
                super::error(&status)
            )
        })?;
        Ok(())
    };
    checked.await.into()
}
