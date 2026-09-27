//! The heartbeat's clause: every ping answered with its sequence number, in order.

use rdlt_connector::Role;
use rdlt_connector::wire::v1;
use rdlt_wire::tonic::Status;
use tokio_stream::StreamExt as _;

use super::{Found, Violation, handshaken};
use crate::target::Target;

/// How many pings the clause sends.
const PINGS: u64 = 5;

/// Checks `P-HEARTBEAT`.
pub(super) async fn echoed(target: &Target, role: Role, config: &str) -> Found {
    let checked = async {
        let (mut client, _) = handshaken(target, role, config).await?;
        let sent = tokio_stream::iter((1..=PINGS).map(|seq| v1::Ping { seq }));
        let failed =
            |status: &Status| Violation(format!("the heartbeat failed: {}", super::error(status)));
        let mut pongs = client
            .heartbeat(sent)
            .await
            .map_err(|status| failed(&status))?
            .into_inner();
        // The pings end with their stream, and so must the answers.
        let mut answered = Vec::new();
        while let Some(pong) = pongs.next().await {
            answered.push(pong.map_err(|status| failed(&status))?.seq);
        }
        let expected: Vec<u64> = (1..=PINGS).collect();
        if answered == expected {
            Ok(())
        } else {
            Err(Violation(format!(
                "pings {expected:?} were answered with {answered:?}"
            )))
        }
    };
    checked.await.into()
}
