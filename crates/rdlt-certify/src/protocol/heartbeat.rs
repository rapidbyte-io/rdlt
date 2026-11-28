//! The heartbeat's clause: every ping answered with its sequence number, in order, each while the
//! pings' stream stays open, as a host's does.

use rdlt_connector::Role;
use rdlt_connector::wire::v1;
use rdlt_wire::tonic::Status;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;

use super::{Found, Violation, handshaken};
use crate::target::Target;

/// How many pings the clause sends.
const PINGS: u64 = 5;

/// Checks `P-HEARTBEAT`: each ping goes once the ping before it was answered, on a stream that
/// ends only after the last answer, and each answer is waited for within the target's heartbeat
/// interval, as its host pings.
pub(super) async fn echoed(target: &Target, role: Role, config: &str) -> Found {
    let checked = async {
        let (mut client, _) = handshaken(target, role, config).await?;
        let failed =
            |status: &Status| Violation(format!("the heartbeat failed: {}", target.error(status)));
        let interval = target.heartbeat();
        let (heartbeats, sent) = mpsc::channel(1);
        heartbeats
            .send(v1::Ping { seq: 1 })
            .await
            .map_err(|_| Violation::from("the first ping could not be sent"))?;
        let mut answers = client
            .rpc
            .heartbeat(ReceiverStream::new(sent))
            .await
            .map_err(|status| failed(&status))?
            .into_inner();
        for seq in 1..=PINGS {
            // A call that ended dropped its pings: its answers' stream says so next.
            if seq > 1 {
                heartbeats.send(v1::Ping { seq }).await.ok();
            }
            let Ok(pong) = tokio::time::timeout(interval, answers.message()).await else {
                return Err(Violation(format!(
                    "ping {seq} was not answered within {interval:?}, the pings' stream still \
                     open"
                )));
            };
            match pong.map_err(|status| failed(&status))? {
                Some(pong) if pong.seq == seq => {}
                Some(pong) => {
                    return Err(Violation(format!(
                        "ping {seq} was answered with {}",
                        pong.seq
                    )));
                }
                None => {
                    return Err(Violation(format!(
                        "the heartbeat ended before ping {seq} was answered"
                    )));
                }
            }
        }
        Ok(())
    };
    checked.await.into()
}
