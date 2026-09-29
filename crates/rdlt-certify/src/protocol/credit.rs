//! The credit's clause: a read sends nothing more once its credit is spent, and the rest once
//! more is granted.

#[cfg(test)]
mod tests;

use std::time::Duration;

use rdlt_connector::Role;
use rdlt_connector::wire::v1;
use rdlt_host::remote::Client;
use rdlt_wire::tonic::{Status, Streaming};
use tokio::sync::mpsc;
use tokio_stream::StreamExt as _;
use tokio_stream::wrappers::ReceiverStream;

use super::{Found, Violation, handshaken};
use crate::target::Target;

/// How long a read whose credit is spent is watched for frames it must not send.
const QUIET: Duration = Duration::from_secs(1);

/// The credit granted once the read has shown it waits: enough for the rest of any partition.
const PLENTY: u64 = 1 << 40;

/// Checks `P-CREDIT`.
pub(super) async fn respected(target: &Target, role: Role, config: &str) -> Found {
    if role == Role::Destination {
        return Found::Inapplicable("a destination grants credit, and spends none".to_owned());
    }
    let checked = async {
        let (mut client, _) = handshaken(target, role, config).await?;
        let Some((stream, partition)) = first_partition(&mut client).await? else {
            return Ok(Some("the source has no partition to read".to_owned()));
        };
        let (controls, receiver) = mpsc::channel(4);
        let start = v1::read_control::Control::Start(v1::ReadStart {
            stream: Some(stream),
            partition,
            cursor: None,
            barrier: 0,
            unbounded: false,
        });
        send(&controls, start).await?;
        send(
            &controls,
            v1::read_control::Control::Credit(v1::Credit { bytes: 1 }),
        )
        .await?;
        let mut frames = client
            .read(ReceiverStream::new(receiver))
            .await
            .map_err(|status| format!("the read failed: {}", super::error(&status)))?
            .into_inner();
        // One frame may go while any credit remains, and spend it below zero.
        let first = frames
            .next()
            .await
            .ok_or(Violation::from("the read ended without a frame"))?
            .map_err(|status| failed(&status))?;
        if matches!(first.frame, Some(v1::read_frame::Frame::Done(_))) {
            return Ok(Some("the read ended within its first credit".to_owned()));
        }
        waits_then_resumes(&controls, &mut frames).await?;
        Ok(None)
    };
    checked.await.into()
}

/// Checks that a read whose credit is spent sends nothing more, and goes on once granted more;
/// then stops it, since the rest of the partition, however long, need not be read.
async fn waits_then_resumes(
    controls: &mpsc::Sender<v1::ReadControl>,
    frames: &mut Streaming<v1::ReadFrame>,
) -> Result<(), Violation> {
    match tokio::time::timeout(QUIET, frames.next()).await {
        Ok(Some(Ok(_))) => {
            return Err(Violation::from(
                "the read sent a frame after its credit was spent",
            ));
        }
        Ok(Some(Err(status))) => return Err(failed(&status)),
        Ok(None) => return Err(Violation::from("the read ended without its done frame")),
        Err(_) => {}
    }
    send(
        controls,
        v1::read_control::Control::Credit(v1::Credit { bytes: PLENTY }),
    )
    .await?;
    match frames.next().await {
        Some(Ok(_)) => {
            let stop = v1::Stop {
                mode: v1::StopMode::Now as i32,
            };
            // A read already done takes no more controls.
            send(controls, v1::read_control::Control::Stop(stop))
                .await
                .ok();
            Ok(())
        }
        Some(Err(status)) => Err(failed(&status)),
        None => Err(Violation::from(
            "the read ended without its done frame once credit was granted",
        )),
    }
}

/// A violation for a read that failed with `status`.
fn failed(status: &Status) -> Violation {
    Violation::from(format!("the read failed: {}", super::error(status)))
}

/// The first partition of the source's first stream, as it plans them from the beginning.
pub(super) async fn first_partition(
    client: &mut Client,
) -> Result<Option<(v1::StreamName, String)>, Violation> {
    let failed = |what: &str, status: &Status| format!("{what} failed: {}", super::error(status));
    let catalog = client
        .discover(v1::DiscoverRequest {})
        .await
        .map_err(|status| failed("the discovery", &status))?
        .into_inner();
    let Some(stream) = catalog.streams.into_iter().find_map(|spec| spec.name) else {
        return Ok(None);
    };
    let planned = client
        .plan(v1::PlanRequest {
            stream: Some(stream.clone()),
            state: Some(v1::StreamState::default()),
        })
        .await
        .map_err(|status| failed("the plan", &status))?
        .into_inner();
    Ok(planned
        .partitions
        .into_iter()
        .next()
        .map(|partition| (stream, partition)))
}

async fn send(
    controls: &mpsc::Sender<v1::ReadControl>,
    control: v1::read_control::Control,
) -> Result<(), Violation> {
    controls
        .send(v1::ReadControl {
            control: Some(control),
        })
        .await
        .map_err(|_| Violation::from("the read ended before its controls were sent"))
}
