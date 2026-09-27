//! The credit's clause: a read sends nothing more once its credit is spent, and the rest once
//! more is granted.

use std::time::Duration;

use rdlt_connector::Role;
use rdlt_connector::wire::v1;
use rdlt_host::remote::Client;
use rdlt_wire::tonic::Status;
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
            .map_err(|status| format!("the read failed: {}", super::error(&status)))?;
        if matches!(first.frame, Some(v1::read_frame::Frame::Done(_))) {
            return Ok(Some("the read ended within its first credit".to_owned()));
        }
        if let Ok(Some(_)) = tokio::time::timeout(QUIET, frames.next()).await {
            return Err(Violation::from(
                "the read sent a frame after its credit was spent",
            ));
        }
        send(
            &controls,
            v1::read_control::Control::Credit(v1::Credit { bytes: PLENTY }),
        )
        .await?;
        while let Some(frame) = frames.next().await {
            let frame =
                frame.map_err(|status| format!("the read failed: {}", super::error(&status)))?;
            if matches!(frame.frame, Some(v1::read_frame::Frame::Done(_))) {
                return Ok(None);
            }
        }
        Err(Violation::from(
            "the read ended without its done frame once credit was granted",
        ))
    };
    checked.await.into()
}

/// The first partition of the source's first stream, as it plans them from the beginning.
async fn first_partition(
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
