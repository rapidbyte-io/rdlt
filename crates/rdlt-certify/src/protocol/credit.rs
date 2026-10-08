//! The credit's clause: a read sends nothing more once its credit is spent, and the rest once
//! more is granted.
//!
//! The clause grants one byte and takes the frame it buys, which spends its own size. It then
//! grants again, three times over, a byte or nothing, each too little to bring the credit above
//! nothing, and watches after each for a frame: a source that sends on any grant, or at its own pace,
//! sends one. Silence is watched for a bounded time, so a source slower than the whole watch is
//! not told from one that waits.

#[cfg(test)]
mod tests;

use std::time::Duration;

use rdlt_connector::Role;
use rdlt_connector::testing::Reason;
use rdlt_connector::wire::v1;
use rdlt_host::remote::Client;
use rdlt_wire::plane::Incoming;
use rdlt_wire::prost::Message as _;
use rdlt_wire::tonic::Status;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;

use super::{Found, Violation, handshaken};
use crate::limits::CREDIT_WATCH;
use crate::target::Target;

/// How many grants, each too small to restore the credit, a read is watched after.
const REGRANTS: usize = 3;

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
            return Ok(Found::Unobserved(
                "the source has no partition to read".to_owned(),
            ));
        };
        let (controls, receiver) = mpsc::channel(4);
        let start = v1::read_control::Control::Start(v1::ReadStart {
            stream: Some(stream),
            partition,
            cursor: None,
            barrier: 0,
            unbounded: false,
            follow: false,
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
            .map_err(|status| format!("the read failed: {}", super::error(&status)))?;
        // One frame may go while any credit remains, and spend it below zero.
        let first = frames
            .message()
            .await
            .map_err(|status| failed(&status))?
            .ok_or(Violation::from("the read ended without a frame"))?;
        if matches!(first.frame, Some(v1::read_frame::Frame::Done(_))) {
            return Ok(Found::Unobserved(
                "the read ended within its first credit".to_owned(),
            ));
        }
        // A frame spends its encoded size, as the host that grants credit counts it.
        let spent = u64::try_from(first.encoded_len()).unwrap_or(u64::MAX);
        let quiet = target.chosen_watch().unwrap_or(CREDIT_WATCH);
        waits_then_resumes(&controls, &mut frames, regrants(spent), quiet).await?;
        Ok(Found::Kept)
    };
    checked.await.into()
}

/// What a pass says when the read was watched for other than [`CREDIT_WATCH`] after each grant:
/// a source that sends less often than its watch is not told from one that waits.
pub(super) fn note(target: &Target) -> Option<Reason> {
    let watch = target
        .chosen_watch()
        .filter(|watch| *watch != CREDIT_WATCH)?;
    Some(Reason::from(format!(
        "watched {watch:?} after each grant, not {CREDIT_WATCH:?}"
    )))
}

/// The bytes of each of the [`REGRANTS`] grants that leave the credit of a read spent, once
/// one byte bought a frame of `spent` bytes: a byte while one more keeps the credit at nothing
/// or below, and none after, so a read is granted as often however little its frame spent.
fn regrants(spent: u64) -> [u64; REGRANTS] {
    // One byte was granted and `spent` taken: each byte more is one less below nothing.
    let mut below = spent.saturating_sub(1);
    std::array::from_fn(|_| {
        let granted = u64::from(below > 0);
        below -= granted;
        granted
    })
}

/// Watches `frames` for `quiet`: a violation when the read, its credit spent, sends a frame.
async fn stays_quiet(
    frames: &mut Incoming<v1::ReadFrame>,
    quiet: Duration,
) -> Result<(), Violation> {
    match tokio::time::timeout(quiet, frames.message()).await {
        Ok(Ok(Some(_))) => Err(Violation::from(
            "the read sent a frame after its credit was spent",
        )),
        Ok(Err(status)) => Err(failed(&status)),
        Ok(Ok(None)) => Err(Violation::from("the read ended without its done frame")),
        Err(_) => Ok(()),
    }
}

/// Checks that a read whose credit is spent sends nothing more for `quiet`, whatever it is
/// granted that does not restore its credit, each of `regrants` in turn, and goes on once granted
/// more; then stops it, since the rest of the partition, however long, need not be read.
async fn waits_then_resumes(
    controls: &mpsc::Sender<v1::ReadControl>,
    frames: &mut Incoming<v1::ReadFrame>,
    regrants: [u64; REGRANTS],
    quiet: Duration,
) -> Result<(), Violation> {
    stays_quiet(frames, quiet).await?;
    for bytes in regrants {
        send(
            controls,
            v1::read_control::Control::Credit(v1::Credit { bytes }),
        )
        .await?;
        stays_quiet(frames, quiet).await?;
    }
    send(
        controls,
        v1::read_control::Control::Credit(v1::Credit { bytes: PLENTY }),
    )
    .await?;
    match frames.message().await {
        Ok(Some(_)) => {
            let stop = v1::Stop {
                mode: v1::StopMode::Now as i32,
            };
            // A read already done takes no more controls.
            send(controls, v1::read_control::Control::Stop(stop))
                .await
                .ok();
            Ok(())
        }
        Err(status) => Err(failed(&status)),
        Ok(None) => Err(Violation::from(
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
        .rpc
        .discover(v1::DiscoverRequest {})
        .await
        .map_err(|status| failed("the discovery", &status))?
        .into_inner();
    let Some(stream) = catalog.streams.into_iter().find_map(|spec| spec.name) else {
        return Ok(None);
    };
    let planned = client
        .rpc
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
