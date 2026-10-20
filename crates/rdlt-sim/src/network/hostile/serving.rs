//! Attacks on serving's order of calls, its configuration, its sessions and its limits.

use std::time::Duration;

use rdlt_connector::limits::ListenLimits;
use rdlt_connector::wire::v1;
use rdlt_connector::{LoadId, SchemaVersion, TablePath, TableRef};
use rdlt_host::remote::Client;
use rdlt_wire::Limits;
use rdlt_wire::limits::LIMIT_EXCEEDED;

use super::factories::Asked;
use super::{Attacker, PATIENCE, answer, handshake};
use crate::network::Side;
use crate::rng::SplitMix64;

/// The pipeline a hostile host opens sessions for, which no run loads.
const PIPELINE: &str = "sim-h";

impl Attacker<'_> {
    /// Calls before the handshake and the configuration they need are refused, and so is a second
    /// handshake.
    pub(super) async fn order(&self, side: Side) {
        let Some(mut client) = self.client(side).await else {
            return;
        };
        let checked = answer(client.rpc.check(v1::CheckRequest {})).await;
        self.refused("check before the handshake", checked, "no_handshake");
        let configured = answer(client.rpc.configure(self.configuration(Asked::idle()))).await;
        self.refused(
            "configuration before the handshake",
            configured,
            "no_handshake",
        );
        let handshook = answer(client.rpc.handshake(handshake(side))).await;
        if self.answered("handshake", handshook).is_none() {
            return;
        }
        let checked = answer(client.rpc.check(v1::CheckRequest {})).await;
        self.refused("check before the configuration", checked, "not_configured");
        let again = answer(client.rpc.handshake(handshake(side))).await;
        self.refused("second handshake", again, "handshake_repeated");
    }

    /// Two configurations of one connection at once: one connects and the other is refused as
    /// repeated, and the watched connector never connects two at once.
    pub(super) async fn configurations(&self, side: Side, rng: &mut SplitMix64) {
        let Some(client) = self.handshaken(side).await else {
            return;
        };
        let connection = rng.next_u64();
        let asked = |pause: u64| {
            self.configuration(Asked::connecting(connection, Duration::from_millis(pause)))
        };
        // The first pauses as it connects, so the second may arrive while it does.
        let (first, second) = (asked(1 + rng.below(20)), asked(rng.below(20)));
        let (mut one, mut other) = (client.rpc.clone(), client.rpc.clone());
        let (first, second) = tokio::join!(
            answer(one.configure(first)),
            answer(other.configure(second))
        );
        let (Some(first), Some(second)) = (first, second) else {
            self.lost("configurations at once");
            return;
        };
        let repeated =
            |error: &rdlt_connector::ConnectorError| error.code() == Some("configure_repeated");
        let kept = match (&first, &second) {
            (Ok(_), Err(error)) | (Err(error), Ok(_)) => repeated(error),
            _ => false,
        };
        if self.strict && !kept {
            self.found(
                "configurations at once",
                &format!(
                    "answered {first:?} and {second:?}, not one configured and one refused as \
                     repeated"
                ),
            );
        }
    }

    /// A configuration given up on while its connector connects leaves the connection to the next.
    pub(super) async fn abandoned(&self, side: Side, rng: &mut SplitMix64) {
        let Some(client) = self.handshaken(side).await else {
            return;
        };
        let connection = rng.next_u64();
        // Long enough that the call's cancellation, sent a quarter of the way through, reaches
        // serving at least four times the network's longest latency before the connector connects.
        let pause = Duration::from_millis(140 + rng.below(100));
        let mut abandoned = client.rpc.clone();
        let abandoning =
            abandoned.configure(self.configuration(Asked::connecting(connection, pause)));
        tokio::time::timeout(pause / 4, abandoning).await.ok();
        let mut next = client.rpc.clone();
        let configured = answer(
            next.configure(self.configuration(Asked::connecting(connection, Duration::ZERO))),
        )
        .await;
        self.answered("configuration after one given up on", configured);
    }

    /// Sessions opened beyond those one connection holds.
    ///
    /// A connection keeps its newest sessions, closing its oldest for room, and the destination
    /// never holds more than it may.
    pub(super) async fn sessions(&self, rng: &mut SplitMix64) {
        let Some(mut client) = self.handshaken(Side::Destination).await else {
            return;
        };
        let connection = rng.next_u64();
        let opening = Duration::from_millis(140 + rng.below(100));
        let asked = self.configuration(Asked::opening(connection, opening));
        if self
            .answered("configuration", answer(client.rpc.configure(asked)).await)
            .is_none()
        {
            return;
        }
        let held = ListenLimits::default().connection_sessions();
        for _ in 0..=rng.below(3) {
            let mut abandoned = client.rpc.clone();
            // Given up on a quarter of the way through its destination's pause.
            tokio::time::timeout(opening / 4, abandoned.open(open(rng)))
                .await
                .ok();
        }
        let mut opened = Vec::new();
        for _ in 0..held + 1 + usize::try_from(rng.below(3)).unwrap_or(0) {
            let answered = answer(client.rpc.open(open(rng))).await;
            if let Some(answered) = self.answered("open", answered) {
                opened.push(answered.session);
            }
        }
        // Each open beyond those the connection holds closed the oldest.
        let closed_for_room = opened.len().saturating_sub(held);
        for (index, session) in opened.iter().enumerate() {
            let closed = answer(client.rpc.close(v1::CloseRequest { session: *session })).await;
            if index < closed_for_room {
                self.refused("close of a session closed for room", closed, "no_session");
            } else {
                self.answered("close of a session held", closed);
            }
        }
        self.in_flight(&mut client, held, opening, rng).await;
        let at_once = (0..=held).map(|_| {
            let (mut rpc, request) = (client.rpc.clone(), open(rng));
            async move { answer(rpc.open(request)).await }
        });
        for answered in futures_util::future::join_all(at_once).await {
            self.answered("open beside others", answered);
        }
    }

    /// A session beyond the `held` a connection holds, opened while a call is in flight on its
    /// oldest: serving closes the oldest only once that call ends, so the destination never holds
    /// more than `held`, and the open is answered after the call.
    async fn in_flight(
        &self,
        client: &mut Client,
        held: usize,
        opening: Duration,
        rng: &mut SplitMix64,
    ) {
        let mut opened = Vec::new();
        for _ in 0..held {
            if let Some(answered) = self.answered("open", answer(client.rpc.open(open(rng))).await)
            {
                opened.push(answered.session);
            }
        }
        let Some(&oldest) = opened.first() else {
            return;
        };
        // The oldest's call for a writer stays in flight for the destination's pause.
        let start = v1::WriteFrame {
            frame: Some(v1::write_frame::Frame::Start(v1::WriteStart {
                session: oldest,
                table: Some(v1::TableRef::from(&table())),
            })),
        };
        let mut writing = client.clone();
        let write = async move {
            let frames = futures_util::stream::iter([start]);
            tokio::time::timeout(PATIENCE, writing.write(frames))
                .await
                .is_ok()
        };
        let mut beyond = client.rpc.clone();
        let request = open(rng);
        let open_beyond = async move {
            // Sent once the write's call has reached serving and holds the oldest.
            tokio::time::sleep(opening / 4).await;
            answer(beyond.open(request)).await
        };
        let (written, opened_beyond) = tokio::join!(write, open_beyond);
        if !written {
            self.lost("write on a session in flight");
        }
        self.answered(
            "open beyond the sessions held, beside a call in flight",
            opened_beyond,
        );
    }

    /// A configuration beyond the configuration limit is refused as beyond it.
    pub(super) async fn oversized(&self, side: Side) {
        let Some(client) = self.handshaken(side).await else {
            return;
        };
        // This end sends whatever the size: what refuses it is the connector's.
        let mut client = client.max_encoding_message_size(usize::MAX);
        let over = usize::try_from(Limits::default().config_bytes)
            .unwrap_or(usize::MAX - 1)
            .saturating_add(1);
        let config_json = format!("{{\"padding\":\"{}\"}}", "x".repeat(over));
        let configured = answer(client.rpc.configure(v1::ConfigureRequest { config_json })).await;
        self.refused(
            "configuration beyond the configuration limit",
            configured,
            LIMIT_EXCEEDED,
        );
    }
}

/// An open of a session for [`PIPELINE`], under a load drawn from `rng`.
fn open(rng: &mut SplitMix64) -> v1::OpenRequest {
    let at = std::time::UNIX_EPOCH + Duration::from_secs(1);
    let load = LoadId::from_parts(at, u128::from(rng.next_u64()));
    v1::OpenRequest {
        pipeline: PIPELINE.to_owned(),
        load_id: load.as_bytes().to_vec().into(),
    }
}

/// The table a hostile session asks a writer for, which its watched destination never gives.
fn table() -> TableRef {
    TableRef {
        path: TablePath::new(["h"]).expect("a one-segment path is valid"),
        name: "h".into(),
        version: SchemaVersion(1),
        generation: None,
        merge: None,
    }
}
