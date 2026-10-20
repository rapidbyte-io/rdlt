//! A hostile host: beside a seed's runs it attacks the connectors' serving as no host that keeps to
//! the protocol would, and checks what serving answers.
//!
//! Where the network is disrupted an answer may be lost or cut, so serving's answers are judged
//! only where the run is clean and the network healed; what the connectors are made to do is
//! watched always.

mod factories;
mod serving;
#[cfg(test)]
mod tests;
mod window;

use std::future::Future;
use std::time::Duration;

use rdlt_connector::wire::{TRANSPORT, v1};
use rdlt_connector::{ConnectorError, ConnectorId, destination_factory, source_factory};
use rdlt_host::remote::Client;
use rdlt_wire::tonic::{Response, Status};
use rdlt_wire::{Limits, PROTOCOL_MAJOR};

use super::{Placing, Side, reference};
use crate::destination::SimDestination;
use crate::reached::Reach;
use crate::rng::SplitMix64;
use crate::source::SimSource;
use crate::world::World;
use factories::Asked;
pub(crate) use factories::Watch;

/// How long a hostile call may go unanswered before it counts as lost to the network.
const PATIENCE: Duration = Duration::from_secs(30);

/// One attack on the connectors' serving.
#[derive(Clone, Copy, Debug)]
enum Attack {
    /// Calls before the handshake and the configuration they need, and a second handshake.
    Order(Side),
    /// Two configurations of one connection at once.
    Configurations(Side),
    /// A configuration given up on as its connector connects, and another after it.
    Abandoned(Side),
    /// Sessions opened beyond those one connection holds, some given up on as they open.
    Sessions,
    /// A configuration beyond the configuration limit.
    Oversized(Side),
    /// Writes whose bodies hold the connection's window, and writes waiting for room behind them.
    Window,
}

impl Attack {
    /// The attack `rng` draws, against the side it draws; a configuration beyond its limit one
    /// time in eighteen.
    fn draw(rng: &mut SplitMix64) -> Self {
        let side = if rng.chance(500) {
            Side::Source
        } else {
            Side::Destination
        };
        match rng.below(18) {
            0..=3 => Self::Order(side),
            4..=7 => Self::Configurations(side),
            8..=10 => Self::Abandoned(side),
            11..=14 => Self::Sessions,
            15 => Self::Oversized(side),
            _ => Self::Window,
        }
    }
}

/// A hostile host making one attack: where it dials, the world it names and records findings in,
/// and whether the network is healed, so every call must be answered as the protocol says.
struct Attacker<'a> {
    placing: &'a Placing,
    world: &'a World,
    name: &'a str,
    strict: bool,
}

impl Placing {
    /// Runs `work` while a hostile host makes the attack `rng` draws on the connectors' serving.
    ///
    /// What breaks is recorded in `world`, registered as `name`. Where `strict`, the network is
    /// healed and the connectors are up, so every answer is judged.
    pub(crate) async fn attacking<T>(
        &self,
        mut rng: SplitMix64,
        world: &World,
        name: &str,
        strict: bool,
        work: impl Future<Output = T>,
    ) -> T {
        let attack = Attack::draw(&mut rng);
        match attack {
            Attack::Configurations(_) => world.reach(Reach::ConfiguredTwice),
            Attack::Sessions => world.reach(Reach::SessionsBeyond),
            _ => {}
        }
        let attacker = Attacker {
            placing: self,
            world,
            name,
            strict,
        };
        let (done, ()) = tokio::join!(work, Box::pin(attacker.attack(attack, rng)));
        done
    }

    /// A raw connection to `side`'s connector, before its handshake; none where the dial failed.
    pub(super) async fn wire(&self, side: Side) -> Option<rdlt_host::Wire> {
        let id: ConnectorId = match side {
            Side::Source => source_factory::<SimSource>().spec().id.clone(),
            Side::Destination => destination_factory::<SimDestination>().spec().id.clone(),
        };
        self.remote.wire(&reference(side.host(), id)).await.ok()
    }
}

impl Attacker<'_> {
    /// Makes `attack`, drawing what it varies from `rng`.
    async fn attack(&self, attack: Attack, mut rng: SplitMix64) {
        match attack {
            Attack::Order(side) => self.order(side).await,
            Attack::Configurations(side) => self.configurations(side, &mut rng).await,
            Attack::Abandoned(side) => self.abandoned(side, &mut rng).await,
            Attack::Sessions => self.sessions(&mut rng).await,
            Attack::Oversized(side) => self.oversized(side).await,
            Attack::Window => self.window(&mut rng).await,
        }
    }

    /// Records `finding` of the hostile host's `what`.
    fn found(&self, what: &str, finding: &str) {
        self.world
            .violation(format!("a hostile host's {what}: {finding}"));
    }

    /// Records that `what` went unanswered, where the network is healed.
    fn lost(&self, what: &str) {
        if self.strict {
            self.found(what, "unanswered on a healed network");
        }
    }

    /// `answer` where it is one; where the network is healed, a refusal or no answer is recorded.
    fn answered<T>(&self, what: &str, answer: Option<Result<T, ConnectorError>>) -> Option<T> {
        match answer {
            Some(Ok(answered)) => Some(answered),
            Some(Err(error)) => {
                if self.strict {
                    self.found(what, &format!("refused with {error}"));
                }
                None
            }
            None => {
                self.lost(what);
                None
            }
        }
    }

    /// Records, where the network is healed, that `what` was answered or refused with any code
    /// but `code`.
    fn refused<T: std::fmt::Debug>(
        &self,
        what: &str,
        answer: Option<Result<T, ConnectorError>>,
        code: &str,
    ) {
        match answer {
            Some(Err(error)) if error.code() == Some(code) => {}
            Some(other) if self.strict => {
                self.found(what, &format!("{other:?}, not a refusal coded {code}"));
            }
            Some(_) => {}
            None => self.lost(what),
        }
    }

    /// A client of `side`'s connector, with no handshake yet.
    async fn client(&self, side: Side) -> Option<Client> {
        let client = match self.placing.wire(side).await {
            Some(wire) => rdlt_host::remote::client(wire, self.placing.remote_options)
                .await
                .ok(),
            None => None,
        };
        if client.is_none() {
            self.lost("dial");
        }
        client
    }

    /// A client of `side`'s connector whose handshake was answered.
    async fn handshaken(&self, side: Side) -> Option<Client> {
        let mut client = self.client(side).await?;
        let answer = answer(client.rpc.handshake(handshake(side))).await;
        self.answered("handshake", answer).map(|_| client)
    }

    /// A configuration of the world this host names, asking `asked`.
    fn configuration(&self, asked: Asked) -> v1::ConfigureRequest {
        v1::ConfigureRequest {
            config_json: asked.config(self.name),
        }
    }
}

/// `call`'s answer; none where the network lost it, by no answer within [`PATIENCE`] or a
/// transport that failed.
async fn answer<T>(
    call: impl Future<Output = Result<Response<T>, Status>>,
) -> Option<Result<T, ConnectorError>> {
    match tokio::time::timeout(PATIENCE, call).await {
        Err(_) => None,
        Ok(Ok(answer)) => Some(Ok(answer.into_inner())),
        Ok(Err(status)) => {
            let error = rdlt_connector::wire::error(&status);
            (error.code() != Some(TRANSPORT)).then_some(Err(error))
        }
    }
}

/// A handshake as `side`'s role, at this host's protocol and limits.
fn handshake(side: Side) -> v1::HandshakeRequest {
    let role = match side {
        Side::Source => v1::Role::Source,
        Side::Destination => v1::Role::Destination,
    };
    v1::HandshakeRequest {
        protocol_major: PROTOCOL_MAJOR,
        features: Vec::new(),
        role: role as i32,
        limits: Some(Limits::default().into()),
    }
}
