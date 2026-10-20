//! Secrets in every simulated configuration, which nothing the engine, its host or a run's report
//! may hold: their values, the vault that resolves them, and the checks that look for them.

mod logged;
#[cfg(test)]
mod tests;

use std::fmt::{self, Write as _};

use rdlt_connector::{BoxFuture, Secret};
use rdlt_engine::RunOutcome;
use rdlt_host::{
    ProviderError, Registry, SecretFault, SecretKind, SecretReference, SecretResolver,
};

pub(crate) use logged::Logged;

use crate::changes::SimChangeSource;
use crate::destination::SimDestination;
use crate::oracle::aside;
use crate::source::SimSource;
use crate::world::World;

/// The secrets one world's configurations refer to.
///
/// One ends in whitespace, which a scrub that trims first misses. One holds several lines, as a
/// key read from a file does, of which each line long enough to be found alone is. One is shaped
/// as a version, so a connector can name itself by it.
#[derive(Clone)]
pub(crate) struct Canary {
    world: String,
}

impl fmt::Debug for Canary {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "Canary({})", self.world)
    }
}

impl Canary {
    /// The secrets of the world registered as `world`.
    pub(crate) fn of(world: &str) -> Self {
        Self {
            world: world.to_owned(),
        }
    }

    /// The configuration a connector of the world is placed with: the world, and a reference to
    /// each secret.
    pub(crate) fn config(&self) -> serde_json::Value {
        serde_json::json!({
            "world": self.world,
            "told": {
                "canary": "${secret:canary}",
                "lines": "${secret:lines}",
                "token": "${secret:token}",
            },
        })
    }

    /// The secret named `name`.
    fn value(&self, name: &str) -> Option<String> {
        let world = &self.world;
        match name {
            "canary" => Some(format!("rdlt-canary-{world} \t")),
            "lines" => Some(format!(
                "rdlt-canary-lines-{world}\n{{\nsecond-line-of-{world}\n}}\n"
            )),
            "token" => Some(format!("rdlt-canary-token-{world}")),
            _ => None,
        }
    }

    /// What no text the engine or its host keeps may hold: each secret without its whitespace, and
    /// each line of the secret of several lines long enough to be found alone.
    fn needles(&self) -> [String; 4] {
        let world = &self.world;
        [
            format!("rdlt-canary-{world}"),
            format!("rdlt-canary-lines-{world}"),
            format!("second-line-of-{world}"),
            format!("rdlt-canary-token-{world}"),
        ]
    }

    /// The first secret `text` holds, where it holds one.
    pub(crate) fn found(&self, text: &str) -> Option<String> {
        self.needles()
            .into_iter()
            .find(|needle| text.contains(needle.as_str()))
    }

    /// Records in `world` each secret `outcome`'s error or report holds.
    pub(crate) fn kept(&self, world: &World, outcome: &RunOutcome) {
        // The simulation's own work: what it builds is not the engine's to hold.
        let found = aside(|| {
            let report = serde_json::to_string(&outcome.report).unwrap_or_default();
            let shown = format!("{:?}", outcome.report);
            let error = outcome
                .error
                .as_ref()
                .map(|error| texts(error))
                .unwrap_or_default();
            [
                ("report", report),
                ("report, shown", shown),
                ("error", error),
            ]
            .into_iter()
            .filter_map(|(what, text)| {
                let secret = self.found(&text)?;
                Some(format!("a run's {what} holds the secret {secret}: {text}"))
            })
            .collect::<Vec<_>>()
        });
        for finding in found {
            world.violation(finding);
        }
    }

    /// Records in `world` the secret `error`, which the engine answered the `what` with, holds.
    pub(crate) fn answered(
        &self,
        world: &World,
        what: &str,
        error: &(dyn std::error::Error + 'static),
    ) {
        let text = aside(|| texts(error));
        if let Some(secret) = self.found(&text) {
            world.violation(format!(
                "the {what}'s error holds the secret {secret}: {text}"
            ));
        }
    }

    /// Fails where `error`, met placing the `what`, holds a secret.
    ///
    /// # Panics
    ///
    /// Panics when it holds one.
    pub(crate) fn placed(&self, what: &str, error: &ProviderError) {
        let text = texts(error);
        if let Some(secret) = self.found(&text) {
            panic!("placing the {what} failed with the secret {secret} in its error: {text}");
        }
    }

    /// Looks for the secrets in every event and span this thread logs until the guard drops.
    ///
    /// Turmoil's hosts all run on the thread, so it sees the connectors' hosts and the engine's
    /// alike.
    pub(crate) fn watching(&self) -> (Logged, tracing::subscriber::DefaultGuard) {
        let logged = Logged::new(self.clone());
        let guard = tracing::subscriber::set_default(logged.clone());
        (logged, guard)
    }

    /// What resolves `${secret:canary}`, `${secret:lines}` and `${secret:token}`.
    pub(crate) fn vault(&self) -> Vault {
        Vault(self.clone())
    }

    /// The simulation's connectors, placed in process as an embedder places those it trusts, each
    /// configuration's secrets resolved by the vault.
    pub(crate) fn registry(&self) -> Registry {
        Registry::trusted()
            .trusted_source::<SimSource>()
            .trusted_source::<SimChangeSource>()
            .trusted_destination::<SimDestination>()
            .secrets(self.vault())
    }
}

/// Every text an error holds: its own and each of its causes', as `Display` and `Debug` write them.
fn texts(error: &(dyn std::error::Error + 'static)) -> String {
    let mut texts = format!("{error} {error:?}");
    let mut cause = error.source();
    while let Some(error) = cause {
        write!(texts, " {error} {error:?}").ok();
        cause = error.source();
    }
    texts
}

/// Resolves the secrets of one [`Canary`], and no other reference.
#[derive(Clone)]
pub(crate) struct Vault(Canary);

impl fmt::Debug for Vault {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("Vault(***)")
    }
}

impl SecretResolver for Vault {
    fn resolve<'a>(
        &'a self,
        reference: &'a SecretReference,
    ) -> BoxFuture<'a, Result<Secret<String>, SecretFault>> {
        let value = match reference.kind {
            SecretKind::Named => self.0.value(&reference.name),
            SecretKind::Env | SecretKind::File => None,
        };
        Box::pin(async move { value.map(Secret::new).ok_or(SecretFault::Missing) })
    }
}
