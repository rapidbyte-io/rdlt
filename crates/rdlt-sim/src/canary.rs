//! Secrets in every simulated configuration, which nothing the engine, its host or a run's report
//! may hold: their values, the vault that resolves them, and the checks that look for them.

#[cfg(test)]
mod tests;

use std::fmt;

use rdlt_connector::{BoxFuture, Secret};
use rdlt_host::{Registry, SecretFault, SecretKind, SecretReference, SecretResolver};

use crate::changes::SimChangeSource;
use crate::destination::SimDestination;
use crate::source::SimSource;

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
    #[cfg(test)]
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
    #[cfg(test)]
    pub(crate) fn found(&self, text: &str) -> Option<String> {
        self.needles()
            .into_iter()
            .find(|needle| text.contains(needle.as_str()))
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
