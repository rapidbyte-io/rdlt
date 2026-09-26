//! Schema settings as the simulation draws them, at the pipeline, each stream and each drift
//! column, and as an operator relaxes them after a refusal.

#[cfg(test)]
mod tests;

use rdlt_engine::{Nested, OnUnsupported, SchemaPolicy, SchemaSettings};

use crate::rng::SplitMix64;

/// Schema settings at one level; a setting left `None` inherits from the level above.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Level {
    /// The schema policy.
    pub policy: Option<SchemaPolicy>,
    /// What happens to changes the destination cannot apply.
    pub on_unsupported: Option<OnUnsupported>,
    /// How nested values are stored.
    pub nested: Option<Nested>,
}

/// The settings one column resolves to.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Resolved {
    /// The schema policy.
    pub policy: SchemaPolicy,
    /// What happens to changes the destination cannot apply.
    pub on_unsupported: OnUnsupported,
    /// How nested values are stored.
    pub nested: Nested,
}

/// What an operator relaxed after a stream's runs were refused: frozen schemas now evolve, and
/// refused changes take variant columns.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Relaxed {
    /// Whether `Freeze` became `Evolve`.
    pub frozen: bool,
    /// Whether `Refuse` became `VariantColumn`.
    pub refused: bool,
}

impl Level {
    /// Settings drawn from `rng`, each set about a third of the time: for a `column`, never to
    /// normalize, and otherwise to normalize only where `normalize`.
    pub(crate) fn draw(rng: &mut SplitMix64, column: bool, normalize: bool) -> Self {
        let policy = rng.chance(400).then(|| match rng.below(10) {
            0..=3 => SchemaPolicy::Evolve,
            4 | 5 => SchemaPolicy::Freeze,
            6 | 7 => SchemaPolicy::DiscardRow,
            _ => SchemaPolicy::DiscardValue,
        });
        let on_unsupported = rng.chance(300).then(|| {
            if rng.chance(500) {
                OnUnsupported::Refuse
            } else {
                OnUnsupported::VariantColumn
            }
        });
        let nested = rng.chance(300).then(|| match rng.below(3) {
            0 => Nested::Native,
            1 => Nested::Json,
            _ if column || !normalize => Nested::Native,
            _ => super::normalized(rng),
        });
        Self {
            policy,
            on_unsupported,
            nested,
        }
    }

    /// These settings as the engine takes them.
    pub fn engine(self) -> SchemaSettings {
        let mut settings = SchemaSettings::new();
        if let Some(policy) = self.policy {
            settings = settings.policy(policy);
        }
        if let Some(on_unsupported) = self.on_unsupported {
            settings = settings.on_unsupported(on_unsupported);
        }
        if let Some(nested) = self.nested {
            settings = settings.nested(nested);
        }
        settings
    }

    /// These settings as `relaxed` leaves them.
    #[must_use]
    pub fn relaxed(self, relaxed: Relaxed) -> Self {
        Self {
            policy: self.policy.map(|policy| match policy {
                SchemaPolicy::Freeze if relaxed.frozen => SchemaPolicy::Evolve,
                other => other,
            }),
            on_unsupported: self.on_unsupported.map(|on| match on {
                OnUnsupported::Refuse if relaxed.refused => OnUnsupported::VariantColumn,
                other => other,
            }),
            nested: self.nested,
        }
    }

    /// The level above, `parent`, as this one relaxed by `relaxed` would inherit it: a setting
    /// this level leaves unset that relaxing changes in `parent` is set here.
    #[must_use]
    pub fn relaxing(self, parent: Self, relaxed: Relaxed) -> Self {
        let own = self.relaxed(relaxed);
        let inherited = parent.relaxed(relaxed);
        Self {
            policy: own.policy.or((inherited.policy != parent.policy)
                .then_some(inherited.policy)
                .flatten()),
            on_unsupported: own
                .on_unsupported
                .or((inherited.on_unsupported != parent.on_unsupported)
                    .then_some(inherited.on_unsupported)
                    .flatten()),
            nested: own.nested,
        }
    }
}

/// The settings `levels`, most specific first, resolve to: each from the first level setting it,
/// and the default where none does.
pub(crate) fn resolve(levels: &[Level]) -> Resolved {
    Resolved {
        policy: levels
            .iter()
            .find_map(|level| level.policy)
            .unwrap_or_default(),
        on_unsupported: levels
            .iter()
            .find_map(|level| level.on_unsupported)
            .unwrap_or_default(),
        nested: levels
            .iter()
            .find_map(|level| level.nested)
            .unwrap_or_default(),
    }
}
