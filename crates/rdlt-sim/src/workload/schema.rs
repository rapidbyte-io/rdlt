//! How a stream's schema settings are drawn: its own, and each drift column's settings, hint and
//! declared type.

use rdlt_connector::LogicalType;
use rdlt_engine::{Nested, SchemaPolicy};
use rdlt_testkit::draw::draw;
use rdlt_testkit::drawn::{Shape, neighbors};

use super::{Level, Relaxed, SimStream, normalized};
use crate::rng::SplitMix64;
use crate::swarm::Features;

impl SimStream {
    /// Draws each drift column's own settings, the type the plan hints for it and the type the
    /// source declares it as; a stream that normalizes declares no column its policy discards.
    pub(super) fn draw_columns(&mut self, rng: &mut SplitMix64, features: Features) {
        let normalized = self.normalized();
        for index in 0..self.drift.len() {
            self.drift[index].settings = Level::draw(rng, true, false);
            if rng.chance(200) {
                self.drift[index].hint = Some(self.typed(rng, index, features));
            }
            let discards = matches!(
                self.resolved(Some(index), Relaxed::default()).policy,
                SchemaPolicy::DiscardRow | SchemaPolicy::DiscardValue
            );
            if rng.chance(350) && !(normalized && discards) {
                self.drift[index].declared = Some(self.typed(rng, index, features));
            }
        }
    }

    /// A type to hint or declare drift column `column` as: one its values arrive as, the join of
    /// them all, a neighbor of one, or another; for a JSON stream, a type JSON values are inferred
    /// as, or `Json`.
    fn typed(&self, rng: &mut SplitMix64, column: usize, features: Features) -> LogicalType {
        use LogicalType as T;
        if self.json {
            let types = [T::Bool, T::Int64, T::Float64, T::Utf8, T::Json];
            return types[usize::try_from(rng.below(5)).unwrap_or(0)].clone();
        }
        let shapes: Vec<&Shape> = self.drift[column]
            .shapes
            .iter()
            .flatten()
            .flatten()
            .collect();
        let picked = usize::try_from(rng.below(shapes.len().max(1) as u64)).unwrap_or(0);
        let logical = match rng.below(4) {
            0 => shapes.get(picked).map(|shape| shape.logical.clone()),
            1 => shapes
                .iter()
                .map(|shape| shape.logical.clone())
                .reduce(|joined, next| joined.join(&next)),
            2 => shapes.get(picked).map(|shape| {
                let seed = rng.next_u64();
                draw(&neighbors::neighbor(shape), seed).logical
            }),
            _ => {
                let seed = rng.next_u64();
                Some(draw(&rdlt_testkit::drawn::values::shape(features.depth), seed).logical)
            }
        };
        match logical {
            Some(T::Null) | None => T::Json,
            Some(logical) => logical,
        }
    }
}

/// A stream's own schema settings: drawn like any level's where the seed exercises settings, and
/// otherwise a policy and a way to store nested values set on every stream.
pub(super) fn level(rng: &mut SplitMix64, features: Features) -> Level {
    if features.settings {
        return Level::draw(rng, false, features.normalize);
    }
    Level {
        policy: Some(match rng.below(10) {
            0 => SchemaPolicy::DiscardRow,
            1 => SchemaPolicy::DiscardValue,
            _ => SchemaPolicy::Evolve,
        }),
        on_unsupported: None,
        nested: Some(if features.normalize && rng.chance(500) {
            normalized(rng)
        } else if rng.chance(300) {
            Nested::Json
        } else {
            Nested::Native
        }),
    }
}
