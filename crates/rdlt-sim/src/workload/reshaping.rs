//! What a stream's include and transforms do to its rows: the columns it lists, a filter, a
//! rename and a constant column.

use rdlt_connector::META_PREFIX;
use rdlt_engine::WriteMode;

use super::{Row, SimStream};
use crate::rng::SplitMix64;
use crate::swarm::Features;

/// The column a stream's constant transform adds.
pub(crate) const CONSTANT_COLUMN: &str = "sim_constant";

/// The text that column holds in every row.
pub(crate) const CONSTANT: &str = "c";

/// The columns every allow-list names: those the oracle finds each row by.
pub(crate) const LISTED: [&str; 4] = ["id", "partition", "offset", "value"];

/// What a stream's include and transforms do to its rows, drawn apart from every other draw.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Reshaping {
    /// Where the plan lists the columns it includes, those it lists beside `id`, `partition`,
    /// `offset` and `value`, which every list names: drift columns and the fat column, never the
    /// key, which every stream keeps; a stream that excludes columns uses no list, as a plan with
    /// both is refused.
    pub listed: Option<Vec<String>>,
    /// Rows whose id this divides are filtered out; a merge stream, fed by full or incremental
    /// reads, filters none, as its plan would be refused.
    pub filter: Option<i64>,
    /// Whether `offset` lands as `sim_offset`.
    pub rename: bool,
    /// Whether every row carries `sim_constant`, holding `c`.
    pub constant: bool,
    /// Whether the filter answers a row it does not keep with null rather than false.
    pub cased: bool,
}

impl Reshaping {
    /// What `stream` does, where `features` transform, drawn from `seed` apart from every other
    /// draw: nothing, or an allow-list one time in three, naming each drift column and the fat
    /// column one time in two, and a filter, a rename and a constant column one time in two each;
    /// a filter says no with null one time in two.
    ///
    /// No stream with a drift column whose name begins as the engine's own columns' do filters:
    /// a SQL query outputs no such name.
    pub(super) fn drawn(features: Features, stream: &SimStream, seed: u64) -> Self {
        let mut rng = SplitMix64::new(seed);
        if !features.transform || !rng.chance(700) {
            return Self::default();
        }
        let listing = rng.chance(333);
        let drift = stream.drift.iter().map(|drift| drift.name.as_str());
        let named: Vec<String> = drift
            .chain([crate::fat::COLUMN])
            .filter(|_| rng.chance(500))
            .map(str::to_owned)
            .collect();
        let modulus = 2 + i64::try_from(rng.below(4)).unwrap_or(0);
        let engine = stream
            .drift
            .iter()
            .any(|drift| drift.name.starts_with(META_PREFIX));
        let filters = rng.chance(500) && stream.write != WriteMode::Merge && !engine;
        Self {
            listed: listing.then_some(named),
            filter: filters.then_some(modulus),
            rename: rng.chance(500),
            constant: rng.chance(500),
            cased: rng.chance(500),
        }
    }

    /// Whether the filter keeps `row`: every row where there is none.
    pub fn keeps(&self, row: &Row) -> bool {
        self.filter.is_none_or(|modulus| row.id % modulus != 0)
    }

    /// The column a row's offset lands in.
    pub fn offset(&self) -> &'static str {
        if self.rename { "sim_offset" } else { "offset" }
    }
}

impl SimStream {
    /// The columns the plan includes beside those every list names and its key, where it lists
    /// them: none where it excludes columns, which a stream that lists them cannot.
    pub fn included(&self) -> Option<&[String]> {
        let listed = self.reshaping.listed.as_deref();
        listed.filter(|_| self.excluded.columns.is_empty())
    }

    /// Whether the column `name`, neither the key nor one every list names, lands: it is
    /// included, as every column is where the plan lists none.
    pub fn lands(&self, name: &str) -> bool {
        self.included()
            .is_none_or(|included| included.iter().any(|listed| listed == name))
    }

    /// Whether the stream's include or transforms change what lands.
    pub fn reshapes(&self) -> bool {
        let reshaping = &self.reshaping;
        self.included().is_some()
            || reshaping.filter.is_some()
            || reshaping.rename
            || reshaping.constant
    }
}
