//! The schema refusals a phase's runs may meet, and whether every run meets one: each column's
//! own column followed through every order the phase's batches may arrive in, as schema
//! resolution changes it.

mod keys;
#[cfg(test)]
mod tests;

use std::collections::{BTreeMap, BTreeSet};

use rdlt_connector::{Capabilities, LogicalType};
use rdlt_engine::{Error, ErrorKind, Nested, OnUnsupported, SchemaPolicy};
use rdlt_testkit::canon::storage;

use super::arrivals::{Arrival, arrival, widest};
use super::expected;
use crate::workload::{Relaxed, SimStream};
use crate::world::World;
use keys::key_outcome;

/// The code of a refused change to a frozen schema.
pub(super) const FROZEN: &str = "schema_frozen";
/// The code of a change the destination cannot apply and the policy refuses a variant for.
pub(super) const UNSUPPORTED: &str = "schema_change_unsupported";
/// The code of a merge key column whose type cannot change.
pub(super) const KEY_CHANGED: &str = "merge_key_changed";

/// The refusals a phase's runs may meet.
#[derive(Debug, Default, PartialEq, Eq)]
pub(super) struct Prediction {
    /// Each refusal some run may meet: its stream and code.
    pub(super) may: BTreeSet<(String, &'static str)>,
    /// Whether every run meets one, so none succeeds.
    pub(super) must: bool,
}

/// How a run failed.
#[derive(Clone, Debug)]
pub(super) struct Failure {
    kind: ErrorKind,
    code: Option<String>,
    stream: Option<String>,
    /// The error, printed.
    pub(super) text: String,
}

impl Failure {
    /// The failure `error` reports.
    pub(super) fn of(error: &Error) -> Self {
        Self {
            kind: error.kind(),
            code: error.code().map(ToOwned::to_owned),
            stream: error.stream().map(ToString::to_string),
            text: format!("{error:?}"),
        }
    }
}

/// A schema failure a phase's runs met that the model does not predict.
#[derive(Debug)]
pub(super) struct Unpredicted {
    failure: String,
    predicted: BTreeSet<(String, &'static str)>,
}

impl std::fmt::Display for Unpredicted {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            formatter,
            "a run failed with {}; the model predicts only {:?}",
            self.failure, self.predicted
        )
    }
}

/// The refusal among `failures` the phase's runs met, as `prediction` allows.
///
/// # Errors
///
/// A schema failure `prediction` does not name.
pub(super) fn refused(
    failures: &[Failure],
    prediction: &Prediction,
) -> Result<Option<(String, &'static str)>, Unpredicted> {
    let mut met = None;
    for failure in failures
        .iter()
        .filter(|failure| failure.kind == ErrorKind::Schema)
    {
        let known = [FROZEN, UNSUPPORTED, KEY_CHANGED]
            .into_iter()
            .find(|code| failure.code.as_deref() == Some(code));
        let refusal = known
            .zip(failure.stream.clone())
            .map(|(code, stream)| (stream, code));
        match refusal {
            Some((stream, code)) if prediction.may.contains(&(stream.clone(), code)) => {
                met = Some((stream, code));
            }
            _ => {
                return Err(Unpredicted {
                    failure: failure.text.clone(),
                    predicted: prediction.may.clone(),
                });
            }
        }
    }
    Ok(met)
}

/// Relaxes what refused `stream`'s runs with `code`, as an operator would: a frozen schema
/// evolves; a change the destination cannot apply takes a variant column, and the destination
/// is granted adding columns.
pub(super) fn relax(world: &World, relaxed: &mut [Relaxed], stream: &str, code: &str) {
    let index = world
        .workload
        .streams
        .iter()
        .position(|candidate| candidate.name == stream)
        .expect("refusals name the workload's streams");
    if code == FROZEN {
        relaxed[index].frozen = true;
    } else {
        relaxed[index].refused = true;
        world.grant_add_column();
    }
}

/// What `phase`'s runs of `world`'s workload may meet, each stream's settings as `relaxed`
/// leaves them.
pub(super) fn predict(world: &World, relaxed: &[Relaxed], phase: usize) -> Prediction {
    let capabilities = world.capabilities();
    let mut prediction = Prediction::default();
    for (stream, relaxed) in world.workload.streams.iter().zip(relaxed) {
        let columns = (0..stream.drift.len())
            .map(|column| Column::new(stream, column, *relaxed, &capabilities).outcome(phase));
        let key = (stream.keys > 0).then(|| key_outcome(stream, *relaxed, &capabilities, phase));
        let pruning = prunes(stream, phase);
        for outcome in columns.chain(key) {
            if let Some(code) = outcome.code {
                prediction.may.insert((stream.name.clone(), code));
                prediction.must |= outcome.must && !pruning;
            }
        }
    }
    prediction
}

/// Whether rows of `stream` go before their batch's schema is resolved, in `phase` or before: in a
/// stream that normalizes, rows holding items of a new array whose column drops rows go, taking
/// the columns only they hold values in; which columns arrive then depends on how the engine
/// groups batches, so the stream's refusals are only ever possible.
fn prunes(stream: &SimStream, phase: usize) -> bool {
    (0..=phase).any(|at| {
        (0..stream.partitions.len()).any(|partition| {
            stream
                .read(partition, at)
                .iter()
                .any(|row| expected::pruned(stream, row))
        })
    })
}

/// What one column's batches in a phase may meet.
#[derive(Debug, Default, PartialEq, Eq)]
struct Outcome {
    /// The code of a refusal they may meet.
    code: Option<&'static str>,
    /// Whether they surely meet it.
    must: bool,
}

impl Outcome {
    fn may(code: &'static str) -> Self {
        Self {
            code: Some(code),
            must: false,
        }
    }

    fn must(code: &'static str) -> Self {
        Self {
            code: Some(code),
            must: true,
        }
    }
}

/// One drift column, with what decides how schema resolution treats it.
struct Column<'a> {
    stream: &'a SimStream,
    index: usize,
    rules: Rules<'a>,
}

/// What decides how schema resolution treats a column's batches.
#[derive(Clone, Debug)]
struct Rules<'a> {
    policy: SchemaPolicy,
    /// Whether a change needing a variant column is refused.
    refuses: bool,
    nested: Nested,
    hint: Option<LogicalType>,
    capabilities: &'a Capabilities,
}

/// What one batch column does to its own column.
#[derive(Debug, PartialEq)]
enum Step {
    /// It fits, or widens the column to this type, or goes to a variant.
    To(LogicalType),
    /// It is refused.
    Refused,
    /// The model cannot say.
    Unknown,
}

impl<'a> Column<'a> {
    fn new(
        stream: &'a SimStream,
        index: usize,
        relaxed: Relaxed,
        capabilities: &'a Capabilities,
    ) -> Self {
        let resolved = stream.resolved(Some(index), relaxed);
        let rules = Rules {
            policy: resolved.policy,
            refuses: resolved.on_unsupported == OnUnsupported::Refuse
                || !capabilities.schema_changes.add_column,
            nested: resolved.nested,
            hint: stream.drift[index].hint.clone(),
            capabilities,
        };
        Self {
            stream,
            index,
            rules,
        }
    }

    /// What the column's batches in `phase` may meet.
    fn outcome(&self, phase: usize) -> Outcome {
        let drift = &self.stream.drift[self.index];
        let rules = &self.rules;
        if !rules.strict() {
            return Outcome::default();
        }
        // The declared schema is resolved as each run plans, before anything is read.
        if let (Some(declared), Some(hint)) = (&drift.declared, &drift.hint)
            && hint.join(declared) != *hint
        {
            return Outcome::must(rules.code());
        }
        if self.stream.normalized() && !self.stream.whole(self.index) && !self.scalar(phase) {
            let arrives = !self.arrivals(phase).is_empty();
            return if arrives {
                Outcome::may(rules.code())
            } else {
                Outcome::default()
            };
        }
        let initial = drift
            .declared
            .as_ref()
            .map(|declared| drift.hint.clone().unwrap_or_else(|| declared.clone()));
        let arrivals: Vec<Vec<Arrival>> = (0..=phase).map(|at| self.arrivals(at)).collect();
        let outcome = outcome(initial, &arrivals, rules.code(), |current, arrival| {
            rules.step(current, arrival)
        });
        // Which pushes the engine shreds together decides a column whose pushes differ in type.
        if outcome.must && self.gathered(phase) {
            return Outcome::may(rules.code());
        }
        outcome
    }

    /// Whether some push of the column by `phase` may be shredded with another of another type.
    fn gathered(&self, phase: usize) -> bool {
        (0..=phase).any(|at| {
            (0..self.stream.partitions.len()).any(|partition| {
                self.stream.read(partition, at).iter().any(|row| {
                    arrival(self.stream, row, self.index) != widest(self.stream, row, self.index)
                })
            })
        })
    }

    /// Whether the column's values by `phase`, and its declared type, are all scalars, which a
    /// stream that normalizes keeps in one column of its own table, as other streams do.
    fn scalar(&self, phase: usize) -> bool {
        let scalar = |logical: &LogicalType| {
            !matches!(
                logical,
                LogicalType::Struct(_) | LogicalType::List(_) | LogicalType::Json
            )
        };
        let declared = self.stream.drift[self.index].declared.as_ref();
        declared.is_none_or(scalar)
            && (0..=phase)
                .flat_map(|at| self.arrivals(at))
                .all(|arrival| matches!(arrival, Arrival::Typed(logical) if scalar(&logical)))
    }

    /// The distinct types the column arrives as in `phase`, other than nulls: its pushes', and
    /// the widest the engine may shred them into.
    fn arrivals(&self, phase: usize) -> Vec<Arrival> {
        let mut arrivals = Vec::new();
        for partition in 0..self.stream.partitions.len() {
            for row in self.stream.read(partition, phase) {
                let pushes = [
                    arrival(self.stream, row, self.index),
                    widest(self.stream, row, self.index),
                ];
                for arrival in pushes.into_iter().flatten() {
                    if !arrival.is_null() && !arrivals.contains(&arrival) {
                        arrivals.push(arrival);
                    }
                }
            }
        }
        arrivals
    }
}

impl Rules<'_> {
    /// Whether a column's changes may be refused: its schema is frozen, or it evolves but may
    /// not take a variant column.
    fn strict(&self) -> bool {
        self.policy == SchemaPolicy::Freeze || (self.policy == SchemaPolicy::Evolve && self.refuses)
    }

    /// The code a column's changes are refused with.
    fn code(&self) -> &'static str {
        if self.policy == SchemaPolicy::Freeze {
            FROZEN
        } else {
            UNSUPPORTED
        }
    }

    /// What a batch column arriving as `arrival` does to an own column of `current`, or to none
    /// yet: the steps schema resolution takes.
    fn step(&self, current: Option<&LogicalType>, arrival: &Arrival) -> Step {
        let current = match (current, &self.hint, arrival) {
            (Some(current), ..) => current.clone(),
            (None, ..) if self.policy == SchemaPolicy::Freeze => return Step::Refused,
            (None, ..) if !self.capabilities.schema_changes.add_column => {
                return Step::Refused;
            }
            (None, Some(hint), _) => hint.clone(),
            (None, None, Arrival::Typed(logical)) => logical.clone(),
            (None, None, Arrival::Container) => return Step::Unknown,
        };
        match arrival.fits(&current) {
            Some(true) => return Step::To(current),
            None => return Step::Unknown,
            Some(false) => {}
        }
        let joined = match arrival {
            Arrival::Typed(logical) => current.join(logical),
            Arrival::Container => LogicalType::Json,
        };
        if self.policy == SchemaPolicy::Freeze {
            return Step::Refused;
        }
        if self.hint.is_none()
            && joined != LogicalType::Json
            && widens(&current, &joined, self.nested, self.capabilities)
        {
            return Step::To(joined);
        }
        if self.refuses {
            return Step::Refused;
        }
        Step::To(current)
    }
}

/// What the batches of the last of `arrivals`, the distinct types a column arrives as in each
/// phase so far, may meet, `step` taking its own column from `initial` through them: refused with
/// `code` in some order of them, or in every one.
fn outcome(
    initial: Option<LogicalType>,
    arrivals: &[Vec<Arrival>],
    code: &'static str,
    step: impl Fn(Option<&LogicalType>, &Arrival) -> Step,
) -> Outcome {
    let Some((last, earlier)) = arrivals.split_last() else {
        return Outcome::default();
    };
    let mut states = vec![initial];
    for phase in earlier {
        let followed = follow(&states, phase, &step);
        if followed.unknown || followed.finals.is_empty() {
            return Outcome::may(code);
        }
        states = followed.finals;
    }
    let followed = follow(&states, last, &step);
    match (
        followed.any || followed.unknown,
        followed.all && !followed.unknown,
    ) {
        (_, true) => Outcome::must(code),
        (true, false) => Outcome::may(code),
        (false, false) => Outcome::default(),
    }
}

/// Where a column's own column goes through every order of one phase's batches.
struct Followed {
    /// The types it may end as where no batch is refused.
    finals: Vec<Option<LogicalType>>,
    /// Whether some order meets a refusal.
    any: bool,
    /// Whether every order does.
    all: bool,
    /// Whether the model cannot say.
    unknown: bool,
}

/// Where an own column goes from each of `states` through every order of `arrivals`, each batch
/// taking it a `step`.
fn follow(
    states: &[Option<LogicalType>],
    arrivals: &[Arrival],
    step: impl Fn(Option<&LogicalType>, &Arrival) -> Step,
) -> Followed {
    let mut finals: BTreeMap<String, Option<LogicalType>> = BTreeMap::new();
    let (mut any, mut all, mut unknown) = (false, true, false);
    for state in states {
        for order in orders(arrivals.len()) {
            let mut current = state.clone();
            let mut refused = false;
            for index in order {
                match step(current.as_ref(), &arrivals[index]) {
                    Step::To(next) => current = Some(next),
                    Step::Refused => {
                        refused = true;
                        break;
                    }
                    Step::Unknown => {
                        unknown = true;
                        break;
                    }
                }
            }
            any |= refused;
            all &= refused;
            if !refused {
                let name = current
                    .as_ref()
                    .map(ToString::to_string)
                    .unwrap_or_default();
                finals.insert(name, current);
            }
        }
    }
    Followed {
        finals: finals.into_values().collect(),
        any,
        all,
        unknown,
    }
}

/// Whether a destination with `capabilities` changes a column of `from` to `to` in place, nested
/// values stored as `nested` says.
fn widens(
    from: &LogicalType,
    to: &LogicalType,
    nested: Nested,
    capabilities: &Capabilities,
) -> bool {
    let native = nested == Nested::Native;
    let from = storage(from, native, capabilities);
    let to = storage(to, native, capabilities);
    from == to || capabilities.schema_changes.widens(from.kind(), to.kind())
}

/// Every order of `count` items, by index.
fn orders(count: usize) -> Vec<Vec<usize>> {
    if count == 0 {
        return vec![Vec::new()];
    }
    let mut all = Vec::new();
    for rest in orders(count - 1) {
        for position in 0..=rest.len() {
            let mut order = rest.clone();
            order.insert(position, count - 1);
            all.push(order);
        }
    }
    all
}
