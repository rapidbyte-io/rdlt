//! How runs failed, which failures the model predicts, and how an operator relaxes a refusal.

#[cfg(test)]
mod tests;

use std::collections::BTreeSet;

use rdlt_connector::wire::INVALID_MESSAGE;
use rdlt_engine::{Code, Error, ErrorKind, Origin, codes};

use super::Prediction;
use crate::workload::Relaxed;
use crate::world::World;

/// How a run failed.
#[derive(Clone, Debug)]
pub(in crate::oracle) struct Failure {
    kind: ErrorKind,
    origin: Origin,
    code: Option<Code>,
    connector_code: Option<String>,
    stream: Option<String>,
    /// The error, printed.
    pub(in crate::oracle) text: String,
}

impl Failure {
    /// The failure `error` reports.
    pub(in crate::oracle) fn of(error: &Error) -> Self {
        Self {
            kind: error.kind(),
            origin: error.origin(),
            code: error.code(),
            connector_code: error.connector_code().map(ToOwned::to_owned),
            stream: error.stream().map(ToString::to_string),
            text: format!("{error:?}"),
        }
    }

    /// Whether this failure is the engine's own, coded `code`.
    pub(in crate::oracle) fn is(&self, code: Code) -> bool {
        self.origin == Origin::Engine && self.code == Some(code)
    }

    /// The refusal this failure is, where `prediction` names it: its stream and code, the
    /// engine's refusal of a schema change or of a stream's reads.
    fn refusal(&self, prediction: &Prediction) -> Option<(String, Code)> {
        let (code, kind) = REFUSALS.into_iter().find(|(code, _)| self.is(*code))?;
        let refusal = (self.stream.clone()?, code);
        (self.kind == kind && prediction.may.contains(&refusal)).then_some(refusal)
    }

    /// Whether this failure is a refusal of the model's: the engine's refusal of a schema change
    /// or of a stream's reads, which no fault causes.
    fn refuses(&self) -> bool {
        self.kind == ErrorKind::Schema || REFUSALS.iter().any(|(code, _)| self.is(*code))
    }
}

/// The engine's refusals the model predicts, and the kind of error each is.
const REFUSALS: [(Code, ErrorKind); 5] = [
    (codes::SCHEMA_FROZEN, ErrorKind::Schema),
    (codes::SCHEMA_CHANGE_UNSUPPORTED, ErrorKind::Schema),
    (codes::MERGE_KEY_CHANGED, ErrorKind::Schema),
    (codes::CHECKPOINT_MISSING, ErrorKind::Source),
    (codes::CURSOR_MISSING, ErrorKind::Config),
];

/// A refusal a phase's runs met that the model does not predict.
#[derive(Debug)]
pub(in crate::oracle) struct Unpredicted {
    failure: String,
    predicted: BTreeSet<(String, Code)>,
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
/// A schema failure, or a refusal of a stream's reads, that `prediction` does not name.
pub(in crate::oracle) fn refused(
    failures: &[Failure],
    prediction: &Prediction,
) -> Result<Option<(String, Code)>, Unpredicted> {
    let mut met = None;
    for failure in failures.iter().filter(|failure| failure.refuses()) {
        match failure.refusal(prediction) {
            Some(refusal) => met = Some(refusal),
            None => {
                return Err(Unpredicted {
                    failure: failure.text.clone(),
                    predicted: prediction.may.clone(),
                });
            }
        }
    }
    Ok(met)
}

/// The first of `failures` a run without faults or disruptions must not meet: anything but a
/// refusal `prediction` names.
pub(in crate::oracle) fn unexplained<'a>(
    failures: &'a [Failure],
    prediction: &Prediction,
) -> Option<&'a Failure> {
    failures
        .iter()
        .find(|failure| failure.refusal(prediction).is_none())
}

/// The first of `failures` that breaks the wire protocol, which no fault excuses: a connector and
/// a host that keep to the protocol never meet one, however the network or the connector fails.
///
/// The host or the connector at either end finds it, so the failure is a side's.
pub(in crate::oracle) fn violation(failures: &[Failure]) -> Option<&Failure> {
    failures.iter().find(|failure| {
        failure.origin != Origin::Engine
            && failure.connector_code.as_deref() == Some(INVALID_MESSAGE)
    })
}

/// Relaxes what refused `stream`'s runs with `code`, as an operator would: a frozen schema
/// evolves; a source whose read sealed none of its rows is mended to checkpoint after its reads'
/// last batch; a change the destination cannot apply takes a variant column, and the destination
/// is granted adding columns.
pub(in crate::oracle) fn relax(world: &World, relaxed: &mut [Relaxed], stream: &str, code: Code) {
    let index = world
        .workload
        .streams
        .iter()
        .position(|candidate| candidate.name == stream)
        .expect("refusals name the workload's streams");
    if code == codes::SCHEMA_FROZEN {
        relaxed[index].frozen = true;
    } else if code == codes::CHECKPOINT_MISSING {
        relaxed[index].mended = true;
        world.mend(index);
    } else {
        relaxed[index].refused = true;
        world.grant_add_column();
    }
}
