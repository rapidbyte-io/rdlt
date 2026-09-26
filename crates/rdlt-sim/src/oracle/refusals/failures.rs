//! How runs failed, which failures the model predicts, and how an operator relaxes a refusal.

use std::collections::BTreeSet;

use rdlt_engine::{Error, ErrorKind};

use super::{FROZEN, KEY_CHANGED, Prediction, UNSUPPORTED};
use crate::workload::Relaxed;
use crate::world::World;

/// How a run failed.
#[derive(Clone, Debug)]
pub(in crate::oracle) struct Failure {
    kind: ErrorKind,
    code: Option<String>,
    stream: Option<String>,
    /// The error, printed.
    pub(in crate::oracle) text: String,
}

impl Failure {
    /// The failure `error` reports.
    pub(in crate::oracle) fn of(error: &Error) -> Self {
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
pub(in crate::oracle) struct Unpredicted {
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
pub(in crate::oracle) fn refused(
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
pub(in crate::oracle) fn relax(world: &World, relaxed: &mut [Relaxed], stream: &str, code: &str) {
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
