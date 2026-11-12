//! `_native.certify_*`: rdlt-certify's clauses against Python connectors and transforms, as JSON.

#[cfg(test)]
mod tests;

use std::path::PathBuf;

use arrow_array::RecordBatch;
use arrow_pyarrow::FromPyArrow as _;
use pyo3::exceptions::PyValueError;
use pyo3::{Bound, PyAny, PyErr, Python, pyfunction};
use rdlt_certify::{
    Clause, KILL_CLAUSES, PROTOCOL_CLAUSES, Probe, Report, SOURCE_CLAUSES, TRANSFORM_CLAUSES,
    Target, TransformInputs, Unprobed,
};
use rdlt_connector::ConnectorId;
use rdlt_connector::serve::Served;
use rdlt_connector::testing::{Outcome, Reason};

use crate::destination::NativeDestinationFactory;
use crate::source::NativeSourceFactory;
use crate::transform::NativeTransform;
use crate::{errors, runtime};

fn listed(role: &str) -> Result<Vec<Clause>, PyErr> {
    let families: Vec<&[Clause]> = match role {
        "source" => vec![PROTOCOL_CLAUSES, SOURCE_CLAUSES, &KILL_CLAUSES[..1]],
        "destination" => vec![
            PROTOCOL_CLAUSES,
            rdlt_certify::DESTINATION_CLAUSES,
            &KILL_CLAUSES[1..],
        ],
        "transform" => vec![TRANSFORM_CLAUSES],
        other => {
            return Err(PyValueError::new_err(format!(
                "no clauses are listed for {other:?}"
            )));
        }
    };
    Ok(families.into_iter().flatten().copied().collect())
}

/// Each clause's id and statement, for `role`, in the order a certification reports them.
#[pyfunction]
pub(crate) fn clauses(role: &str) -> Result<Vec<(String, String)>, PyErr> {
    Ok(listed(role)?
        .into_iter()
        .map(|clause| (clause.id.to_owned(), clause.statement.to_owned()))
        .collect())
}

/// `report` as the JSON `rdlt.testing` reads.
fn reported(report: &Report) -> String {
    let results: Vec<serde_json::Value> = report
        .results
        .iter()
        .map(|result| {
            let (outcome, reason) = match &result.outcome {
                Outcome::Passed => ("passed", None),
                Outcome::Failed(reason) => ("failed", Some(reason.to_string())),
                Outcome::Inapplicable(reason) => ("inapplicable", Some(reason.to_string())),
                Outcome::Unobserved(reason) => ("unobserved", Some(reason.to_string())),
            };
            serde_json::json!({
                "clause": result.clause.id,
                "statement": result.clause.statement,
                "outcome": outcome,
                "reason": reason,
                "note": result.note.as_ref().map(ToString::to_string),
            })
        })
        .collect();
    serde_json::json!({ "connector": report.connector, "results": results }).to_string()
}

/// Certifies the source `factory` makes, served in this process over a socket pair, telling where
/// it stands where its class says it does; with `launcher`, the installed binary of the same
/// connector, its kill clause spawns that.
#[pyfunction]
#[pyo3(signature = (factory, config, launcher = None))]
pub(crate) fn certify_source(
    py: Python<'_>,
    factory: &NativeSourceFactory,
    config: &str,
    launcher: Option<PathBuf>,
) -> Result<String, PyErr> {
    let config = configured(config)?;
    let source = factory.acknowledging();
    let id: ConnectorId = source.spec().id.clone();
    let mut target = Target::served(Served::new().with_source(source));
    if let Some(launcher) = launcher {
        target = target.kill_target(Target::trusted_binary(id, launcher));
    }
    let report = runtime::wait(py, async move {
        rdlt_certify::certify_source(&target, config).await
    })?;
    Ok(reported(&report))
}

/// Certifies the destination `factory` makes, served in this process over a socket pair and
/// read back through it; with `launcher`, the installed binary of the same connector, its kill
/// clause spawns that.
#[pyfunction]
#[pyo3(signature = (factory, config, launcher = None))]
pub(crate) fn certify_destination(
    py: Python<'_>,
    factory: &NativeDestinationFactory,
    config: &str,
    launcher: Option<PathBuf>,
) -> Result<String, PyErr> {
    let config = configured(config)?;
    let destination = factory.readable();
    let id: ConnectorId = destination.spec().id.clone();
    let mut target = Target::served(Served::new().with_destination(destination));
    let killed_elsewhere = launcher.is_some();
    if let Some(launcher) = launcher {
        target = target.kill_target(Target::trusted_binary(id, launcher));
    }
    let report = runtime::wait(py, async move {
        let read_back = rdlt_certify::read_back(&target, &config).await;
        let probe: &dyn Probe = match &read_back {
            Some(read_back) => read_back,
            None => &Unprobed,
        };
        rdlt_certify::certify_destination(&target, config, probe).await
    })?;
    Ok(reported(&if killed_elsewhere {
        read_back_here(report)
    } else {
        report
    }))
}

/// `report`, its kill clause's reason, where it has one, saying where what the launcher
/// published is read back: in this process, through the connector served here, which finds only
/// a store that outlives the killed process.
fn read_back_here(mut report: Report) -> Report {
    for result in &mut report.results {
        if result.clause.id != KILL_CLAUSES[1].id {
            continue;
        }
        let read_here = |reason: &Reason| {
            Reason::new(format_args!(
                "what the launcher published is read back in this process: {reason}"
            ))
        };
        result.outcome = match &result.outcome {
            Outcome::Failed(reason) => Outcome::Failed(read_here(reason)),
            Outcome::Unobserved(reason) => Outcome::Unobserved(read_here(reason)),
            kept => kept.clone(),
        };
    }
    report
}

/// `config`, a connector's configuration as JSON.
fn configured(config: &str) -> Result<serde_json::Value, PyErr> {
    serde_json::from_str(config)
        .map_err(|error| PyValueError::new_err(format!("a configuration is JSON: {error}")))
}

/// Certifies `transform` with the `T` family over `inputs` and the batches drawn from them, on a
/// thread of the runtime's blocking pool: every clause waits for the transform's answers.
#[pyfunction]
#[expect(
    clippy::needless_pass_by_value,
    reason = "PyO3 extracts each argument by value"
)]
pub(crate) fn certify_transform(
    py: Python<'_>,
    transform: &NativeTransform,
    inputs: Vec<Bound<'_, PyAny>>,
) -> Result<String, PyErr> {
    let batches = inputs
        .iter()
        .map(RecordBatch::from_pyarrow_bound)
        .collect::<Result<Vec<_>, PyErr>>()?;
    let transform = transform.transform();
    let report = runtime::wait(py, async move {
        tokio::task::spawn_blocking(move || {
            let inputs = TransformInputs {
                batches,
                keys: Vec::new(),
                change_time: None,
            };
            rdlt_certify::certify_transform(&transform, &inputs)
        })
        .await
    })?
    .map_err(|joined| {
        let message = format!("certifying the transform ended without its answer: {joined}");
        errors::internal(py, &message)
    })?;
    Ok(reported(&report))
}
