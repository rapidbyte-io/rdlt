//! `rdlt.connector.answer`'s native half: the Rust SDK's own answer to a commit, by a fence row.

#[cfg(test)]
mod tests;

use pyo3::exceptions::PyValueError;
use pyo3::{PyErr, pyfunction};
use rdlt_connector::{Answer, Epoch, Fence};
use serde::de::DeserializeOwned;

/// How a destination whose fence row holds `epoch` and the receipt `landed` answers the commit
/// keyed `key` that expects `after` landed, each given as its JSON: `landed`, `fenced`, `stale`
/// or `apply`, as [`rdlt_connector::answer`] decides.
///
/// A receipt or a key rdlt cannot read is refused, not compared.
#[pyfunction]
#[pyo3(signature = (epoch, landed, key, after))]
pub(crate) fn answer(
    epoch: u64,
    landed: Option<&str>,
    key: &str,
    after: Option<&str>,
) -> Result<&'static str, PyErr> {
    let fence = Fence {
        epoch: Epoch(epoch),
        landed: landed.map(|text| read("receipt", text)).transpose()?,
    };
    let key = read("commit key", key)?;
    let after = after.map(|text| read("commit key", text)).transpose()?;
    Ok(match rdlt_connector::answer(&fence, key, after) {
        Answer::Landed(_) => "landed",
        Answer::Fenced => "fenced",
        Answer::Stale => "stale",
        Answer::Apply => "apply",
    })
}

/// `text`, a `what` as its JSON.
fn read<T: DeserializeOwned>(what: &str, text: &str) -> Result<T, PyErr> {
    serde_json::from_str(text)
        .map_err(|error| PyValueError::new_err(format!("no {what} rdlt can read: {error}")))
}
