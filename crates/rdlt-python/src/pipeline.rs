//! `_native.Pipeline`: a pipeline built against a host, and the calls on it.

#[cfg(test)]
pub(crate) mod tests;

use std::path::PathBuf;
use std::sync::Arc;

use pyo3::types::{PyString, PyStringMethods as _};
use pyo3::{Bound, FromPyObject, PyErr, Python, pyclass, pymethods};
use rdlt::{ResetScope, StreamName, StreamSelector};

use crate::host::NativeHost;
use crate::{errors, runtime};

/// What a document's errors name it as where it came from no file.
const ORIGIN: &str = "<python>";

/// A built pipeline: after the build no configuration error can occur.
#[pyclass(module = "rdlt._native", name = "Pipeline", frozen)]
pub(crate) struct NativePipeline {
    pipeline: Arc<rdlt::Pipeline>,
}

/// A stream a reset names: as the source's catalog shows it, or by its identity.
#[derive(FromPyObject)]
pub(crate) enum ResetStream {
    Shown(String),
    Identity {
        namespace: Option<String>,
        name: String,
    },
}

impl ResetStream {
    /// The facade's selector of the stream; a refused identity quotes nothing of what it holds.
    fn selector(self, py: Python<'_>) -> Result<StreamSelector, PyErr> {
        let (namespace, name) = match self {
            Self::Shown(shown) => return Ok(StreamSelector::from(shown)),
            Self::Identity { namespace, name } => (namespace, name),
        };
        let identity = match namespace {
            Some(namespace) => StreamName::with_namespace(namespace, name),
            None => StreamName::new(name),
        };
        identity.map(StreamSelector::from).map_err(|_| {
            errors::config(
                py,
                rdlt::codes::STREAM_INVALID,
                "a stream's identity is not valid",
            )
        })
    }
}

#[pymethods]
impl NativePipeline {
    /// Parses `text`, a document in YAML or JSON, and builds it against `host`.
    #[staticmethod]
    fn build(py: Python<'_>, text: &Bound<'_, PyString>, host: &NativeHost) -> Result<Self, PyErr> {
        // Refused here so the exception holds nothing of the text: Python's error holds it whole.
        let text = text.to_str().map_err(|_| {
            errors::config(
                py,
                rdlt::codes::DOCUMENT_INVALID,
                "the document is not valid Unicode text",
            )
        })?;
        let builder =
            rdlt::Pipeline::from_text(text, ORIGIN).map_err(|error| errors::raised(py, &error))?;
        Self::built(py, builder, host.shared(py)?)
    }

    /// Reads and parses the document at `path`, and builds it against `host`.
    #[staticmethod]
    fn build_path(py: Python<'_>, path: PathBuf, host: &NativeHost) -> Result<Self, PyErr> {
        let read = runtime::wait(py, rdlt::Pipeline::from_path(path))?;
        let builder = read.map_err(|error| errors::raised(py, &error))?;
        Self::built(py, builder, host.shared(py)?)
    }

    /// The pipeline as its document, in JSON.
    pub(crate) fn document(&self) -> String {
        serde_json::to_string(self.pipeline.document())
            .expect("a built pipeline's document has a key only as a secret reference")
    }

    /// Does what `build` does again, now: validates and handshakes; no data moves.
    pub(crate) fn check(&self, py: Python<'_>) -> Result<(), PyErr> {
        let pipeline = Arc::clone(&self.pipeline);
        let checked = runtime::wait(py, async move { pipeline.check().await })?;
        checked
            .map(drop)
            .map_err(|error| errors::raised(py, &error))
    }

    /// The source's catalog, as JSON.
    pub(crate) fn discover(&self, py: Python<'_>) -> Result<String, PyErr> {
        let pipeline = Arc::clone(&self.pipeline);
        let found = runtime::wait(py, async move { pipeline.discover().await })?;
        let catalog = found.map_err(|error| errors::raised(py, &error))?;
        Ok(serde_json::to_string(&catalog).expect("a catalog serializes"))
    }

    /// Resets `streams`' committed state, with `tables` dropping their tables too, in one commit;
    /// what was reset, as JSON.
    #[pyo3(signature = (streams, *, tables))]
    pub(crate) fn reset(
        &self,
        py: Python<'_>,
        streams: Vec<ResetStream>,
        tables: bool,
    ) -> Result<String, PyErr> {
        let scope = if tables {
            ResetScope::Tables
        } else {
            ResetScope::Positions
        };
        let streams: Vec<StreamSelector> = streams
            .into_iter()
            .map(|stream| stream.selector(py))
            .collect::<Result<_, _>>()?;
        let pipeline = Arc::clone(&self.pipeline);
        let reset = runtime::wait(py, async move { pipeline.reset(streams, scope).await })?;
        let report = reset.map_err(|error| errors::raised(py, &error))?;
        let shown = serde_json::json!({
            "streams": report.streams.iter().map(ToString::to_string).collect::<Vec<_>>(),
            "dropped": report.dropped.iter().map(ToString::to_string).collect::<Vec<_>>(),
            "epoch": report.epoch.0,
        });
        Ok(shown.to_string())
    }
}

impl NativePipeline {
    /// `builder` built against `host`, the GIL released while it places and handshakes.
    pub(crate) fn built(
        py: Python<'_>,
        builder: rdlt::PipelineBuilder,
        host: Arc<rdlt::Host>,
    ) -> Result<Self, PyErr> {
        let built = runtime::wait(py, async move { builder.build(&host).await })?;
        let pipeline = built.map_err(|error| errors::raised(py, &error))?;
        Ok(Self {
            pipeline: Arc::new(pipeline),
        })
    }
}
