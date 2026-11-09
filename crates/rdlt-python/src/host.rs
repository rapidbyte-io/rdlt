//! `_native.Host`: the facade's host, built from what `rdlt.Host` says its operator allows.

#[cfg(test)]
mod tests;

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use parking_lot::Mutex;
use pyo3::{FromPyObject, Py, PyAny, PyErr, Python, pyclass, pymethods};

use crate::secrets::PythonSecrets;
use crate::source::NativeSourceFactory;
use crate::transform::NativeTransform;
use crate::{errors, runtime};

/// `rdlt.Host`'s attributes, each one of the facade's words for what an operator allows.
#[derive(FromPyObject)]
pub(crate) struct Settings {
    connectors: Vec<PathBuf>,
    writable: Vec<PathBuf>,
    readable: Vec<PathBuf>,
    always_readable: Vec<PathBuf>,
    allow_network: bool,
    build_wait: Option<Duration>,
    allow_env: Vec<String>,
    allow_files: Vec<PathBuf>,
    #[pyo3(attribute("_weak_secret_store"))]
    secret_store: Option<Py<PyAny>>,
    remote: Option<TlsFiles>,
    trust_binaries: bool,
    #[pyo3(attribute("_sources"))]
    sources: Vec<Py<NativeSourceFactory>>,
    #[pyo3(attribute("_native_transforms"))]
    transforms: Vec<(String, Py<NativeTransform>)>,
}

/// `rdlt.Tls`'s files: the host's certificate and key, and the CA its connectors' certificates
/// chain to.
#[derive(FromPyObject)]
struct TlsFiles {
    cert: PathBuf,
    key: PathBuf,
    ca: PathBuf,
}

/// What a host's operator allows; a pipeline is built against a host made from it.
#[pyclass(module = "rdlt._native", name = "Host", frozen)]
pub(crate) struct NativeHost {
    settings: Settings,
    /// The facade's host once made: every build of this host shares it, and its compute pool.
    built: Mutex<Option<Arc<rdlt::Host>>>,
    /// The host this one was derived from, whose compute pool it shares.
    origin: Option<Py<NativeHost>>,
}

#[pymethods]
impl NativeHost {
    #[new]
    #[pyo3(signature = (settings, origin = None))]
    pub(crate) fn new(settings: Settings, origin: Option<Py<NativeHost>>) -> Self {
        Self {
            settings,
            built: Mutex::new(None),
            origin,
        }
    }
}

impl NativeHost {
    /// The facade's host every build of this host shares, made at the first build.
    pub(crate) fn shared(&self, py: Python<'_>) -> Result<Arc<rdlt::Host>, PyErr> {
        if let Some(built) = self.built.lock().as_ref() {
            return Ok(Arc::clone(built));
        }
        // Made outside the lock, since making it waits with Python's signal handlers running.
        let made = Arc::new(self.host(py)?);
        Ok(Arc::clone(self.built.lock().get_or_insert(made)))
    }

    /// The facade's host these settings describe.
    pub(crate) fn host(&self, py: Python<'_>) -> Result<rdlt::Host, PyErr> {
        let settings = &self.settings;
        let mut host = rdlt::Host::new();
        if settings.trust_binaries {
            host = host.trust_binaries();
        }
        if settings.allow_network {
            host = host.allow_network();
        }
        if let Some(wait) = settings.build_wait {
            host = host.build_wait(wait);
        }
        for dir in &settings.connectors {
            host = host.connectors(dir.clone());
        }
        for root in &settings.writable {
            host = host.writable(root.clone());
        }
        for root in &settings.readable {
            host = host.readable(root.clone());
        }
        for path in &settings.always_readable {
            host = host.always_readable(path.clone());
        }
        host = host.allow_env(settings.allow_env.iter().cloned());
        host = host.allow_files(settings.allow_files.iter().cloned());
        if let Some(store) = &settings.secret_store {
            host = host.secret_store(PythonSecrets::new(store.clone_ref(py)));
        }
        if let Some(tls) = &settings.remote {
            let (cert, key, ca) = (tls.cert.clone(), tls.key.clone(), tls.ca.clone());
            let opened = runtime::wait(py, rdlt::Tls::from_files(cert, key, ca))?;
            host = host.remote(opened.map_err(|error| errors::raised(py, &error))?);
        }
        for source in &settings.sources {
            host = host.source_factory(Arc::from(source.get().factory()));
        }
        for (name, transform) in &settings.transforms {
            host = host.transform(name.clone(), transform.get().transform());
        }
        if let Some(origin) = &self.origin {
            host = host.compute_of(&*origin.get().shared(py)?);
        }
        Ok(host)
    }
}
