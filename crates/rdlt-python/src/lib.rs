//! The native half of the `rdlt` Python package, imported as `rdlt._native`; the package's
//! typed Python layer is its public API.
//!
//! ```text
//! >>> import rdlt
//! >>> rdlt.__version__
//! '0.0.0'
//! ```
//!
//! Fallible functions return `Result<T, PyErr>`, never the `PyResult` alias: mutation testing
//! offers `Ok(..)` only for a type spelled `Result`.

#![forbid(unsafe_code)]

mod arrow;
mod calls;
mod certify;
mod destination;
#[cfg(test)]
mod embedded;
mod errors;
mod host;
mod json;
mod pipeline;
mod run;
mod runtime;
mod secrets;
mod serve;
mod source;
#[cfg(test)]
mod tests;
mod transform;

use pyo3::types::{PyModule, PyModuleMethods as _, PyTuple};
use pyo3::{Bound, PyErr, pymodule};

/// The extension module `rdlt._native`.
#[pymodule]
#[pyo3(name = "_native")]
fn native(module: &Bound<'_, PyModule>) -> Result<(), PyErr> {
    register(module)
}

/// Adds the extension's classes and functions to `module`.
fn register(module: &Bound<'_, PyModule>) -> Result<(), PyErr> {
    module.add("__version__", env!("CARGO_PKG_VERSION"))?;
    // The codes a host keeps for its own findings, which no connector may give.
    let host_codes = PyTuple::new(module.py(), rdlt_connector::wire::HOST_CODES)?;
    module.add("HOST_CODES", host_codes)?;
    // The codes of the failures the Rust SDK names, which a Python connector raises as Rust's do.
    for (name, code) in [
        ("COMMIT_STALE", rdlt_connector::COMMIT_STALE),
        ("TABLE_OWNED", rdlt_connector::TABLE_OWNED),
        ("SCHEMA_CONFLICT", rdlt_connector::SCHEMA_CONFLICT),
        ("COLUMN_MISSING", rdlt_connector::COLUMN_MISSING),
        ("RETENTION_LOST", rdlt_connector::RETENTION_LOST),
        ("CURSOR_UNISSUED", rdlt_connector::CURSOR_UNISSUED),
    ] {
        module.add(name, code)?;
    }
    module.add_class::<host::NativeHost>()?;
    module.add_class::<pipeline::NativePipeline>()?;
    module.add_class::<run::NativeRun>()?;
    module.add_class::<run::NativeEvents>()?;
    module.add_class::<source::context::NativeStreamContext>()?;
    module.add_class::<source::context::NativeCheckpoint>()?;
    module.add_class::<source::NativeSourceFactory>()?;
    module.add_class::<destination::NativeDestinationFactory>()?;
    module.add_class::<transform::NativeTransform>()?;
    module.add_function(pyo3::wrap_pyfunction!(serve::serve, module)?)?;
    module.add_function(pyo3::wrap_pyfunction!(
        destination::minimal_capabilities,
        module
    )?)?;
    module.add_function(pyo3::wrap_pyfunction!(certify::clauses, module)?)?;
    module.add_function(pyo3::wrap_pyfunction!(certify::certify_source, module)?)?;
    module.add_function(pyo3::wrap_pyfunction!(
        certify::certify_destination,
        module
    )?)?;
    module.add_function(pyo3::wrap_pyfunction!(certify::certify_transform, module)?)?;
    Ok(())
}
