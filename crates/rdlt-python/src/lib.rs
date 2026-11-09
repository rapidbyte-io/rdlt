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
#[cfg(test)]
mod embedded;
mod errors;
mod host;
mod json;
mod pipeline;
mod run;
mod runtime;
mod secrets;
mod source;
#[cfg(test)]
mod tests;

use pyo3::types::{PyModule, PyModuleMethods as _};
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
    module.add_class::<host::NativeHost>()?;
    module.add_class::<pipeline::NativePipeline>()?;
    module.add_class::<run::NativeRun>()?;
    module.add_class::<run::NativeEvents>()?;
    module.add_class::<source::context::NativeStreamContext>()?;
    module.add_class::<source::context::NativeCheckpoint>()?;
    module.add_class::<source::NativeSourceFactory>()?;
    Ok(())
}
