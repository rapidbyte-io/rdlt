//! `_native.serve`: the Rust server, inside the extension, with a binary's flags.

#[cfg(test)]
mod tests;

use pyo3::{Python, pyfunction};
use rdlt_connector::serve::Served;

use crate::source::NativeSourceFactory;

/// Serves the source `factory` makes until its host stops it, as a connector binary's `main`
/// does with `args`: `--rdlt-fd`, or `--listen` and the TLS flags.
///
/// The GIL is released while it serves; none is answered once it served cleanly, and otherwise
/// why it could not.
#[pyfunction]
pub(crate) fn serve(
    py: Python<'_>,
    args: Vec<String>,
    factory: &NativeSourceFactory,
) -> Option<String> {
    let factory = factory.factory();
    let served = py.detach(|| Served::serve_args(args, || Served::new().with_source(factory)));
    served.err().map(|failed| failed.to_string())
}
