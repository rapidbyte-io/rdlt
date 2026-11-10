//! `_native.serve`: the Rust server, inside the extension, with a binary's flags.

#[cfg(test)]
mod tests;

use pyo3::{Bound, FromPyObject, Python, pyfunction};
use rdlt_connector::serve::Served;

use crate::destination::NativeDestinationFactory;
use crate::source::NativeSourceFactory;

/// The role a Python connector serves, as its factory says.
#[derive(FromPyObject)]
pub(crate) enum Factory<'py> {
    /// A source's factory.
    Source(Bound<'py, NativeSourceFactory>),
    /// A destination's factory.
    Destination(Bound<'py, NativeDestinationFactory>),
}

/// Serves the source or destination `factory` makes until its host stops it, as a connector
/// binary's `main` does with `args`: `--rdlt-fd`, or `--listen` and the TLS flags.
///
/// The GIL is released while it serves; none is answered once it served cleanly, and otherwise
/// why it could not.
#[pyfunction]
pub(crate) fn serve(py: Python<'_>, args: Vec<String>, factory: Factory<'_>) -> Option<String> {
    let served = match factory {
        Factory::Source(source) => Served::new().with_source(source.get().factory()),
        Factory::Destination(destination) => {
            Served::new().with_destination(destination.get().factory())
        }
    };
    let served = py.detach(|| Served::serve_args(args, || served));
    served.err().map(|failed| failed.to_string())
}
