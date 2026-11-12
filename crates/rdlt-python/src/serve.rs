//! `_native.serve`: the Rust server, inside the extension, with a binary's flags.

#[cfg(test)]
mod tests;

use pyo3::{Bound, Python, pyfunction};
use rdlt_connector::serve::Served;

use crate::destination::NativeDestinationFactory;
use crate::source::NativeSourceFactory;

/// Serves the source `source` makes and the destination `destination` makes, one or both, until
/// its host stops it, as a connector binary's `main` does with `args`: `--rdlt-fd`, or `--listen`
/// and the TLS flags.
///
/// The GIL is released while it serves; none is answered once it served cleanly, and otherwise
/// why it could not.
#[pyfunction]
#[expect(
    clippy::needless_pass_by_value,
    reason = "PyO3 extracts each argument by value"
)]
pub(crate) fn serve(
    py: Python<'_>,
    args: Vec<String>,
    source: Option<Bound<'_, NativeSourceFactory>>,
    destination: Option<Bound<'_, NativeDestinationFactory>>,
) -> Option<String> {
    if source.is_none() && destination.is_none() {
        return Some("nothing is served: serve a source, a destination or both".to_owned());
    }
    let mut served = Served::new();
    if let Some(source) = &source {
        served = served.with_source(source.get().factory());
    }
    if let Some(destination) = &destination {
        served = served.with_destination(destination.get().factory());
    }
    let served = py.detach(|| Served::serve_args(args, || served));
    served.err().map(|failed| failed.to_string())
}
