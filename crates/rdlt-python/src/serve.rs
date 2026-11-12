//! `_native.serve`: the Rust server, inside the extension, with a binary's flags.

#[cfg(test)]
mod tests;

use pyo3::{Bound, Python, pyfunction};
use rdlt_connector::serve::Served;
use rdlt_connector::{DestinationFactory, SourceFactory};

use crate::destination::NativeDestinationFactory;
use crate::source::NativeSourceFactory;

/// Serves the source `source` makes and the destination `destination` makes, one or both, until
/// its host stops it, as a connector binary's `main` does with `args`: `--rdlt-fd`, or `--listen`
/// and the TLS flags.
///
/// Each comes with whether the launcher asked for the certification probes its class answers. The
/// GIL is released while it serves; none is answered once it served cleanly, and otherwise why it
/// could not.
#[pyfunction]
#[expect(
    clippy::needless_pass_by_value,
    reason = "PyO3 extracts each argument by value"
)]
pub(crate) fn serve(
    py: Python<'_>,
    args: Vec<String>,
    source: Option<(Bound<'_, NativeSourceFactory>, bool)>,
    destination: Option<(Bound<'_, NativeDestinationFactory>, bool)>,
) -> Option<String> {
    if source.is_none() && destination.is_none() {
        return Some("nothing is served: serve a source, a destination or both".to_owned());
    }
    let mut served = Served::new();
    if let Some((source, probed)) = &source {
        served = served.with_source(source_served(source.get(), *probed));
    }
    if let Some((destination, probed)) = &destination {
        served = served.with_destination(destination_served(destination.get(), *probed));
    }
    let served = py.detach(|| Served::serve_args(args, || served));
    served.err().map(|failed| failed.to_string())
}

/// What serves `source`: the factory that also tells where it stands, where its class says it
/// does, if the launcher asked for it, as `acknowledging_source_factory` serves a Rust source;
/// the plain factory otherwise.
fn source_served(source: &NativeSourceFactory, probed: bool) -> Box<dyn SourceFactory> {
    if probed {
        source.acknowledging()
    } else {
        source.factory()
    }
}

/// What serves `destination`: the factory that also reads back what it published, if the
/// launcher asked for it, as `readable_destination_factory` serves a Rust destination; the plain
/// factory otherwise.
fn destination_served(
    destination: &NativeDestinationFactory,
    probed: bool,
) -> Box<dyn DestinationFactory> {
    if probed {
        destination.readable()
    } else {
        destination.factory()
    }
}
