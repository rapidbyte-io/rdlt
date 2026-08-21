//! Shared support: the in-memory doubles the builder cases hand the
//! engine's boundary, and the document parse every model case starts
//! from.

use rdlt::document::Document;
use rdlt::sdk::spi::destination::Destination as _;
use rdlt_testkit::memory;

/// An empty in-memory source — enough for `build()`, which never reads.
pub(crate) fn empty_source() -> memory::Source {
    memory::Source::new(vec![])
}

/// An in-memory destination whose declared capabilities refuse Merge.
pub(crate) fn merge_less_destination() -> memory::Destination {
    let caps = memory::Destination::new().capabilities().with_merge(false);
    memory::Destination::new().with_capabilities(caps)
}

/// Parse a pipeline document that the case expects to be well-formed.
pub(crate) fn document(yaml: &str) -> Document {
    serde_yaml_ng::from_str(yaml).expect("the pipeline document parses")
}
