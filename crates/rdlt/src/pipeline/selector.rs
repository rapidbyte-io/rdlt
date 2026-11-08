//! What a reset names a stream by.

use rdlt_connector::StreamName;

/// A stream a reset names: as the source's catalog shows it, or by its identity.
///
/// ```
/// use rdlt::{StreamName, StreamSelector};
///
/// let shown = StreamSelector::from("public.orders");
/// let gone = StreamName::with_namespace("sales", "orders").expect("a stream's name");
/// assert_ne!(shown, StreamSelector::from(gone));
/// ```
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum StreamSelector {
    /// As the source's catalog shows it: its namespace, a dot and its name, or its name alone.
    Shown(String),
    /// By its namespace and name: for a stream neither the pipeline nor the catalog holds now,
    /// since a dot in a shown name does not say where a namespace ends.
    Identity(StreamName),
}

impl From<&str> for StreamSelector {
    fn from(shown: &str) -> Self {
        Self::Shown(shown.to_owned())
    }
}

impl From<&String> for StreamSelector {
    fn from(shown: &String) -> Self {
        Self::Shown(shown.clone())
    }
}

impl From<String> for StreamSelector {
    fn from(shown: String) -> Self {
        Self::Shown(shown)
    }
}

impl From<StreamName> for StreamSelector {
    fn from(identity: StreamName) -> Self {
        Self::Identity(identity)
    }
}
