//! A connector type's factory by its role, so a binary can serve a connector by its type alone.

use std::fmt;

use crate::destination::DestinationFactory;
use crate::source::SourceFactory;

/// A connector's factory, of either role.
pub enum RoleFactory {
    /// A source connector's factory.
    Source(Box<dyn SourceFactory>),
    /// A destination connector's factory.
    Destination(Box<dyn DestinationFactory>),
}

impl fmt::Debug for RoleFactory {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Source(factory) => formatter
                .debug_tuple("Source")
                .field(&factory.spec().id)
                .finish(),
            Self::Destination(factory) => formatter
                .debug_tuple("Destination")
                .field(&factory.spec().id)
                .finish(),
        }
    }
}

/// A connector a binary serves by its type alone, as `rdlt_connector::serve::<C>()` does.
///
/// The `#[source]` and `#[destination]` attributes implement it. A type that is both a source and
/// a destination, or a binary serving two types, uses `serve::Served` instead.
pub trait Serve {
    /// The connector's factory.
    fn factory() -> RoleFactory;
}
