//! The data plane: the protocol's streaming calls of batches, served and called by hand rather
//! than through generated stubs, on the same bytes gRPC and prost put on the wire.
//!
//! A receiver decodes each message from the whole, scanned and charged bytes its [`Bounded`]
//! body passes on, so every bound holds before any decode, and a batch's body is a slice of
//! those bytes. A sender sends each message as its [`Chunks`]: a batch's body goes out as the
//! bytes it already is, after a head that prost encodes.
//!
//! [`Bounded`]: crate::bounded::Bounded

mod chained;
mod incoming;
mod outgoing;
#[cfg(test)]
mod tests;

use tonic::body::Body;
use tonic::codegen::http;
use tonic::codegen::tokio_stream::Stream;

pub use self::chained::{Chained, Chunks};
pub use self::incoming::Incoming;
pub use self::outgoing::Outgoing;

/// The path of the `Write` call.
pub const WRITE: &str = "/rdlt.connector.v1.Connector/Write";

/// Bytes: a message's prefix, a flag and its length.
const PREFIX: usize = 5;

/// A request to the call at `path` of `messages`, each at most `most` bytes, as a gRPC client
/// sends it.
pub fn request<M: Chained + Send + 'static>(
    path: &'static str,
    messages: impl Stream<Item = M> + Send + 'static,
    most: usize,
) -> http::Request<Body> {
    todo!()
}
