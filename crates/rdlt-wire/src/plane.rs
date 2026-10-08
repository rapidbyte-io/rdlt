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
#[cfg(feature = "serve")]
mod router;
mod status;
#[cfg(test)]
mod tests;

use tonic::body::Body;
use tonic::codegen::http::{self, HeaderValue, Uri};
use tonic::codegen::tokio_stream::Stream;

pub use self::chained::{Chained, Chunks};
pub use self::incoming::Incoming;
pub use self::outgoing::Outgoing;
#[cfg(feature = "serve")]
pub use self::router::{Answers, Plane, Router, Serving};

/// The path of the `Write` call.
pub const WRITE: &str = "/rdlt.connector.v1.Connector/Write";

/// The path of the `Read` call.
pub const READ: &str = "/rdlt.connector.v1.Connector/Read";

/// The path of the `ReadPublished` call.
pub const READ_PUBLISHED: &str = "/rdlt.connector.v1.Connector/ReadPublished";

/// Bytes: a message's prefix, a flag and its length.
const PREFIX: usize = 5;

/// A request to the call at `path` of `messages`, each at most `most` bytes, as a gRPC client
/// sends it.
pub fn request<M: Chained + Send + 'static>(
    path: &'static str,
    messages: impl Stream<Item = M> + Send + 'static,
    most: usize,
) -> http::Request<Body> {
    let mut request = http::Request::new(Body::new(Outgoing::request(messages, most)));
    *request.method_mut() = http::Method::POST;
    *request.uri_mut() = Uri::from_static(path);
    *request.version_mut() = http::Version::HTTP_2;
    let headers = request.headers_mut();
    headers.insert(http::header::TE, HeaderValue::from_static("trailers"));
    headers.insert(
        http::header::CONTENT_TYPE,
        HeaderValue::from_static("application/grpc"),
    );
    request
}
