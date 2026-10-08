//! A served connection's calls: each request held to its bounds, the data plane's served by a
//! [`Plane`], every other by the generated service.

#[cfg(test)]
mod tests;

use std::convert::Infallible;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use tonic::Status;
use tonic::body::Body;
use tonic::codegen::http::{self, HeaderValue};
use tonic::codegen::{BoxFuture, BoxStream, Service};

use super::{Chained, Incoming, Outgoing, READ, READ_PUBLISHED, WRITE, status};
use crate::bounded::{Bounded, Bounds, Window};
use crate::limits::{Class, Limits};
use crate::v1;

/// The answers of a data-plane call, or the status that ends them.
pub type Answers<T> = BoxStream<T>;

/// A data-plane call being served, until its answers start.
pub type Serving<'a, T> = Pin<Box<dyn Future<Output = Result<Answers<T>, Status>> + Send + 'a>>;

/// What serves the data plane's calls.
pub trait Plane: Send + Sync + 'static {
    /// Serves a write of `frames`.
    ///
    /// # Errors
    ///
    /// A status the call fails with before any answer.
    fn write(&self, frames: Incoming<v1::WriteFrame>) -> Serving<'_, v1::WriteAck>;

    /// Serves a read the host controls with `controls`.
    ///
    /// # Errors
    ///
    /// A status the call fails with before any answer.
    fn read(&self, controls: Incoming<v1::ReadControl>) -> Serving<'_, v1::ReadFrame>;

    /// Serves a read-back of what `request`, the call's message, asks for.
    ///
    /// # Errors
    ///
    /// A status the call fails with before any answer.
    fn read_published(&self, request: v1::ReadPublishedRequest) -> Serving<'_, v1::ReadFrame>;
}

/// A connection's service: each request held to its class's bounds and the connection's window
/// before anything reads it, the data plane's calls served by `P`, and the rest by `S`.
#[derive(Debug)]
pub struct Router<S, P> {
    inner: S,
    plane: Arc<P>,
    limits: Limits,
    window: Window,
}

impl<S: Clone, P> Clone for Router<S, P> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
            plane: Arc::clone(&self.plane),
            limits: self.limits,
            window: self.window.clone(),
        }
    }
}

impl<S, P> Router<S, P> {
    /// Calls served by `plane` and `inner`, each request held to `limits` for its class, and
    /// what is still arriving to `window`.
    pub fn new(inner: S, plane: Arc<P>, limits: &Limits, window: Window) -> Self {
        Self {
            inner,
            plane,
            limits: *limits,
            window,
        }
    }
}

/// The method a request of `path` calls.
fn method(path: &str) -> &str {
    path.rsplit('/').next().unwrap_or(path)
}

/// A call of the data plane.
#[derive(Clone, Copy, Debug)]
enum Call {
    Write,
    Read,
    ReadPublished,
}

impl Call {
    /// The data-plane call at `path`, where it is one.
    fn at(path: &str) -> Option<Self> {
        match path {
            WRITE => Some(Self::Write),
            READ => Some(Self::Read),
            READ_PUBLISHED => Some(Self::ReadPublished),
            _ => None,
        }
    }

    /// Serves the call of `body` with `plane`, each answer at most `most` bytes.
    async fn serve<P: Plane>(self, plane: &P, body: Bounded, most: usize) -> http::Response<Body> {
        match self {
            Self::Write => answered(plane.write(Incoming::request(body)).await, most),
            Self::Read => answered(plane.read(Incoming::request(body)).await, most),
            Self::ReadPublished => match Incoming::request(body).unary().await {
                Ok(request) => answered(plane.read_published(request).await, most),
                Err(refused) => refused.into_http(),
            },
        }
    }
}

impl<S, P> Service<http::Request<Body>> for Router<S, P>
where
    S: Service<http::Request<Body>, Response = http::Response<Body>, Error = Infallible>,
    S::Future: Send + 'static,
    P: Plane,
{
    type Response = http::Response<Body>;
    type Error = Infallible;
    type Future = BoxFuture<http::Response<Body>, Infallible>;

    fn poll_ready(&mut self, context: &mut Context<'_>) -> Poll<Result<(), Infallible>> {
        self.inner.poll_ready(context)
    }

    fn call(&mut self, request: http::Request<Body>) -> Self::Future {
        let path = request.uri().path();
        let method = method(path);
        let class = Class::of_request(method);
        let bounds = Bounds::of(&self.limits, class, crate::scan::request(method));
        let window = Some(self.window.clone());
        let Some(call) = Call::at(path) else {
            let request = request.map(|body| Body::new(Bounded::new(body, bounds, window)));
            return Box::pin(self.inner.call(request));
        };
        let plane = Arc::clone(&self.plane);
        let most = self.limits.largest();
        let (parts, body) = request.into_parts();
        Box::pin(async move {
            if let Err(refused) = status::uncompressed(&parts.headers) {
                return Ok(refused.into_http());
            }
            let body = Bounded::new(body, bounds, window);
            Ok(call.serve(plane.as_ref(), body, most).await)
        })
    }
}

/// The response whose body is `answers`, each at most `most` bytes, or the refusal before any.
fn answered<M: Chained + Send + 'static>(
    answers: Result<Answers<M>, Status>,
    most: usize,
) -> http::Response<Body> {
    let answers = match answers {
        Ok(answers) => Outgoing::answer(answers, most),
        Err(refused) => return refused.into_http(),
    };
    let mut response = http::Response::new(Body::new(answers));
    *response.version_mut() = http::Version::HTTP_2;
    response.headers_mut().insert(
        http::header::CONTENT_TYPE,
        HeaderValue::from_static("application/grpc"),
    );
    response
}
