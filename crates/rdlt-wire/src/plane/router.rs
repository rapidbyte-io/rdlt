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

use super::{Incoming, Outgoing, WRITE, status};
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
        if path != WRITE {
            let request = request.map(|body| Body::new(Bounded::new(body, bounds, window)));
            return Box::pin(self.inner.call(request));
        }
        let plane = Arc::clone(&self.plane);
        let most = self.limits.largest();
        let (parts, body) = request.into_parts();
        Box::pin(async move {
            if let Err(refused) = status::uncompressed(&parts.headers) {
                return Ok(refused.into_http());
            }
            let frames = Incoming::request(Bounded::new(body, bounds, window));
            Ok(match plane.write(frames).await {
                Ok(answers) => answered(Outgoing::answer(answers, most)),
                Err(refused) => refused.into_http(),
            })
        })
    }
}

/// The response whose body is `answers`.
fn answered<M: super::Chained + Send + 'static>(answers: Outgoing<M>) -> http::Response<Body> {
    let mut response = http::Response::new(Body::new(answers));
    *response.version_mut() = http::Version::HTTP_2;
    response.headers_mut().insert(
        http::header::CONTENT_TYPE,
        HeaderValue::from_static("application/grpc"),
    );
    response
}
