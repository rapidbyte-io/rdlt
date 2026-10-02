use std::pin::Pin;
use std::task::{Context, Poll};

use http::{HeaderMap, HeaderValue};
use hyper::body::{Body, Bytes, Frame};

use super::{CheckedBody, DETAILS, check};

/// Headers carrying `details` as their status's details.
fn detailed(details: &'static str) -> HeaderMap {
    let mut headers = HeaderMap::new();
    headers.insert("grpc-status", HeaderValue::from_static("13"));
    headers.insert(DETAILS, HeaderValue::from_static(details));
    headers
}

#[test]
fn details_that_decode_stay_and_others_go() {
    for kept in ["", "AA", "AAA=", "AAAA", "CgVoZWxsbw"] {
        let mut headers = detailed(kept);
        check(&mut headers);
        assert_eq!(
            headers.get(DETAILS).map(HeaderValue::as_bytes),
            Some(kept.as_bytes())
        );
    }
    for dropped in ["!", "A", "AAAAA", "AA=A", "\u{7f}"] {
        let Ok(value) = HeaderValue::from_str(dropped) else {
            continue;
        };
        let mut headers = HeaderMap::new();
        headers.insert(DETAILS, value);
        headers.append(DETAILS, HeaderValue::from_static("AAAA"));
        check(&mut headers);
        assert!(headers.get(DETAILS).is_none(), "{dropped:?}");
    }
}

/// A body that ends with `trailers`.
struct Trailing(Option<HeaderMap>);

impl Body for Trailing {
    type Data = Bytes;
    type Error = tonic::Status;

    fn poll_frame(
        mut self: Pin<&mut Self>,
        _context: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Bytes>, Self::Error>>> {
        Poll::Ready(self.0.take().map(|trailers| Ok(Frame::trailers(trailers))))
    }
}

#[test]
fn a_body_s_trailers_keep_only_details_that_decode() {
    let body = tonic::body::Body::new(Trailing(Some(detailed("!"))));
    let mut checked = CheckedBody(body);
    let waker = std::task::Waker::noop();
    let mut context = Context::from_waker(waker);
    let Poll::Ready(Some(Ok(frame))) = Pin::new(&mut checked).poll_frame(&mut context) else {
        panic!("the body ends with its trailers");
    };
    let trailers = frame.into_trailers().expect("trailers");
    assert!(trailers.get(DETAILS).is_none());
    assert!(trailers.get("grpc-status").is_some());
}
