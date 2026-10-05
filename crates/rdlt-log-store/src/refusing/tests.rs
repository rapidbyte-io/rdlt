use std::io;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Duration;

use bytes::Bytes;
use http::StatusCode;
use http_body::Frame;
use object_store::client::{HttpError, HttpErrorKind, HttpResponse, HttpResponseBody};
use rdlt_engine::StoreRefusal;

use super::{REFUSAL_BYTES, malformed, refused, tls};

fn answer(status: StatusCode, body: &str) -> HttpResponse {
    let mut response = HttpResponse::new(body.to_owned().into());
    *response.status_mut() = status;
    response
}

#[test]
fn only_a_request_the_store_takes_as_malformed_or_never_takes_is_refused_for_good() {
    for status in [400, 405, 411, 413, 414, 431, 501, 505] {
        let status = StatusCode::from_u16(status).expect("a status");
        assert!(refused(status), "{status}");
    }
    for status in [
        200, 204, 301, 304, 401, 403, 404, 409, 412, 416, 429, 500, 502, 503, 504,
    ] {
        let status = StatusCode::from_u16(status).expect("a status");
        assert!(!refused(status), "{status}");
    }
}

#[tokio::test]
async fn a_malformed_request_is_refused_for_good_unless_it_timed_out_or_its_token_expired() {
    for code in ["RequestTimeout", "ExpiredToken"] {
        let body = format!("<Error><Code>{code}</Code><Message>try again</Message></Error>");
        let passed = malformed(answer(StatusCode::BAD_REQUEST, &body)).await;
        let passed = passed.expect("passed to the log as it came");
        assert_eq!(passed.status(), StatusCode::BAD_REQUEST);
        let read = passed.into_body().bytes().await.expect("its body");
        assert_eq!(read, body.as_bytes(), "{code}");
    }
    // A code past what is read of the body is not read.
    let late = format!("{}<Code>RequestTimeout</Code>", " ".repeat(REFUSAL_BYTES));
    for body in [
        "<Error><Code>InvalidArgument</Code></Error>",
        "",
        late.as_str(),
    ] {
        let error = malformed(answer(StatusCode::BAD_REQUEST, body)).await;
        let error = error.expect_err("refused for good");
        let refusal = std::error::Error::source(&error).expect("a cause");
        assert!(refusal.is::<StoreRefusal>(), "{body:?}");
    }
}

#[test]
fn a_tls_error_wherever_among_the_causes_is_told_apart() {
    let certificate = || rustls::Error::InvalidCertificate(rustls::CertificateError::UnknownIssuer);
    let direct = HttpError::new(HttpErrorKind::Connect, certificate());
    assert!(tls(&direct));
    let held = HttpError::new(
        HttpErrorKind::Connect,
        io::Error::new(io::ErrorKind::InvalidData, certificate()),
    );
    assert!(tls(&held));
    let nested = HttpError::new(
        HttpErrorKind::Connect,
        io::Error::other(io::Error::new(io::ErrorKind::InvalidData, certificate())),
    );
    assert!(tls(&nested));
    let reset = HttpError::new(HttpErrorKind::Connect, io::Error::other("reset"));
    assert!(!tls(&reset));
}

/// A body of its bytes, and then nothing ever again, as from a store that stops sending.
struct Stalled(Option<Bytes>);

impl http_body::Body for Stalled {
    type Data = Bytes;
    type Error = HttpError;

    fn poll_frame(
        mut self: Pin<&mut Self>,
        _: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Bytes>, HttpError>>> {
        self.0.take().map_or(Poll::Pending, |bytes| {
            Poll::Ready(Some(Ok(Frame::data(bytes))))
        })
    }
}

#[tokio::test(start_paused = true)]
async fn a_refusal_is_judged_by_what_is_read_of_it_without_waiting_for_more() {
    let read = Bytes::from(vec![b' '; REFUSAL_BYTES]);
    let mut response = HttpResponse::new(HttpResponseBody::new(Stalled(Some(read))));
    *response.status_mut() = StatusCode::BAD_REQUEST;
    let judged = tokio::time::timeout(Duration::from_secs(60), malformed(response)).await;
    let error = judged
        .expect("judged without waiting")
        .expect_err("refused for good");
    let refusal = std::error::Error::source(&error).expect("a cause");
    assert!(refusal.is::<StoreRefusal>());
}
