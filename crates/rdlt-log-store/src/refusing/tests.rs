use std::io;

use http::StatusCode;
use object_store::client::{HttpError, HttpErrorKind, HttpResponse};
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
