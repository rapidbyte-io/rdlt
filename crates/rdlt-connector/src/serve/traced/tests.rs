use std::convert::Infallible;
use std::task::{Context, Poll};

use rdlt_testkit::spans::Spans;
use tower::{Service, ServiceExt as _, service_fn};

use super::Traced;

const GOOD: &str = "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01";

/// The fields of the span `Traced` opens for a call to `path` carrying `header`, if any.
async fn served(path: &str, header: Option<&str>) -> String {
    let spans = Spans::default();
    let _traced = tracing::subscriber::set_default(spans.clone());
    let mut service = Traced(service_fn(|_: http::Request<()>| async {
        Ok::<_, Infallible>(())
    }));
    let mut request = http::Request::builder().uri(path);
    if let Some(header) = header {
        request = request.header("traceparent", header);
    }
    service
        .ready()
        .await
        .unwrap()
        .call(request.body(()).unwrap())
        .await
        .unwrap();
    let mut calls = spans.named("rdlt.serve");
    assert_eq!(calls.len(), 1);
    calls.remove(0)
}

#[tokio::test]
async fn a_well_formed_header_names_the_hosts_trace_in_the_call_s_span() {
    let fields = served("/rdlt.connector.v1.Connector/Check", Some(GOOD)).await;
    assert!(
        fields.contains("rpc=/rdlt.connector.v1.Connector/Check"),
        "{fields}"
    );
    assert!(
        fields.contains("trace_id=0af7651916cd43dd8448eb211c80319c"),
        "{fields}"
    );
    assert!(fields.contains("parent_id=b7ad6b7169203331"), "{fields}");
}

#[tokio::test]
async fn a_header_that_is_not_a_traceparent_is_ignored_and_the_call_still_answers() {
    for header in [
        "00-0AF7651916CD43DD8448EB211C80319C-b7ad6b7169203331-01",
        "garbage",
        "00-00000000000000000000000000000000-b7ad6b7169203331-01",
    ] {
        let fields = served("/x/Check", Some(header)).await;
        assert!(
            !fields.contains("trace_id") && !fields.contains("parent_id"),
            "{fields}"
        );
    }
    let none = served("/x/Check", None).await;
    assert!(!none.contains("trace_id"), "{none}");
}

#[tokio::test]
async fn a_path_that_is_long_or_not_plain_text_is_shown_as_other() {
    let long = format!("/{}", "a".repeat(200));
    assert!(served(&long, None).await.contains("rpc=other"));
    assert!(served("/x%C3%A9", None).await.contains("rpc=/x%C3%A9"));
}

/// A service that is ready at its second ask, and counts them.
struct Slow(u32);

impl Service<http::Request<()>> for Slow {
    type Response = ();
    type Error = Infallible;
    type Future = std::future::Ready<Result<(), Infallible>>;

    fn poll_ready(&mut self, context: &mut Context<'_>) -> Poll<Result<(), Infallible>> {
        self.0 += 1;
        if self.0 < 2 {
            context.waker().wake_by_ref();
            return Poll::Pending;
        }
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, _: http::Request<()>) -> Self::Future {
        std::future::ready(Ok(()))
    }
}

#[tokio::test]
async fn a_traced_service_is_ready_when_the_service_it_wraps_is() {
    let mut service = Traced(Slow(0));
    service.ready().await.unwrap();
    assert_eq!(service.0.0, 2, "it waited for the service it wraps");
}
