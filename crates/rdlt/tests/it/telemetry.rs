//! Export of telemetry installs a trace the host sends, and shuts down within a bound.

use std::time::Duration;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn otlp_export_installs_the_trace_the_host_sends_and_shuts_down() {
    let telemetry = rdlt::telemetry::otlp().expect("installed");
    let sent = tracing::info_span!("test.parent").in_scope(rdlt_host::trace::current);
    assert!(sent.is_some(), "inside a span, the host sends its trace");
    assert!(
        rdlt_host::trace::current().is_none(),
        "outside any span, it sends none"
    );
    let shutting = tokio::task::spawn_blocking(move || drop(telemetry));
    tokio::time::timeout(Duration::from_secs(60), shutting)
        .await
        .expect("it shuts down")
        .expect("it ends");
}
