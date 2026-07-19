//! T055: REST source against a mock API — pagination, resume, error classification —
//! plus the public source conformance suite (the certification gate).

use rdlt_connector::{Cursor, ReadRequest, Source, SourceError, records_channel};
use rdlt_source_rest::RestSource;
use rdlt_testkit::assert_conformant;
use rdlt_testkit::conformance::source::verify_source;
use serde_json::json;
use wiremock::matchers::{method, path, query_param};
use wiremock::{Mock, MockServer, ResponseTemplate};

fn config_yaml(base_url: &str) -> String {
    format!(
        r#"
base_url: "{base_url}"
streams:
  - name: events
    path: /events
    pagination:
      type: page
    cursor_field: seq
    cursor_param: since
    primary_key: [seq]
"#
    )
}

/// Mock a cursor-aware paginated API: rows 1..=3 across two pages; `since` filters
/// server-side (what makes the resume law hold for a real API).
async fn mock_api() -> MockServer {
    let server = MockServer::start().await;
    // Resume mocks FIRST: wiremock is first-match-wins, and these are more specific
    // (the API filters server-side by `since` — what makes the resume law hold).
    for (since, rows) in [("2", json!([{"seq": 3, "v": "c"}])), ("3", json!([]))] {
        Mock::given(method("GET"))
            .and(path("/events"))
            .and(query_param("since", since))
            .and(query_param("page", "1"))
            .respond_with(ResponseTemplate::new(200).set_body_json(rows))
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/events"))
            .and(query_param("since", since))
            .and(query_param("page", "2"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!([])))
            .mount(&server)
            .await;
    }
    // Full reads (no `since`).
    Mock::given(method("GET"))
        .and(path("/events"))
        .and(query_param("page", "1"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!([
            {"seq": 1, "v": "a"}, {"seq": 2, "v": "b"}
        ])))
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/events"))
        .and(query_param("page", "2"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!([{"seq": 3, "v": "c"}])))
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/events"))
        .and(query_param("page", "3"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!([])))
        .mount(&server)
        .await;
    server
}

#[tokio::test]
async fn paginates_and_checkpoints_max_cursor() {
    let server = mock_api().await;
    let source = RestSource::from_yaml(&config_yaml(&server.uri())).expect("config");
    let (out, mut input) = records_channel(1 << 20);
    let spec = source.streams().await.expect("streams")[0].clone();
    let read = tokio::spawn(async move { source.read(ReadRequest::new(spec, None, out)).await });

    let mut rows = 0usize;
    let mut checkpoints = Vec::new();
    while let Some(push) = input.recv().await {
        match push.payload {
            rdlt_connector::PushPayload::RawJson(bytes) => {
                let v: serde_json::Value = serde_json::from_slice(&bytes).expect("json");
                rows += v.as_array().expect("array").len();
            }
            rdlt_connector::PushPayload::Checkpoint(c) => checkpoints.push(c),
            other => panic!("unexpected push {other:?}"),
        }
    }
    read.await.expect("join").expect("read ok");
    assert_eq!(rows, 3, "both pages consumed");
    assert_eq!(
        checkpoints.last(),
        Some(&Cursor::new("3")),
        "max cursor checkpointed"
    );
}

#[tokio::test]
async fn resume_sends_cursor_param_and_skips_completed_ranges() {
    let server = mock_api().await;
    let source = RestSource::from_yaml(&config_yaml(&server.uri())).expect("config");
    let (out, mut input) = records_channel(1 << 20);
    let spec = source.streams().await.expect("streams")[0].clone();
    let read = tokio::spawn(async move {
        source
            .read(ReadRequest::new(spec, Some(Cursor::new("2")), out))
            .await
    });
    let mut rows = Vec::new();
    while let Some(push) = input.recv().await {
        if let rdlt_connector::PushPayload::RawJson(bytes) = push.payload {
            let v: serde_json::Value = serde_json::from_slice(&bytes).expect("json");
            rows.extend(v.as_array().expect("array").clone());
        }
    }
    read.await.expect("join").expect("read ok");
    assert_eq!(
        rows,
        vec![json!({"seq": 3, "v": "c"})],
        "only rows after the cursor"
    );
}

#[tokio::test]
async fn error_classification_matches_the_contract() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/limited"))
        .respond_with(ResponseTemplate::new(429).insert_header("retry-after", "7"))
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/broken"))
        .respond_with(ResponseTemplate::new(500))
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/forbidden"))
        .respond_with(ResponseTemplate::new(403))
        .mount(&server)
        .await;

    for (endpoint, want) in [
        ("limited", "rate"),
        ("broken", "transient"),
        ("forbidden", "fatal"),
    ] {
        let yaml = format!(
            "base_url: \"{}\"\nstreams:\n  - name: s\n    path: /{endpoint}\n",
            server.uri()
        );
        let source = RestSource::from_yaml(&yaml).expect("config");
        let (out, _input) = records_channel(1 << 20);
        let spec = source.streams().await.expect("streams")[0].clone();
        let err = source
            .read(ReadRequest::new(spec, None, out))
            .await
            .expect_err("must fail");
        match (want, &err) {
            ("rate", SourceError::RateLimited { retry_after, .. }) => {
                assert_eq!(*retry_after, Some(std::time::Duration::from_secs(7)));
            }
            ("transient", SourceError::Transient(_)) => {}
            ("fatal", SourceError::Fatal(_)) => {}
            (want, got) => panic!("{endpoint}: wanted {want}, got {got:?}"),
        }
    }
}

/// The certification gate: the REST source passes the public conformance suite
/// against a cursor-honoring API.
#[tokio::test]
async fn rest_source_is_conformant() {
    let server = mock_api().await;
    let source = RestSource::from_yaml(&config_yaml(&server.uri())).expect("config");
    assert_conformant(verify_source(&source).await);
}
