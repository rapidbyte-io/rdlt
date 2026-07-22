//! Feature 014 US1 (T005/T007): selectors, response actions, POST bodies.

mod common;

use common::{read_err, read_ok, read_stream};
use serde_json::json;
use wiremock::matchers::{body_partial_json, method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

/// Wildcard selectors extract nested records.
#[tokio::test]
async fn wildcard_selector_extracts_nested() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/items"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "data": {"groups": [
                {"payload": {"id": 1}},
                {"payload": {"id": 2}}
            ]}
        })))
        .mount(&server)
        .await;
    let yaml = format!(
        r#"
base_url: "{}"
streams:
  - name: items
    path: /items
    records_path: data.groups[*].payload
"#,
        server.uri()
    );
    let rows = read_ok(&yaml, "items").await;
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0]["id"], 1);
}

/// No-match selector: typed, naming the path and the response shape.
#[tokio::test]
async fn selector_no_match_is_typed() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/items"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "meta": 1, "rows": []
        })))
        .mount(&server)
        .await;
    let yaml = format!(
        r#"
base_url: "{}"
streams:
  - name: items
    path: /items
    records_path: data.items
"#,
        server.uri()
    );
    let err = read_err(&yaml, "items").await;
    assert!(err.contains("data.items") && err.contains("meta"), "{err}");
}

/// Invalid selector syntax fails AT CONFIG PARSE, naming the subset.
#[tokio::test]
async fn invalid_selector_fails_at_parse() {
    let err = rdlt_connector_rest::RestConfig::from_yaml(
        r#"
base_url: http://x
streams:
  - name: a
    path: /a
    records_path: "data[x]"
"#,
    )
    .expect_err("bad selector")
    .to_string();
    assert!(err.contains("records_path") && err.contains("[*]"), "{err}");
}

/// 404 → end_stream: declared action; totals = what arrived before.
#[tokio::test]
async fn action_404_end_stream() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/items"))
        .respond_with(ResponseTemplate::new(404))
        .mount(&server)
        .await;
    let yaml = format!(
        r#"
base_url: "{}"
streams:
  - name: items
    path: /items
    response_actions:
      - {{status: 404, action: end_stream}}
"#,
        server.uri()
    );
    let rows = read_ok(&yaml, "items").await;
    assert!(rows.is_empty(), "clean end, zero rows");
}

/// Undeclared 4xx stays a typed error (allow-list posture).
#[tokio::test]
async fn undeclared_4xx_stays_typed() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/items"))
        .respond_with(ResponseTemplate::new(403))
        .mount(&server)
        .await;
    let yaml = format!(
        r#"
base_url: "{}"
streams:
  - name: items
    path: /items
    response_actions:
      - {{status: 404, action: end_stream}}
"#,
        server.uri()
    );
    let err = read_err(&yaml, "items").await;
    assert!(err.contains("403"), "{err}");
}

/// content_contains → ignore: matching page treated as empty; pagination
/// still terminates per its family.
#[tokio::test]
async fn action_content_ignore() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/items"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "warning": "quota_soft_limit", "data": [{"id": 1}]
        })))
        .mount(&server)
        .await;
    let yaml = format!(
        r#"
base_url: "{}"
streams:
  - name: items
    path: /items
    records_path: data
    response_actions:
      - {{content_contains: quota_soft_limit, action: ignore}}
"#,
        server.uri()
    );
    let rows = read_ok(&yaml, "items").await;
    assert!(rows.is_empty(), "ignored page contributes nothing");
}

/// Unconditional actions are rejected at parse (they'd swallow everything).
#[tokio::test]
async fn unconditional_action_rejected_at_parse() {
    let err = rdlt_connector_rest::RestConfig::from_yaml(
        r#"
base_url: http://x
streams:
  - name: a
    path: /a
    response_actions:
      - {action: ignore}
"#,
    )
    .expect_err("unconditional")
    .to_string();
    assert!(err.contains("response_actions[0]"), "{err}");
}

/// POST body template + body-merged cursor pagination (the search-endpoint
/// pattern).
#[tokio::test]
async fn post_body_with_cursor_pagination() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/search"))
        .and(body_partial_json(json!({"query": "x", "cursor": "c2"})))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "hits": [{"id": 2}], "next": null
        })))
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .and(path("/search"))
        .and(body_partial_json(json!({"query": "x"})))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "hits": [{"id": 1}], "next": "c2"
        })))
        .mount(&server)
        .await;
    let yaml = format!(
        r#"
base_url: "{}"
streams:
  - name: hits
    path: /search
    method: post
    body: {{query: x}}
    records_path: hits
    pagination: {{type: cursor, cursor_path: next, cursor_param: cursor}}
"#,
        server.uri()
    );
    let rows = read_ok(&yaml, "hits").await;
    assert_eq!(rows.len(), 2);
}

/// body without method: post is typed at parse.
#[tokio::test]
async fn body_requires_post() {
    let err = rdlt_connector_rest::RestConfig::from_yaml(
        r#"
base_url: http://x
streams:
  - name: a
    path: /a
    body: {q: 1}
"#,
    )
    .expect_err("body on GET")
    .to_string();
    assert!(err.contains("method: post"), "{err}");
}

/// Incremental aliases and the block are mutually exclusive (typed).
#[tokio::test]
async fn incremental_block_and_aliases_are_exclusive() {
    let err = rdlt_connector_rest::RestConfig::from_yaml(
        r#"
base_url: http://x
streams:
  - name: a
    path: /a
    cursor_field: seq
    cursor_param: since
    incremental: {cursor_field: seq}
"#,
    )
    .expect_err("mixed")
    .to_string();
    assert!(err.contains("not both"), "{err}");
}

/// The remaining validation arms, and the JSON text entry point rides the
/// same validation: alias halves set together, empty incremental
/// cursor_field, both total stops declared.
#[tokio::test]
async fn validation_matrix_covers_remaining_arms() {
    for (frag, needle) in [
        ("    cursor_field: seq\n", "set together"),
        (
            "    incremental: {cursor_field: \" \"}\n",
            "must not be empty",
        ),
        (
            "    pagination: {type: page, total_pages_path: meta.pages, total_count_path: meta.total}\n",
            "pick one stop condition",
        ),
    ] {
        let yaml = format!("base_url: http://x\nstreams:\n  - name: a\n    path: /a\n{frag}");
        let err = rdlt_connector_rest::RestConfig::from_yaml(&yaml)
            .expect_err(needle)
            .to_string();
        assert!(err.contains(needle), "expected `{needle}` in: {err}");
    }
    // from_json: same document shape, same validation.
    rdlt_connector_rest::RestConfig::from_json(
        r#"{"base_url": "http://x", "streams": [{"name": "a", "path": "/a"}]}"#,
    )
    .expect("valid JSON document parses");
    let err =
        rdlt_connector_rest::RestConfig::from_json(r#"{"base_url": "http://x", "streams": []}"#)
            .expect_err("empty streams via JSON")
            .to_string();
    assert!(err.contains("at least one stream"), "{err}");
}

/// end_param requires end_value (closed windows are explicit).
#[tokio::test]
async fn end_param_requires_end_value() {
    let err = rdlt_connector_rest::RestConfig::from_yaml(
        r#"
base_url: http://x
streams:
  - name: a
    path: /a
    incremental: {cursor_field: seq, start_param: since, end_param: until}
"#,
    )
    .expect_err("end without value")
    .to_string();
    assert!(err.contains("end_value"), "{err}");
}

/// The incremental block binds start AND end params on requests.
#[tokio::test]
async fn incremental_start_and_end_params_bind() {
    use wiremock::matchers::query_param;
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/items"))
        .and(query_param("since", "5"))
        .and(query_param("until", "9"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!([{"seq": 7}])))
        .mount(&server)
        .await;
    let yaml = format!(
        r#"
base_url: "{}"
streams:
  - name: items
    path: /items
    incremental: {{cursor_field: seq, start_param: since, end_param: until, end_value: "9"}}
"#,
        server.uri()
    );
    let outcome = read_stream(&yaml, "items", Some(rdlt_connector::Cursor::new("5"))).await;
    outcome.result.expect("windowed read");
    assert_eq!(outcome.rows.len(), 1);
    assert_eq!(
        outcome.checkpoints.last(),
        Some(&rdlt_connector::Cursor::new("7"))
    );
}
