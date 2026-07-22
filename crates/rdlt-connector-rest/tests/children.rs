//! Feature 014 US3 (T011/T012): parent-child composition + the composed
//! example connector — engine-driven (the real wiring, not just the source).

mod common;

use common::read_err;
use rdlt_connector_duckdb::dest::DuckDb;
use rdlt_connector_rest::RestSource;
use rdlt_engine::{Engine, EngineConfig};
use serde_json::json;
use wiremock::matchers::{method, path, query_param};
use wiremock::{Mock, MockServer, ResponseTemplate};

/// A nested mock API: /repos (2 repos, paginated) and per-repo /issues.
async fn nested_api() -> MockServer {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/repos"))
        .and(query_param("page", "1"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!([
            {"owner": "acme", "name": "one", "stars": 5},
            {"owner": "acme", "name": "two", "stars": 9}
        ])))
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/repos"))
        .and(query_param("page", "2"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!([])))
        .mount(&server)
        .await;
    for (repo, issues) in [
        (
            "one",
            json!([{"id": 11, "title": "a"}, {"id": 12, "title": "b"}]),
        ),
        ("two", json!([{"id": 21, "title": "c"}])),
    ] {
        Mock::given(method("GET"))
            .and(path(format!("/repos/acme/{repo}/issues")))
            .respond_with(ResponseTemplate::new(200).set_body_json(issues))
            .mount(&server)
            .await;
    }
    server
}

fn nested_yaml(base: &str) -> String {
    format!(
        r#"
base_url: "{base}"
streams:
  - name: repos
    path: /repos
    pagination: {{type: page}}
  - name: issues
    path: /repos/{{owner}}/{{repo}}/issues
    parent:
      stream: repos
      placeholders: {{owner: owner, repo: name}}
      include: [name, stars]
"#
    )
}

/// The full engine path: parent fan-out, resolved paths, parent-field
/// embedding, exact totals in a real destination.
#[tokio::test(flavor = "multi_thread")]
async fn parent_child_reads_through_the_engine() {
    let server = nested_api().await;
    let source = RestSource::from_yaml(&nested_yaml(&server.uri())).expect("config");
    let dir = tempfile::tempdir().unwrap();
    let dest = DuckDb::open(dir.path().join("nested.duckdb")).unwrap();
    let mut config = EngineConfig::new("nested");
    config.workdir = Some(dir.path().join("work"));
    Engine::new(config, source, dest.clone())
        .run()
        .await
        .expect("run");

    assert_eq!(dest.count_rows("repos").unwrap(), 2);
    assert_eq!(
        dest.count_rows("issues").unwrap(),
        3,
        "2 + 1 across parents"
    );
    // Parent fields embedded.
    assert_eq!(
        dest.query_string("SELECT _parent_name FROM issues WHERE id = 21")
            .unwrap(),
        "two"
    );
    assert_eq!(
        dest.query_string("SELECT count(*)::VARCHAR FROM issues WHERE _parent_stars = 5")
            .unwrap(),
        "2",
        "both issues of repo `one` carry its stars"
    );
}

/// Child failure names the parent's resolved values (RS7).
#[tokio::test]
async fn child_failure_names_resolved_values() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/repos"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!([
            {"owner": "acme", "name": "ghost"}
        ])))
        .mount(&server)
        .await;
    // No /repos/acme/ghost/issues mock → 404.
    let yaml = format!(
        r#"
base_url: "{}"
streams:
  - name: repos
    path: /repos
  - name: issues
    path: /repos/{{owner}}/{{repo}}/issues
    parent:
      stream: repos
      placeholders: {{owner: owner, repo: name}}
"#,
        server.uri()
    );
    let err = read_err(&yaml, "issues").await;
    assert!(
        err.contains("owner=acme") && err.contains("repo=ghost"),
        "{err}"
    );
}

/// Parent validation matrix: undeclared parent, self-parent, nested child,
/// unused placeholder, placeholder without parent.
#[tokio::test]
async fn parent_validation_matrix() {
    for (yaml_frag, needle) in [
        (
            "  - name: c\n    path: /c/{x}\n    parent: {stream: ghost, placeholders: {x: id}}\n",
            "not a declared stream",
        ),
        (
            "  - name: c\n    path: /c/{x}\n    parent: {stream: c, placeholders: {x: id}}\n",
            "parent.stream is itself",
        ),
        (
            "  - name: c\n    path: /c/{x}\n    parent: {stream: a, placeholders: {y: id}}\n",
            "never used",
        ),
        ("  - name: c\n    path: /c/{x}\n", "no `parent` block"),
    ] {
        let yaml = format!("base_url: http://x\nstreams:\n  - name: a\n    path: /a\n{yaml_frag}");
        let err = rdlt_connector_rest::RestConfig::from_yaml(&yaml)
            .expect_err(needle)
            .to_string();
        assert!(err.contains(needle), "expected `{needle}` in: {err}");
    }
}

/// T012: the composed example connector (public pieces only) runs against
/// the same nested API through the engine — the US3 seam proof.
#[tokio::test(flavor = "multi_thread")]
async fn composed_example_runs_through_the_engine() {
    let server = nested_api().await;
    let source = composed_api::build(&server.uri()).expect("composed connector builds");
    let dir = tempfile::tempdir().unwrap();
    let dest = DuckDb::open(dir.path().join("composed.duckdb")).unwrap();
    let mut config = EngineConfig::new("composed");
    config.workdir = Some(dir.path().join("work"));
    Engine::new(config, source, dest.clone())
        .run()
        .await
        .expect("run");
    assert_eq!(dest.count_rows("repos").unwrap(), 2);
    assert_eq!(dest.count_rows("issues").unwrap(), 3);
}

/// The example "named API connector": a config GENERATOR over the public
/// surface — the shape a Google-Search-Console-style wrapper takes. No raw
/// HTTP anywhere; auth/pagination/extraction/engine wiring all inherited.
mod composed_api {
    use rdlt_connector_rest::{RestConfig, RestSource};

    /// "MiniHub": repos + their issues, as a one-call constructor.
    pub fn build(base_url: &str) -> Result<RestSource, Box<dyn std::error::Error>> {
        let config = RestConfig::from_value(serde_json::json!({
            "base_url": base_url,
            "streams": [
                {"name": "repos", "path": "/repos", "pagination": {"type": "page"}},
                {"name": "issues", "path": "/repos/{owner}/{repo}/issues",
                 "parent": {"stream": "repos",
                             "placeholders": {"owner": "owner", "repo": "name"},
                             "include": ["name"]}}
            ]
        }))?;
        Ok(RestSource::new(config))
    }
}
