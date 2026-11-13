//! What each failure of the API is to the engine: a wait, a retry, or the end of the run.

use std::path::Path;
use std::time::Duration;

use rdlt::{Connector, Host, Origin, Pipeline, Side, Stream};
use rdlt_connector::{ConnectContext, ConnectorError, ConnectorErrorKind, SourceConnector as _};
use rdlt_connector_reference::SqliteDestination;
use serde_json::{Value, json};

use crate::server::{Answer, Api};

/// The error the source's check fails with, connected with `config`.
async fn checked(config: Value) -> ConnectorError {
    let config = serde_json::from_value(config).expect("the configuration is the source's");
    let context = ConnectContext::new();
    let source = pokeapi::PokeApi::connect(config, &context).await;
    let source = source.expect("a source connects without asking the API anything");
    source.check().await.expect_err("the API fails the check")
}

#[tokio::test]
async fn each_status_the_api_fails_with_is_an_error_of_its_kind() {
    use ConnectorErrorKind::{Auth, Config, RateLimited, Transient};
    let waits = |seconds| Some(Duration::from_secs(seconds));
    for (status, retry_after, kind, wait) in [
        (429, Some(7), RateLimited, waits(7)),
        (429, None, RateLimited, waits(1)),
        (500, None, Transient, None),
        (503, None, Transient, None),
        (401, None, Auth, None),
        (403, None, Auth, None),
        (404, None, Config, None),
    ] {
        let path = "/api/v2/pokemon";
        let answer = match retry_after {
            Some(seconds) => Answer::waiting(path, seconds),
            None => Answer::failed(path, status),
        };
        let api = Api::answering(1, vec![answer]);
        let error = checked(json!({ "base_url": api.url })).await;
        assert_eq!(
            (error.kind(), error.retry_after()),
            (kind, wait),
            "{status}"
        );
    }
}

#[tokio::test]
async fn a_response_cut_short_is_transient() {
    let api = Api::answering(1, vec![Answer::cut("/api/v2/pokemon")]);
    let error = checked(json!({ "base_url": api.url })).await;
    assert_eq!(error.kind(), ConnectorErrorKind::Transient);
}

#[tokio::test]
async fn a_connection_the_api_refuses_is_transient() {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let url = format!("http://{}/api/v2", listener.local_addr().unwrap());
    drop(listener);
    let error = checked(json!({ "base_url": url })).await;
    assert_eq!(error.kind(), ConnectorErrorKind::Transient);
}

#[tokio::test]
async fn a_configuration_the_source_cannot_read_with_is_refused() {
    for config in [
        json!({ "page_size": 0 }),
        json!({ "page_size": 101 }),
        json!({ "base_url": "ftp://pokeapi.co/api/v2" }),
        json!({ "base_url": "pokeapi" }),
    ] {
        let config = serde_json::from_value(config).unwrap();
        let connected = pokeapi::PokeApi::connect(config, &ConnectContext::new()).await;
        let refused = connected.err().map(|error| error.kind());
        assert_eq!(refused, Some(ConnectorErrorKind::Config));
    }
}

#[tokio::test]
async fn a_body_of_another_shape_is_a_data_error() {
    for body in [
        "not JSON",
        "[1, 2]",
        r#"{ "next": null, "results": null }"#,
        r#"{ "next": null, "results": [{ "name": "pokemon-1", "url": 1 }] }"#,
    ] {
        let api = Api::answering(1, vec![Answer::body("/api/v2/pokemon", body)]);
        let error = checked(json!({ "base_url": api.url })).await;
        assert_eq!(error.kind(), ConnectorErrorKind::Data, "{body}");
    }
}

#[tokio::test]
async fn a_page_that_names_no_next_is_read() {
    let body = r#"{ "results": [{ "name": "pokemon-1", "url": "/pokemon/1/" }] }"#;
    let api = Api::answering(1, vec![Answer::body("/api/v2/pokemon", body)]);
    let config = serde_json::from_value(json!({ "base_url": api.url }));
    let config = config.expect("the configuration is the source's");
    let source = pokeapi::PokeApi::connect(config, &ConnectContext::new()).await;
    let checked = source.expect("a source connects").check().await;
    assert!(checked.is_ok(), "{checked:?}");
}

#[tokio::test]
async fn a_body_a_read_cannot_take_ends_the_run_with_no_retry() {
    // A pokemon whose `field` is `value`, and whose other fields are as the API sends them.
    let detail = |field: &str, value: &str| {
        let number = |name: &str, kept: &'static str| if name == field { value } else { kept };
        let (id, height, weight) = (
            number("id", "1"),
            number("height", "5"),
            number("weight", "10"),
        );
        format!(
            r#"{{ "id": {id}, "name": "pokemon-1", "height": {height}, "weight": {weight}, \
             "types": [], "abilities": [], "stats": [] }}"#
        )
    };
    let entry = |url: &str| {
        let entry = format!(r#"{{ "name": "type-5", "url": "{url}" }}"#);
        format!(r#"{{ "next": null, "results": [{entry}] }}"#)
    };
    for (stream, path, body) in [
        ("pokemon", "/api/v2/pokemon/1", detail("height", r#""5""#)),
        ("pokemon", "/api/v2/pokemon/1", detail("height", "-1")),
        ("pokemon", "/api/v2/pokemon/1", detail("weight", "7.0")),
        (
            "pokemon",
            "/api/v2/pokemon/1",
            detail("id", "18446744073709551616"),
        ),
        ("types", "/api/v2/type", entry("/type/+5/")),
        ("types", "/api/v2/type", entry("/type/5a/")),
        (
            "types",
            "/api/v2/type",
            entry("/type/18446744073709551616/"),
        ),
    ] {
        let api = Api::answering(1, vec![Answer::body(path, body.as_str())]);
        let directory = tempfile::tempdir().unwrap();
        let outcome = loaded(&api, stream, &directory.path().join("pokeapi.db")).await;
        // A data error is the source's, and no retry passes it: the run ends in its first attempt.
        let error = outcome.error.expect("the run fails");
        let ended = (
            error.origin(),
            error.is_retryable(),
            outcome.report.attempted,
        );
        assert_eq!(ended, (Origin::Source, false, 1), "{body}: {error}");
    }
}

/// How a run that reads `stream` from `api` into the SQLite database at `database` ends.
async fn loaded(api: &Api, stream: &str, database: &Path) -> rdlt::Outcome {
    let host = Host::new()
        .source::<pokeapi::PokeApi>()
        .destination::<SqliteDestination>();
    let source = Side::new(Connector::new("io.example.pokeapi"));
    let source = source.config(json!({ "base_url": api.url }));
    let destination = Side::new(Connector::new("io.rapidbyte.sqlite"));
    let destination = destination.config(json!({ "path": database }));
    let pipeline = Pipeline::builder("pokeapi", source, destination)
        .stream(Stream::new(stream))
        .build(&host)
        .await
        .expect("the pipeline builds");
    pipeline.start().await
}
