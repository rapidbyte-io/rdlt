//! Shared harness for the 014 wiremock suites: build a source from YAML,
//! read one stream, collect rows + checkpoints.
#![allow(dead_code)]

use rdlt_connector::{Cursor, PushPayload, ReadRequest, Source, SourceError, records_channel};
use rdlt_connector_rest::RestSource;

pub struct ReadOutcome {
    pub rows: Vec<serde_json::Value>,
    pub checkpoints: Vec<Cursor>,
    pub result: Result<(), SourceError>,
}

/// Read `stream` fully; panics only on harness plumbing, never on the read.
pub async fn read_stream(yaml: &str, stream: &str, since: Option<Cursor>) -> ReadOutcome {
    let source = RestSource::from_yaml(yaml).expect("config parses");
    let (out, mut input) = records_channel(1 << 20);
    let specs = source.streams().await.expect("streams");
    let spec = specs
        .iter()
        .find(|s| s.name.as_str() == stream)
        .unwrap_or_else(|| panic!("stream {stream} declared"))
        .clone();
    let read = tokio::spawn(async move { source.read(ReadRequest::new(spec, since, out)).await });

    let mut rows = Vec::new();
    let mut checkpoints = Vec::new();
    while let Some(push) = input.recv().await {
        match push.payload {
            PushPayload::RawJson(bytes) => {
                let v: serde_json::Value = serde_json::from_slice(&bytes).expect("valid json");
                rows.extend(v.as_array().expect("array").clone());
            }
            PushPayload::Checkpoint(c) => checkpoints.push(c),
            _ => {}
        }
    }
    let result = read.await.expect("join");
    ReadOutcome {
        rows,
        checkpoints,
        result,
    }
}

/// Convenience: read expecting success; returns the rows.
pub async fn read_ok(yaml: &str, stream: &str) -> Vec<serde_json::Value> {
    let outcome = read_stream(yaml, stream, None).await;
    outcome.result.expect("read succeeds");
    outcome.rows
}

/// Convenience: read expecting a typed error; returns its rendering.
pub async fn read_err(yaml: &str, stream: &str) -> String {
    let outcome = read_stream(yaml, stream, None).await;
    outcome.result.expect_err("read fails typed").to_string()
}
