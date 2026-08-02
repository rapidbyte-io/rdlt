//! Shared plumbing for the suites. The container fixture lives here
//! once the live cells land; the offline cells share only the
//! document helpers.
#![allow(dead_code)] // shared across many case files; not every file uses every helper

/// A minimal valid document over a bearer token.
pub fn minimal_doc() -> serde_json::Value {
    serde_json::json!({
        "catalog": {
            "uri": "http://localhost:8181/api/catalog",
            "warehouse": "wh",
            "auth": {"bearer": {"token": "t"}},
        },
        "namespace": "raw",
    })
}
