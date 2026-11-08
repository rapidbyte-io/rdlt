use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use pyo3::types::{PyAnyMethods as _, PyTypeMethods as _};
use rdlt_connector::prelude::*;
use rdlt_connector_reference::{MemoryDestination, MemorySource};

use super::NativePipeline;
use crate::embedded;

/// A document that loads `rows` rows from the memory source into the memory store `name`.
pub(crate) fn memory_document(name: &str, rows: usize) -> String {
    let rows: Vec<serde_json::Value> = (0..rows).map(|n| serde_json::json!({ "n": n })).collect();
    serde_json::json!({
        "version": 1,
        "pipeline": name,
        "source": { "connector": "io.rapidbyte.memory", "config": { "streams": { "rows": rows } } },
        "destination": { "connector": "io.rapidbyte.memory", "config": { "store": name } },
        "streams": [{ "name": "rows" }],
    })
    .to_string()
}

/// A host that places the memory connectors in process.
pub(crate) fn memory_host() -> Arc<rdlt::Host> {
    Arc::new(
        rdlt::Host::new()
            .source::<MemorySource>()
            .destination::<MemoryDestination>(),
    )
}

#[test]
fn a_document_builds_checks_and_prints_itself() {
    embedded::with(|py| {
        let builder = rdlt::Pipeline::from_text(&memory_document("built", 3), "test").unwrap();
        let pipeline = NativePipeline::built(py, builder, memory_host()).expect("it builds");
        pipeline.check(py).expect("it checks");
        let document: serde_json::Value = serde_json::from_str(&pipeline.document()).unwrap();
        assert_eq!(document["pipeline"], "built");
    });
}

#[test]
fn a_mistake_raises_the_config_error_and_never_its_value() {
    embedded::with(|py| {
        let mut document: serde_json::Value =
            serde_json::from_str(&memory_document("mistaken", 1)).unwrap();
        document["destination"]["config"]["store_name"] = "hunter2-secret".into();
        let builder = rdlt::Pipeline::from_text(&document.to_string(), "test").unwrap();
        let raised = NativePipeline::built(py, builder, memory_host())
            .err()
            .expect("refused");
        assert_eq!(
            raised.get_type(py).name().unwrap().to_string(),
            "ConfigError"
        );
        let said = raised.value(py).str().unwrap().to_string();
        assert!(!said.contains("hunter2-secret"), "{said}");
    });
}

#[derive(serde::Deserialize, schemars::JsonSchema)]
struct Nothing {}

/// How many times the test source's check has run: it passes the first, a build's, and fails later.
static CHECKS: AtomicUsize = AtomicUsize::new(0);

/// A source whose check passes the first time and fails every other time.
struct CheckedOnce;

#[source(id = "test.checked-once")]
impl SourceConnector for CheckedOnce {
    type Config = Nothing;

    async fn connect(_config: Nothing, _context: &ConnectContext) -> Result<Self> {
        Ok(Self)
    }

    async fn check(&self) -> Result<()> {
        if CHECKS.fetch_add(1, Ordering::SeqCst) == 0 {
            Ok(())
        } else {
            Err(ConnectorError::data("only the first check passes"))
        }
    }

    fn streams(&self) -> Streams<Self> {
        Streams::new().with(Rowless)
    }
}

struct Rowless;

impl ReadStream<CheckedOnce> for Rowless {
    type Cursor = u64;

    fn spec(&self) -> StreamSpec {
        StreamSpec::new(StreamName::new("rows").expect("a valid name"))
    }

    async fn read(
        &self,
        _: &CheckedOnce,
        _: &Partition,
        _: u64,
        _: &mut Emitter<u64>,
    ) -> Result<()> {
        Ok(())
    }
}

#[test]
fn a_check_fails_where_the_connector_fails_after_the_build_passed() {
    embedded::with(|py| {
        let mut document: serde_json::Value =
            serde_json::from_str(&memory_document("rechecked", 1)).unwrap();
        document["source"] = serde_json::json!({ "connector": "test.checked-once" });
        let builder = rdlt::Pipeline::from_text(&document.to_string(), "test").unwrap();
        let host = rdlt::Host::new()
            .source::<CheckedOnce>()
            .destination::<MemoryDestination>();
        let pipeline =
            NativePipeline::built(py, builder, Arc::new(host)).expect("the first check passes");
        let raised = pipeline.check(py).expect_err("the second check fails");
        assert_eq!(
            raised.get_type(py).name().unwrap().to_string(),
            "SourceError"
        );
    });
}
