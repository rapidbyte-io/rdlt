use std::error::Error as _;

use rdlt_connector::{PipelineId, ReadMode, StreamName};
use rdlt_engine::{EngineConfig, ErrorKind, PipelinePlan, StreamPlan, WriteMode};

use super::Error;

#[test]
fn an_engine_error_reports_as_the_engine_reports_it() {
    let engine = EngineConfig::builder().memory(0).build().unwrap_err();
    let report = engine.report();
    let (kind, origin, code) = (engine.kind(), engine.origin(), engine.code());
    let display = engine.to_string();
    let error = Error::engine(engine);
    assert_eq!(error.report(), report);
    assert_eq!(
        (error.kind(), error.origin(), error.code()),
        (kind, origin, code)
    );
    assert_eq!(error.kind(), ErrorKind::Config);
    assert_eq!(error.to_string(), display);
    assert_eq!((error.stream(), error.connector_code()), (None, None));
    assert!(!error.is_retryable());
    assert!(error.source().is_none());
    let debug = format!("{error:?}");
    for part in ["Config", "Engine", "config_invalid", &display] {
        assert!(debug.contains(part), "{debug}");
    }
}

#[test]
fn an_engine_error_names_its_stream_as_the_stream_displays() {
    let stream = StreamName::with_namespace("public", "orders").unwrap();
    let modes = StreamPlan::new(stream)
        .read(ReadMode::Incremental)
        .write(WriteMode::Replace);
    let refused = PipelinePlan::new(PipelineId::parse("modes").unwrap(), [modes]).unwrap_err();
    let error = Error::engine(refused);
    assert_eq!(error.stream(), Some("public.orders"));
    assert_eq!(error.report().stream.as_deref(), Some("public.orders"));
}
