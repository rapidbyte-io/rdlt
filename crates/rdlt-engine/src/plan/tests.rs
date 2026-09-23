use rdlt_connector::{PipelineId, ReadMode, StreamName};

use super::{PipelinePlan, StreamPlan, WriteMode};
use crate::error::ErrorKind;

fn stream(name: &str) -> StreamPlan {
    StreamPlan::new(StreamName::new(name).unwrap())
}

fn pipeline() -> PipelineId {
    PipelineId::parse("orders").unwrap()
}

#[test]
fn a_stream_defaults_to_a_full_read_appended() {
    let plan = stream("a");
    assert_eq!(plan.name(), &StreamName::new("a").unwrap());
    assert_eq!(plan.read_mode(), ReadMode::Full);
    assert_eq!(plan.write_mode(), WriteMode::Append);
}

#[test]
fn supported_mode_combinations_are_accepted() {
    let streams = [
        stream("a"),
        stream("b").write(WriteMode::Replace),
        stream("c").read(ReadMode::Incremental),
    ];
    let plan = PipelinePlan::new(pipeline(), streams.clone()).unwrap();
    assert_eq!(plan.pipeline(), &pipeline());
    assert_eq!(plan.streams(), streams);
}

#[test]
fn unsupported_plans_are_configuration_errors() {
    let cases = [
        (vec![], "plan_empty"),
        (vec![stream("a"), stream("a")], "plan_duplicate_stream"),
        (
            vec![
                stream("a")
                    .read(ReadMode::Incremental)
                    .write(WriteMode::Replace),
            ],
            "plan_mode_invalid",
        ),
        (
            vec![stream("a").read(ReadMode::Cdc)],
            "plan_mode_unsupported",
        ),
    ];
    for (streams, code) in cases {
        let error = PipelinePlan::new(pipeline(), streams).unwrap_err();
        assert_eq!(error.kind(), ErrorKind::Config);
        assert_eq!(error.code(), Some(code));
    }
    let duplicate = PipelinePlan::new(pipeline(), [stream("a"), stream("a")]).unwrap_err();
    assert_eq!(duplicate.stream(), Some(&StreamName::new("a").unwrap()));
}
