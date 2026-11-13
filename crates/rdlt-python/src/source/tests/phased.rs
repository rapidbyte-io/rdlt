//! A Python stream read in phases.

use rdlt_connector::{ConnectContext, Partition, PartitionId, StreamName, StreamState};

use super::factory;
use crate::embedded;
use crate::runtime;

/// A stream `rows` read as changes, which plans with the Python function `plan` and reads with
/// `read`, both defined before it.
const ROWS: &str = "\
stream = types.SimpleNamespace(read=read, plan=plan, spec=json.dumps({
    'spec': {'name': 'rows', 'schema': None, 'primary_key': ['id'], 'read_modes': ['cdc'],
             'checkpointing': 'natural', 'replayable': True, 'change_time': None},
    'planned': True, 'stated': False, 'committed': False, 'acknowledged': False,
    'cursor_version': 1}))
streams = [stream]";

#[test]
fn a_plan_reaches_the_engine_with_its_phase_its_partitions_and_where_they_start() {
    embedded::with(|py| {
        let body = format!(
            "\
def read(partition, cursor, context):
    yield from ()
def plan(phase, positions, generation, completed):
    return (1, [('changes', True)], [('changes', json.dumps({{'next': 101}}))])
{ROWS}"
        );
        let source_factory = factory(py, &body).factory();
        let runtime = runtime::get(py).unwrap();
        let planned = py.detach(|| {
            runtime.block_on(async {
                let source = source_factory
                    .connect(serde_json::json!({}), ConnectContext::new())
                    .await
                    .expect("the source connects");
                let rows = StreamName::new("rows").unwrap();
                source.plan(&rows, &StreamState::default()).await
            })
        });
        let planned = planned.expect("the stream plans");
        let changes = PartitionId::parse("changes").unwrap();
        assert_eq!(planned.phase, Some(1));
        assert_eq!(
            planned.partitions,
            [Partition::new(changes.clone()).unbounded()]
        );
        let start: serde_json::Value = planned.starts[&changes].decode(1).unwrap();
        assert_eq!(start, serde_json::json!({ "next": 101 }));
    });
}
