use std::sync::Arc;

use arrow_array::{
    ArrayRef, BinaryArray, FixedSizeBinaryArray, Int8Array, Int64Array, RecordBatch, StringArray,
};
use rdlt_connector::{ChangeOp, OP_COLUMN, SEQ_COLUMN, UNCHANGED_COLUMN};
use rdlt_engine::bench::{Transformed, Untransformed};
use rdlt_engine::{DeleteMode, ErrorKind, WriteMode, codes};

use super::plan;
use crate::changes::{ChangeWorkload, Filter, Filtered, Value};
use crate::rng::SplitMix64;
use crate::swarm::Features;

/// An update of key 2 setting counter 4 and leaving `value` unchanged, as the change source
/// sends one.
fn partial_update() -> RecordBatch {
    let seq = FixedSizeBinaryArray::try_from_iter([[0_u8; 16]].into_iter()).unwrap();
    let unchanged: &[u8] = &[0b0000_0010];
    RecordBatch::try_from_iter([
        ("id", Arc::new(Int64Array::from(vec![2])) as ArrayRef),
        ("value", Arc::new(StringArray::from(vec![None::<&str>]))),
        ("n", Arc::new(Int64Array::from(vec![4]))),
        (
            OP_COLUMN,
            Arc::new(Int8Array::from(vec![ChangeOp::Update.code()])),
        ),
        (SEQ_COLUMN, Arc::new(seq)),
        (
            UNCHANGED_COLUMN,
            Arc::new(BinaryArray::from(vec![Some(unchanged)])),
        ),
    ])
    .unwrap()
}

#[test]
fn a_counter_filter_on_a_merge_refuses_a_partial_update_it_keeps_and_a_key_filter_takes_it() {
    // A row the counter's filter dropped as a delete could come back through such an update
    // without its value, so the plan the simulation never draws is refused, not lossy.
    for (reads, refused) in [(Filtered::Counter, true), (Filtered::Key, false)] {
        let workload = ChangeWorkload::generate(
            &mut SplitMix64::new(0),
            Features::ALL,
            &mut SplitMix64::new(!0),
        );
        let mut stream = workload.streams[0].clone();
        stream.write = WriteMode::Merge;
        stream.history = false;
        stream.deletes = DeleteMode::Hard;
        stream.value = Value::Loaded;
        stream.filter = Some(Filter {
            reads,
            modulus: 2,
            cased: false,
        });
        let plan = plan(&[stream]);
        let transformed =
            Transformed::try_new(&plan.streams()[0], &["id"], partial_update()).unwrap();
        match (transformed.batches(), refused) {
            (Err(Untransformed::Refused(error)), true) => assert_eq!(
                (error.kind(), error.code()),
                (ErrorKind::Transform, Some(codes::TRANSFORM_PARTIAL))
            ),
            (Ok(batches), false) => {
                let rows: usize = batches.iter().map(RecordBatch::num_rows).sum();
                assert_eq!(rows, 1);
            }
            (outcome, _) => panic!("{reads:?}: {outcome:?}"),
        }
    }
}
