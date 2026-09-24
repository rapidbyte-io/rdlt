use std::num::NonZeroUsize;

use arrow_array::RecordBatch;
use bytes::Bytes;

use super::{Refused, shred, shred_on};
use crate::compute::RayonPool;

#[test]
fn shredding_inline_and_on_a_pool_gives_the_same_batches() {
    let pushes = [
        Bytes::from_static(b"{\"a\":1}\n{\"a\":2}"),
        Bytes::from_static(b"[{\"a\":3}]"),
    ];
    let inline = shred(&pushes, 8).unwrap();
    let pool = RayonPool::new(NonZeroUsize::new(2).unwrap()).unwrap();
    let runtime = tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap();
    let pooled = runtime.block_on(shred_on(&pool, &pushes, 8)).unwrap();
    assert_eq!(inline, pooled);
    assert_eq!(inline.iter().map(RecordBatch::num_rows).sum::<usize>(), 3);
}

#[test]
fn a_refusal_carries_the_errors_code_and_message() {
    let refused = shred(&[Bytes::from_static(b"[1]")], 8).unwrap_err();
    assert_eq!(
        refused,
        Refused {
            code: "json_not_object",
            message: "a record is not a JSON object".to_owned(),
        }
    );
    assert_eq!(
        shred(&[Bytes::from_static(b"[")], 8).unwrap_err().code,
        "json_invalid"
    );
}
