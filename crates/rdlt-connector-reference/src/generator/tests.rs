use rdlt_connector::{Partition, PartitionId};

use super::{Generated, GeneratedStream, mix};

#[test]
fn rows_draw_from_the_splitmix64_output_function() {
    // The first two outputs of SplitMix64 seeded with 0, as its reference publishes them.
    assert_eq!(mix(0), 0xE220_A839_7B1D_CDAF);
    assert_eq!(mix(0x9E37_79B9_7F4A_7C15), 0x6E78_9E6A_A1B9_65F4);
}

fn stream(json: serde_json::Value) -> GeneratedStream {
    serde_json::from_value(json).expect("a valid stream")
}

#[test]
fn a_stream_is_one_partition_read_a_hundred_rows_a_batch_by_default() {
    let stream = stream(serde_json::json!({ "name": "rows", "rows": 1 }));
    assert_eq!((stream.partitions, stream.batch_rows), (1, 100));
}

#[test]
fn a_partition_is_an_index_below_the_stream_s_partitions() {
    let generated = Generated(stream(
        serde_json::json!({ "name": "rows", "rows": 1, "partitions": 2 }),
    ));
    let index =
        |id: &str| generated.partition_index(&Partition::new(PartitionId::parse(id).unwrap()));
    assert_eq!(index("1").expect("a partition"), 1);
    assert!(index("2").is_err());
}
