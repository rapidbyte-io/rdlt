use crate::config::EngineConfig;
use crate::error::codes;

mod keys {
    use rdlt_connector::{ColumnPath, StreamName, StreamSpec};

    use super::super::streams::merge_key;
    use crate::error::codes;
    use crate::plan::{StreamPlan, WriteMode};

    fn spec(key: Option<ColumnPath>) -> StreamSpec {
        let spec = StreamSpec::new(StreamName::new("s").unwrap());
        match key {
            Some(column) => spec.with_primary_key([column]),
            None => spec,
        }
    }

    fn merging() -> StreamPlan {
        StreamPlan::new(StreamName::new("s").unwrap()).write(WriteMode::Merge)
    }

    #[test]
    fn a_merge_key_comes_from_the_plan_then_the_catalog() {
        let catalog = spec(Some(ColumnPath::from("id")));
        assert_eq!(
            merge_key(&merging(), &catalog).unwrap(),
            [ColumnPath::from("id")]
        );
        let planned = merging().key(["code"]);
        assert_eq!(
            merge_key(&planned, &catalog).unwrap(),
            [ColumnPath::from("code")]
        );
        let appending = StreamPlan::new(StreamName::new("s").unwrap());
        assert!(merge_key(&appending, &catalog).unwrap().is_empty());
    }

    #[test]
    fn a_missing_or_nested_merge_key_is_refused() {
        let missing = merge_key(&merging(), &spec(None)).unwrap_err();
        assert_eq!(missing.code(), Some(codes::MERGE_KEY_MISSING));
        let nested = spec(Some(ColumnPath::new(["a", "b"]).unwrap()));
        let error = merge_key(&merging(), &nested).unwrap_err();
        assert_eq!(error.code(), Some(codes::PLAN_COLUMN_NESTED));
    }
}

#[test]
fn an_attempt_reads_as_many_partitions_at_once_as_a_plan_may_name_and_no_more() {
    use rdlt_connector::limits::MAX_PLAN_PARTITIONS;
    assert!(super::within_partition_limit(MAX_PLAN_PARTITIONS).is_ok());
    let refused = super::within_partition_limit(MAX_PLAN_PARTITIONS + 1).unwrap_err();
    assert_eq!(refused.code(), Some(codes::PLAN_INVALID));
}

#[test]
fn the_least_memory_s_share_for_logs_holds_a_carry_s_read() {
    for partitions in [1, 16, 17, 600_000] {
        let least = EngineConfig::least_memory(partitions).expect("a least memory");
        let shares = crate::budget::Shares::of(least);
        assert!(
            shares.log >= crate::limits::LOG_COPY_BYTES,
            "{partitions} partitions: {shares:?}"
        );
    }
}
