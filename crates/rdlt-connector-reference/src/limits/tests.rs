use std::time::Duration;

use super::{
    BUSY_WAIT, CATALOG_BYTES, CHUNK_BYTES, COMPACT_BYTES, FILE_BYTES, FOLD_CELLS, JOURNAL_BYTES,
    KEEPER_BYTES, KEEPER_POSITIONS, KEPT_VERSIONS, LINE_BYTES, LOCK_WAIT, MANIFEST_BYTES,
    MAX_CHANGES, MAX_MESSAGE_ROWS, MAX_PARTITIONS, MAX_PER_SECOND, MAX_SHAPES, MAX_SNAPSHOT_KEYS,
    MAX_TRUNCATES, OWNER_BYTES, PUBLISH_ATTEMPTS, READ_BATCH_ROWS, RUN_ABSENT_CELLS, RUN_ROWS,
    TABLE_NAME_BYTES, TEMPORARY_AGE, TREE_DEPTH,
};

#[test]
fn the_limits_are_those_documented() {
    assert_eq!(FILE_BYTES, 17_179_869_184);
    assert_eq!(LINE_BYTES, 33_554_432);
    assert_eq!(MANIFEST_BYTES, 134_217_728);
    assert_eq!(CATALOG_BYTES, 16_777_216);
    assert_eq!(OWNER_BYTES, 128);
    assert_eq!(TABLE_NAME_BYTES, 128);
    assert_eq!(KEPT_VERSIONS, 8);
    assert_eq!(PUBLISH_ATTEMPTS, 64);
    assert_eq!(LOCK_WAIT, Duration::from_secs(30));
    assert_eq!(TEMPORARY_AGE, Duration::from_secs(3_600));
    assert_eq!(CHUNK_BYTES, 8_388_608);
    assert_eq!(READ_BATCH_ROWS, 1_024);
    assert_eq!(COMPACT_BYTES, 67_108_864);
    assert_eq!(TREE_DEPTH, 32);
    assert_eq!(KEEPER_BYTES, 4_194_304);
    assert_eq!(KEEPER_POSITIONS, 4_096);
    assert_eq!(BUSY_WAIT, Duration::from_secs(30));
    assert_eq!(JOURNAL_BYTES, 67_108_864);
    assert_eq!(MAX_PARTITIONS, 1_024);
    assert_eq!(MAX_MESSAGE_ROWS, 100_000);
    assert_eq!(MAX_PER_SECOND, 1_000_000_000);
    assert_eq!(MAX_SNAPSHOT_KEYS, 1_000_000);
    assert_eq!(MAX_CHANGES, 9_223_372_036_854_775_807);
    assert_eq!(MAX_TRUNCATES, 1_024);
    assert_eq!(MAX_SHAPES, 16);
    assert_eq!(FOLD_CELLS, 1_048_576);
    assert_eq!(RUN_ROWS, 256);
    assert_eq!(RUN_ABSENT_CELLS, 65_536);
}
