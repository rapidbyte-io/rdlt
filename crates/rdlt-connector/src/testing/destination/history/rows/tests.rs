use arrow_array::Array as _;

use super::{Kind, delete, stored, truncate, upsert, written};
use crate::meta::{DELETED_AT_COLUMN, ROW_HASH_COLUMN};

#[test]
fn deletes_and_truncates_carry_no_hash_as_the_engine_writes_them() {
    let rows = [upsert(1, "a", 1, 10), delete(1, 2, 20), truncate(3, 30)];
    for soft in [false, true] {
        let batch = written(&rows, Kind::Changes { soft });
        let hashes = batch
            .column_by_name(ROW_HASH_COLUMN)
            .expect("history rows carry a hash column");
        let carried: Vec<bool> = (0..hashes.len()).map(|row| hashes.is_valid(row)).collect();
        assert_eq!(carried, [true, false, false], "soft deletes: {soft}");
    }
}

#[test]
fn only_a_history_of_soft_deletes_writes_and_keeps_when_a_row_was_deleted() {
    let rows = [upsert(1, "a", 1, 10), delete(1, 2, 20)];
    for (kind, soft) in [
        (Kind::Plain, false),
        (Kind::Changes { soft: false }, false),
        (Kind::Changes { soft: true }, true),
    ] {
        let batch = written(&rows, kind);
        let written = batch.column_by_name(DELETED_AT_COLUMN).is_some();
        let kept = stored(kind).field(DELETED_AT_COLUMN).is_some();
        assert_eq!((written, kept), (soft, soft), "soft deletes: {soft}");
    }
}
