use arrow_array::Array as _;

use super::{Kind, delete, truncate, upsert, written};
use crate::meta::ROW_HASH_COLUMN;

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
