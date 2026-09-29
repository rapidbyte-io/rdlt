use arrow_array::cast::AsArray;
use arrow_array::types::Int64Type;

use super::rows;

#[test]
fn no_two_segments_stage_a_row_of_one_id() {
    let mut ids: Vec<i64> = (1..=9)
        .flat_map(|segment| {
            let batch = rows(segment);
            batch
                .column(0)
                .as_primitive::<Int64Type>()
                .values()
                .to_vec()
        })
        .collect();
    let staged = ids.len();
    ids.sort_unstable();
    ids.dedup();
    assert_eq!(ids.len(), staged, "{ids:?}");
}
