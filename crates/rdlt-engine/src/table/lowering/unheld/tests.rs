use std::sync::Arc;

use arrow_array::cast::AsArray;
use arrow_array::types::Int64Type;
use arrow_array::{ArrayRef, BooleanArray, Int32Array, Int64Array, RecordBatch};

use super::Converted;

#[test]
fn a_column_that_converted_to_itself_is_taken_from_the_kept_rows_and_another_is_filtered() {
    let same: ArrayRef = Arc::new(Int64Array::from(vec![1, 2, 3]));
    let narrow: ArrayRef = Arc::new(Int32Array::from(vec![4, 5, 6]));
    let unconverted: ArrayRef = Arc::new(Int64Array::from(vec![7, 8, 9]));
    let whole = RecordBatch::try_from_iter([
        ("same", Arc::clone(&same)),
        ("narrow", narrow),
        ("unconverted", unconverted),
    ])
    .unwrap();
    let keep = BooleanArray::from(vec![true, false, true]);
    let kept = arrow_select::filter::filter_record_batch(&whole, &keep).unwrap();
    let widened: ArrayRef = Arc::new(Int64Array::from(vec![4, 5, 6]));
    let mut converted = Converted(vec![Some(same), Some(widened), None]);
    converted.filter(&whole, &kept, &keep).unwrap();
    assert!(Arc::ptr_eq(converted.get(0).unwrap(), kept.column(0)));
    let filtered = converted.get(1).unwrap().as_primitive::<Int64Type>();
    assert_eq!(filtered.values().to_vec(), [4, 6]);
    assert!(converted.get(2).is_none());
}
