//! What weighing a batch costs its sender, counted in what it looks at.

use std::sync::Arc;

use arrow_array::types::Int32Type;
use arrow_array::{
    ArrayRef, DictionaryArray, Int32Array, Int64Array, ListArray, ListViewArray, RecordBatch,
    RunArray, StringViewArray,
};
use arrow_buffer::OffsetBuffer;
use arrow_schema::{DataType, Field};

use super::{cost, cut, least};
use crate::codec::tests::samples::batch_of;
use crate::error::WireError;
use crate::limits::Limits;

/// One list of `items` texts too long for a view to hold.
fn texts(items: usize) -> ArrayRef {
    let views = (0..items).map(|at| format!("a text of over twelve bytes, {at}"));
    let views = StringViewArray::from_iter_values(views);
    let field = Arc::new(Field::new("item", DataType::Utf8View, true));
    let offsets = OffsetBuffer::from_lengths([items]);
    Arc::new(ListArray::new(field, offsets, Arc::new(views), None))
}

#[test]
fn plain_columns_are_weighed_by_arithmetic_however_many_rows_they_hold() {
    // A million rows of eight integers: a frame as it is, and 977 at a peer's least limits.
    let column: ArrayRef = Arc::new(Int64Array::from(vec![1; 1_000_000]));
    let columns = (0..8).map(|at| (format!("c{at}"), Arc::clone(&column)));
    let batch = RecordBatch::try_from_iter(columns).unwrap();
    let whole = cost(&batch, Limits::default());
    assert_eq!(whole.pieces, 1);
    // Each stretch is twice the last: twenty-one of them, of eight columns.
    assert!(whole.visits <= 8 * 24, "{whole:?}");
    let pieces = cost(&batch, least());
    assert_eq!(pieces.pieces, 977);
    assert!(pieces.visits <= 977 * 8 * 16, "{pieces:?}");
}

#[test]
fn a_value_many_keys_name_is_weighed_once() {
    // Fifty thousand keys naming one list of twenty thousand texts.
    let keys = Int32Array::from(vec![0; 50_000]);
    let keyed = DictionaryArray::try_new(keys, texts(20_000)).unwrap();
    let batch = batch_of(Arc::new(keyed));
    let whole = cost(&batch, Limits::default());
    assert_eq!(whole.pieces, 1);
    // Each key once in a stretch that fits, and the value's texts once.
    assert!(whole.visits <= 50_000 + 20_000 + 100, "{whole:?}");
}

#[test]
fn a_run_of_many_rows_is_weighed_once() {
    let values = texts(30_000);
    let runs = RunArray::<Int32Type>::try_new(&Int32Array::from(vec![30_000]), &values).unwrap();
    let batch = batch_of(Arc::new(runs));
    let whole = cost(&batch, Limits::default());
    assert_eq!(whole.pieces, 1);
    // The run's value in the frame, once more for what it takes expanded, and each stretch.
    assert!(whole.visits <= 2 * 30_000 + 100, "{whole:?}");
}

#[test]
fn a_row_beyond_the_values_of_a_frame_is_weighed_no_further_than_them() {
    // One row naming three million texts, for a receiver of a million values a frame.
    let texts = StringViewArray::from_iter_values((0..3_000_000).map(|_| "t"));
    let field = Arc::new(Field::new("item", DataType::Utf8View, true));
    let (offsets, sizes) = (vec![0], vec![3_000_000]);
    let lists = ListViewArray::new(field, offsets.into(), sizes.into(), Arc::new(texts), None);
    let batch = batch_of(Arc::new(lists));
    let limits = least();
    let mut sender = crate::codec::Encoder::default();
    sender.schema(&batch.schema()).unwrap();
    let mut cutting = crate::codec::Cut::new(batch.clone(), limits);
    let refused = sender.piece(&mut cutting).unwrap_err();
    assert!(matches!(refused, WireError::Refused(_)), "{refused}");
    assert!(refused.to_string().contains("batch values"), "{refused}");
    let visits = cutting.weigher.visits();
    assert!(visits <= limits.batch_values + 16, "{visits}");
    assert_eq!(cutting.probe.compactions, 0);
    assert!(cut(&batch, &Limits::default()).is_ok());
}

#[test]
fn a_row_beyond_a_frame_is_refused_from_its_weight_before_it_is_narrowed_or_encoded() {
    // Five mebibytes in one row, for a receiver of four a frame: as bytes, as a view, and as
    // the items one list view names of a child of twice as many.
    let limits = least();
    let blob = vec![7_u8; 5 << 20];
    let text = "t".repeat(5 << 20);
    let items = Arc::new(arrow_array::Int8Array::from(vec![7; 10 << 20]));
    let field = Arc::new(Field::new("item", DataType::Int8, true));
    let lists = ListViewArray::new(field, vec![9].into(), vec![5 << 20].into(), items, None);
    let rows: [ArrayRef; 3] = [
        Arc::new(arrow_array::BinaryArray::from_iter_values([blob])),
        Arc::new(StringViewArray::from_iter_values([text])),
        Arc::new(lists),
    ];
    for row in rows {
        let batch = batch_of(row);
        let mut sender = crate::codec::Encoder::default();
        sender.schema(&batch.schema()).unwrap();
        let mut cutting = crate::codec::Cut::new(batch.clone(), limits);
        let refused = sender.piece(&mut cutting).unwrap_err();
        let WireError::Refused(refusal) = &refused else {
            panic!("{}: {refused}", batch.schema());
        };
        // The list view's row is also beyond a frame's values, which are weighed first.
        let named = ["frame bytes", "view bytes", "batch values"];
        assert!(named.contains(&refusal.field), "{refusal:?}");
        assert!(refusal.actual >= 5 << 20, "{refusal:?}");
        assert_eq!(
            (cutting.probe.compactions, sender.encodes),
            (0, 0),
            "{}",
            batch.schema()
        );
    }
}
