use std::sync::Arc;

use arrow_array::types::Int32Type;
use arrow_array::{
    Array, ArrayRef, DictionaryArray, Int8Array, Int32Array, ListArray, NullArray, RecordBatch,
    StringArray, StructArray,
};
use arrow_buffer::OffsetBuffer;
use arrow_schema::{Field, Fields};
use rdlt_connector::{ConnectorErrorKind, Result};
use rdlt_wire::Limits;
use rdlt_wire::limits::{BATCH_ROWS, FRAME_BYTES};

use super::chunks;
use crate::files::FileFormat;
use crate::rooted::Dir;

/// The rows one batch holds, as a count.
fn most() -> usize {
    usize::try_from(BATCH_ROWS).unwrap()
}

fn single(column: ArrayRef) -> RecordBatch {
    RecordBatch::try_from_iter_with_nullable([("c", column, true)]).unwrap()
}

/// Writes `batch` as an Arrow file and reads it back.
///
/// What is written reads back, row for row, in batches a reader accepts; what is refused is
/// refused for a limit, and leaves no file.
fn written(batch: &RecordBatch) -> Result<Vec<RecordBatch>> {
    let root = tempfile::tempdir().unwrap();
    let dir = Dir::ambient(root.path()).unwrap();
    let outcome = FileFormat::Arrow.write(&dir, "rows.arrow", std::slice::from_ref(batch));
    match outcome {
        Ok(written) => {
            assert_eq!(written.rows, u64::try_from(batch.num_rows()).unwrap());
            let read = FileFormat::Arrow
                .read(&dir, "rows.arrow", batch.schema_ref())
                .expect("what was written reads back");
            let rows: usize = read.iter().map(RecordBatch::num_rows).sum();
            assert_eq!(rows, batch.num_rows());
            assert!(read.iter().all(|read| read.num_rows() <= most()));
            let joined = arrow_select::concat::concat_batches(batch.schema_ref(), &read).unwrap();
            assert_eq!(joined.column(0).to_data(), batch.column(0).to_data());
            Ok(read)
        }
        Err(error) => {
            assert_eq!(error.kind(), ConnectorErrorKind::Data, "{error}");
            assert_eq!(error.code(), Some("limit_exceeded"), "{error}");
            let left = std::fs::read_dir(root.path()).unwrap().count();
            assert_eq!(left, 0, "a refused file was left");
            Err(error)
        }
    }
}

/// The name of the limit a write of `batch` is refused for.
fn refused(batch: &RecordBatch) -> &'static str {
    let error = written(batch).expect_err("no reader accepts the batch");
    error.limit().expect("a limit").name
}

#[test]
fn rows_however_narrow_are_written_as_batches_of_at_most_the_rows_a_reader_accepts() {
    for (rows, batches) in [(most(), 1), (most() + 1, 2), (3 * most(), 3)] {
        let batch = single(Arc::new(Int8Array::from(vec![1_i8; rows])));
        let read = written(&batch).expect("narrow rows are written");
        assert_eq!(read.len(), batches, "{rows}");
    }
    // Rows of no bytes at all, too.
    let read = written(&single(Arc::new(NullArray::new(most() + 1)))).unwrap();
    assert_eq!(read.len(), 2);
}

#[test]
fn a_batch_is_cut_by_rows_and_by_bytes_whichever_is_less() {
    let limits = Limits::default();
    let narrow = single(Arc::new(Int8Array::from(vec![1_i8; 2 * most() + 5])));
    let rows: Vec<usize> = chunks(&narrow, &limits)
        .map(|chunk| chunk.num_rows())
        .collect();
    assert_eq!(rows, [most(), most(), 5]);
    let limits = Limits {
        batch_rows: 3,
        ..limits
    };
    let rows: Vec<usize> = chunks(&narrow.slice(0, 7), &limits)
        .map(|chunk| chunk.num_rows())
        .collect();
    assert_eq!(rows, [3, 3, 1]);
    assert_eq!(chunks(&narrow.slice(0, 0), &limits).count(), 0);
}

/// A dictionary of `values` distinct values, each used once.
fn dictionary(values: usize) -> ArrayRef {
    let count = i32::try_from(values).unwrap();
    let keys = Int32Array::from_iter_values(0..count);
    let values = Int32Array::from_iter_values(0..count);
    Arc::new(DictionaryArray::<Int32Type>::try_new(keys, Arc::new(values)).unwrap())
}

#[test]
fn a_dictionary_of_more_values_than_a_batch_holds_rows_is_refused_unwritten() {
    // A reader takes a dictionary as a batch of its own, so its values are bounded as rows are.
    written(&single(dictionary(most()))).expect("a dictionary a reader accepts");
    assert_eq!(refused(&single(dictionary(most() + 1))), "batch rows");
    // Nested in a struct, the same.
    let field = Field::new("d", dictionary(1).data_type().clone(), true);
    let nested = |values| {
        let fields = Fields::from(vec![field.clone()]);
        Arc::new(StructArray::new(fields, vec![dictionary(values)], None)) as ArrayRef
    };
    written(&single(nested(most()))).expect("a dictionary a reader accepts");
    assert_eq!(refused(&single(nested(most() + 1))), "batch rows");
}

/// One row holding the list `items`.
fn listed(items: ArrayRef) -> ArrayRef {
    let field = Arc::new(Field::new("item", items.data_type().clone(), true));
    let offsets = OffsetBuffer::from_lengths([items.len()]);
    Arc::new(ListArray::new(field, offsets, items, None))
}

#[test]
fn values_beyond_a_batch_s_rows_are_written_only_where_their_bytes_bound_them() {
    // A reader bounds the values of a nested column by the rows of a batch, or by the bits of
    // the batch's body where those are more: values of no bytes are bounded by the rows alone.
    written(&single(listed(Arc::new(NullArray::new(most()))))).expect("as many as rows");
    let nulls = single(listed(Arc::new(NullArray::new(most() + 1))));
    assert_eq!(refused(&nulls), "values per node");
    let empty = Arc::new(StructArray::new_empty_fields(most() + 1, None));
    assert_eq!(refused(&single(listed(empty))), "values per node");
    // Values that take bytes are bounded by the frame they are written in.
    let bytes = single(listed(Arc::new(Int8Array::from(vec![7_i8; most() + 1]))));
    written(&bytes).expect("values of a byte each");
}

#[test]
fn a_row_larger_than_a_frame_is_refused_unwritten() {
    let frame = usize::try_from(FRAME_BYTES).unwrap();
    let text = "x".repeat(frame + 1);
    let batch = single(Arc::new(StringArray::from(vec![text.as_str()])));
    assert_eq!(refused(&batch), "frame bytes");
}
