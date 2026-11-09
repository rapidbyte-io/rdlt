use std::sync::Arc;

use arrow_array::{ArrayRef, Int64Array, RecordBatch};
use arrow_schema::{Schema, SchemaRef};

use super::{Transform, TransformError};

/// A transform that returns its input.
struct Identity;

impl Transform for Identity {
    fn name(&self) -> &'static str {
        "identity"
    }

    fn output(&self, input: &Schema) -> Result<SchemaRef, TransformError> {
        Ok(Arc::new(input.clone()))
    }

    fn apply(&self, batch: RecordBatch) -> Result<RecordBatch, TransformError> {
        Ok(batch)
    }
}

#[test]
fn a_transform_by_default_filters_nothing_keeps_every_row_and_declares_nothing_it_reads() {
    let ids: ArrayRef = Arc::new(Int64Array::from(vec![1, 2, 3]));
    let batch = RecordBatch::try_from_iter([("id", ids)]).unwrap();
    assert!(!Identity.filters());
    let keep = Identity.keep(&batch).unwrap();
    assert_eq!((keep.len(), keep.true_count()), (3, 3));
    assert_eq!(Identity.bound(batch.schema_ref()), None);
    assert_eq!(Identity.lineage(batch.schema_ref()), None);
    assert_eq!(Identity.keep_reads(batch.schema_ref()), None);
}

#[test]
fn a_transform_error_names_its_column_beside_its_message() {
    let error = TransformError::new("cannot hash a number").with_column("amount");
    assert_eq!(error.message(), "cannot hash a number");
    assert_eq!(error.column(), Some("amount"));
    assert_eq!(TransformError::new("refused").column(), None);
}

#[test]
fn a_transform_error_keeps_its_cause_as_text_and_compares_by_it() {
    let error = TransformError::new("failed").with_cause("what it came from");
    let cause = std::error::Error::source(&error).expect("its cause");
    assert_eq!(cause.to_string(), "what it came from");
    assert_eq!(error, error.clone());
    assert_ne!(error, TransformError::new("failed"));
    assert!(std::error::Error::source(&TransformError::new("failed")).is_none());
}

#[test]
fn steps_compare_ops_by_value_and_code_by_identity() {
    use super::{Op, TransformStep};
    let code: Arc<dyn Transform> = Arc::new(Identity);
    assert_eq!(
        TransformStep::from(Op::rename([("a", "b")])),
        TransformStep::from(Op::rename([("a", "b")]))
    );
    assert_ne!(
        TransformStep::from(Op::rename([("a", "b")])),
        TransformStep::from(Op::rename([("a", "c")]))
    );
    assert_eq!(
        TransformStep::from(Arc::clone(&code)),
        TransformStep::from(Arc::clone(&code))
    );
    let other: Arc<dyn Transform> = Arc::new(Identity);
    assert_ne!(TransformStep::from(code), TransformStep::from(other));
}

#[test]
fn a_code_step_shows_as_its_name() {
    let code: Arc<dyn Transform> = Arc::new(Identity);
    assert_eq!(
        format!("{:?}", super::TransformStep::from(code)),
        "Code(\"identity\")"
    );
}

#[test]
fn a_transform_error_reads_as_its_message_beside_its_column() {
    assert_eq!(TransformError::new("refused").to_string(), "refused");
    assert_eq!(
        TransformError::new("refused").with_column("id").to_string(),
        "column id: refused"
    );
}
