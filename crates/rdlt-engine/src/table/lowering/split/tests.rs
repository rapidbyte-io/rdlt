use std::error::Error as _;

use arrow_array::{BooleanArray, StringArray};
use arrow_schema::ArrowError;
use rdlt_connector::LogicalType;

use super::Fitted;
use crate::shred::ShredError;

#[test]
fn a_value_its_own_column_cannot_read_fails_with_the_shredder_s_error_as_its_cause() {
    let fitted = Fitted(vec![(
        0,
        StringArray::from(vec!["{"]),
        BooleanArray::from(vec![true]),
    )]);
    let error = fitted
        .own(0, &LogicalType::Int64)
        .expect_err("the text is no JSON");
    assert!(matches!(error, ArrowError::ExternalError(_)), "{error:?}");
    let cause = error
        .source()
        .and_then(|cause| cause.downcast_ref::<ShredError>());
    assert!(matches!(cause, Some(ShredError::Invalid(_))), "{error:?}");
}
