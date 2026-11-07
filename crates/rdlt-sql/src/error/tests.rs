use rdlt_engine::codes;

use super::{SqlError, SqlErrorKind};

#[test]
fn every_refusal_of_a_query_is_coded_transform_invalid() {
    let refused = SqlError::new(SqlErrorKind::Shape, "a join is not a projection of input");
    assert_eq!(
        (refused.kind(), refused.code()),
        (SqlErrorKind::Shape, codes::TRANSFORM_INVALID)
    );
    assert_eq!(refused.to_string(), "a join is not a projection of input");
}
