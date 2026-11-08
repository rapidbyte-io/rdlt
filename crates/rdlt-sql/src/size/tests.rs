use arrow_schema::DataType;
use datafusion_common::ScalarValue;
use datafusion_expr::{Expr, cast, lit};

use super::text;

#[test]
fn a_text_literal_is_read_through_an_alias_and_a_cast_to_text_alone() {
    let literal = || lit(ScalarValue::Utf8(Some("ab".to_owned())));
    assert_eq!(text(&literal()), Some("ab"));
    assert_eq!(text(&literal().alias("x")), Some("ab"));
    assert_eq!(text(&cast(literal(), DataType::LargeUtf8)), Some("ab"));
    assert_eq!(text(&cast(literal(), DataType::Utf8View)), Some("ab"));
    assert_eq!(text(&cast(literal(), DataType::Int32)), None);
    assert_eq!(text(&lit(7_i64)), None);
    assert_eq!(text(&Expr::Literal(ScalarValue::Utf8(None), None)), None);
}
