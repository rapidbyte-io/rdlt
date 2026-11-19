use arrow_schema::{DataType, Field, Schema};
use datafusion_common::{DFSchema, ScalarValue};
use datafusion_expr::{Expr, cast, col, lit, when};
use rdlt_engine::Bound;

use super::{parted, text};
use crate::functions::ARRAY;

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

#[test]
fn a_case_holds_ten_arrays_and_two_copies_in_three_arrays_of_each_column_it_reads() {
    let fields = vec![
        Field::new("t", DataType::Utf8, true),
        Field::new("n", DataType::Int64, false),
    ];
    let schema = DFSchema::try_from(Schema::new(fields)).unwrap();
    let case = when(col("n").not_eq(lit(1_i64)), col("t")).end().unwrap();
    // Its places, selections and answers: ten arrays and 32 bytes a row; and for `t` and `n`,
    // three arrays each and two copies of each byte.
    let held = Bound::new(16 * ARRAY)
        .with_rows(32)
        .with_column(0, 2)
        .with_column(1, 2);
    assert_eq!(parted(&case, &schema), held);
}
