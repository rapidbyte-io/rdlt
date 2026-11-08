use std::sync::Arc;

use arrow_schema::{DataType, Field, Schema};
use datafusion_common::TableReference;
use datafusion_expr::planner::ContextProvider as _;

use super::Provider;

#[test]
fn the_table_input_alone_is_found() {
    let schema = Arc::new(Schema::new(vec![Field::new("n", DataType::Int64, true)]));
    let provider = Provider::new(schema);
    assert!(
        provider
            .get_table_source(TableReference::bare("input"))
            .is_ok()
    );
    for other in [
        TableReference::bare("other"),
        TableReference::bare("Input"),
        TableReference::partial("public", "input"),
        TableReference::full("catalog", "public", "input"),
    ] {
        assert!(provider.get_table_source(other.clone()).is_err(), "{other}");
    }
}
