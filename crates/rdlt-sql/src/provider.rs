//! The context a SQL transform is planned in: one table, `input`, and only the listed
//! functions, in UTC.

use std::sync::{Arc, LazyLock};

use arrow_schema::{DataType, SchemaRef};
use datafusion_common::config::ConfigOptions;
use datafusion_common::{TableReference, plan_err};
use datafusion_expr::logical_plan::builder::LogicalTableSource;
use datafusion_expr::planner::{ContextProvider, ExprPlanner};
use datafusion_expr::{AggregateUDF, HigherOrderUDF, ScalarUDF, TableSource, WindowUDF};
use datafusion_functions::unicode::planner::UnicodeFunctionPlanner;

use crate::functions::{LISTED, listed};

#[cfg(test)]
mod tests;

/// The options every plan is made and evaluated under: identifiers as written, text as `Utf8`,
/// and time in UTC.
///
/// They are made once, since `ConfigOptions::default` reads the host's cores, which no
/// expression a transform holds uses.
pub(crate) static OPTIONS: LazyLock<Arc<ConfigOptions>> = LazyLock::new(|| {
    let mut options = ConfigOptions::default();
    options.execution.time_zone = Some("UTC".to_owned());
    options.sql_parser.enable_ident_normalization = false;
    options.sql_parser.map_string_types_to_utf8view = false;
    Arc::new(options)
});

/// The planners of what the parser reads as syntax of its own and plans as a listed function:
/// `SUBSTRING`, as `substr`.
static PLANNERS: LazyLock<[Arc<dyn ExprPlanner>; 1]> =
    LazyLock::new(|| [Arc::new(UnicodeFunctionPlanner)]);

/// The planner's view of a batch's schema as the table `input`.
pub(crate) struct Provider {
    input: Arc<dyn TableSource>,
}

impl Provider {
    pub(crate) fn new(schema: SchemaRef) -> Self {
        Self {
            input: Arc::new(LogicalTableSource::new(schema)),
        }
    }
}

impl ContextProvider for Provider {
    fn get_table_source(
        &self,
        name: TableReference,
    ) -> datafusion_common::Result<Arc<dyn TableSource>> {
        if name.schema().is_none() && name.table() == "input" {
            return Ok(Arc::clone(&self.input));
        }
        plan_err!("a SQL transform reads input alone")
    }

    fn get_expr_planners(&self) -> &[Arc<dyn ExprPlanner>] {
        PLANNERS.as_slice()
    }

    fn get_function_meta(&self, name: &str) -> Option<Arc<ScalarUDF>> {
        listed(&name.to_ascii_lowercase()).map(|listed| (listed.udf)())
    }

    fn get_higher_order_meta(&self, _name: &str) -> Option<Arc<HigherOrderUDF>> {
        None
    }

    fn get_aggregate_meta(&self, _name: &str) -> Option<Arc<AggregateUDF>> {
        None
    }

    fn get_window_meta(&self, _name: &str) -> Option<Arc<WindowUDF>> {
        None
    }

    fn get_variable_type(&self, _variable_names: &[String]) -> Option<DataType> {
        None
    }

    fn options(&self) -> &ConfigOptions {
        &OPTIONS
    }

    fn udf_names(&self) -> Vec<String> {
        LISTED.iter().map(|listed| listed.name.to_owned()).collect()
    }

    fn higher_order_function_names(&self) -> Vec<String> {
        Vec::new()
    }

    fn udaf_names(&self) -> Vec<String> {
        Vec::new()
    }

    fn udwf_names(&self) -> Vec<String> {
        Vec::new()
    }
}
