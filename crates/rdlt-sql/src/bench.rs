//! Internals the fuzz targets drive; not part of rdlt-sql's API.

#[cfg(test)]
mod tests;

use std::sync::Arc;

use arrow_schema::Schema;
use datafusion_common::tree_node::{TreeNode as _, TreeNodeRecursion};
use datafusion_expr::{Expr, LogicalPlan};
use rdlt_engine::{Transform as _, TransformError};

use crate::planned::analyzed;
use crate::{Sql, stack};

/// What an accepted query's plan holds: its nodes, each by its first word as DataFusion shows
/// it, its subqueries' among them, and the scalar functions its expressions call.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Facts {
    /// The plan's nodes, outermost first, and each expression of a kind in [`FOREIGN`] by the
    /// name DataFusion gives its kind.
    pub nodes: Vec<String>,
    /// The scalar functions called, in the order met.
    pub functions: Vec<String>,
}

/// The kinds of expression no accepted plan holds, as DataFusion names them: subqueries,
/// unnesting, grouping sets, and calls other than scalar ones.
pub const FOREIGN: [&str; 9] = [
    "Exists",
    "InSubquery",
    "ScalarSubquery",
    "SetComparison",
    "Unnest",
    "GroupingSet",
    "AggregateFunction",
    "WindowFunction",
    "HigherOrderFunction",
];

/// What `sql`, planned for batches of `input`, holds, where `output` accepts it, walked on the
/// stack its planning takes.
///
/// # Errors
///
/// The refusal `output` gives.
///
/// # Panics
///
/// Panics where walking the plan fails, which it never does.
pub fn facts(sql: &Sql, input: &Schema) -> Result<Facts, TransformError> {
    sql.output(input)?;
    stack::grown(|| {
        let plan = analyzed(sql.statement(), &Arc::new(input.clone()))?;
        Ok(walked(&plan))
    })
}

/// The nodes `plan` and its subqueries hold, and the calls their expressions make.
fn walked(plan: &LogicalPlan) -> Facts {
    let mut facts = Facts::default();
    plan.apply_with_subqueries(|node| {
        let shown = node.display().to_string();
        let first = shown.split([':', ' ']).next().unwrap_or_default();
        facts.nodes.push(first.to_owned());
        node.apply_expressions(|expr| {
            expr.apply(|inner| {
                seen(inner, &mut facts);
                Ok(TreeNodeRecursion::Continue)
            })
        })
    })
    .expect("walking a plan fails nowhere");
    facts
}

/// Records in `facts` the scalar function `expr` calls, or its kind where it is foreign.
fn seen(expr: &Expr, facts: &mut Facts) {
    if let Expr::ScalarFunction(call) = expr {
        facts.functions.push(call.name().to_owned());
    }
    let kind = expr.variant_name();
    if FOREIGN.contains(&kind) {
        facts.nodes.push(kind.to_owned());
    }
}
