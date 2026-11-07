use std::sync::Arc;

use arrow_schema::{DataType, Field, Schema};
use datafusion_expr::expr::{GroupingSet, Unnest};
use datafusion_expr::{Expr, LogicalPlanBuilder, col, exists, in_subquery, lit, scalar_subquery};
use datafusion_sql::sqlparser::dialect::GenericDialect;
use datafusion_sql::sqlparser::parser::Parser;

use super::{FOREIGN, Facts, seen, walked};
use crate::planned::analyzed;

#[test]
fn a_walk_meets_the_nodes_and_calls_of_a_plan_s_subqueries() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, true),
        Field::new("email", DataType::Utf8, true),
    ]));
    let text = "SELECT id FROM input WHERE id IN (SELECT id FROM input WHERE lower(email) = 'a')";
    let statement = Parser::parse_sql(&GenericDialect {}, text)
        .unwrap()
        .remove(0);
    let facts = walked(&analyzed(&statement, &schema).unwrap());
    // The subquery's plan, its scan among its nodes, and the expression holding it.
    let count = |name: &str| facts.nodes.iter().filter(|node| *node == name).count();
    assert_eq!(
        (count("Subquery"), count("TableScan"), count("InSubquery")),
        (1, 2, 1),
        "{facts:?}"
    );
    assert_eq!(facts.functions, ["lower"]);
}

#[test]
fn an_expression_of_a_foreign_kind_is_seen_as_a_node_by_its_kind() {
    let subquery = Arc::new(LogicalPlanBuilder::empty(false).build().unwrap());
    let expressions = [
        exists(Arc::clone(&subquery)),
        in_subquery(col("id"), Arc::clone(&subquery)),
        scalar_subquery(subquery),
        Expr::Unnest(Unnest::new(col("list"))),
        Expr::GroupingSet(GroupingSet::Rollup(vec![col("id")])),
    ];
    for expression in &expressions {
        let mut facts = Facts::default();
        seen(expression, &mut facts);
        let kind = expression.variant_name();
        assert!(FOREIGN.contains(&kind), "{kind}");
        assert_eq!(facts.nodes, [kind]);
        assert!(facts.functions.is_empty());
    }
    let mut facts = Facts::default();
    seen(&col("id").eq(lit(1)), &mut facts);
    assert_eq!(facts, Facts::default());
}
