//! How deep a parsed query's expressions nest, measured on the tree the parser built.
//!
//! The parser builds a chain such as `a + b + c` in a loop, into a tree as deep as the chain is
//! long, which its recursion limit does not bound. The measure counts expressions alone: a
//! query's types nest too, and shape admits none but flat scalars, while the text's size bounds
//! both on the stack the work on the text runs on, a refused tree's drop among it. Walking a
//! tree grows the stack as it goes, but cloning and evaluating one do not: a query is measured
//! and refused on its tree before either meets it.

#[cfg(test)]
mod tests;

use std::convert::Infallible;
use std::ops::ControlFlow;

use datafusion_sql::sqlparser::ast::{Expr, Statement, Visit, Visitor};

/// The most expressions any expression of `statement` lies within: a name or a literal alone is
/// no level deep, `lower(t)` one, and a chain of operators as deep as it has operators.
pub(crate) fn depth(statement: &Statement) -> usize {
    let mut measured = Measured::default();
    let ControlFlow::Continue(()) = Visit::visit(statement, &mut measured);
    measured.deepest
}

/// How many expressions a walk is within, and the most it has been.
#[derive(Default)]
struct Measured {
    within: usize,
    deepest: usize,
}

impl Visitor for Measured {
    type Break = Infallible;

    fn pre_visit_expr(&mut self, _expr: &Expr) -> ControlFlow<Infallible> {
        self.deepest = self.deepest.max(self.within);
        self.within = self.within.saturating_add(1);
        ControlFlow::Continue(())
    }

    fn post_visit_expr(&mut self, _expr: &Expr) -> ControlFlow<Infallible> {
        self.within = self.within.saturating_sub(1);
        ControlFlow::Continue(())
    }
}
