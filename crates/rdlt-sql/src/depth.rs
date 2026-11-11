//! How deep a parsed query's expressions nest, measured on the tree the parser built, how many
//! conditions its `CASE`s lie within, and how deep its conditions nest.
//!
//! The parser builds a chain such as `a + b + c` in a loop, into a tree as deep as the chain is
//! long, which its recursion limit does not bound. The measure counts expressions alone: a
//! query's types nest too, and shape admits none but flat scalars, while the text's size bounds
//! both on the stack the work on the text runs on, a refused tree's drop among it. Walking a
//! tree grows the stack as it goes, but cloning and evaluating one do not: a query is measured
//! and refused on its tree before either meets it.
//!
//! The planner reads a `CASE` lying in a condition again for each operator above it there, so
//! its work multiplies with each condition a `CASE` lies within, however shallow the tree, and
//! grows within one condition as a power of the condition's depth.

#[cfg(test)]
mod tests;

use std::collections::BTreeSet;
use std::convert::Infallible;
use std::ops::ControlFlow;
use std::ptr;

use datafusion_sql::sqlparser::ast::{
    Expr, FunctionArg, FunctionArgExpr, FunctionArguments, Statement, Visit, Visitor,
};

/// How a parsed query's expressions nest.
#[derive(Debug, PartialEq, Eq)]
pub(crate) struct Nesting {
    /// The most expressions any expression lies within: a name or a literal alone is no level
    /// deep, `lower(t)` one, and a chain of operators as deep as it has operators.
    pub(crate) depth: usize,
    /// The most conditions any `CASE` lies within, a call of `coalesce` among them: the `WHEN`s
    /// of the `CASE`s around it, and the arguments but the last of the calls of `coalesce`
    /// around it, which is planned as a `CASE` testing each.
    pub(crate) conditions: usize,
    /// The most expressions any expression of a condition lies within, counted in its outermost
    /// condition alone: `n > 0` is one deep, and a `CASE` in it takes its own conditions along.
    pub(crate) condition_depth: usize,
}

/// How the expressions of `statement` nest.
pub(crate) fn nesting(statement: &Statement) -> Nesting {
    let mut measured = Measured::default();
    let ControlFlow::Continue(()) = Visit::visit(statement, &mut measured);
    Nesting {
        depth: measured.deepest,
        conditions: measured.most_conditioned,
        condition_depth: measured.deepest_tested,
    }
}

/// How many expressions a walk is within, and the most it has been; how many conditions it is
/// within, and the most a `CASE` it met has been; and how deep in its outermost condition it is,
/// and the most it has been.
#[derive(Default)]
struct Measured {
    within: usize,
    deepest: usize,
    /// The conditions of the `CASE`s the walk has met, by address, until it leaves each.
    tested: BTreeSet<usize>,
    conditioned: usize,
    most_conditioned: usize,
    /// How many expressions the outermost condition the walk is within lies within.
    outermost: usize,
    deepest_tested: usize,
}

impl Visitor for Measured {
    type Break = Infallible;

    fn pre_visit_expr(&mut self, expr: &Expr) -> ControlFlow<Infallible> {
        let depth = self.within;
        self.deepest = self.deepest.max(depth);
        self.within = self.within.saturating_add(1);
        if self.tested.contains(&address(expr)) {
            if self.conditioned == 0 {
                self.outermost = depth;
            }
            self.conditioned = self.conditioned.saturating_add(1);
        }
        if self.conditioned > 0 {
            let tested = depth.saturating_sub(self.outermost);
            self.deepest_tested = self.deepest_tested.max(tested);
        }
        if let Some(conditions) = conditions(expr) {
            self.most_conditioned = self.most_conditioned.max(self.conditioned);
            self.tested.extend(conditions.into_iter().map(address));
        }
        ControlFlow::Continue(())
    }

    fn post_visit_expr(&mut self, expr: &Expr) -> ControlFlow<Infallible> {
        self.within = self.within.saturating_sub(1);
        if self.tested.remove(&address(expr)) {
            self.conditioned = self.conditioned.saturating_sub(1);
        }
        ControlFlow::Continue(())
    }
}

/// Where `expr` lies, which the walk meets it at again.
fn address(expr: &Expr) -> usize {
    ptr::from_ref(expr).addr()
}

/// The conditions of `expr` where it is a `CASE` or is planned as one: a `CASE`'s `WHEN`s, and
/// the arguments of a call of `coalesce` but its last.
fn conditions(expr: &Expr) -> Option<Vec<&Expr>> {
    match expr {
        Expr::Case { conditions, .. } => {
            Some(conditions.iter().map(|when| &when.condition).collect())
        }
        Expr::Function(function) if function.name.to_string().eq_ignore_ascii_case("coalesce") => {
            let FunctionArguments::List(list) = &function.args else {
                return None;
            };
            let tested = list.args.split_last().map_or(&[][..], |(_, tested)| tested);
            Some(tested.iter().filter_map(argument).collect())
        }
        _ => None,
    }
}

/// The expression `arg` passes, where it passes one.
fn argument(arg: &FunctionArg) -> Option<&Expr> {
    match arg {
        FunctionArg::Named {
            arg: FunctionArgExpr::Expr(expr),
            ..
        }
        | FunctionArg::ExprNamed {
            arg: FunctionArgExpr::Expr(expr),
            ..
        }
        | FunctionArg::Unnamed(FunctionArgExpr::Expr(expr)) => Some(expr),
        _ => None,
    }
}
