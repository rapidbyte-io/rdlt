//! A query's physical expressions made to fail where a value overflows its type, as a division by
//! zero fails, where DataFusion plans them to wrap.

use std::sync::Arc;

use arrow_schema::{DataType, Schema, TimeUnit};
use datafusion_common::tree_node::{Transformed, TreeNode as _};
use datafusion_common::{DataFusionError, ScalarValue};
use datafusion_expr::Operator;
use datafusion_physical_expr::PhysicalExpr;
use datafusion_physical_expr::expressions::{
    self, BinaryExpr, CastExpr, InListExpr, Literal, NegativeExpr, TryCastExpr,
};

/// `expr`, over batches of `schema`, with each operator checked, each negation of an integer its
/// checked subtraction from zero, each `IN` list's constants evaluated again once checked, and
/// each `TRY_CAST` of a `Date64` to a finer timestamp made from the timestamp of its milliseconds,
/// whose scaling is checked.
pub(super) fn checked(
    expr: Arc<dyn PhysicalExpr>,
    schema: &Schema,
) -> Result<Arc<dyn PhysicalExpr>, DataFusionError> {
    let rewritten = expr.transform_up(|node| {
        if let Some(binary) = node.downcast_ref::<BinaryExpr>() {
            let (left, right) = (Arc::clone(binary.left()), Arc::clone(binary.right()));
            return Ok(Transformed::yes(operated(left, *binary.op(), right)));
        }
        // A list of constants is evaluated as it is planned, so again once its operators are
        // checked: one that overflows is then evaluated with each batch, and fails it.
        if let Some(listed) = node.downcast_ref::<InListExpr>() {
            let (operand, list) = (Arc::clone(listed.expr()), listed.list().to_vec());
            let rebuilt = expressions::in_list(operand, list, &listed.negated(), schema)?;
            return Ok(Transformed::yes(rebuilt));
        }
        if let Some(negative) = node.downcast_ref::<NegativeExpr>() {
            let operand = Arc::clone(negative.arg());
            let data_type = operand.data_type(schema)?;
            if data_type.is_integer() {
                let zero = Arc::new(Literal::new(ScalarValue::new_zero(&data_type)?));
                return Ok(Transformed::yes(operated(zero, Operator::Minus, operand)));
            }
        }
        let held = milliseconds(&node, schema)?;
        match held {
            Some(held) => Ok(Transformed::yes(node.with_new_children(vec![held])?)),
            None => Ok(Transformed::no(node)),
        }
    })?;
    Ok(rewritten.data)
}

/// `op` over `left` and `right`, failing where its value overflows its type.
fn operated(
    left: Arc<dyn PhysicalExpr>,
    op: Operator,
    right: Arc<dyn PhysicalExpr>,
) -> Arc<dyn PhysicalExpr> {
    Arc::new(BinaryExpr::new(left, op, right).with_fail_on_overflow(true))
}

/// The operand of `node`, a `TRY_CAST` of a `Date64` to microseconds or nanoseconds, as the
/// timestamp of its milliseconds: Arrow scales a `Date64` itself unchecked, which DataFusion
/// checks before a cast but not before a `TRY_CAST`, and a timestamp checked.
fn milliseconds(
    node: &Arc<dyn PhysicalExpr>,
    schema: &Schema,
) -> Result<Option<Arc<dyn PhysicalExpr>>, DataFusionError> {
    if !node.is::<TryCastExpr>() {
        return Ok(None);
    }
    let children = node.children();
    let [operand] = *children.as_slice() else {
        return Ok(None);
    };
    let finer = matches!(
        node.data_type(schema)?,
        DataType::Timestamp(TimeUnit::Microsecond | TimeUnit::Nanosecond, _)
    );
    if !finer || operand.data_type(schema)? != DataType::Date64 {
        return Ok(None);
    }
    let millis = DataType::Timestamp(TimeUnit::Millisecond, None);
    Ok(Some(Arc::new(CastExpr::new(
        Arc::clone(operand),
        millis,
        None,
    ))))
}
