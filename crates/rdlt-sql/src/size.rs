//! What evaluating a SQL transform's expressions allocates at most, in terms of its input's
//! rows and column bytes, and which input columns each reads.

use std::collections::BTreeSet;

use arrow_schema::DataType;
use datafusion_common::tree_node::{TreeNode as _, TreeNodeRecursion};
use datafusion_common::{DFSchema, ScalarValue};
use datafusion_expr::expr::{BinaryExpr, Case, Cast, ScalarFunction, TryCast};
use datafusion_expr::{Expr, ExprSchemable as _, Operator};
use rdlt_engine::{Bound, TransformError};

use crate::functions::{ARRAY, Call, TEXT, TEXT_ARRAY, listed};
use crate::stack;

/// Builders that grow by doubling hold up to twice what they keep.
const SLACK: u64 = 2;

/// What an expression outputs at most, and what evaluating it allocates at most, its children
/// included.
struct Sized {
    output: Bound,
    allocated: Bound,
}

/// What evaluating `expr` over batches of `schema` allocates at most.
pub(crate) fn allocated(expr: &Expr, schema: &DFSchema) -> Result<Bound, TransformError> {
    Ok(sized(expr, schema)?.allocated)
}

/// The input columns, by ordinal in `schema`, `expr` reads, in order.
pub(crate) fn columns(expr: &Expr, schema: &DFSchema) -> Vec<usize> {
    let mut read = BTreeSet::new();
    expr.apply(|node| {
        if let Expr::Column(column) = node
            && let Ok(index) = schema.index_of_column(column)
        {
            read.insert(index);
        }
        Ok(TreeNodeRecursion::Continue)
    })
    .expect("collecting columns fails nowhere");
    read.into_iter().collect()
}

/// The functions `expr` calls, for errors: `the expression` where it calls none.
pub(crate) fn functions(expr: &Expr) -> String {
    let mut names = Vec::new();
    expr.apply(|node| {
        if let Expr::ScalarFunction(call) = node {
            names.push(call.name().to_owned());
        }
        Ok(TreeNodeRecursion::Continue)
    })
    .expect("collecting names fails nowhere");
    if names.is_empty() {
        "the expression".to_owned()
    } else {
        names.join(", ")
    }
}

fn refused() -> TransformError {
    TransformError::new("the query holds an expression whose output its input does not bound")
}

/// Bytes a row of `data_type` takes, its validity with it; none for a type of no fixed width.
fn width(data_type: &DataType) -> Option<u64> {
    match data_type {
        DataType::Null => Some(0),
        DataType::Boolean => Some(1),
        other => other
            .primitive_width()
            .and_then(|width| u64::try_from(width).ok())
            .map(|width| width.saturating_add(1)),
    }
}

/// Refuses `expr` where its values may take more than its bytes once decoded, as those of a
/// dictionary or of runs do: an output bounded by its arguments' bytes reads only flat ones.
fn flat(expr: &Expr, schema: &DFSchema) -> Result<(), TransformError> {
    let data_type = expr.get_type(schema).map_err(|_| refused())?;
    let flat = width(&data_type).is_some()
        || matches!(
            data_type,
            DataType::Utf8
                | DataType::LargeUtf8
                | DataType::Utf8View
                | DataType::Binary
                | DataType::LargeBinary
                | DataType::BinaryView
                | DataType::FixedSizeBinary(_)
        );
    if flat { Ok(()) } else { Err(refused()) }
}

/// A row of `expr`'s fixed-width type.
fn fixed(expr: &Expr, schema: &DFSchema) -> Result<Bound, TransformError> {
    let data_type = expr.get_type(schema).map_err(|_| refused())?;
    let width = width(&data_type).ok_or_else(refused)?;
    Ok(Bound::new(ARRAY).with_rows(width))
}

/// What an array of text takes beside its text.
fn text_rows() -> Bound {
    Bound::new(TEXT_ARRAY).with_rows(TEXT)
}

fn made(output: Bound, children: impl IntoIterator<Item = Sized>) -> Sized {
    let allocated = children
        .into_iter()
        .fold(output.clone().times(SLACK), |sum, child| {
            sum.plus(&child.allocated)
        });
    Sized { output, allocated }
}

fn all<'a>(
    exprs: impl IntoIterator<Item = &'a Expr>,
    schema: &DFSchema,
) -> Result<Vec<Sized>, TransformError> {
    exprs.into_iter().map(|expr| sized(expr, schema)).collect()
}

fn sized(expr: &Expr, schema: &DFSchema) -> Result<Sized, TransformError> {
    stack::nest(|| level(expr, schema))
}

/// What `expr` outputs and allocates, its children sized a level down.
fn level(expr: &Expr, schema: &DFSchema) -> Result<Sized, TransformError> {
    match expr {
        Expr::Alias(alias) => sized(&alias.expr, schema),
        Expr::Column(column) => {
            let index = schema.index_of_column(column).map_err(|_| {
                TransformError::new("the query names a column the batch lacks")
                    .with_column(column.name.clone())
            })?;
            let output = Bound::new(0).with_column(index, 1);
            Ok(Sized {
                output,
                allocated: Bound::new(0),
            })
        }
        Expr::Literal(value, _) => {
            let bytes = u64::try_from(value.size()).unwrap_or(u64::MAX);
            Ok(made(Bound::new(ARRAY).with_rows(bytes), []))
        }
        Expr::BinaryExpr(binary) => operated(expr, binary, schema),
        Expr::Not(inner)
        | Expr::IsNull(inner)
        | Expr::IsNotNull(inner)
        | Expr::IsTrue(inner)
        | Expr::IsFalse(inner)
        | Expr::IsUnknown(inner)
        | Expr::IsNotTrue(inner)
        | Expr::IsNotFalse(inner)
        | Expr::IsNotUnknown(inner)
        | Expr::Negative(inner) => Ok(made(fixed(expr, schema)?, [sized(inner, schema)?])),
        Expr::Between(between) => {
            let children = all([&*between.expr, &*between.low, &*between.high], schema)?;
            Ok(made(fixed(expr, schema)?, children))
        }
        Expr::Like(like) => {
            let children = all([&*like.expr, &*like.pattern], schema)?;
            Ok(made(fixed(expr, schema)?, children))
        }
        Expr::InList(list) => {
            let mut children = vec![sized(&list.expr, schema)?];
            children.extend(all(&list.list, schema)?);
            Ok(made(fixed(expr, schema)?, children))
        }
        Expr::Case(case) => cased(expr, case, schema),
        Expr::Cast(Cast { expr, field }) | Expr::TryCast(TryCast { expr, field }) => {
            let child = sized(expr, schema)?;
            let from = expr.get_type(schema).map_err(|_| refused())?;
            let output = cast(&from, field.data_type(), &child.output).ok_or_else(refused)?;
            Ok(made(output, [child]))
        }
        Expr::ScalarFunction(call) => called(expr, call, schema),
        _ => Err(refused()),
    }
}

/// An operator on two values: a concatenation holds both, and every other operator outputs a
/// number or a truth.
fn operated(expr: &Expr, binary: &BinaryExpr, schema: &DFSchema) -> Result<Sized, TransformError> {
    let (left, right) = (sized(&binary.left, schema)?, sized(&binary.right, schema)?);
    let output = match binary.op {
        Operator::StringConcat => {
            flat(&binary.left, schema)?;
            flat(&binary.right, schema)?;
            left.output.clone().plus(&right.output).plus(&text_rows())
        }
        _ => fixed(expr, schema)?,
    };
    Ok(made(output, [left, right]))
}

/// A call of a listed function: what the function's bound makes of its arguments.
fn called(expr: &Expr, call: &ScalarFunction, schema: &DFSchema) -> Result<Sized, TransformError> {
    let listed = listed(call.name()).ok_or_else(refused)?;
    let args = all(&call.args, schema)?;
    let bounds: Vec<Bound> = args.iter().map(|arg| arg.output.clone()).collect();
    let texts: Vec<Option<&str>> = call.args.iter().map(text).collect();
    let data_type = expr.get_type(schema).map_err(|_| refused())?;
    let width = width(&data_type);
    if width.is_none() {
        call.args.iter().try_for_each(|arg| flat(arg, schema))?;
    }
    let called = Call {
        args: &bounds,
        texts: &texts,
        width,
    };
    let output = (listed.output)(&called).ok_or_else(refused)?;
    Ok(made(output, args))
}

/// A `CASE`: each row takes one branch's value, so its output holds at most every branch's.
fn cased(expr: &Expr, case: &Case, schema: &DFSchema) -> Result<Sized, TransformError> {
    let values = case.when_then_expr.iter().map(|(_, then)| then.as_ref());
    let values: Vec<&Expr> = values.chain(case.else_expr.as_deref()).collect();
    let data_type = expr.get_type(schema).map_err(|_| refused())?;
    if width(&data_type).is_none() {
        values.iter().try_for_each(|value| flat(value, schema))?;
    }
    let mut output = text_rows();
    let mut children = all(case.expr.as_deref(), schema)?;
    for (when, then) in &case.when_then_expr {
        children.push(sized(when, schema)?);
        let then = sized(then, schema)?;
        output = output.plus(&then.output);
        children.push(then);
    }
    for other in all(case.else_expr.as_deref(), schema)? {
        output = output.plus(&other.output);
        children.push(other);
    }
    Ok(made(output, children))
}

/// `expr`'s text, where it is a text literal, cast to another layout of text as coercion may.
fn text(expr: &Expr) -> Option<&str> {
    match expr {
        Expr::Literal(
            ScalarValue::Utf8(Some(text))
            | ScalarValue::LargeUtf8(Some(text))
            | ScalarValue::Utf8View(Some(text)),
            _,
        ) => Some(text),
        Expr::Alias(alias) => text(&alias.expr),
        Expr::Cast(Cast { expr, field }) | Expr::TryCast(TryCast { expr, field })
            if texty(field.data_type()) =>
        {
            text(expr)
        }
        _ => None,
    }
}

/// Whether `data_type` is a layout of text.
fn texty(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View
    )
}

/// What a cast of a column of `from` to `to` outputs at most, where `child` is what its input
/// outputs; none for a cast whose output its input does not bound.
fn cast(from: &DataType, to: &DataType, child: &Bound) -> Option<Bound> {
    if !texty(to) {
        return width(to).map(|width| Bound::new(ARRAY).with_rows(width));
    }
    // The longest text of a value of each type, in bytes.
    let longest: u64 = match from {
        from if texty(from) => return Some(child.clone().plus(&text_rows())),
        DataType::Null => 0,
        DataType::Boolean => 5,
        DataType::Int8
        | DataType::Int16
        | DataType::Int32
        | DataType::Int64
        | DataType::UInt8
        | DataType::UInt16
        | DataType::UInt32
        | DataType::UInt64 => 20,
        DataType::Float16
        | DataType::Float32
        | DataType::Float64
        | DataType::Date32
        | DataType::Date64 => 32,
        // Its digits, a zero a place of a negative scale, a sign, a point and a leading zero.
        DataType::Decimal32(precision, scale)
        | DataType::Decimal64(precision, scale)
        | DataType::Decimal128(precision, scale)
        | DataType::Decimal256(precision, scale) => {
            u64::from(*precision) + u64::from(scale.unsigned_abs()) + 3
        }
        DataType::Time32(_) | DataType::Time64(_) | DataType::Timestamp(..) => 48,
        _ => return None,
    };
    Some(text_rows().plus(&Bound::new(0).with_rows(longest)))
}
