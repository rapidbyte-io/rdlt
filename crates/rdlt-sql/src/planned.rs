//! A SQL transform planned for one input schema: its output schema, its physical expressions,
//! the input columns each reads, and the bound on what evaluating them allocates.

mod compact;
#[cfg(test)]
mod tests;

use std::collections::BTreeSet;
use std::mem;
use std::sync::Arc;

use arrow_array::cast::AsArray as _;
use arrow_array::{BooleanArray, RecordBatch, RecordBatchOptions};
use arrow_buffer::BooleanBuffer;
use arrow_schema::SchemaRef;
use datafusion_common::tree_node::{Transformed, TreeNode as _};
use datafusion_common::{DFSchemaRef, DataFusionError, SchemaError};
use datafusion_expr::execution_props::ExecutionProps;
use datafusion_expr::expr::ScalarFunction;
use datafusion_expr::logical_plan::Projection;
use datafusion_expr::physical_planning_context::PhysicalPlanningContext;
use datafusion_expr::planner::ContextProvider as _;
use datafusion_expr::simplify::{ExprSimplifyResult, SimplifyContext};
use datafusion_expr::{Expr, LogicalPlan};
use datafusion_optimizer::analyzer::AnalyzerRule as _;
use datafusion_optimizer::analyzer::type_coercion::TypeCoercion;
use datafusion_physical_expr::{PhysicalExpr, create_physical_expr};
use datafusion_sql::planner::{ParserOptions, SqlToRel};
use datafusion_sql::sqlparser::ast::Statement;
use rdlt_connector::META_PREFIX;
use rdlt_engine::{Bound, NAMED_COLUMNS, TransformError};

use self::compact::{compacted, variable};
use crate::functions::ARRAY;
use crate::provider::{OPTIONS, Provider};
use crate::{size, stack};

/// A SQL transform planned for batches of one schema.
pub(crate) struct Planned {
    output: SchemaRef,
    projection: Vec<Projected>,
    predicate: Option<Arc<dyn PhysicalExpr>>,
    bound: Bound,
    /// For each output column, the input columns it reads.
    lineage: Vec<Vec<usize>>,
    /// The input columns the filter reads.
    keep_reads: Vec<usize>,
    /// For each output column, the input column it passes on unchanged, where it does.
    passes: Vec<Option<usize>>,
    /// The input columns of text or bytes an output column computes from, which `apply` copies
    /// first where a batch holds them in a slice of a larger buffer.
    computed: Vec<usize>,
    /// The input columns of text or bytes the filter reads, which `keep` copies first alike.
    filtered: Vec<usize>,
}

/// One output column: its expression, its name, the functions it calls, for errors, and
/// whether it passes an input column on as it is, reading the batch as it arrived.
struct Projected {
    expr: Arc<dyn PhysicalExpr>,
    column: String,
    functions: String,
    bare: bool,
}

/// What the planner made of a query: its projection, and its filter's predicate, over the
/// input as the table `input`, of `schema`.
struct Logical<'a> {
    projection: &'a Projection,
    predicate: Option<&'a Expr>,
    schema: &'a DFSchemaRef,
}

impl Planned {
    /// `statement` planned for batches of `input`.
    pub(crate) fn plan(statement: &Statement, input: &SchemaRef) -> Result<Self, TransformError> {
        stack::grown(|| Self::planning(statement, input))
    }

    fn planning(statement: &Statement, input: &SchemaRef) -> Result<Self, TransformError> {
        let plan = analyzed(statement, input)?;
        let Logical {
            projection,
            predicate,
            schema,
        } = logical(&plan)?;
        within(projection.expr.len())?;
        let making = Compiler::new(schema);
        let mut planned = Self {
            output: Arc::new(projection.schema.as_arrow().clone()),
            projection: Vec::with_capacity(projection.expr.len()),
            predicate: None,
            bound: Bound::new(0),
            lineage: Vec::with_capacity(projection.expr.len()),
            keep_reads: Vec::new(),
            passes: Vec::with_capacity(projection.expr.len()),
            computed: Vec::new(),
            filtered: Vec::new(),
        };
        let mut computed = BTreeSet::new();
        for (expr, field) in projection.expr.iter().zip(projection.schema.fields()) {
            let column = field.name().clone();
            if column.starts_with(META_PREFIX) {
                let refused = TransformError::new(format!(
                    "the query names an output column with the prefix {META_PREFIX}, which the \
                     engine keeps for its own"
                ));
                return Err(refused.with_column(column));
            }
            let compiled = making
                .compile(expr)
                .map_err(|error| named(error, &column))?;
            if compiled.passes.is_none() {
                computed.extend(compiled.reads.iter().copied());
            }
            planned.add(compiled, column);
        }
        if let Some(predicate) = predicate {
            let compiled = making.compile(predicate)?;
            planned.bound = planned.bound.plus(&compiled.allocated);
            planned.predicate = Some(compiled.expr);
            planned.keep_reads = compiled.reads;
        }
        let fields = schema.fields();
        let textual = |read: &usize| variable(fields[*read].data_type());
        planned.computed = computed.into_iter().filter(textual).collect();
        planned.filtered = planned.keep_reads.iter().copied().filter(textual).collect();
        // Each column is copied once at a time: `keep`'s copies are dropped before `apply` runs.
        let mut copies = BTreeSet::new();
        copies.extend(planned.computed.iter().chain(&planned.filtered).copied());
        for read in copies {
            let copy = Bound::new(ARRAY).with_column(read, 1);
            planned.bound = planned.bound.plus(&copy);
        }
        Ok(planned)
    }

    /// Adds `compiled`, the expression of output column `column`.
    fn add(&mut self, compiled: Compiled, column: String) {
        self.bound = mem::take(&mut self.bound).plus(&compiled.allocated);
        self.lineage.push(compiled.reads);
        self.passes.push(compiled.passes);
        self.projection.push(Projected {
            expr: compiled.expr,
            column,
            functions: compiled.functions,
            bare: compiled.passes.is_some(),
        });
    }

    pub(crate) fn output(&self) -> &SchemaRef {
        &self.output
    }

    pub(crate) fn bound(&self) -> &Bound {
        &self.bound
    }

    pub(crate) fn lineage(&self) -> &[Vec<usize>] {
        &self.lineage
    }

    pub(crate) fn keep_reads(&self) -> &[usize] {
        &self.keep_reads
    }

    pub(crate) fn passes(&self) -> &[Option<usize>] {
        &self.passes
    }

    /// Which rows of `batch` the query's filter keeps: every row of a query that filters none.
    pub(crate) fn keep(&self, batch: &RecordBatch) -> Result<BooleanArray, TransformError> {
        let rows = batch.num_rows();
        let Some(predicate) = &self.predicate else {
            return Ok(BooleanArray::new(BooleanBuffer::new_set(rows), None));
        };
        let failed = || TransformError::new("evaluating the filter failed");
        let value = predicate
            .evaluate(&compact(batch, &self.filtered)?)
            .and_then(|value| value.into_array(rows));
        let value = value.map_err(|_| failed())?;
        value.as_boolean_opt().cloned().ok_or_else(failed)
    }

    /// `batch` projected as the query says.
    pub(crate) fn apply(&self, batch: &RecordBatch) -> Result<RecordBatch, TransformError> {
        let rows = batch.num_rows();
        let copied = compact(batch, &self.computed)?;
        let mut columns = Vec::with_capacity(self.projection.len());
        for projected in &self.projection {
            let read = if projected.bare { batch } else { &copied };
            let value = projected
                .expr
                .evaluate(read)
                .and_then(|value| value.into_array(rows));
            let failed = || {
                TransformError::new(format!("evaluating {} failed", projected.functions))
                    .with_column(projected.column.clone())
            };
            columns.push(value.map_err(|_| failed())?);
        }
        let options = RecordBatchOptions::new().with_row_count(Some(rows));
        RecordBatch::try_new_with_options(Arc::clone(&self.output), columns, &options).map_err(
            |_| TransformError::new("the query's columns do not fit the schema it declares"),
        )
    }
}

/// `batch` with each of `reads`, columns of text or bytes, holding its own rows alone.
fn compact(batch: &RecordBatch, reads: &[usize]) -> Result<RecordBatch, TransformError> {
    if reads.is_empty() {
        return Ok(batch.clone());
    }
    let failed = || TransformError::new("copying a column cut from a larger one failed");
    let mut columns = batch.columns().to_vec();
    for read in reads {
        if let Some(compact) = compacted(&columns[*read]).map_err(|_| failed())? {
            columns[*read] = compact;
        }
    }
    let options = RecordBatchOptions::new().with_row_count(Some(batch.num_rows()));
    RecordBatch::try_new_with_options(batch.schema(), columns, &options).map_err(|_| failed())
}

/// What makes a query's expressions evaluable on batches of its input, of `schema`.
struct Compiler<'a> {
    schema: &'a DFSchemaRef,
    simplifying: SimplifyContext,
    props: ExecutionProps,
    context: PhysicalPlanningContext,
}

/// An expression made evaluable: its physical form, what evaluating it allocates at most, the
/// input columns it reads, the input column it passes on unchanged where it is that column, and
/// the functions it calls, for errors.
struct Compiled {
    expr: Arc<dyn PhysicalExpr>,
    allocated: Bound,
    reads: Vec<usize>,
    passes: Option<usize>,
    functions: String,
}

impl<'a> Compiler<'a> {
    fn new(schema: &'a DFSchemaRef) -> Self {
        let simplifying = SimplifyContext::builder()
            .with_schema(Arc::clone(schema))
            .with_config_options(Arc::clone(&OPTIONS))
            .build();
        let mut props = ExecutionProps::new();
        props.config_options = Some(Arc::clone(&OPTIONS));
        Self {
            schema,
            simplifying,
            props,
            context: PhysicalPlanningContext::default(),
        }
    }

    fn compile(&self, expr: &Expr) -> Result<Compiled, TransformError> {
        let expr = simplified(expr, &self.simplifying)?;
        let allocated = size::allocated(&expr, self.schema)?;
        let physical = create_physical_expr(&expr, self.schema, &self.props, &self.context)
            .map_err(|error| planning(&error))?;
        let mut bare = &expr;
        while let Expr::Alias(alias) = bare {
            bare = &alias.expr;
        }
        let passes = match bare {
            Expr::Column(column) => self.schema.index_of_column(column).ok(),
            _ => None,
        };
        Ok(Compiled {
            expr: physical,
            allocated,
            reads: size::columns(&expr, self.schema),
            passes,
            functions: size::functions(&expr),
        })
    }
}

/// `statement` planned against batches of `input` as the table `input`, its types coerced.
pub(crate) fn analyzed(
    statement: &Statement,
    input: &SchemaRef,
) -> Result<LogicalPlan, TransformError> {
    let provider = Provider::new(Arc::clone(input));
    let options = ParserOptions {
        enable_ident_normalization: false,
        map_string_types_to_utf8view: false,
        ..ParserOptions::new()
    };
    let planner = SqlToRel::new_with_options(&provider, options);
    let plan = planner
        .sql_statement_to_plan(statement.clone())
        .map_err(|error| planning(&error))?;
    TypeCoercion::new()
        .analyze(plan, provider.options())
        .map_err(|error| planning(&error))
}

/// The projection, the filter and the input of `plan`, which must be a projection over an
/// optional filter over all of `input`, perhaps under another name.
fn logical(plan: &LogicalPlan) -> Result<Logical<'_>, TransformError> {
    let LogicalPlan::Projection(projection) = plan else {
        return Err(node(plan));
    };
    let (predicate, scanned) = match projection.input.as_ref() {
        LogicalPlan::Filter(filter) => (Some(&filter.predicate), filter.input.as_ref()),
        other => (None, other),
    };
    let scanned = match scanned {
        LogicalPlan::SubqueryAlias(alias) => alias.input.as_ref(),
        other => other,
    };
    let LogicalPlan::TableScan(scan) = scanned else {
        return Err(node(scanned));
    };
    let whole = scan.projection.is_none() && scan.filters.is_empty() && scan.fetch.is_none();
    if scan.table_name.table() != "input" || !whole {
        return Err(node(scanned));
    }
    Ok(Logical {
        projection,
        predicate,
        schema: projection.input.schema(),
    })
}

/// `expr` with each call rewritten as its function rewrites it before DataFusion runs it, where
/// the rewrite makes another kind of expression: `coalesce`, for one, runs only as the `CASE` it
/// becomes.
///
/// A rewrite into another call is not taken. A listed function's only folds its literals, and
/// `concat`'s folds its empty literals away into a call it refuses to run, so the call runs as
/// written.
fn simplified(expr: &Expr, context: &SimplifyContext) -> Result<Expr, TransformError> {
    let rewritten = expr.clone().transform_up(|node| match node {
        Expr::ScalarFunction(ScalarFunction { func, args }) => {
            let written = args.clone();
            match func.simplify(args, context)? {
                ExprSimplifyResult::Simplified(Expr::ScalarFunction(_)) => {
                    Ok(Transformed::no(Expr::ScalarFunction(ScalarFunction {
                        func,
                        args: written,
                    })))
                }
                ExprSimplifyResult::Simplified(expr) => Ok(Transformed::yes(expr)),
                ExprSimplifyResult::Original(args) => {
                    Ok(Transformed::no(Expr::ScalarFunction(ScalarFunction {
                        func,
                        args,
                    })))
                }
            }
        }
        other => Ok(Transformed::no(other)),
    });
    rewritten
        .map(|rewritten| rewritten.data)
        .map_err(|error| planning(&error))
}

/// Refuses a query outputting `columns` columns where they are more than the engine names.
fn within(columns: usize) -> Result<(), TransformError> {
    if columns > NAMED_COLUMNS {
        let message = format!("the query outputs more than {NAMED_COLUMNS} columns");
        return Err(TransformError::new(message));
    }
    Ok(())
}

/// `error`, naming `column` where it names none.
fn named(error: TransformError, column: &str) -> TransformError {
    match error.column() {
        Some(_) => error,
        None => error.with_column(column),
    }
}

/// The refusal of a plan node a SQL transform may not hold.
fn node(plan: &LogicalPlan) -> TransformError {
    let what = match plan {
        LogicalPlan::Aggregate(_) => "an aggregate",
        LogicalPlan::Sort(_) => "a sort",
        LogicalPlan::Join(_) => "a join",
        LogicalPlan::Window(_) => "a window",
        LogicalPlan::Limit(_) => "a limit",
        LogicalPlan::Distinct(_) => "a distinct",
        LogicalPlan::Union(_) => "a union",
        LogicalPlan::Subquery(_) => "a subquery",
        LogicalPlan::Unnest(_) => "an unnest",
        _ => "a plan node",
    };
    TransformError::new(format!("{what} is not a projection of input"))
}

/// A planner's `error`, as a refusal naming the column it is about and never its text, which
/// can quote a literal of the query.
fn planning(error: &DataFusionError) -> TransformError {
    if let DataFusionError::SchemaError(schema, _) = error.find_root()
        && let SchemaError::FieldNotFound { field, .. } = schema.as_ref()
    {
        return TransformError::new("the query names a column the batch lacks")
            .with_column(field.name.clone());
    }
    TransformError::new("the query cannot be planned against the batch's columns")
}
