//! What a SQL transform's text may ask for, checked on its syntax before any plan: one query
//! of `input`, projected and optionally filtered, calling only listed functions with plain
//! arguments, with short literals, and with regular expressions and `LIKE` patterns given as
//! literals.

#[cfg(test)]
mod tests;

use std::convert::Infallible;
use std::ops::ControlFlow;
use std::sync::LazyLock;

use datafusion_sql::sqlparser::ast::{
    BinaryOperator, DataType, ExactNumberInfo, Expr, Function, FunctionArg, FunctionArgExpr,
    FunctionArgumentList, FunctionArguments, GroupByExpr, ObjectName, Query, Select, SelectItem,
    SetExpr, Statement, TableFactor, TimezoneInfo, UnaryOperator, Value, ValueWithSpan, Visit as _,
    VisitMut, Visitor, VisitorMut,
};
use datafusion_sql::sqlparser::parser::ParserError;
use datafusion_sql::sqlparser::tokenizer::Location;
use rdlt_connector::META_PREFIX;
use rdlt_engine::Bound;
use regex::{Regex, RegexBuilder};

use crate::error::{SqlError, SqlErrorKind};
use crate::functions::{Call, Listed, listed};
use crate::limits::{REGEX_BYTES, SQL_DEPTH, SQL_LITERAL_BYTES};

/// Whether `statement`, a parsed query, filters; refused where it is not one a SQL transform
/// may hold.
pub(crate) fn check(statement: &Statement) -> Result<bool, SqlError> {
    let Statement::Query(query) = statement else {
        return Err(shape("the text is not a query"));
    };
    let select = selected(query)?;
    for item in &select.projection {
        if let SelectItem::UnnamedExpr(expr) = item
            && !matches!(expr, Expr::Identifier(_) | Expr::CompoundIdentifier(_))
        {
            return Err(SqlError::new(
                SqlErrorKind::Unnamed,
                "an output column that is no bare column needs a name: give it `AS name`",
            ));
        }
        if let SelectItem::ExprWithAlias { alias, .. } = item
            && alias.value.starts_with(META_PREFIX)
        {
            let message = format!(
                "an output column is named with the prefix {META_PREFIX}, which the engine keeps \
                 for its own"
            );
            return Err(SqlError::new(SqlErrorKind::Shape, message));
        }
    }
    if let ControlFlow::Break(refused) = statement.visit(&mut Checking::default()) {
        return Err(refused);
    }
    Ok(select.selection.is_some())
}

/// The refusal of text that nests deeper than [`SQL_DEPTH`].
pub(crate) fn nested() -> SqlError {
    SqlError::new(
        SqlErrorKind::Syntax,
        format!("the text nests deeper than {SQL_DEPTH}"),
    )
}

/// The refusal of text that broke at `at`, naming the place and never what it held.
pub(crate) fn broken(at: Location) -> SqlError {
    let message = format!(
        "the text is not one query, at line {} column {}",
        at.line, at.column
    );
    SqlError::new(SqlErrorKind::Syntax, message)
}

/// The refusal of the parser's `error`, naming where the text broke and never what it held.
pub(crate) fn syntax(error: &ParserError) -> SqlError {
    static AT: LazyLock<Regex> =
        LazyLock::new(|| Regex::new(r"Line: (\d+), Column: (\d+)").expect("a valid pattern"));
    if matches!(error, ParserError::RecursionLimitExceeded) {
        return nested();
    }
    let text = error.to_string();
    let place = |at: &str| at.parse().unwrap_or(0);
    match AT.captures(&text) {
        Some(at) => broken(Location::new(place(&at[1]), place(&at[2]))),
        None => SqlError::new(SqlErrorKind::Syntax, "the text is not one query"),
    }
}

/// The refusal of what a SQL transform may not hold.
pub(crate) fn shape(what: &str) -> SqlError {
    SqlError::new(
        SqlErrorKind::Shape,
        format!("{what}: a SQL transform is a projection of input"),
    )
}

fn selected(query: &Query) -> Result<&Select, SqlError> {
    let held = [
        (query.with.is_some(), "a `WITH` clause"),
        (query.order_by.is_some(), "an `ORDER BY`"),
        (
            query.limit_clause.is_some() || query.fetch.is_some(),
            "a `LIMIT`",
        ),
        (
            !query.locks.is_empty() || query.for_clause.is_some(),
            "a lock",
        ),
        (
            query.settings.is_some() || query.format_clause.is_some(),
            "settings",
        ),
        (!query.pipe_operators.is_empty(), "a pipe"),
    ];
    if let Some((_, what)) = held.iter().find(|(held, _)| *held) {
        return Err(shape(what));
    }
    let SetExpr::Select(select) = query.body.as_ref() else {
        return Err(shape("a set operation or a `VALUES` list"));
    };
    held_by(select)?;
    match select.from.as_slice() {
        [from] if from.joins.is_empty() && plain(&from.relation) => Ok(select),
        _ => Err(shape("a query reading other than `FROM input`")),
    }
}

/// Refuses a `SELECT` holding a clause beside its projection, its table and its filter.
fn held_by(select: &Select) -> Result<(), SqlError> {
    let grouped = !matches!(&select.group_by, GroupByExpr::Expressions(columns, modifiers)
        if columns.is_empty() && modifiers.is_empty());
    let held = [
        (
            select.distinct.is_some() || select.top.is_some(),
            "a `DISTINCT` or a `TOP`",
        ),
        (
            select.select_modifiers.is_some()
                || select.into.is_some()
                || !select.optimizer_hints.is_empty(),
            "a modifier",
        ),
        (
            select.exclude.is_some() || !select.lateral_views.is_empty(),
            "an exclusion or a lateral view",
        ),
        (
            select.prewhere.is_some() || !select.connect_by.is_empty(),
            "a `PREWHERE` or a `CONNECT BY`",
        ),
        (
            grouped || select.having.is_some(),
            "a `GROUP BY` or a `HAVING`",
        ),
        (
            !select.cluster_by.is_empty()
                || !select.distribute_by.is_empty()
                || !select.sort_by.is_empty(),
            "a sort",
        ),
        (
            !select.named_window.is_empty() || select.qualify.is_some(),
            "a window",
        ),
        (select.value_table_mode.is_some(), "a value table"),
    ];
    match held.iter().find(|(held, _)| *held) {
        Some((_, what)) => Err(shape(what)),
        None => Ok(()),
    }
}

/// Whether `relation` is a table read as it is, perhaps under another name: no arguments,
/// sample, version, hints or renamed columns.
fn plain(relation: &TableFactor) -> bool {
    let TableFactor::Table {
        alias,
        args: None,
        with_hints,
        version: None,
        with_ordinality: false,
        partitions,
        json_path: None,
        sample: None,
        index_hints,
        ..
    } = relation
    else {
        return false;
    };
    with_hints.is_empty()
        && partitions.is_empty()
        && index_hints.is_empty()
        && alias.as_ref().is_none_or(|alias| alias.columns.is_empty())
}

/// The type a cast naming `data_type` casts to, where it may name it: a scalar type, holding no
/// other, that DataFusion maps to one a query's bound covers, spelled as DataFusion plans it.
///
/// A type is the only part of an admitted query whose nesting its expressions' depth does not
/// bound, so none other is admitted.
fn scalar(data_type: &DataType) -> Option<DataType> {
    if as_planned(data_type) {
        Some(data_type.clone())
    } else {
        respelled(data_type)
    }
}

/// Whether `data_type` is a listed scalar type as DataFusion plans it.
fn as_planned(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Boolean
            | DataType::Bool
            | DataType::TinyInt(_)
            | DataType::SmallInt(_)
            | DataType::Int2(_)
            | DataType::Int(_)
            | DataType::Integer(_)
            | DataType::Int4(_)
            | DataType::BigInt(_)
            | DataType::Int8(_)
            | DataType::TinyIntUnsigned(_)
            | DataType::SmallIntUnsigned(_)
            | DataType::Int2Unsigned(_)
            | DataType::IntUnsigned(_)
            | DataType::IntegerUnsigned(_)
            | DataType::Int4Unsigned(_)
            | DataType::BigIntUnsigned(_)
            | DataType::Int8Unsigned(_)
            | DataType::Float(_)
            | DataType::Real
            | DataType::Float4
            | DataType::Double(_)
            | DataType::DoublePrecision
            | DataType::Float8
            | DataType::Decimal(_)
            | DataType::Numeric(_)
            | DataType::Char(_)
            | DataType::Varchar(_)
            | DataType::Text
            | DataType::String(_)
            | DataType::Date
            | DataType::Time(..)
            | DataType::Timestamp(..)
    )
}

/// The listed scalar type `data_type` spells otherwise, as DataFusion plans it, where it is one:
/// a timestamp only at a precision DataFusion plans.
fn respelled(data_type: &DataType) -> Option<DataType> {
    let planned = |precision: Option<u64>| matches!(precision, None | Some(0 | 3 | 6 | 9));
    let listed = match data_type {
        DataType::Character(length) => DataType::Char(*length),
        DataType::CharacterVarying(length)
        | DataType::CharVarying(length)
        | DataType::Nvarchar(length) => DataType::Varchar(*length),
        DataType::CharacterLargeObject(_)
        | DataType::CharLargeObject(_)
        | DataType::Clob(_)
        | DataType::TinyText
        | DataType::MediumText
        | DataType::LongText => DataType::Text,
        DataType::Dec(info) => DataType::Decimal(*info),
        DataType::Datetime(precision) | DataType::TimestampNtz(precision)
            if planned(*precision) =>
        {
            DataType::Timestamp(*precision, TimezoneInfo::None)
        }
        DataType::Datetime64(precision, None) if planned(Some(*precision)) => {
            DataType::Timestamp(Some(*precision), TimezoneInfo::None)
        }
        DataType::Date32 => DataType::Date,
        // These count bits, as ClickHouse and BigQuery name them; `INT8`, listed, is Postgres's
        // name, which counts bytes.
        DataType::Int16 => DataType::SmallInt(None),
        DataType::Int32 => DataType::Int(None),
        DataType::Int64 | DataType::Signed | DataType::SignedInteger => DataType::BigInt(None),
        DataType::UInt8 | DataType::UTinyInt => DataType::TinyIntUnsigned(None),
        DataType::UInt16 | DataType::USmallInt => DataType::SmallIntUnsigned(None),
        DataType::UInt32 => DataType::IntUnsigned(None),
        DataType::UInt64 | DataType::UBigInt | DataType::Unsigned | DataType::UnsignedInteger => {
            DataType::BigIntUnsigned(None)
        }
        DataType::Float32 => DataType::Real,
        DataType::Float64 => DataType::Double(ExactNumberInfo::None),
        _ => return None,
    };
    Some(listed)
}

/// `statement`, admitted, with each cast naming its type as DataFusion plans it.
pub(crate) fn respell(statement: &mut Statement) {
    let ControlFlow::Continue(()) = VisitMut::visit(statement, &mut Respelling);
}

/// Spells each cast's type as DataFusion plans it.
struct Respelling;

impl VisitorMut for Respelling {
    type Break = Infallible;

    fn post_visit_expr(&mut self, expr: &mut Expr) -> ControlFlow<Infallible> {
        if let Expr::Cast { data_type, .. } = expr
            && let Some(spelled) = scalar(data_type)
        {
            *data_type = spelled;
        }
        ControlFlow::Continue(())
    }
}

/// Walks a query's syntax, refusing what a SQL transform may not hold.
#[derive(Default)]
struct Checking {
    queries: usize,
}

impl Visitor for Checking {
    type Break = SqlError;

    fn pre_visit_query(&mut self, _query: &Query) -> ControlFlow<SqlError> {
        self.queries += 1;
        if self.queries > 1 {
            return ControlFlow::Break(shape("a subquery"));
        }
        ControlFlow::Continue(())
    }

    fn pre_visit_relation(&mut self, relation: &ObjectName) -> ControlFlow<SqlError> {
        if relation.to_string() == "input" {
            return ControlFlow::Continue(());
        }
        ControlFlow::Break(shape("a table other than input"))
    }

    fn pre_visit_value(&mut self, value: &ValueWithSpan) -> ControlFlow<SqlError> {
        let bytes = match &value.value {
            Value::Placeholder(_) => return ControlFlow::Break(shape("a placeholder")),
            Value::SingleQuotedString(text)
            | Value::DoubleQuotedString(text)
            | Value::Number(text, _) => text.len(),
            other => other.to_string().len(),
        };
        if bytes > SQL_LITERAL_BYTES {
            let message =
                format!("a literal takes {bytes} bytes, beyond the limit of {SQL_LITERAL_BYTES}");
            return ControlFlow::Break(SqlError::new(SqlErrorKind::Literal, message));
        }
        ControlFlow::Continue(())
    }

    fn pre_visit_expr(&mut self, expr: &Expr) -> ControlFlow<SqlError> {
        match expr {
            Expr::Function(function) => called(function),
            Expr::Like { pattern, .. } | Expr::ILike { pattern, .. } => match literal(pattern) {
                Some(_) => ControlFlow::Continue(()),
                None => ControlFlow::Break(regex("a `LIKE` pattern is not a literal")),
            },
            Expr::BinaryOp { op, right, .. } => operated(op, right),
            Expr::Cast {
                data_type, array, ..
            } => {
                if !*array && scalar(data_type).is_some() {
                    ControlFlow::Continue(())
                } else {
                    ControlFlow::Break(shape("a cast to a type a SQL transform may not hold"))
                }
            }
            Expr::UnaryOp { op, .. } => match op {
                UnaryOperator::Plus | UnaryOperator::Minus | UnaryOperator::Not => {
                    ControlFlow::Continue(())
                }
                _ => ControlFlow::Break(shape("an operator a SQL transform may not hold")),
            },
            Expr::Identifier(_)
            | Expr::CompoundIdentifier(_)
            | Expr::IsFalse(_)
            | Expr::IsNotFalse(_)
            | Expr::IsTrue(_)
            | Expr::IsNotTrue(_)
            | Expr::IsNull(_)
            | Expr::IsNotNull(_)
            | Expr::IsUnknown(_)
            | Expr::IsNotUnknown(_)
            | Expr::IsDistinctFrom(..)
            | Expr::IsNotDistinctFrom(..)
            | Expr::InList { .. }
            | Expr::Between { .. }
            | Expr::Nested(_)
            | Expr::Value(_)
            | Expr::Substring { .. }
            | Expr::Case { .. } => ControlFlow::Continue(()),
            _ => ControlFlow::Break(shape(
                "an expression of a form a SQL transform may not hold",
            )),
        }
    }
}

/// Refuses a binary operator other than arithmetic, a comparison, a logical one, a
/// concatenation or a regular expression's match against a literal.
fn operated(op: &BinaryOperator, right: &Expr) -> ControlFlow<SqlError> {
    match op {
        BinaryOperator::Plus
        | BinaryOperator::Minus
        | BinaryOperator::Multiply
        | BinaryOperator::Divide
        | BinaryOperator::Modulo
        | BinaryOperator::Eq
        | BinaryOperator::NotEq
        | BinaryOperator::Lt
        | BinaryOperator::LtEq
        | BinaryOperator::Gt
        | BinaryOperator::GtEq
        | BinaryOperator::And
        | BinaryOperator::Or
        | BinaryOperator::StringConcat => ControlFlow::Continue(()),
        BinaryOperator::PGRegexMatch | BinaryOperator::PGRegexNotMatch => {
            compiled(literal(right), None)
        }
        BinaryOperator::PGRegexIMatch | BinaryOperator::PGRegexNotIMatch => {
            compiled(literal(right), Some("i"))
        }
        _ => ControlFlow::Break(shape("an operator a SQL transform may not hold")),
    }
}

fn regex(message: &str) -> SqlError {
    SqlError::new(SqlErrorKind::Regex, message)
}

/// `expr`'s text, where it is a text literal.
fn literal(expr: &Expr) -> Option<&str> {
    match expr {
        Expr::Value(ValueWithSpan {
            value: Value::SingleQuotedString(text),
            ..
        }) => Some(text),
        Expr::Nested(inner) => literal(inner),
        _ => None,
    }
}

/// Refuses a regular expression that is no literal, or that, under `flags`, is not one or
/// compiles to more than [`REGEX_BYTES`].
fn compiled(pattern: Option<&str>, flags: Option<&str>) -> ControlFlow<SqlError> {
    let Some(pattern) = pattern else {
        return ControlFlow::Break(regex("a regular expression is not a literal"));
    };
    let flagged;
    let pattern = match flags {
        Some(flags) => {
            flagged = format!("(?{flags}){pattern}");
            flagged.as_str()
        }
        None => pattern,
    };
    match RegexBuilder::new(pattern).size_limit(REGEX_BYTES).build() {
        Ok(_) => ControlFlow::Continue(()),
        Err(_) => ControlFlow::Break(regex(&format!(
            "a regular expression is not one, or compiles to more than {REGEX_BYTES} bytes"
        ))),
    }
}

fn called(function: &Function) -> ControlFlow<SqlError> {
    let name = function.name.to_string().to_ascii_lowercase();
    let shown = shown(&name);
    let refused = |what: &str| {
        ControlFlow::Break(SqlError::new(
            SqlErrorKind::Function,
            format!("function {shown} {what}"),
        ))
    };
    let Some(listed) = listed(&name) else {
        return refused("is not one a SQL transform may call");
    };
    let plain = !function.uses_odbc_syntax
        && function.over.is_none()
        && function.filter.is_none()
        && function.within_group.is_empty()
        && function.null_treatment.is_none()
        && matches!(function.parameters, FunctionArguments::None);
    let FunctionArguments::List(list) = &function.args else {
        return refused("is called without its arguments");
    };
    if !plain || list.duplicate_treatment.is_some() || !list.clauses.is_empty() {
        return refused("is called with a clause a SQL transform may not hold");
    }
    let named = |arg: &FunctionArg| !matches!(arg, FunctionArg::Unnamed(FunctionArgExpr::Expr(_)));
    if list.args.iter().any(named) {
        return refused("takes its arguments by place alone");
    }
    arguments(listed, list, refused)
}

/// Characters of a function's name a refusal shows at most.
const SHOWN: usize = 64;

/// `name` as a refusal shows it: its first [`SHOWN`] characters, and an ellipsis where it has
/// more.
fn shown(name: &str) -> String {
    match name.char_indices().nth(SHOWN) {
        Some((end, _)) => format!("{}...", &name[..end]),
        None => name.to_owned(),
    }
}

fn arguments(
    listed: &Listed,
    list: &FunctionArgumentList,
    refused: impl Fn(&str) -> ControlFlow<SqlError>,
) -> ControlFlow<SqlError> {
    let text = |place: usize| match list.args.get(place) {
        Some(FunctionArg::Unnamed(FunctionArgExpr::Expr(expr))) => literal(expr),
        _ => None,
    };
    let pattern = listed.regex.as_ref();
    for place in listed.literals {
        if *place < list.args.len() && text(*place).is_none() {
            if pattern.is_some_and(|pattern| pattern.at == *place) {
                return ControlFlow::Break(regex("a regular expression is not a literal"));
            }
            return refused("takes a literal there");
        }
    }
    if let Some(pattern) = pattern
        && pattern.at < list.args.len()
    {
        // The flags as the function applies them: `g` alone adds none.
        let flags = match text(pattern.flags) {
            Some("g") if pattern.global => None,
            Some(flags) if pattern.global => Some(flags.replace('g', "")),
            flags => flags.map(str::to_owned),
        };
        compiled(text(pattern.at), flags.as_deref())?;
    }
    // The bound of zero-sized arguments, to refuse the literals no bound carries.
    let texts: Vec<Option<&str>> = (0..list.args.len()).map(text).collect();
    let args = vec![Bound::new(0); list.args.len()];
    let call = Call {
        args: &args,
        texts: &texts,
        width: Some(0),
    };
    match (listed.output)(&call) {
        Some(_) => ControlFlow::Continue(()),
        None => refused("is called with literals it does not bound"),
    }
}
