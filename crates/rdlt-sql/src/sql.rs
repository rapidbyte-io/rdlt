//! A SQL transform: one query of `input`, parsed once and planned for each input schema it
//! meets.

#[cfg(test)]
mod tests;

use std::fmt;
use std::sync::{Arc, OnceLock};

use arrow_array::{BooleanArray, RecordBatch};
use arrow_schema::{Schema, SchemaRef};
use datafusion_sql::sqlparser::ast::Statement;
use datafusion_sql::sqlparser::parser::Parser;
use datafusion_sql::sqlparser::tokenizer::{TokenWithSpan, Tokenizer};
use parking_lot::Mutex;
use rdlt_engine::{Bound, Transform, TransformError, TransformStep};

use crate::contain::contained;
use crate::dialect::{Guarded, Refused};
use crate::error::{SqlError, SqlErrorKind};
use crate::limits::{
    PARSER_DEPTH, PLANS, SQL_BYTES, SQL_CONDITION_DEPTH, SQL_DEPTH, SQL_NESTED_CONDITIONS,
};
use crate::planned::Planned;
use crate::{depth, shape, stack};

/// A stateless SQL transform: a projection of the table `input`, optionally filtered, calling
/// only the functions the list bounds.
pub struct Sql {
    statement: Statement,
    filters: bool,
    /// The plans for the input schemas asked for most recently, the latest last.
    plans: Mutex<Vec<Kept>>,
}

/// The place of a plan, and the schema it was last asked for by, which a batch of it holds
/// again.
struct Kept {
    input: SchemaRef,
    plan: Arc<OnceLock<Plan>>,
}

/// A query planned for a schema, or why it is refused.
type Plan = Result<Arc<Planned>, TransformError>;

impl Sql {
    /// Parses `text`, one query of `input`.
    ///
    /// # Errors
    ///
    /// A [`SqlError`], coded `transform_invalid`, where `text` is longer than [`SQL_BYTES`], is
    /// not one query, nests deeper than [`SQL_DEPTH`], nests a `CASE` within more conditions
    /// than [`SQL_NESTED_CONDITIONS`] or a condition deeper than [`SQL_CONDITION_DEPTH`], asks
    /// for what a SQL transform may not, or makes a library it is parsed by panic.
    pub fn parse(text: &str) -> Result<Self, SqlError> {
        let parsed = contained(|| Self::parsed(text));
        parsed.unwrap_or_else(|| {
            let message = "parsing the text failed inside the library that parses it";
            Err(SqlError::new(SqlErrorKind::Failed, message))
        })
    }

    /// `text` parsed and checked.
    fn parsed(text: &str) -> Result<Self, SqlError> {
        #[cfg(test)]
        crate::contain::panics_at(text);
        if text.len() > SQL_BYTES {
            let message = format!(
                "the text takes {} bytes, beyond the limit of {SQL_BYTES}",
                text.len()
            );
            return Err(SqlError::new(SqlErrorKind::TooLong, message));
        }
        let tokens = Tokenizer::new(&Guarded::reading(&[]), text)
            .tokenize_with_location()
            .map_err(|error| shape::broken(error.location))?;
        let (statement, filters) = stack::text(text.len(), || read(tokens))?;
        Ok(Self {
            statement,
            filters,
            plans: Mutex::new(Vec::new()),
        })
    }

    /// The query, as parsed.
    #[cfg(feature = "bench")]
    pub(crate) fn statement(&self) -> &Statement {
        &self.statement
    }

    /// The plan for batches of `input`, asked for by `asked`, the schema a batch holds, where
    /// one does: one kept, or made and kept.
    ///
    /// A schema is planned once however many ask for it at once, and the plans' lock is held
    /// only to find or add its place, so asking for another schema never waits on a plan.
    fn planned(&self, input: &Schema, asked: Option<&SchemaRef>) -> Plan {
        let (schema, plan) = self.place(input, asked);
        let planning = || {
            #[cfg(test)]
            for field in schema.fields() {
                crate::contain::panics_at(field.name());
            }
            Planned::plan(&self.statement, &schema).map(Arc::new)
        };
        let made = || {
            contained(planning).unwrap_or_else(|| {
                let message = "planning the query failed inside the library that plans it";
                Err(TransformError::new(message))
            })
        };
        plan.get_or_init(made).clone()
    }

    /// The place of the plan for batches of `input`, and the schema it is planned for: one kept,
    /// found by the very schema `asked` or by an equal one, which becomes its key, or one added,
    /// the plan asked for least recently given up where [`PLANS`] are kept.
    ///
    /// The place found or added becomes the latest.
    fn place(&self, input: &Schema, asked: Option<&SchemaRef>) -> (SchemaRef, Arc<OnceLock<Plan>>) {
        let mut plans = self.plans.lock();
        let found = asked
            .and_then(|asked| {
                plans
                    .iter()
                    .position(|kept| Arc::ptr_eq(&kept.input, asked))
            })
            .or_else(|| plans.iter().rposition(|kept| *kept.input == *input));
        if let Some(place) = found {
            let mut kept = plans.remove(place);
            if let Some(asked) = asked {
                kept.input = Arc::clone(asked);
            }
            let found = (Arc::clone(&kept.input), Arc::clone(&kept.plan));
            plans.push(kept);
            return found;
        }
        let schema = asked.map_or_else(|| Arc::new(input.clone()), Arc::clone);
        if plans.len() == PLANS {
            plans.remove(0);
        }
        let plan = Arc::new(OnceLock::new());
        plans.push(Kept {
            input: Arc::clone(&schema),
            plan: Arc::clone(&plan),
        });
        (schema, plan)
    }
}

/// The query `tokens` hold, a statement of their own, and whether it filters; refused where it
/// is not a query a SQL transform may hold.
///
/// The parser reads it within a budget, and its depth and the conditions its `CASE`s lie within
/// are measured on the tree before anything else reads it.
fn read(tokens: Vec<TokenWithSpan>) -> Result<(Statement, bool), SqlError> {
    let dialect = Guarded::reading(&tokens);
    let parsed = Parser::new(&dialect)
        .with_recursion_limit(PARSER_DEPTH)
        .with_tokens_with_locations(tokens)
        .parse_statements();
    match dialect.refused() {
        Some(Refused::Work) => {
            return Err(shape::shape(
                "text whose parse takes more work than its length allows",
            ));
        }
        Some(Refused::Nested) => return Err(shape::shape("a nested type")),
        Some(Refused::Interval) => return Err(shape::shape("an interval")),
        None => {}
    }
    let mut statements = parsed.map_err(|error| shape::syntax(&error))?;
    for statement in &statements {
        let nesting = depth::nesting(statement);
        if nesting.depth > SQL_DEPTH {
            return Err(shape::nested());
        }
        if nesting.conditions > SQL_NESTED_CONDITIONS {
            return Err(shape::conditioned());
        }
        if nesting.condition_depth > SQL_CONDITION_DEPTH {
            return Err(shape::deep_condition());
        }
    }
    let (Some(mut statement), true) = (statements.pop(), statements.is_empty()) else {
        let message = "the text is not one query";
        return Err(SqlError::new(SqlErrorKind::Syntax, message));
    };
    let filters = shape::check(&statement)?;
    shape::respell(&mut statement);
    Ok((statement, filters))
}

impl Transform for Sql {
    fn name(&self) -> &'static str {
        "sql"
    }

    fn output(&self, input: &Schema) -> Result<SchemaRef, TransformError> {
        Ok(Arc::clone(self.planned(input, None)?.output()))
    }

    fn filters(&self) -> bool {
        self.filters
    }

    fn keep(&self, batch: &RecordBatch) -> Result<BooleanArray, TransformError> {
        self.planned(batch.schema_ref(), Some(batch.schema_ref()))?
            .keep(batch)
    }

    fn apply(&self, batch: RecordBatch) -> Result<RecordBatch, TransformError> {
        self.planned(batch.schema_ref(), Some(batch.schema_ref()))?
            .apply(&batch)
    }

    fn bound(&self, input: &Schema) -> Option<Bound> {
        let planned = self.planned(input, None).ok()?;
        Some(planned.bound().clone())
    }

    fn lineage(&self, input: &Schema) -> Option<Vec<Vec<usize>>> {
        let planned = self.planned(input, None).ok()?;
        Some(planned.lineage().to_vec())
    }

    fn keep_reads(&self, input: &Schema) -> Option<Vec<usize>> {
        let planned = self.planned(input, None).ok()?;
        Some(planned.keep_reads().to_vec())
    }

    fn passes(&self, input: &Schema) -> Option<Vec<Option<usize>>> {
        let planned = self.planned(input, None).ok()?;
        Some(planned.passes().to_vec())
    }
}

impl From<Sql> for TransformStep {
    fn from(sql: Sql) -> Self {
        Self::Code(Arc::new(sql))
    }
}

/// Shows whether the query filters, never its text, which may hold literals a tenant keeps to
/// itself.
impl fmt::Debug for Sql {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Sql")
            .field("filters", &self.filters)
            .finish_non_exhaustive()
    }
}
