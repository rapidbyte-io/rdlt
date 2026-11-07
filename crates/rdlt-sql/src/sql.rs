//! A SQL transform: one query of `input`, parsed once.

#[cfg(test)]
mod tests;

use std::fmt;

use datafusion_sql::sqlparser::ast::Statement;
use datafusion_sql::sqlparser::parser::Parser;
use datafusion_sql::sqlparser::tokenizer::{TokenWithSpan, Tokenizer};

use crate::dialect::{Guarded, Refused};
use crate::error::{SqlError, SqlErrorKind};
use crate::limits::{PARSER_DEPTH, SQL_BYTES, SQL_DEPTH};
use crate::{depth, shape, stack};

/// A stateless SQL transform: a projection of the table `input`, optionally filtered, calling
/// only the functions the list bounds.
pub struct Sql {
    filters: bool,
}

impl Sql {
    /// Parses `text`, one query of `input`.
    ///
    /// # Errors
    ///
    /// A [`SqlError`], coded `transform_invalid`, where `text` is longer than [`SQL_BYTES`], is
    /// not one query, nests deeper than [`SQL_DEPTH`], or asks for what a SQL transform may
    /// not.
    pub fn parse(text: &str) -> Result<Self, SqlError> {
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
        let filters = stack::text(|| read(tokens).map(|(_, filters)| filters))?;
        Ok(Self { filters })
    }
}

/// The query `tokens` hold, a statement of their own, and whether it filters; refused where it
/// is not a query a SQL transform may hold.
///
/// The parser reads it within a budget, and its depth is measured on the tree before anything
/// else reads it.
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
    if statements
        .iter()
        .any(|statement| depth::depth(statement) > SQL_DEPTH)
    {
        return Err(shape::nested());
    }
    let (Some(mut statement), true) = (statements.pop(), statements.is_empty()) else {
        let message = "the text is not one query";
        return Err(SqlError::new(SqlErrorKind::Syntax, message));
    };
    let filters = shape::check(&statement)?;
    shape::respell(&mut statement);
    Ok((statement, filters))
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
