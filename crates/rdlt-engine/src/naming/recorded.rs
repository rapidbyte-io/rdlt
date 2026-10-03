//! Identifiers a destination's state records, held to the rules that could have given them.
//!
//! The engine changes, writes and drops a table under the name state records for it, so a name
//! no assignment could have made, as one under a prefix the destination keeps for its own tables,
//! is refused before anything uses it; one two tables share is refused as the tables are
//! read from state (`Tables::committed`). A source column recorded under a metadata column's
//! identifier is refused too: the table would hold one column for two.

use rdlt_connector::PipelineState;

use super::Naming;
use crate::error::{Error, ErrorKind};

/// Checks every identifier `state` records against `naming`.
///
/// # Errors
///
/// A table or column name the rules could not have given, and a column named as a metadata
/// column is, are `state_invalid`, a Destination error.
pub(crate) fn check(naming: &Naming, state: &PipelineState) -> Result<(), Error> {
    for (path, table) in &state.tables {
        if let Some(physical) = table.physical.as_deref()
            && !naming.admits_table(physical)
        {
            return Err(refused(format!(
                "table {path} is recorded under a name the destination's rules do not give"
            )));
        }
        if table.names.iter().any(|(_, name)| naming.is_metadata(name)) {
            return Err(refused(format!(
                "a column of table {path} is recorded under a metadata column's name"
            )));
        }
        if !table.names.iter().all(|(_, name)| naming.admits(name)) {
            return Err(refused(format!(
                "a column of table {path} is recorded under a name the destination's rules do \
                 not give"
            )));
        }
    }
    Ok(())
}

fn refused(message: impl std::fmt::Display) -> Error {
    Error::new(
        ErrorKind::Destination,
        format!("reading pipeline state: {message}"),
    )
    .with_code("state_invalid")
}
