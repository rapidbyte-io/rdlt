//! What the merge refuses, each under a code that stays the same.
//!
//! A refusal is a condition whoever wrote the rows or changed the schema caused. It travels as
//! Arrow's error, which the merge's own failures are too, and [`failed`] gives it back as a
//! connector's error with its code.

use arrow_schema::ArrowError;
use rdlt_connector::ConnectorError;

/// A change flags its key or its sequence, which every change sets, unchanged.
pub(crate) const FLAG_ON_KEY: &str = "flag_on_key";
/// A change flags a field that is no column of its table unchanged.
pub(crate) const FLAG_ON_MISSING_COLUMN: &str = "flag_on_missing_column";
/// A change's unchanged flags are no bitmap of bytes.
pub(crate) const FLAGS_INVALID: &str = "flags_invalid";
/// A change names no op, or none a change stream has.
pub(crate) const OP_INVALID: &str = "op_invalid";
/// A row has no sequence.
pub(crate) const SEQUENCE_MISSING: &str = "sequence_missing";
/// A delete or a truncate of a history table whose deletes are soft says no deletion time.
pub(crate) const DELETION_UNTIMED: &str = "deletion_untimed";
/// A column's type converts to its table's by no conversion that keeps every value.
pub(crate) const TYPE_UNCONVERTIBLE: &str = "type_unconvertible";
/// A value does not fit the wider type its column took.
pub(crate) const VALUE_UNHOLDABLE: &str = "value_unholdable";
/// The table's merge key names no column, or one the rows lack.
pub(crate) const MERGE_KEY_INVALID: &str = "merge_key_invalid";

/// A refusal of the merge: its code and what was refused.
#[derive(Debug)]
struct Refused {
    code: &'static str,
    message: String,
}

impl std::fmt::Display for Refused {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.message)
    }
}

impl std::error::Error for Refused {}

/// The refusal coded `code` of what `message` says.
pub(crate) fn refused(code: &'static str, message: impl Into<String>) -> ArrowError {
    ArrowError::ExternalError(Box::new(Refused {
        code,
        message: message.into(),
    }))
}

/// The code `error` was refused under; none where it is a failure of the merge itself.
pub(crate) fn code(error: &ArrowError) -> Option<&'static str> {
    match error {
        ArrowError::ExternalError(source) => {
            source.downcast_ref::<Refused>().map(|refused| refused.code)
        }
        _ => None,
    }
}

/// `error`, met while `doing`, as a connector's: a refusal is a `Data` error under its code, and
/// anything else a failure of the merge itself.
pub(crate) fn failed(doing: &str, error: &ArrowError) -> ConnectorError {
    match code(error) {
        Some(code) => ConnectorError::data(format!("{doing}: {error}")).with_code(code),
        None => ConnectorError::internal(format!("{doing}: {error}")),
    }
}
