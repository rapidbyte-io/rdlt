//! Refusing a stream's include and transforms where no batch is needed to refuse them.

#[cfg(test)]
mod tests;

use std::collections::BTreeSet;

use rdlt_connector::{ColumnPath, META_PREFIX, OP_COLUMN, ReadMode, SEQ_COLUMN, UNCHANGED_COLUMN};

use super::{DeleteMode, StreamPlan, WriteMode};
use crate::error::{Code, Error, codes};
use crate::limits::{LITERAL_BYTES, NAMED_COLUMNS, TRANSFORMS_PER_STREAM};
use crate::transform::{Mask, Op, TransformStep};

/// Refuses `stream`'s include and transforms where the stream cannot carry them.
pub(super) fn check(stream: &StreamPlan) -> Result<(), Error> {
    check_include(stream)?;
    let steps = stream.transforms();
    if steps.len() > TRANSFORMS_PER_STREAM {
        let reason = format!(
            "it lists {} transforms, beyond the limit of {TRANSFORMS_PER_STREAM}",
            steps.len()
        );
        return refuse(stream, codes::TRANSFORM_INVALID, &reason);
    }
    let mut written = BTreeSet::new();
    for step in steps {
        if let TransformStep::Op(op) = step {
            check_op(stream, op, &mut written)?;
        }
    }
    check_filters(stream)
}

fn refuse(stream: &StreamPlan, code: Code, reason: &str) -> Result<(), Error> {
    Err(Error::config(format!("stream {}: {reason}", stream.name()))
        .with_code(code)
        .with_stream(stream.name()))
}

fn check_include(stream: &StreamPlan) -> Result<(), Error> {
    let Some(included) = stream.included() else {
        return Ok(());
    };
    let invalid = |reason: &str| refuse(stream, codes::PLAN_INCLUDE_INVALID, reason);
    if stream.excluded().next().is_some() {
        return invalid("it includes and excludes columns; a stream takes one or the other");
    }
    if included.is_empty() {
        return invalid("it includes no column");
    }
    if included.len() > NAMED_COLUMNS {
        return invalid(&format!("it includes more than {NAMED_COLUMNS} columns"));
    }
    if let Some(column) = included.iter().find(|column| column.segments().count() > 1) {
        let reason = format!("column {column} is nested; a stream includes top-level columns");
        return refuse(stream, codes::PLAN_COLUMN_NESTED, &reason);
    }
    Ok(())
}

/// What `name` is that no op may name: a change column, or a column of the plan's key.
fn held(stream: &StreamPlan, name: &str) -> Option<&'static str> {
    if matches!(name, OP_COLUMN | SEQ_COLUMN | UNCHANGED_COLUMN) {
        return Some("a change column");
    }
    let mut key = stream.merge_key().into_iter().flatten();
    key.any(|column| column.segments().next() == Some(name))
        .then_some("a column of its key")
}

/// Refuses `op` where it names more columns than an op may, a column the stream keeps as it
/// arrives, a name an earlier op gave (which `written` holds), or a name or literal it may not
/// hold.
fn check_op(stream: &StreamPlan, op: &Op, written: &mut BTreeSet<String>) -> Result<(), Error> {
    let invalid = |reason: &str| refuse(stream, codes::TRANSFORM_INVALID, reason);
    let (reads, writes) = (op.reads(), op.writes());
    if reads.len().max(writes.len()) > NAMED_COLUMNS {
        return invalid(&format!("an op names more than {NAMED_COLUMNS} columns"));
    }
    for name in reads.iter().chain(&writes) {
        if name.is_empty() {
            return invalid("an op names a column with no name");
        }
        if let Some(what) = held(stream, name) {
            return invalid(&format!(
                "an op names column {name}, which is {what}; keys and change columns pass every \
                 transform as they arrive"
            ));
        }
    }
    for name in writes {
        if name.starts_with(META_PREFIX) {
            return invalid(&format!(
                "an op names column {name}, beginning {META_PREFIX} as the engine's own columns do"
            ));
        }
        if written.contains(&name) {
            return invalid(&format!("its ops give column {name} its name twice"));
        }
        written.insert(name);
    }
    match op {
        Op::Rename(pairs) => check_rename(stream, pairs),
        Op::Mask { columns, with } => check_mask(stream, columns, with),
        Op::Constant(columns) => {
            let mut long = columns
                .iter()
                .filter(|(_, value)| value.bytes() > LITERAL_BYTES);
            match long.next() {
                Some((name, _)) => invalid(&format!(
                    "constant {name} holds more than {LITERAL_BYTES} bytes"
                )),
                None => Ok(()),
            }
        }
    }
}

/// Refuses a rename naming one column twice, or giving a column a name it renames, its own
/// among them.
fn check_rename(stream: &StreamPlan, pairs: &[(String, String)]) -> Result<(), Error> {
    let invalid = |reason: &str| refuse(stream, codes::TRANSFORM_INVALID, reason);
    let from: BTreeSet<&str> = pairs.iter().map(|(from, _)| from.as_str()).collect();
    if from.len() < pairs.len() {
        return invalid("a rename names one column twice");
    }
    match pairs.iter().find(|(_, to)| from.contains(to.as_str())) {
        Some((_, to)) => invalid(&format!("a rename gives column {to} a name it renames")),
        None => Ok(()),
    }
}

/// Refuses a mask naming no column, one path twice or within another, an empty key, or a
/// redaction longer than a literal may be.
fn check_mask(stream: &StreamPlan, columns: &[ColumnPath], with: &Mask) -> Result<(), Error> {
    let invalid = |reason: &str| refuse(stream, codes::TRANSFORM_INVALID, reason);
    if columns.is_empty() {
        return invalid("a mask names no column");
    }
    // A path within another sorts after it, and so does every path between them, so comparing
    // neighbours finds any two paths one within the other.
    let mut sorted: Vec<&ColumnPath> = columns.iter().collect();
    sorted.sort_unstable();
    for pair in sorted.windows(2) {
        let (outer, inner) = (pair[0], pair[1]);
        if within(outer, inner) {
            return invalid(&format!(
                "a mask names {outer} and {inner}, one within the other"
            ));
        }
    }
    match with {
        Mask::HmacSha256(key) if key.expose().is_empty() => {
            invalid("an `HMAC-SHA256` mask's key is empty")
        }
        Mask::Redact(text) if text.len() > LITERAL_BYTES => invalid(&format!(
            "a redaction holds more than {LITERAL_BYTES} bytes"
        )),
        Mask::HmacSha256(_) | Mask::Redact(_) | Mask::Null => Ok(()),
    }
}

/// Whether `inner` is `outer` or a field within it.
fn within(outer: &ColumnPath, inner: &ColumnPath) -> bool {
    let mut inner = inner.segments();
    outer
        .segments()
        .all(|segment| inner.next() == Some(segment))
}

/// Refuses a filter where a row it stops keeping would stay in the stream's table.
fn check_filters(stream: &StreamPlan) -> Result<(), Error> {
    let mut steps = stream.transforms().iter();
    if !steps.any(|step| step.as_transform().filters()) {
        return Ok(());
    }
    let reason = match (
        stream.write_mode(),
        stream.read_mode(),
        stream.delete_mode(),
    ) {
        (WriteMode::Append | WriteMode::Replace, _, _)
        | (
            WriteMode::Merge | WriteMode::History,
            ReadMode::Cdc,
            DeleteMode::Hard | DeleteMode::Soft,
        ) => {
            return Ok(());
        }
        (WriteMode::Merge | WriteMode::History, ReadMode::Cdc, DeleteMode::Ignore) => {
            "a row that stops matching its filter becomes a delete, which the stream ignores, so \
             the row would stay in its table"
        }
        (WriteMode::Merge | WriteMode::History, _, _) => {
            "a row that stops matching its filter would stay in its table: only a change stream \
             carries the delete that removes it"
        }
    };
    refuse(stream, codes::TRANSFORM_INVALID, reason)
}
