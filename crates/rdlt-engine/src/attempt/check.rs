//! Checking a planned stream against the source's catalog and the destination's capabilities.

use rdlt_connector::{Catalog, Field, LogicalType, ReadMode, StreamSpec, TypeKind};

use super::RunContext;
use crate::error::{Code, Error, codes};
use crate::plan::{DeleteMode, OnTruncate, RetentionLoss, StreamPlan, WriteMode};

/// The stream's catalog entry, once the source can read it as planned and the destination can
/// write it as planned.
pub(super) fn check_stream<'a>(
    context: &RunContext,
    plan: &StreamPlan,
    catalog: &'a Catalog,
) -> Result<&'a StreamSpec, Error> {
    let name = plan.name();
    let refuse = |code: Code, detail: &str| {
        Error::config(format!("stream {name}: {detail}"))
            .with_code(code)
            .with_stream(name)
    };
    let spec = catalog.get(name).ok_or_else(|| {
        refuse(
            codes::STREAM_NOT_FOUND,
            "the source's catalog has no such stream",
        )
    })?;
    if plan.read_mode() == ReadMode::Full && !spec.is_replayable() {
        let detail = "a full read starts again from the beginning, which a source that cannot \
                      read again no longer holds once it was told a cursor past it";
        return Err(refuse(codes::FULL_READ_UNREPLAYABLE, detail));
    }
    if plan.retention_loss() == RetentionLoss::Reset
        && (plan.read_mode() != ReadMode::Incremental || !spec.is_replayable())
    {
        let detail = "only a stream read incrementally, from a source that can read it again, \
                      reads a partition again from its earliest after a retention loss: a full \
                      read would load its rows twice, a change stream would miss changes, and a \
                      source that cannot read again has no earliest to serve";
        return Err(refuse(codes::RETENTION_RESET_UNSUPPORTED, detail));
    }
    if !spec.supports(plan.read_mode()) {
        let detail = format!("the source cannot read it as {:?}", plan.read_mode());
        return Err(refuse(codes::READ_MODE_UNSUPPORTED, &detail));
    }
    let modes = context.destination.capabilities().write_modes;
    let writable = match plan.write_mode() {
        WriteMode::Append => modes.append,
        WriteMode::Replace => modes.replace,
        WriteMode::Merge => modes.merge,
        WriteMode::History => modes.history,
    };
    if !writable {
        let detail = format!("the destination cannot write {:?}", plan.write_mode());
        return Err(refuse(codes::WRITE_MODE_UNSUPPORTED, &detail));
    }
    if plan.keeps_history()
        && let Some(detail) = change_time_invalid(spec)
    {
        return Err(refuse(codes::CHANGE_TIME_INVALID, &detail));
    }
    let types = &context.destination.capabilities().types;
    if plan.keeps_history()
        && !types.contains(&TypeKind::Timestamp)
        && !types.contains(&TypeKind::Int64)
    {
        let detail = "the destination stores neither timestamps nor 64-bit integers, one of which \
                      holds when a history table's versions begin and end";
        return Err(refuse(codes::HISTORY_VALIDITY_UNSUPPORTED, detail));
    }
    if let Some((code, detail)) = unmergeable(context, plan) {
        return Err(refuse(code, &detail));
    }
    Ok(spec)
}

/// Refuses `plan`'s exclusion of a column its stream, `spec`, needs: one of the source's primary
/// key, or the source's change time.
pub(super) fn check_excluded(plan: &StreamPlan, spec: &StreamSpec) -> Result<(), Error> {
    let mut key = spec.primary_key().into_iter().flatten();
    let needed = match key.find(|column| plan.excludes(column)) {
        Some(column) => format!("column {column} is of the source's primary key"),
        None => match spec.change_time().filter(|time| plan.excludes(time)) {
            Some(time) => format!("column {time} is the source's change time"),
            None => return Ok(()),
        },
    };
    let name = plan.name();
    Err(
        Error::config(format!("stream {name}: {needed}, which it cannot exclude"))
            .with_code(codes::PLAN_EXCLUDE_INVALID)
            .with_stream(name),
    )
}

/// Why the destination cannot merge `plan`'s change stream as planned, if it is one and cannot.
fn unmergeable(context: &RunContext, plan: &StreamPlan) -> Option<(Code, String)> {
    if !plan.merges_changes() {
        return None;
    }
    let capabilities = context.destination.capabilities();
    if !capabilities.merge_changes {
        let detail = "the destination does not merge change streams".to_owned();
        return Some((codes::CHANGE_MERGE_UNSUPPORTED, detail));
    }
    let deletes = capabilities.delete_modes;
    let (hard, soft) = removals(plan);
    if (hard && !deletes.hard) || (soft && !deletes.soft) {
        let detail = format!(
            "the destination cannot remove rows as its deletes ({:?}) and truncates ({:?}) do",
            plan.delete_mode(),
            plan.truncate_mode()
        );
        return Some((codes::DELETE_MODE_UNSUPPORTED, detail));
    }
    None
}

/// Why the stream's change time cannot begin its versions, if it cannot: it names a nested
/// column, or a column the stream's declared schema lacks or holds as something other than a
/// date or timestamp.
fn change_time_invalid(spec: &StreamSpec) -> Option<String> {
    let column = spec.change_time()?;
    let mut segments = column.segments();
    let (Some(name), None) = (segments.next(), segments.next()) else {
        return Some(format!("its change time {column} names a nested column"));
    };
    let declared = spec.schema()?;
    match declared.field(name).map(Field::logical_type) {
        None => Some(format!("its change time {column} is none of its columns")),
        Some(LogicalType::Timestamp(..) | LogicalType::Date) => None,
        Some(other) => Some(format!(
            "its change time {column} holds {other}, not a time"
        )),
    }
}

/// Whether a change stream merged by key removes rows outright, and whether it marks them
/// deleted: its deletes as their mode says, and its truncates as its deletes do (outright when
/// deletes are ignored).
fn removals(plan: &StreamPlan) -> (bool, bool) {
    let truncates = plan.truncate_mode() == OnTruncate::Apply;
    match plan.delete_mode() {
        DeleteMode::Soft => (false, true),
        DeleteMode::Ignore => (truncates, false),
        _ => (true, false),
    }
}
