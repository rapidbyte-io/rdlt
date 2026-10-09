//! Checking a planned stream against the source's catalog and the destination's capabilities.

use rdlt_connector::{Catalog, Field, LogicalType, ReadMode, StreamSpec, TypeKind};

use super::RunContext;
use crate::error::Error;
use crate::plan::{DeleteMode, OnTruncate, RetentionLoss, StreamPlan, WriteMode};

/// The stream's catalog entry, once the source can read it as planned and the destination can
/// write it as planned.
pub(super) fn check_stream<'a>(
    context: &RunContext,
    plan: &StreamPlan,
    catalog: &'a Catalog,
) -> Result<&'a StreamSpec, Error> {
    let name = plan.name();
    let refuse = |code: &str, detail: &str| {
        Error::config(format!("stream {name}: {detail}"))
            .with_code(code)
            .with_stream(name)
    };
    let spec = catalog.get(name).ok_or_else(|| {
        refuse(
            "stream_not_found",
            "the source's catalog has no such stream",
        )
    })?;
    if let Some((code, detail)) = unreplayable(context, plan, spec) {
        return Err(refuse(code, detail));
    }
    if plan.retention_loss() == RetentionLoss::Reset
        && (plan.read_mode() != ReadMode::Incremental || !spec.is_replayable())
    {
        let detail = "only a stream read incrementally, from a source that can read it again, \
                      reads a partition again from its earliest after a retention loss: a full \
                      read would load its rows twice, a change stream would miss changes, and a \
                      source that cannot read again has no earliest to serve";
        return Err(refuse("retention_reset_unsupported", detail));
    }
    if !spec.supports(plan.read_mode()) {
        let detail = format!("the source cannot read it as {:?}", plan.read_mode());
        return Err(refuse("read_mode_unsupported", &detail));
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
        return Err(refuse("write_mode_unsupported", &detail));
    }
    if plan.keeps_history()
        && let Some(detail) = change_time_invalid(spec)
    {
        return Err(refuse("change_time_invalid", &detail));
    }
    let types = &context.destination.capabilities().types;
    if plan.keeps_history()
        && !types.contains(&TypeKind::Timestamp)
        && !types.contains(&TypeKind::Int64)
    {
        let detail = "the destination stores neither timestamps nor 64-bit integers, one of which \
                      holds when a history table's versions begin and end";
        return Err(refuse("history_validity_unsupported", detail));
    }
    if let Some((code, detail)) = unmergeable(context, plan) {
        return Err(refuse(code, &detail));
    }
    Ok(spec)
}

/// Why the destination cannot merge `plan`'s change stream as planned, if it is one and cannot.
fn unmergeable(context: &RunContext, plan: &StreamPlan) -> Option<(&'static str, String)> {
    if !plan.merges_changes() {
        return None;
    }
    let capabilities = context.destination.capabilities();
    if !capabilities.merge_changes {
        let detail = "the destination does not merge change streams".to_owned();
        return Some(("change_merge_unsupported", detail));
    }
    let deletes = capabilities.delete_modes;
    let (hard, soft) = removals(plan);
    if (hard && !deletes.hard) || (soft && !deletes.soft) {
        let detail = format!(
            "the destination cannot remove rows as its deletes ({:?}) and truncates ({:?}) do",
            plan.delete_mode(),
            plan.truncate_mode()
        );
        return Some(("delete_mode_unsupported", detail));
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

/// Why a stream whose source cannot read again what it acknowledged cannot load as planned: replay
/// goes by partitions' positions, which a full read does not keep, and needs a log.
fn unreplayable(
    context: &RunContext,
    plan: &StreamPlan,
    spec: &StreamSpec,
) -> Option<(&'static str, &'static str)> {
    if spec.is_replayable() {
        return None;
    }
    match plan.read_mode() {
        ReadMode::Full => Some((
            "full_read_unreplayable",
            "a full read starts again from the beginning, which its source cannot read again once \
             it acknowledged it",
        )),
        ReadMode::Incremental | ReadMode::Cdc if context.wal.is_none() => Some((
            "wal_required",
            "its source cannot read again what it acknowledged, so its loads keep a write-ahead \
             log, and the engine has nowhere to keep one",
        )),
        ReadMode::Incremental | ReadMode::Cdc => None,
    }
}
