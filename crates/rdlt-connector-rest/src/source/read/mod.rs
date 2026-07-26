//! The per-stream read loop: dispatches a stream to a parentless sequence or
//! a parent-child fan-out, tracks the incremental cursor, and checkpoints.
//! The request sequence itself lives in `driver`; the fan-out in `fanout`;
//! placeholder resolution in `resolve`; the paginator families in
//! `paginate`; record extraction in `extract` (all crate-private).

pub(crate) mod driver;
pub(crate) mod extract;
pub(crate) mod fanout;
pub mod paginate;
pub(crate) mod resolve;

use rdlt_connector::core::crash_point;
use rdlt_connector::{Cursor, RecordsOut, SourceError};
use serde_json::Value;

use super::client::RestClient;
use super::config::{Incremental, RestConfig, RestStream};
use driver::SequenceDriver;
use extract::Selector;

/// Read one stream (with parent fan-out when declared) into `out`.
pub async fn read_stream(
    config: &RestConfig,
    client: &RestClient,
    stream: &RestStream,
    since: Option<&Cursor>,
    out: &mut RecordsOut,
) -> Result<(), SourceError> {
    let incremental = stream.effective_incremental();
    let since_value: Option<String> = since.and_then(|c| render_scalar(c.as_value()));
    // Max observed cursor value; starts at the resume point so an empty page
    // can never move the cursor backwards.
    let mut max_cursor: Option<String> = since_value
        .clone()
        .or_else(|| incremental.as_ref().and_then(|i| i.initial_value.clone()));

    match &stream.parent {
        None => {
            read_sequence(config, client, stream, &incremental, &mut max_cursor, out).await?;
        }
        Some(parent_config) => {
            // Fan-out: re-read the parent stream's pages (a fresh, cursor-less
            // pass — the parent stream itself is separately synced if
            // declared for loading) and issue one child sequence per parent
            // record. Bounded buffering: values only, never whole parent records.
            let parent_stream = config
                .streams
                .iter()
                .find(|s| s.name == parent_config.stream)
                .ok_or_else(|| {
                    SourceError::fatal(format!(
                        "stream `{}`: parent stream `{}` is not declared",
                        stream.name, parent_config.stream
                    ))
                })?;
            let mut parent_values = Vec::new();
            fanout::collect_parents(
                config,
                client,
                parent_stream,
                parent_config,
                &mut parent_values,
            )
            .await?;
            // Every child window starts at the SAME resume point (the
            // committed cursor / initial_value already seeded into
            // max_cursor) — never at a sibling's in-flight progress.
            let start_value = max_cursor.clone();
            fanout::read_children(
                config,
                client,
                stream,
                &incremental,
                &start_value,
                &mut max_cursor,
                parent_values,
                out,
            )
            .await?;
        }
    }
    // Child streams and cursor streams checkpoint only at FEED END: their pages
    // do not carry a monotonic per-page position, so an intermediate checkpoint
    // could not be safely resumed from. Per-page checkpoints happen inside
    // read_sequence only for parentless streams.
    if stream.parent.is_some()
        && incremental.is_some()
        && let Some(max) = &max_cursor
    {
        crash_point!(
            "rest.checkpoint",
            Err(SourceError::fatal("injected crash at rest.checkpoint"))
        );
        let _ = out.checkpoint(Cursor::new(max.clone())).await;
    }
    Ok(())
}

async fn read_sequence(
    config: &RestConfig,
    client: &RestClient,
    stream: &RestStream,
    incremental: &Option<Incremental>,
    max_cursor: &mut Option<String>,
    out: &mut RecordsOut,
) -> Result<(), SourceError> {
    let selector = parse_selector(stream)?;
    let mut paginator = paginate::from_config(&stream.pagination).map_err(SourceError::fatal)?;
    let need_values = incremental.is_some();
    let mut driver =
        SequenceDriver::new(config, stream, client, &mut *paginator, need_values, None);
    // max_cursor was already seeded from the resume point (committed cursor or
    // initial_value) in read_stream, so it is the value to bind.
    driver.bind_incremental(incremental, &max_cursor.clone());

    loop {
        let Some(extracted) = driver.fetch_page(&selector).await? else {
            break;
        };
        if extracted.count > 0 {
            // Cursor update BEFORE pushing, so the checkpoint that follows
            // the rows covers exactly them.
            if let Some(inc) = incremental {
                let values = extracted.values.as_deref().unwrap_or_default();
                update_max_cursor(values, &inc.cursor_field, max_cursor);
            }
            if out.raw_json(extracted.records.clone()).await.is_err() {
                return Ok(()); // closed channel = cancellation
            }
            // Parentless streams checkpoint per page (unchanged behavior);
            // children checkpoint at feed end (read_stream).
            if incremental.is_some()
                && let Some(max) = &*max_cursor
            {
                crash_point!(
                    "rest.checkpoint",
                    Err(SourceError::fatal("injected crash at rest.checkpoint"))
                );
                if out.checkpoint(Cursor::new(max.clone())).await.is_err() {
                    return Ok(());
                }
            }
        }
        if !driver.advance(&extracted)? {
            break;
        }
    }
    Ok(())
}

/// Parse a stream's `records_path` into a selector (absent = body-is-array).
pub(crate) fn parse_selector(stream: &RestStream) -> Result<Option<Selector>, SourceError> {
    stream
        .records_path
        .as_deref()
        .map(|p| Selector::parse(p).map_err(SourceError::fatal))
        .transpose()
}

/// Advance the max-observed cursor across a page's records. ISO-8601
/// timestamps and equal-width numerics order lexicographically — the
/// documented constraint on REST cursor fields.
pub(crate) fn update_max_cursor(records: &[Value], field: &str, max: &mut Option<String>) {
    for item in records {
        let Some(value) = item.get(field) else {
            continue;
        };
        let Some(rendered) = render_scalar(value) else {
            continue;
        };
        if max.as_ref().is_none_or(|current| rendered > *current) {
            *max = Some(rendered);
        }
    }
}

/// Render a JSON scalar (string or number) to its cursor/param string form.
/// Cursor and resume-param values are string-or-number by contract; anything
/// else yields `None` (skipped, never guessed).
fn render_scalar(value: &Value) -> Option<String> {
    match value {
        Value::String(s) => Some(s.clone()),
        Value::Number(n) => Some(n.to_string()),
        _ => None,
    }
}
