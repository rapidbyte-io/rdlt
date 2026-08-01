//! The per-stream read loop: dispatches a stream to a parentless sequence
//! or a parent-child fan-out, tracks the incremental cursor, and
//! checkpoints. The request sequence itself lives in `sequence`; the
//! fan-out in `fanout`; placeholder resolution in `resolve`; the
//! paginator families in [`paginate`]; record extraction in `extract`;
//! cursor tracking in `cursor`.

pub(crate) mod cursor;
pub(crate) mod extract;
pub(crate) mod fanout;
pub mod paginate;
pub(crate) mod resolve;
pub(crate) mod sequence;

use rdlt_connector::core::crash_point;
use rdlt_connector::{Cursor, RecordsOut, SourceError};

use super::config::{Config, Incremental, Stream};
use super::http::Client;
use super::select::Selector;
use sequence::Sequence;

/// Read one stream (with parent fan-out when declared) into `out`.
pub(crate) async fn deliver(
    config: &Config,
    client: &Client,
    stream: &Stream,
    since: Option<&Cursor>,
    out: &mut RecordsOut,
) -> Result<(), SourceError> {
    let incremental = stream.effective_incremental();
    let since_value: Option<String> = since.and_then(|c| cursor::render_scalar(c.as_value()));
    // Max observed cursor value; starts at the resume point so an empty
    // page can never move the cursor backwards.
    let mut max_cursor: Option<String> = since_value
        .clone()
        .or_else(|| incremental.as_ref().and_then(|i| i.initial_value.clone()));

    match &stream.parent {
        None => {
            deliver_sequence(config, client, stream, &incremental, &mut max_cursor, out).await?;
        }
        Some(parent_link) => {
            // Fan-out: re-read the parent stream's pages (a fresh,
            // cursor-less pass — the parent stream itself is separately
            // synced if declared for loading) and issue one child sequence
            // per parent record. Bounded buffering: values only, never
            // whole parent records.
            let parent_stream = config
                .streams
                .iter()
                .find(|s| s.name == parent_link.stream)
                .ok_or_else(|| {
                    SourceError::fatal(format!(
                        "stream `{}`: parent stream `{}` is not declared",
                        stream.name, parent_link.stream
                    ))
                })?;
            let mut parent_values = Vec::new();
            fanout::collect_parents(
                config,
                client,
                parent_stream,
                parent_link,
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
    // Child streams checkpoint only at FEED END: their pages do not carry a
    // monotonic per-page position, so an intermediate checkpoint could not
    // be safely resumed from. Per-page checkpoints happen inside
    // `deliver_sequence` only, for parentless streams.
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

async fn deliver_sequence(
    config: &Config,
    client: &Client,
    stream: &Stream,
    incremental: &Option<Incremental>,
    max_cursor: &mut Option<String>,
    out: &mut RecordsOut,
) -> Result<(), SourceError> {
    let selector = parse_selector(stream)?;
    let mut paginator = paginate::build(&stream.pagination).map_err(SourceError::fatal)?;
    let need_values = incremental.is_some();
    let mut sequence = Sequence::new(config, stream, client, &mut *paginator, need_values, None);
    // max_cursor was already seeded from the resume point (committed cursor
    // or initial_value) in `deliver`, so it is the value to bind.
    sequence.bind_incremental(incremental, &max_cursor.clone());

    loop {
        let Some(page) = sequence.fetch_page(&selector).await? else {
            break;
        };
        if page.count > 0 {
            // Cursor update BEFORE pushing, so the checkpoint that follows
            // the rows covers exactly them.
            if let Some(incremental) = incremental {
                let values = page.values.as_deref().unwrap_or_default();
                cursor::observe(values, &incremental.cursor_field, max_cursor);
            }
            if out.raw_json(page.records.clone()).await.is_err() {
                return Ok(()); // closed channel = cancellation
            }
            // Parentless streams checkpoint per page; children checkpoint
            // at feed end (`deliver`).
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
        if !sequence.advance(&page)? {
            break;
        }
    }
    Ok(())
}

/// Parse a stream's `records_path` into a selector (absent =
/// body-is-array).
pub(crate) fn parse_selector(stream: &Stream) -> Result<Option<Selector>, SourceError> {
    stream
        .records_path
        .as_deref()
        .map(|path| Selector::parse(path).map_err(SourceError::fatal))
        .transpose()
}
