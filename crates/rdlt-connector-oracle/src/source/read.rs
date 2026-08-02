//! The read path: one stream, paged by a keyset the CURSOR defines,
//! with LOB content fetched through the driver's locator reads.
//!
//! The keyset is `(cursor, ROWID)` when a cursor column is
//! configured, and `ROWID` alone otherwise. That choice is the whole
//! correctness story for resume: a checkpoint may only promise that
//! everything BELOW the watermark has been delivered, and that is
//! true only when rows arrive in watermark order. Paging by ROWID
//! while checkpointing a cursor value would promise rows the run had
//! never looked at — ROWID order has no relation to column order —
//! so a mid-stream failure would skip them forever.
//!
//! Page SIZE comes from the widths the server described (see
//! [`super::schema::rows_per_page`]) because one reply must fit one
//! SDU packet, and the driver's prefetch is raised to match so a
//! short page always means "the last page" rather than "the driver
//! stopped early".

use std::ops::ControlFlow;

use rdlt_connector_sdk::source::Feed;
use rdlt_connector_sdk::spi::SourceError;
use rdlt_connector_sdk::spi::core::LogicalType;

use super::client::{Client, quote_upper, value_to_json_typed};
use super::config::Stream;
use super::cursor::{OracleCursor, checked_watermark_literal};
use super::schema::{is_lob, logical_type, rows_per_page};

/// The types a watermark can be built from: they order the same way
/// in SQL and in the persisted rendering.
fn cursor_capable(kind: LogicalType) -> bool {
    matches!(
        kind,
        LogicalType::Int64
            | LogicalType::Float64
            | LogicalType::Decimal { .. }
            | LogicalType::TimestampNaive
            | LogicalType::TimestampTz
            | LogicalType::Date
    )
}

/// Read one stream to completion, checkpointing as pages land.
///
/// Returns `false` when the host hung up (the sdk's cancellation
/// contract) — the caller returns Ok promptly.
pub(crate) async fn read_stream(
    mut client: Client,
    stream: &Stream,
    tuning: &super::config::Tuning,
    cursor: &mut OracleCursor,
    feed: &mut Feed,
) -> Result<bool, SourceError> {
    let table = quote_upper(&stream.table);
    let cursor_column = stream.cursor.as_deref().map(quote_upper);

    // Learn the shape BEFORE the first data page: the page size is
    // derived from the described widths, so asking for rows before
    // knowing them is the one request that could overflow the SDU.
    let describe_sql = format!("SELECT t.* FROM {table} t WHERE 1 = 0");
    let (returned, described) = client
        .query(&format!("describing `{}`", stream.name), &describe_sql, &[])
        .await?;
    client = returned;

    let mut types = Vec::with_capacity(described.columns.len());
    for column in &described.columns {
        types.push(logical_type(column).map_err(SourceError::fatal)?);
    }
    // A cursor column that is not in the projection, or cannot order,
    // would silently disable incremental reads — every run re-reading
    // the whole table while reporting success.
    if let Some(name) = &stream.cursor {
        let at = described
            .columns
            .iter()
            .position(|c| c.name.eq_ignore_ascii_case(name))
            .ok_or_else(|| {
                SourceError::fatal(format!(
                    "stream `{}`: cursor column `{name}` is not a column of `{}` — \
                     incremental reads would silently re-read everything",
                    stream.name, stream.table
                ))
            })?;
        if !cursor_capable(types[at]) {
            return Err(SourceError::fatal(format!(
                "stream `{}`: cursor column `{name}` is {:?}, which has no usable order \
                 for a watermark — choose a numeric or timestamp column",
                stream.name, types[at]
            )));
        }
    }
    // The rowid column rides along at the END of every page's
    // projection, so its index is known from the described width.
    let rowid_at = described.columns.len();
    // The derived page is the SAFETY bound (one reply, one SDU); an
    // operator's `page_rows` may only lower it — raising it past what
    // the SDU holds would truncate replies, which is the defect this
    // whole design exists to prevent.
    let derived = rows_per_page(&described.columns, tuning.sdu_bytes);
    let page_rows = tuning.page_rows.map_or(derived, |asked| asked.min(derived));
    // The driver returns at most its prefetch, and this read treats a
    // SHORT page as the last page — so the prefetch must cover the
    // page or the stream would end early and call it success.
    client = client.with_page_size(page_rows);

    let mut resume = cursor.streams.get(&stream.name).cloned();

    loop {
        let where_clause = match (&cursor_column, &resume) {
            (Some(column), Some(state)) => {
                let literal = checked_watermark_literal(&state.watermark)?;
                match &state.tie {
                    Some(tie) => format!(
                        " WHERE {column} > {literal} OR ({column} = {literal} AND \
                         t.ROWID > {})",
                        rowid_literal(tie)?
                    ),
                    None => format!(" WHERE {column} > {literal}"),
                }
            }
            // No cursor configured: a full read every run, ordered by
            // ROWID so the paging itself is stable.
            (None, _) => match &resume {
                Some(state) => match &state.tie {
                    Some(tie) => format!(" WHERE t.ROWID > {}", rowid_literal(tie)?),
                    None => String::new(),
                },
                None => String::new(),
            },
            (Some(_), None) => String::new(),
        };
        let order = match &cursor_column {
            Some(column) => format!("{column}, t.ROWID"),
            None => "t.ROWID".to_owned(),
        };
        let sql = format!(
            "SELECT t.*, ROWIDTOCHAR(t.ROWID) AS RDLT_ROWID FROM {table} t{where_clause} \
             ORDER BY {order} FETCH FIRST {page_rows} ROWS ONLY"
        );

        let (returned, page) = client
            .query(&format!("reading `{}`", stream.name), &sql, &[])
            .await?;
        client = returned;
        if page.rows.is_empty() {
            break;
        }

        let cursor_at = stream.cursor.as_ref().and_then(|name| {
            page.columns
                .iter()
                .position(|c| c.name.eq_ignore_ascii_case(name))
        });

        let mut ndjson = Vec::new();
        let mut last_seen: Option<(String, String)> = None;
        for row in &page.rows {
            let mut object = serde_json::Map::new();
            for (index, column) in page.columns.iter().enumerate() {
                if index == rowid_at {
                    continue; // bookkeeping, not data
                }
                let value = row
                    .values()
                    .get(index)
                    .ok_or_else(|| SourceError::fatal("row shorter than its column list"))?;
                let rendered = if is_lob(column) {
                    client.read_lob(value).await?
                } else {
                    let declared = types.get(index).copied().unwrap_or(LogicalType::Utf8);
                    value_to_json_typed(value, declared)
                        .map_err(|e| SourceError::fatal(format!("column `{}`: {e}", column.name)))?
                };
                object.insert(column.name.to_lowercase(), rendered);
            }
            serde_json::to_writer(&mut ndjson, &serde_json::Value::Object(object))
                .map_err(|e| SourceError::fatal(format!("rendering a row: {e}")))?;
            ndjson.push(b'\n');

            // The resume key of the row just rendered. Its absence is
            // fatal: without it the next page would repeat this one
            // forever, duplicating rows at full speed.
            let rowid = row
                .values()
                .get(rowid_at)
                .and_then(|value| match value {
                    oracle_rs::row::Value::String(s) => Some(s.clone()),
                    _ => None,
                })
                .ok_or_else(|| {
                    SourceError::fatal(
                        "a page arrived without its rowid column — refusing to page blindly",
                    )
                })?;
            let watermark = match cursor_at {
                Some(at) => row.values().get(at).and_then(watermark_text),
                None => None,
            };
            last_seen = Some((watermark.unwrap_or_default(), rowid));
        }

        if feed.raw_json(bytes::Bytes::from(ndjson)).await == ControlFlow::Break(()) {
            return Ok(false);
        }

        // Rows arrived in watermark order, so the LAST row's values
        // are a true low-water boundary: everything at or below them
        // has been delivered.
        if let Some((watermark, rowid)) = last_seen {
            let state = super::cursor::StreamCursor {
                watermark: watermark.clone(),
                tie: Some(rowid.clone()),
            };
            resume = Some(state);
            if cursor_at.is_some() {
                cursor.advance(&stream.name, &watermark, Some(&rowid));
                rdlt_connector_sdk::spi::core::crash_point!(
                    "ora.checkpoint",
                    Err(SourceError::fatal("injected crash at ora.checkpoint"))
                );
                if feed.checkpoint(cursor.encode()).await == ControlFlow::Break(()) {
                    return Ok(false);
                }
            }
        }

        if (page.rows.len() as u32) < page_rows {
            break; // a short page is the last page — the prefetch covers a full one
        }
    }
    Ok(true)
}

/// A ROWID travelling back into SQL as a literal. The shape check is
/// the injection gate: the persisted cursor is an operator-editable
/// document.
fn rowid_literal(rowid: &str) -> Result<String, SourceError> {
    if rowid.is_empty()
        || !rowid
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '+' || c == '/')
    {
        return Err(SourceError::fatal(format!(
            "resume rowid `{rowid}` has an unexpected shape — refusing to interpolate it \
             into SQL; clear the pipeline state"
        )));
    }
    Ok(format!("CHARTOROWID('{rowid}')"))
}

/// The comparable rendering of a cursor value.
///
/// Timestamps are rendered in ONE canonical shape — fraction and
/// offset always present — because the resume predicate parses them
/// back with a fixed format model. A shorter rendering (what a naive
/// `TIMESTAMP` or a `DATE` produces) would fail that parse on every
/// run after the first.
fn watermark_text(value: &oracle_rs::row::Value) -> Option<String> {
    use oracle_rs::row::Value;
    match value {
        Value::Integer(i) => Some(i.to_string()),
        Value::Float(f) => Some(f.to_string()),
        Value::Number(n) => Some(n.as_str().to_owned()),
        Value::String(s) => Some(s.clone()),
        Value::Date(d) => Some(format!(
            "{:04}-{:02}-{:02}T{:02}:{:02}:{:02}.000000+00:00",
            d.year, d.month, d.day, d.hour, d.minute, d.second
        )),
        Value::Timestamp(t) => Some(format!(
            "{:04}-{:02}-{:02}T{:02}:{:02}:{:02}.{:06}{:+03}:{:02}",
            t.year,
            t.month,
            t.day,
            t.hour,
            t.minute,
            t.second,
            t.microsecond,
            t.tz_hour_offset,
            t.tz_minute_offset.unsigned_abs()
        )),
        _ => None,
    }
}
