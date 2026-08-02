//! The read path: one stream, paged by keyset inside a consistent
//! snapshot, with LOB content fetched through the driver's locator
//! reads.
//!
//! Paging is by ROWID keyset rather than OFFSET because OFFSET makes
//! the server re-walk the prefix on every page (quadratic on a large
//! table) while ROWID is a physical address with a total order and a
//! usable index. Page SIZE comes from the described column widths
//! (see [`super::schema::rows_per_page`]) so a reply always fits one
//! SDU packet.

use std::ops::ControlFlow;

use rdlt_connector_sdk::source::Feed;
use rdlt_connector_sdk::spi::SourceError;

use super::client::{Client, quote_upper, value_to_json_typed};
use super::config::Stream;
use super::cursor::{OracleCursor, checked_watermark_literal};
use super::schema::{DEFAULT_SDU_BYTES, is_lob, logical_type, rows_per_page};

/// Read one stream to completion, checkpointing as pages land.
///
/// Returns `false` when the host hung up (the sdk's cancellation
/// contract) — the caller returns Ok promptly.
pub(crate) async fn read_stream(
    mut client: Client,
    stream: &Stream,
    cursor: &mut OracleCursor,
    feed: &mut Feed,
) -> Result<bool, SourceError> {
    let table = quote_upper(&stream.table);
    // CONSISTENCY, stated exactly: each PAGE is its own read-consistent
    // snapshot; the stream as a whole is not one snapshot. A
    // transaction-wide snapshot (`SET TRANSACTION READ ONLY`) was
    // implemented and REMOVED on evidence — probed, it leaves this
    // driver unable to answer a re-executed statement afterwards
    // (statement-cache/cursor reuse across the transaction change),
    // turning a healthy read into a dead connection. What makes the
    // stream converge is the WATERMARK: rows committed mid-read either
    // land in a later page or above the recorded watermark, so the
    // next run collects them. Append-only and watermarked tables are
    // therefore complete; a table rewritten beneath a long read is not
    // (recorded — the shape the postgres source's non-CDC reads carry).

    // The watermark predicate rides into SQL as a literal, so its
    // shape is checked first (the cursor document is operator-
    // editable — see checked_watermark_literal).
    let watermark_predicate = match (&stream.cursor, cursor.streams.get(&stream.name)) {
        (Some(column), Some(state)) => {
            let literal = checked_watermark_literal(&state.watermark)?;
            Some(format!("{} > {literal}", quote_upper(column)))
        }
        _ => None,
    };

    let mut page_rows: Option<u32> = None;
    let mut types: Vec<Option<rdlt_connector_sdk::spi::core::LogicalType>> = Vec::new();
    let mut last_rowid: Option<String> = None;

    loop {
        let mut predicates = Vec::new();
        if let Some(predicate) = &watermark_predicate {
            predicates.push(predicate.clone());
        }
        if let Some(rowid) = &last_rowid {
            // ROWID literals are the server's own opaque strings; the
            // shape check keeps an edited cursor from injecting.
            if !rowid
                .chars()
                .all(|c| c.is_ascii_alphanumeric() || c == '+' || c == '/')
            {
                return Err(SourceError::fatal(
                    "resume rowid has an unexpected shape — refusing to interpolate it",
                ));
            }
            predicates.push(format!("ROWID > CHARTOROWID('{rowid}')"));
        }
        let where_clause = if predicates.is_empty() {
            String::new()
        } else {
            format!(" WHERE {}", predicates.join(" AND "))
        };
        let limit = page_rows.unwrap_or(100);
        let sql = format!(
            "SELECT t.*, ROWIDTOCHAR(t.ROWID) AS RDLT_ROWID FROM {table} t{where_clause} \
             ORDER BY t.ROWID FETCH FIRST {limit} ROWS ONLY"
        );

        let (returned, page) = client
            .query(&format!("reading `{}`", stream.name), &sql, &[])
            .await?;
        client = returned;

        // The FIRST page teaches us the widths AND settles the types:
        // an unreadable column must refuse before any row is pushed,
        // not halfway through a load.
        if page_rows.is_none() {
            let mut resolved = Vec::with_capacity(page.columns.len());
            for column in &page.columns {
                if column.name.eq_ignore_ascii_case("RDLT_ROWID") {
                    resolved.push(None);
                    continue;
                }
                resolved.push(Some(logical_type(column).map_err(SourceError::fatal)?));
            }
            types = resolved;
            page_rows = Some(rows_per_page(&page.columns, DEFAULT_SDU_BYTES));
        }
        if page.rows.is_empty() {
            break;
        }

        let rowid_at = page
            .columns
            .iter()
            .position(|c| c.name.eq_ignore_ascii_case("RDLT_ROWID"))
            .ok_or_else(|| SourceError::fatal("the page did not carry its rowid column"))?;
        let cursor_at = stream.cursor.as_ref().and_then(|name| {
            page.columns
                .iter()
                .position(|c| c.name.eq_ignore_ascii_case(name))
        });

        let mut saw_cursor_value = false;
        let mut ndjson = Vec::new();
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
                    let declared = types
                        .get(index)
                        .copied()
                        .flatten()
                        .unwrap_or(rdlt_connector_sdk::spi::core::LogicalType::Utf8);
                    value_to_json_typed(value, declared)
                        .map_err(|e| SourceError::fatal(format!("column `{}`: {e}", column.name)))?
                };
                object.insert(column.name.to_lowercase(), rendered);
            }
            serde_json::to_writer(&mut ndjson, &serde_json::Value::Object(object))
                .map_err(|e| SourceError::fatal(format!("rendering a row: {e}")))?;
            ndjson.push(b'\n');

            // The watermark is raised through the cursor's OWN ordering
            // (numeric where both sides are numbers, lexical otherwise).
            // Comparing here with `String` ordering was a real defect:
            // "99" sorts above "150", so a 150-row read checkpointed 99
            // and the next run re-read 51 rows.
            if let Some(at) = cursor_at
                && let Some(value) = row.values().get(at)
                && let Some(text) = watermark_text(value)
            {
                saw_cursor_value = true;
                cursor.advance(&stream.name, &text);
            }
        }

        last_rowid = page
            .rows
            .last()
            .and_then(|row| row.values().get(rowid_at))
            .and_then(|value| match value {
                oracle_rs::row::Value::String(s) => Some(s.clone()),
                _ => None,
            });

        if feed.raw_json(bytes::Bytes::from(ndjson)).await == ControlFlow::Break(()) {
            return Ok(false);
        }
        // The checkpoint lands only after the rows it covers REACHED
        // the host — a cursor promising rows the host never saw would
        // lose them on resume.
        if saw_cursor_value {
            rdlt_connector_sdk::spi::core::crash_point!(
                "ora.checkpoint",
                Err(SourceError::fatal("injected crash at ora.checkpoint"))
            );
            if feed.checkpoint(cursor.encode()).await == ControlFlow::Break(()) {
                return Ok(false);
            }
        }

        if (page.rows.len() as u32) < limit {
            break; // a short page is the last page
        }
    }
    Ok(true)
}

/// The comparable rendering of a cursor value.
fn watermark_text(value: &oracle_rs::row::Value) -> Option<String> {
    use oracle_rs::row::Value;
    match value {
        Value::Integer(i) => Some(i.to_string()),
        Value::Float(f) => Some(f.to_string()),
        Value::Number(n) => Some(n.as_str().to_owned()),
        Value::String(s) => Some(s.clone()),
        Value::Date(d) => Some(format!(
            "{:04}-{:02}-{:02}T{:02}:{:02}:{:02}",
            d.year, d.month, d.day, d.hour, d.minute, d.second
        )),
        Value::Timestamp(t) => Some(format!(
            "{:04}-{:02}-{:02}T{:02}:{:02}:{:02}.{:06}",
            t.year, t.month, t.day, t.hour, t.minute, t.second, t.microsecond
        )),
        _ => None,
    }
}
