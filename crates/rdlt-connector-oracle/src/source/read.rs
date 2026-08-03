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

use super::client::{Client, quote_table, quote_upper, value_to_json_typed};
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
    )
    // No `Date` arm: Oracle's DATE carries a time and maps to
    // TimestampNaive, so `logical_type` can never yield `Date`.
    // Listing it advertised a case that cannot occur.
}

/// Read one stream to completion, checkpointing as pages land.
///
/// Returns `false` when the host hung up (the sdk's cancellation
/// contract) — the caller returns Ok promptly.
/// How many pages one connection may issue before it is recycled.
///
/// EVERY page is a distinct SQL text (the watermark and ROWID are
/// interpolated), the driver's statement cache holds a server cursor
/// per distinct text, and NOTHING in the driver ever closes one:
/// `FunctionCode::CloseCursors` has no sender, `close_cursor` only
/// flips a local flag, and cache eviction drops entries silently. So
/// a long read walks into `ORA-01000`.
///
/// MEASURED, twice and independently: reads die at ~297 pages,
/// whatever the page size — stock Oracle allows 300 open cursors, and
/// raising the server to 20,000 lets the identical read finish. Below
/// that wall the connector reconnects, which costs one connect per
/// 250 pages and returns every cursor at once.
///
/// The RIGHT fix is for the driver to close its cursors; this is the
/// fix that does not require inventing an unprobed protocol message
/// against a driver whose own source warns that raw function messages
/// make this server hang up.
const PAGES_PER_CONNECTION: u32 = 250;

pub(crate) async fn read_stream(
    mut client: Client,
    config: &super::config::Config,
    stream: &Stream,
    tuning: &super::config::Tuning,
    cursor: &mut OracleCursor,
    feed: &mut Feed,
) -> Result<bool, SourceError> {
    let table = quote_table(&stream.table);
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
        // A NULLABLE cursor column is refused HERE, before a single
        // row moves.
        //
        // Refusing at the row was worse than useless: Oracle sorts
        // NULLs LAST, so the failure landed on the final page of the
        // FIRST run — after most of the table was delivered and
        // checkpointed. Every later run then resumed with
        // `WHERE c > :watermark`, which never matches NULL, so the
        // run went green and those rows were silently absent for the
        // life of the pipeline. The loud refusal was unreachable
        // exactly when it mattered.
        if described.columns[at].nullable {
            return Err(SourceError::fatal(format!(
                "stream `{}`: cursor column `{name}` admits NULLs, which a watermark cannot \
                 order — rows holding one would be delivered once and then skipped by every \
                 resume. Make the column NOT NULL, choose another cursor, or drop `cursor` \
                 to read the stream in full",
                stream.name
            )));
        }
        // A bare NUMBER carries exact digits as TEXT (Oracle accepts
        // any magnitude in it), but it still orders numerically —
        // and it is how estates spell a sequence-backed surrogate
        // key, so refusing it rejected the commonest cursor there is
        // with a message claiming the column was not numeric.
        if !cursor_capable(types[at]) && !super::schema::is_numeric(&described.columns[at]) {
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
    // Row keys are lower-cased for the JSON payload, so two columns
    // differing only in case would collapse into one and the first
    // value would vanish with no diagnostic — 030's duplicate-CSV-
    // header defect wearing a different costume. Refuse by name.
    {
        let mut seen = std::collections::HashSet::new();
        for column in &described.columns {
            let key = column.name.to_lowercase();
            if !seen.insert(key.clone()) {
                return Err(SourceError::fatal(format!(
                    "stream `{}`: `{}` has two columns that differ only in case (`{key}`) — \
                     the emitted row would keep only one of them",
                    stream.name, stream.table
                )));
            }
        }
    }
    // The derived page is the SAFETY bound (one reply, one SDU); an
    // operator's `page_rows` may only lower it — raising it past what
    // the SDU holds would truncate replies, which is the defect this
    // whole design exists to prevent.
    let Some(derived) = rows_per_page(&described.columns, tuning.sdu_bytes) else {
        return Err(SourceError::fatal(format!(
            "stream `{}`: a single row of `{}` is wider than one session data unit \
             ({} bytes), and this build reads one packet per reply — no page size can \
             make it readable",
            stream.name, stream.table, tuning.sdu_bytes
        )));
    };
    let page_rows = tuning.page_rows.map_or(derived, |asked| asked.min(derived));
    // The driver returns at most its prefetch, and this read treats a
    // SHORT page as the last page — so the prefetch must cover the
    // page or the stream would end early and call it success.
    client = client.with_page_size(page_rows);

    // The persisted state steers ONLY a cursor-paged stream. Carrying
    // a leftover tie into a full read would start every run at that
    // ROWID and silently skip everything below it — and the full-read
    // path never checkpoints, so nothing would ever correct it.
    let mut resume = match &cursor_column {
        Some(_) => cursor.streams.get(&stream.name).cloned(),
        None => None,
    };

    let mut pages_on_connection: u32 = 0;
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

        // Recycle before the server's cursor limit rather than after.
        if pages_on_connection == PAGES_PER_CONNECTION {
            client = Client::connect(config).await?;
            client = client.with_page_size(page_rows);
            pages_on_connection = 0;
        }
        pages_on_connection += 1;

        // Armed INSIDE the loop, on purpose. Above the connection it
        // aborted every sweep cell before a single row moved, so
        // recovery always restarted from nothing and the assertion
        // held no matter what the read path did — a vacuous cell of
        // the 024 class. Here it can fail on page 3 with pages 1-2
        // already delivered and checkpointed, which is the case the
        // sweep exists to prove.
        rdlt_connector_sdk::spi::core::crash_point!(
            "ora.query",
            Err(SourceError::fatal("injected crash at ora.query"))
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
                Some(at) => {
                    let value = row.values().get(at);
                    let text = value.and_then(watermark_text);
                    if text.is_none() {
                        // The column was proven NOT NULL at describe,
                        // so reaching here means the value could not
                        // be rendered as a watermark at all. Persisting
                        // an empty one would poison the cursor for
                        // every later run; skipping the row would hide
                        // it behind `c > w` forever.
                        return Err(SourceError::fatal(format!(
                            "stream `{}`: cursor column `{}` yielded a value no watermark can \
                             represent — choose another cursor, or read the stream without one",
                            stream.name,
                            stream.cursor.as_deref().unwrap_or_default()
                        )));
                    }
                    text
                }
                None => None,
            };
            last_seen = Some((watermark.unwrap_or_default(), rowid));
        }

        // Every index into a page row — the payload columns, their
        // declared types, and the ROWID that rides at the end — was
        // fixed from the describe taken before the first page. A
        // concurrent `ALTER TABLE … ADD` shifts them all, which would
        // silently drop the new column as bookkeeping and read the
        // resume key out of it. The arity is the cheap proof.
        if let Some(row) = page.rows.first()
            && row.values().len() != rowid_at + 1
        {
            return Err(SourceError::fatal(format!(
                "stream `{}`: `{}` returned {} columns where the describe found {} — the \
                 table's shape changed mid-read; re-run to pick up the new shape",
                stream.name,
                stream.table,
                row.values().len().saturating_sub(1),
                rowid_at
            )));
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

        // END OF STREAM, and ONLY from evidence.
        //
        // A short page alone proves nothing: a reply is read as ONE
        // packet, so a page can also come back short because the
        // packet ended on a message boundary before the server's
        // terminator. Treating that as "done" would checkpoint the
        // truncation and report success — the very defect the
        // driver's `has_more_rows` was patched into existence to
        // report (it hardcoded `false` and so called every truncated
        // batch complete).
        //
        // So: a FULL page always continues. A short page ends the
        // stream only when the server said it was exhausted; a short
        // page WITH continuation is a truncated reply, and that is an
        // error, never a quiet finish.
        if (page.rows.len() as u32) < page_rows {
            if page.has_more_rows {
                return Err(SourceError::fatal(format!(
                    "stream `{}`: the server returned {} of {page_rows} requested rows and \
                     reported more to come — the reply did not fit one session data unit; \
                     lower `tuning.page_rows` for this table",
                    stream.name,
                    page.rows.len()
                )));
            }
            break;
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
