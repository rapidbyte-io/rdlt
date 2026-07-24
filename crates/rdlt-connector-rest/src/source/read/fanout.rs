//! Parent-child fan-out: collect each parent record's resolved values (a
//! fresh cursor-less pass over the parent stream), then drive one child
//! sequence per parent record — up to `max_concurrency` in flight, records
//! forwarded through one bounded channel. Buffering is BOUNDED: placeholder
//! values and declared include fields only, never whole parent records.

use bytes::Bytes;
use rdlt_connector::SourceError;

use crate::source::client::RestClient;
use crate::source::config::{Incremental, RestConfig, RestStream};

use super::driver::SequenceDriver;
use super::extract::Selector;
use super::resolve::{self, ParentValues};
use super::{paginate, parse_selector};

/// Read the parent stream (all pages) collecting resolved values.
pub(crate) async fn collect_parents(
    config: &RestConfig,
    client: &RestClient,
    parent_stream: &RestStream,
    parent_config: &crate::source::config::Parent,
    into: &mut Vec<ParentValues>,
) -> Result<(), SourceError> {
    let selector = parse_selector(parent_stream)?;
    let mut paginator =
        paginate::from_config(&parent_stream.pagination).map_err(SourceError::fatal)?;
    let mut driver =
        SequenceDriver::new(config, parent_stream, client, &mut *paginator, true, None);
    loop {
        let Some(extracted) = driver.fetch_page(&selector).await? else {
            break;
        };
        into.extend(resolve::collect_parent_values(
            extracted.values.as_deref().unwrap_or_default(),
            parent_config,
        )?);
        if !driver.advance(&extracted)? {
            break;
        }
    }
    Ok(())
}

/// Child fan-out: up to `max_concurrency` child sequences in flight (R5),
/// records forwarded to `out` through one bounded channel (order across
/// children is not meaningful — each child is an independent sequence and
/// checkpointing happens once, at feed end). `max_concurrency: 1` (the
/// default) reads children strictly one at a time.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn read_children(
    config: &RestConfig,
    client: &RestClient,
    stream: &RestStream,
    incremental: &Option<Incremental>,
    start_value: &Option<String>,
    max_cursor: &mut Option<String>,
    parents: Vec<ParentValues>,
    out: &mut rdlt_connector::RecordsOut,
) -> Result<(), SourceError> {
    use futures::StreamExt;
    let selector = parse_selector(stream)?;
    let limit = config.max_concurrency as usize;
    let (tx, mut rx) = tokio::sync::mpsc::channel::<Bytes>(limit * 2);

    let forward = async {
        while let Some(records) = rx.recv().await {
            if out.raw_json(records).await.is_err() {
                break; // closed channel = cancellation
            }
        }
    };
    let drive = async {
        // Collected EAGERLY: the map closure runs here, not lazily inside
        // the stream (a lazy map trips higher-ranked lifetime inference).
        let child_futures: Vec<_> = parents
            .iter()
            .map(|values| {
                let tx = tx.clone();
                let selector = &selector;
                async move {
                    read_child_pages(
                        config,
                        client,
                        stream,
                        selector,
                        incremental,
                        start_value,
                        values,
                        &tx,
                    )
                    .await
                    .map_err(|e| with_parent_context(e, &stream.name, values))
                }
            })
            .collect();
        let mut children = futures::stream::iter(child_futures).buffer_unordered(limit);
        let mut maxes: Vec<Option<String>> = Vec::new();
        while let Some(result) = children.next().await {
            maxes.push(result?);
        }
        drop(children);
        drop(tx); // last sender gone → the forwarder drains and ends
        Ok::<_, SourceError>(maxes)
    };
    let (maxes, ()) = tokio::join!(drive, forward);
    merge_child_maxes(max_cursor, maxes?);
    Ok(())
}

/// Fold each child's max-observed cursor into the stream's running maximum;
/// an empty or absent child cursor never moves it backward.
fn merge_child_maxes(max_cursor: &mut Option<String>, child_maxes: Vec<Option<String>>) {
    for max in child_maxes.into_iter().flatten() {
        if max_cursor.as_ref().is_none_or(|current| max > *current) {
            *max_cursor = Some(max);
        }
    }
}

/// One child's paginated sequence, pushing record pages into the fan-out
/// channel and returning the child's own max-observed cursor.
#[allow(clippy::too_many_arguments)]
async fn read_child_pages(
    config: &RestConfig,
    client: &RestClient,
    stream: &RestStream,
    selector: &Option<Selector>,
    incremental: &Option<Incremental>,
    start_value: &Option<String>,
    values: &ParentValues,
    tx: &tokio::sync::mpsc::Sender<Bytes>,
) -> Result<Option<String>, SourceError> {
    let need_values = incremental.is_some() || !values.include.is_empty();
    let mut paginator = paginate::from_config(&stream.pagination).map_err(SourceError::fatal)?;
    let mut driver = SequenceDriver::new(
        config,
        stream,
        client,
        &mut *paginator,
        need_values,
        Some(values),
    );
    driver.bind_incremental(incremental, start_value);
    let mut local_max = start_value.clone();
    loop {
        let Some(mut extracted) = driver.fetch_page(selector).await? else {
            break;
        };
        if extracted.count > 0 {
            if let Some(inc) = incremental {
                let page_values = extracted.values.as_deref().unwrap_or_default();
                super::update_max_cursor(page_values, &inc.cursor_field, &mut local_max);
            }
            // Parent-field embedding reuses the already-parsed values —
            // no reparse; without `include` the page bytes pass through.
            let records = if values.include.is_empty() {
                extracted.records.clone()
            } else {
                let page_values = extracted.values.take().unwrap_or_default();
                resolve::embed_parent_fields(page_values, &values.include)?
            };
            if tx.send(records).await.is_err() {
                break; // forwarder ended = cancellation
            }
        }
        if !driver.advance(&extracted)? {
            break;
        }
    }
    Ok(local_max)
}

/// Attach parent fan-out context to a child failure WITHOUT changing how the
/// engine will treat it: a transient or rate-limited child error keeps its
/// variant (and any `retry_after`) so the run still spends its retry budget
/// rather than aborting; only an already-fatal error stays fatal.
fn with_parent_context(err: SourceError, stream: &str, values: &ParentValues) -> SourceError {
    let message = format!(
        "child stream `{stream}` failed for parent ({}): {err}",
        resolve::describe(&values.placeholders)
    );
    match err {
        SourceError::Transient(_) => SourceError::transient(message),
        SourceError::RateLimited { retry_after, .. } => SourceError::RateLimited {
            retry_after,
            source: message.into(),
        },
        _ => SourceError::fatal(message),
    }
}
