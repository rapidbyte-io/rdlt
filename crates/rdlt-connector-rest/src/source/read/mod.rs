//! The per-stream read loop: client + paginator + extractor composition,
//! response actions, incremental binding, parent-child fan-out, loop guards,
//! and the crash points (`rest.request`, `rest.decode`, `rest.checkpoint`).

pub mod extract;
pub mod paginate;
pub mod resolve;

use std::collections::{BTreeMap, HashSet};
use std::hash::{DefaultHasher, Hash, Hasher};

use bytes::Bytes;
use rdlt_connector::core::crash_point;
use rdlt_connector::{Cursor, RecordsOut, SourceError};
use serde_json::Value;

use super::client::RestClient;
use super::config::{ActionKind, HttpMethod, Incremental, ResponseAction, RestConfig, RestStream};
use extract::{Extracted, Selector, extract_records};
use paginate::{PageContext, PageDecision, Paginator};
use resolve::ParentValues;

/// Read one stream (with parent fan-out when declared) into `out`.
pub async fn read_stream(
    config: &RestConfig,
    client: &RestClient,
    stream: &RestStream,
    since: Option<&Cursor>,
    out: &mut RecordsOut,
) -> Result<(), SourceError> {
    let incremental = stream.effective_incremental();
    let since_value: Option<String> = since.and_then(|c| cursor_to_param(c.as_value()));
    // Max observed cursor value; starts at the resume point so an empty page
    // can never move the cursor backwards.
    let mut max_cursor: Option<String> = since_value
        .clone()
        .or_else(|| incremental.as_ref().and_then(|i| i.initial_value.clone()));

    match &stream.parent {
        None => {
            read_sequence(
                config,
                client,
                stream,
                &incremental,
                &since_value,
                &mut max_cursor,
                out,
            )
            .await?;
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
                .expect("validated at config parse");
            let mut parent_values: Vec<ParentValues> = Vec::new();
            collect_parents(
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
            read_children(
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

/// Read the parent stream (all pages) collecting resolved values.
async fn collect_parents(
    config: &RestConfig,
    client: &RestClient,
    parent_stream: &RestStream,
    parent_config: &super::config::Parent,
    into: &mut Vec<ParentValues>,
) -> Result<(), SourceError> {
    let selector = parse_selector(parent_stream)?;
    let mut paginator = paginate::from_config(&parent_stream.pagination);
    let mut driver = SequenceDriver::new(config, parent_stream, client, &mut *paginator);
    driver.need_values = true;
    loop {
        let page = driver.fetch_page(&selector, None).await?;
        let Some(extracted) = page else { break };
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

#[allow(clippy::too_many_arguments)]
async fn read_sequence(
    config: &RestConfig,
    client: &RestClient,
    stream: &RestStream,
    incremental: &Option<Incremental>,
    since_value: &Option<String>,
    max_cursor: &mut Option<String>,
    out: &mut RecordsOut,
) -> Result<(), SourceError> {
    let selector = parse_selector(stream)?;
    let mut paginator = paginate::from_config(&stream.pagination);
    let mut driver = SequenceDriver::new(config, stream, client, &mut *paginator);
    driver.need_values = incremental.is_some();
    bind_incremental_params(
        &mut driver,
        incremental,
        &max_cursor.clone().or_else(|| since_value.clone()),
    );

    loop {
        let page = driver.fetch_page(&selector, None).await?;
        let Some(extracted) = page else { break };
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

fn bind_incremental_params(
    driver: &mut SequenceDriver<'_>,
    incremental: &Option<Incremental>,
    start_value: &Option<String>,
) {
    if let Some(inc) = incremental {
        if let (Some(param), Some(value)) = (&inc.start_param, start_value.as_ref()) {
            driver.extra_params.push((param.clone(), value.clone()));
        }
        if let (Some(param), Some(value)) = (&inc.end_param, &inc.end_value) {
            driver.extra_params.push((param.clone(), value.clone()));
        }
    }
}

/// Child fan-out: up to `max_concurrency` child sequences in flight (R5),
/// records forwarded to `out` through one bounded channel (order across
/// children is not meaningful — each child is an independent sequence and
/// checkpointing happens once, at feed end). `max_concurrency: 1` (the
/// default) reads children strictly one at a time.
#[allow(clippy::too_many_arguments)]
async fn read_children(
    config: &RestConfig,
    client: &RestClient,
    stream: &RestStream,
    incremental: &Option<Incremental>,
    start_value: &Option<String>,
    max_cursor: &mut Option<String>,
    parents: Vec<ParentValues>,
    out: &mut RecordsOut,
) -> Result<(), SourceError> {
    use futures::StreamExt;
    let selector = parse_selector(stream)?;
    let limit = config.max_concurrency.max(1) as usize;
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
    for max in maxes?.into_iter().flatten() {
        if max_cursor.as_ref().is_none_or(|current| max > *current) {
            *max_cursor = Some(max);
        }
    }
    Ok(())
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
    let mut paginator = paginate::from_config(&stream.pagination);
    let mut driver = SequenceDriver::new(config, stream, client, &mut *paginator);
    driver.parent = Some(values);
    driver.need_values = incremental.is_some() || !values.include.is_empty();
    bind_incremental_params(&mut driver, incremental, start_value);
    let mut local_max = start_value.clone();
    loop {
        let page = driver.fetch_page(selector, Some(values)).await?;
        let Some(mut extracted) = page else { break };
        if extracted.count > 0 {
            if let Some(inc) = incremental {
                let page_values = extracted.values.as_deref().unwrap_or_default();
                update_max_cursor(page_values, &inc.cursor_field, &mut local_max);
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

fn parse_selector(stream: &RestStream) -> Result<Option<Selector>, SourceError> {
    stream
        .records_path
        .as_deref()
        .map(|p| Selector::parse(p).map_err(SourceError::fatal))
        .transpose()
}

/// One paginated request sequence: request build, actions, guards.
struct SequenceDriver<'a> {
    config: &'a RestConfig,
    stream: &'a RestStream,
    client: &'a RestClient,
    paginator: &'a mut dyn Paginator,
    parent: Option<&'a ParentValues>,
    /// Whether extraction should keep parsed record values on the page
    /// (incremental cursors / parent-child embedding — one parse per page).
    need_values: bool,
    /// Params merged into every request (incremental bindings).
    extra_params: Vec<(String, String)>,
    /// Current page params from the paginator (or a full URL override).
    page_params: Vec<(String, String)>,
    url_override: Option<String>,
    /// Loop guards: request fingerprints, stored as u64 hashes.
    seen_requests: HashSet<u64>,
    pages: u64,
    total_records: u64,
    /// Body of the LAST response (for body-driven paginators).
    last_body: Option<Value>,
    last_headers: reqwest::header::HeaderMap,
    last_count: usize,
    done: bool,
    started: bool,
}

impl<'a> SequenceDriver<'a> {
    fn new(
        config: &'a RestConfig,
        stream: &'a RestStream,
        client: &'a RestClient,
        paginator: &'a mut dyn Paginator,
    ) -> Self {
        let page_params = paginator.first();
        Self {
            config,
            stream,
            client,
            paginator,
            parent: None,
            need_values: false,
            extra_params: Vec::new(),
            page_params,
            url_override: None,
            seen_requests: HashSet::new(),
            pages: 0,
            total_records: 0,
            last_body: None,
            last_headers: reqwest::header::HeaderMap::new(),
            last_count: 0,
            done: false,
            started: false,
        }
    }

    fn base_url(&self) -> String {
        let path = match self.parent {
            Some(values) => resolve::substitute(&self.stream.path, &values.placeholders),
            None => self.stream.path.clone(),
        };
        format!(
            "{}/{}",
            self.config.base_url.trim_end_matches('/'),
            path.trim_start_matches('/')
        )
    }

    /// Issue the next request; returns None when the sequence ended before
    /// this page (single page done / paginator said stop last round).
    async fn fetch_page(
        &mut self,
        selector: &Option<Selector>,
        parent: Option<&ParentValues>,
    ) -> Result<Option<Extracted>, SourceError> {
        if self.done {
            return Ok(None);
        }
        self.started = true;
        let url = self.url_override.clone().unwrap_or_else(|| self.base_url());

        // The same-request guard: fingerprint URL + all params + body,
        // kept as a u64 hash — O(8 bytes) per page, not the rendered string.
        let fingerprint = {
            let mut hasher = DefaultHasher::new();
            url.hash(&mut hasher);
            self.page_params.hash(&mut hasher);
            self.extra_params.hash(&mut hasher);
            format!("{:?}", self.stream.body).hash(&mut hasher);
            hasher.finish()
        };
        if !self.seen_requests.insert(fingerprint) {
            return Err(SourceError::fatal(format!(
                "stream `{}`: paginator produced the SAME request twice (page {}, \
                 params {:?}) — the API is not advancing; check the pagination config",
                self.stream.name, self.pages, self.page_params
            )));
        }
        if self.pages >= self.config.max_pages {
            return Err(SourceError::fatal(format!(
                "stream `{}`: exceeded max_pages ({}) — raise `max_pages` if the \
                 stream is genuinely this long",
                self.stream.name, self.config.max_pages
            )));
        }
        self.pages += 1;

        crash_point!(
            "rest.request",
            Err(SourceError::fatal("injected crash at rest.request"))
        );
        let stream = self.stream;
        let page_params = self.page_params.clone();
        let extra_params = self.extra_params.clone();
        let parent_values = parent.map(|v| v.placeholders.clone());
        let response = self
            .client
            .send(move |http| {
                let mut request = match stream.method {
                    HttpMethod::Get => http.get(&url),
                    HttpMethod::Post => http.post(&url),
                };
                for (name, value) in &stream.headers {
                    request = request.header(name, value);
                }
                let substituted: Vec<(String, String)> = stream
                    .params
                    .iter()
                    .map(|(k, v)| {
                        let v = match &parent_values {
                            Some(values) => resolve::substitute(v, values),
                            None => v.clone(),
                        };
                        (k.clone(), v)
                    })
                    .collect();
                request = request.query(&substituted);
                if !extra_params.is_empty() {
                    request = request.query(&extra_params);
                }
                match stream.method {
                    HttpMethod::Get => {
                        if !page_params.is_empty() {
                            request = request.query(&page_params);
                        }
                    }
                    HttpMethod::Post => {
                        // Pagination params go INTO the body for POST-cursor
                        // APIs (R5); a missing body means query params as GET.
                        if let Some(body) = &stream.body {
                            let mut body = match &parent_values {
                                Some(values) => substitute_body(body, values),
                                None => body.clone(),
                            };
                            if let Value::Object(map) = &mut body {
                                for (k, v) in &page_params {
                                    map.insert(k.clone(), Value::String(v.clone()));
                                }
                            }
                            request = request.json(&body);
                        } else if !page_params.is_empty() {
                            request = request.query(&page_params);
                        }
                    }
                }
                request
            })
            .await;

        let response = response?; // Err = transport-level (no status to match)
        let status = response.status();
        let retry_after = super::client::retry_after(&response);
        let headers = response.headers().clone();
        let body = response
            .bytes()
            .await
            .map_err(super::client::classify_reqwest)?;
        crash_point!(
            "rest.decode",
            Err(SourceError::fatal("injected crash at rest.decode"))
        );
        // Declared response actions match the TYPED status and the body,
        // success and error responses alike — first match wins. This is an
        // allow-list posture: anything undeclared stays classified.
        if let Some(action) = match_action(&self.stream.response_actions, status.as_u16(), &body) {
            let kind = action.action;
            self.last_headers = headers;
            return self.apply_action(kind, status, &body);
        }
        if !status.is_success() {
            return Err(super::client::classify_status(status, retry_after));
        }

        let extracted = extract_records(&body, selector.as_ref(), self.need_values)?;
        self.total_records += extracted.count as u64;
        self.last_count = extracted.count;
        self.last_headers = headers;
        self.last_body = if self.paginator.needs_body() {
            Some(
                serde_json::from_slice(&body)
                    .map_err(|e| SourceError::fatal(format!("response is not valid JSON: {e}")))?,
            )
        } else {
            None
        };
        Ok(Some(extracted))
    }

    fn apply_action(
        &mut self,
        kind: ActionKind,
        status: reqwest::StatusCode,
        body: &Bytes,
    ) -> Result<Option<Extracted>, SourceError> {
        match kind {
            ActionKind::Error => Err(SourceError::fatal(format!(
                "stream `{}`: declared error action matched (HTTP {status})",
                self.stream.name
            ))),
            ActionKind::EndStream => {
                self.done = true;
                Ok(None)
            }
            ActionKind::Ignore => {
                // An empty page — but a body-driven paginator still gets the
                // body: an ignored page may carry the cursor that continues
                // the chain; an unparseable one ends it cleanly.
                self.last_count = 0;
                self.last_body = if self.paginator.needs_body() {
                    serde_json::from_slice(body).ok()
                } else {
                    None
                };
                Ok(Some(Extracted::empty()))
            }
        }
    }

    /// Ask the paginator for the next move. Returns false when done.
    fn advance(&mut self, extracted: &Extracted) -> Result<bool, SourceError> {
        if self.done {
            return Ok(false);
        }
        let ctx = PageContext {
            body: self.last_body.as_ref(),
            headers: &self.last_headers,
            record_count: extracted.count,
            total_records: self.total_records,
        };
        match self
            .paginator
            .next(&ctx)
            .map_err(|e| SourceError::fatal(format!("stream `{}`: {e}", self.stream.name)))?
        {
            PageDecision::Done => {
                self.done = true;
                Ok(false)
            }
            PageDecision::NextParams(params) => {
                self.page_params = params;
                self.url_override = None;
                Ok(true)
            }
            PageDecision::NextUrl(url) => {
                let absolute = if url.starts_with("http://") || url.starts_with("https://") {
                    url
                } else {
                    format!(
                        "{}/{}",
                        self.config.base_url.trim_end_matches('/'),
                        url.trim_start_matches('/')
                    )
                };
                self.url_override = Some(absolute);
                self.page_params = Vec::new();
                Ok(true)
            }
        }
    }
}

fn substitute_body(body: &Value, values: &BTreeMap<String, String>) -> Value {
    match body {
        Value::String(s) => Value::String(resolve::substitute(s, values)),
        Value::Array(items) => {
            Value::Array(items.iter().map(|v| substitute_body(v, values)).collect())
        }
        Value::Object(map) => Value::Object(
            map.iter()
                .map(|(k, v)| (k.clone(), substitute_body(v, values)))
                .collect(),
        ),
        other => other.clone(),
    }
}

/// First action whose declared conditions ALL hold: `status` (typed
/// equality against the actual response status) and/or `content_contains`
/// (within the first 64KiB of the body — success and error bodies alike).
fn match_action<'a>(
    actions: &'a [ResponseAction],
    status: u16,
    body: &Bytes,
) -> Option<&'a ResponseAction> {
    const BOUND: usize = 64 * 1024;
    actions.iter().find(|action| {
        let status_matches = action.status.is_none_or(|declared| declared == status);
        let content_matches = action.content_contains.as_deref().is_none_or(|needle| {
            let window = &body[..body.len().min(BOUND)];
            String::from_utf8_lossy(window).contains(needle)
        });
        status_matches && content_matches
    })
}

fn update_max_cursor(records: &[Value], field: &str, max: &mut Option<String>) {
    for item in records {
        let Some(value) = item.get(field) else {
            continue;
        };
        let rendered = match value {
            Value::String(s) => s.clone(),
            Value::Number(n) => n.to_string(),
            _ => continue,
        };
        // ISO-8601 timestamps and equal-width numerics order lexicographically —
        // the documented constraint on REST cursor fields.
        if max.as_ref().is_none_or(|current| rendered > *current) {
            *max = Some(rendered);
        }
    }
}

fn cursor_to_param(value: &Value) -> Option<String> {
    match value {
        Value::String(s) => Some(s.clone()),
        Value::Number(n) => Some(n.to_string()),
        _ => None,
    }
}
