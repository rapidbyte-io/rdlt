//! The per-stream read loop: client + paginator + extractor composition,
//! response actions, incremental binding, parent-child fan-out, loop guards,
//! and the crash points (`rest.request`, `rest.decode`, `rest.checkpoint`).

pub mod extract;
pub mod paginate;
pub mod resolve;

use std::collections::BTreeMap;
use std::collections::BTreeSet;

use bytes::Bytes;
use rdlt_connector::core::crash_point;
use rdlt_connector::{Cursor, RecordsOut, SourceError};
use serde_json::Value;

use super::client::RestClient;
use super::config::{ActionKind, HttpMethod, Incremental, RestConfig, RestStream};
use extract::{Extracted, Selector, extract_records};
use paginate::{PageContext, PageDecision, Paginator};
use resolve::ParentValues;

/// What a matched response action decided.
enum Handled {
    /// Proceed as an EMPTY page (the `ignore` action).
    EmptyPage,
    /// Stop the stream cleanly.
    EndStream,
}

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
            // record. Bounded buffering: values only (RS7).
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
    // Child streams and cursor streams checkpoint at FEED END (the 010-shape
    // caveat recorded in the plan); per-page checkpoints happen inside
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
    loop {
        let page = driver.fetch_page(&selector, None).await?;
        let Some(extracted) = page else { break };
        into.extend(resolve::collect_parent_values(
            &extracted.records,
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
            // the rows covers exactly them (clause S2).
            if let Some(inc) = incremental {
                update_max_cursor(&extracted.records, &inc.cursor_field, max_cursor);
            }
            if out.raw_json(extracted.records.clone()).await.is_err() {
                return Ok(()); // clause S4: closed channel = cancellation
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
                break; // clause S4: closed channel = cancellation
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
                    .map_err(|e| {
                        SourceError::fatal(format!(
                            "child stream `{}` failed for parent ({}): {e}",
                            stream.name,
                            resolve::describe(&values.placeholders)
                        ))
                    })
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
    bind_incremental_params(&mut driver, incremental, start_value);
    let mut local_max = start_value.clone();
    loop {
        let page = driver.fetch_page(selector, Some(values)).await?;
        let Some(extracted) = page else { break };
        if extracted.count > 0 {
            if let Some(inc) = incremental {
                update_max_cursor(&extracted.records, &inc.cursor_field, &mut local_max);
            }
            let records = resolve::embed_parent_fields(extracted.records.clone(), &values.include)?;
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
    /// Params merged into every request (incremental bindings).
    extra_params: Vec<(String, String)>,
    /// Current page params from the paginator (or a full URL override).
    page_params: Vec<(String, String)>,
    url_override: Option<String>,
    /// Loop guards (RS2).
    seen_requests: BTreeSet<String>,
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
            extra_params: Vec::new(),
            page_params,
            url_override: None,
            seen_requests: BTreeSet::new(),
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

        // The same-request guard (RS2): fingerprint URL + all params + body.
        let fingerprint = format!(
            "{url}|{:?}|{:?}|{:?}",
            self.page_params, self.extra_params, self.stream.body
        );
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

        let response = match response {
            Ok(response) => response,
            Err(e) => {
                // Response actions on error statuses (declared allow-list).
                if let Some(handled) = self.match_action_on_error(&e).await? {
                    return self.apply_handled(handled).await;
                }
                return Err(e);
            }
        };
        let headers = response.headers().clone();
        let body = response
            .bytes()
            .await
            .map_err(super::client::classify_reqwest)?;
        crash_point!(
            "rest.decode",
            Err(SourceError::fatal("injected crash at rest.decode"))
        );
        // Content-based actions on success bodies.
        if let Some(handled) = self.match_action_on_content(&body) {
            self.last_headers = headers;
            return self.apply_handled(handled).await;
        }

        let extracted = extract_records(&body, selector.as_ref())?;
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

    async fn apply_handled(&mut self, handled: Handled) -> Result<Option<Extracted>, SourceError> {
        match handled {
            Handled::EndStream => {
                self.done = true;
                Ok(None)
            }
            Handled::EmptyPage => {
                self.last_count = 0;
                self.last_body = None;
                Ok(Some(Extracted {
                    records: Bytes::from_static(b"[]"),
                    count: 0,
                }))
            }
        }
    }

    /// Response actions for HTTP-status errors: the classified error carries
    /// the status in its message; declared statuses take their action.
    async fn match_action_on_error(
        &self,
        error: &SourceError,
    ) -> Result<Option<Handled>, SourceError> {
        let rendered = error.to_string();
        for action in &self.stream.response_actions {
            let Some(status) = action.status else {
                continue;
            };
            if !rendered.contains(&format!("HTTP {status}"))
                && !rendered.contains(&format!("HTTP {status} "))
            {
                continue;
            }
            // content_contains cannot match here (the body was consumed into
            // the classification) — status-only actions apply.
            if action.content_contains.is_some() {
                continue;
            }
            return Ok(Some(match action.action {
                ActionKind::Ignore => Handled::EmptyPage,
                ActionKind::EndStream => Handled::EndStream,
                ActionKind::Error => {
                    return Err(SourceError::fatal(format!(
                        "stream `{}`: declared error action matched: {rendered}",
                        self.stream.name
                    )));
                }
            }));
        }
        Ok(None)
    }

    /// Content-based actions over successful responses (bounded match).
    fn match_action_on_content(&self, body: &Bytes) -> Option<Handled> {
        const BOUND: usize = 64 * 1024;
        for action in &self.stream.response_actions {
            let Some(needle) = &action.content_contains else {
                continue;
            };
            if action.status.is_some() {
                continue; // status actions handled on the error path
            }
            let window = &body[..body.len().min(BOUND)];
            if !String::from_utf8_lossy(window).contains(needle.as_str()) {
                continue;
            }
            return Some(match action.action {
                ActionKind::Ignore => Handled::EmptyPage,
                ActionKind::EndStream => Handled::EndStream,
                ActionKind::Error => Handled::EndStream, // caller surfaces below
            });
        }
        None
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

fn update_max_cursor(records: &Bytes, field: &str, max: &mut Option<String>) {
    let Ok(Value::Array(items)) = serde_json::from_slice::<Value>(records) else {
        return;
    };
    for item in items {
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
