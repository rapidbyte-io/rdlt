//! HTTP execution: request build, auth attachment, classification, pacing,
//! bounded Retry-After waits. The source classifies errors and the ENGINE retries;
//! any in-source wait is bounded by construction, never a free retry loop.

pub mod auth;
pub mod secret;

use std::time::{Duration, Instant};

use reqwest::header::{HeaderName, HeaderValue};

use rdlt_connector::SourceError;

pub use auth::AuthProvider;
pub use secret::Secret;

/// One configured HTTP client for a source document: reqwest + auth +
/// source-level defaults + pacing state.
#[derive(Debug)]
pub struct RestClient {
    http: reqwest::Client,
    auth: AuthProvider,
    /// Source-level headers, merged UNDER per-stream headers. Parsed once at
    /// construction: config validation guarantees parseability, so per-page
    /// requests reuse the typed name/value pairs without re-parsing strings.
    default_headers: Vec<(HeaderName, HeaderValue)>,
    /// Source-level params, merged UNDER per-stream params.
    default_params: Vec<(String, String)>,
    min_interval: Duration,
    retry_after_cap: Duration,
    last_request_at: tokio::sync::Mutex<Option<Instant>>,
}

impl RestClient {
    pub fn new(
        auth: AuthProvider,
        default_headers: Vec<(String, String)>,
        default_params: Vec<(String, String)>,
        min_request_interval_ms: u64,
        retry_after_cap_secs: u64,
    ) -> Self {
        // Parse once. `RestConfig::validate` rejects an unparseable
        // source-level header before this constructor is reached, so a parse
        // failure here is a config-validation bug, not runtime input.
        let default_headers = default_headers
            .into_iter()
            .map(|(name, value)| {
                let name: HeaderName = name
                    .parse()
                    .expect("source header name validated by RestConfig::validate");
                let value: HeaderValue = value
                    .parse()
                    .expect("source header value validated by RestConfig::validate");
                (name, value)
            })
            .collect();
        Self {
            http: reqwest::Client::new(),
            auth,
            default_headers,
            default_params,
            min_interval: Duration::from_millis(min_request_interval_ms),
            retry_after_cap: Duration::from_secs(retry_after_cap_secs),
            last_request_at: tokio::sync::Mutex::new(None),
        }
    }

    /// Pacing floor: at least `min_interval` between request sends.
    async fn pace(&self) {
        if self.min_interval.is_zero() {
            return;
        }
        let mut last = self.last_request_at.lock().await;
        if let Some(at) = *last {
            let elapsed = at.elapsed();
            if elapsed < self.min_interval {
                tokio::time::sleep(self.min_interval - elapsed).await;
            }
        }
        *last = Some(Instant::now());
    }

    /// Send with auth + defaults + pacing. `Err` is TRANSPORT-level only
    /// (connection failures, classified transient); an HTTP error status
    /// comes back as `Ok(response)` after the bounded in-source handling
    /// below, so the caller can match declared response actions on the
    /// TYPED status before classifying. Retry-After on 429/503 is honored
    /// IN-SOURCE up to the cap (one wait, one retry per occurrence).
    pub async fn send(
        &self,
        build: impl Fn(&reqwest::Client) -> reqwest::RequestBuilder,
    ) -> Result<reqwest::Response, SourceError> {
        // Bounded by construction: at most ONE Retry-After wait and at most one
        // auth re-fetch per send — never a free loop; persistent rate limiting
        // surfaces to the engine's budget with the header value.
        let mut rate_limit_waited = false;
        let mut auth_retried = false;
        loop {
            self.pace().await;
            let request = self.auth.attach(build(&self.http)).await?;
            let mut request = request
                .build()
                .map_err(|e| SourceError::fatal(format!("request build: {e}")))?;
            self.apply_defaults(&mut request)?;
            let response = self.http.execute(request).await.map_err(classify_reqwest)?;
            let status = response.status();
            if status.is_success() {
                return Ok(response);
            }
            // 401 once-through PER SEND: a refreshable credential re-fetches
            // and the loop retries exactly once; a second 401 is fatal.
            if status == reqwest::StatusCode::UNAUTHORIZED
                && !auth_retried
                && self.auth.on_unauthorized().await?
            {
                auth_retried = true;
                continue;
            }
            // In-source Retry-After wait for 429/503 within the cap — once.
            if !rate_limit_waited
                && matches!(
                    status,
                    reqwest::StatusCode::TOO_MANY_REQUESTS
                        | reqwest::StatusCode::SERVICE_UNAVAILABLE
                )
                && let Some(wait) = retry_after(&response)
                && wait <= self.retry_after_cap
            {
                rate_limit_waited = true;
                tokio::time::sleep(wait).await;
                continue;
            }
            return Ok(response);
        }
    }

    /// Source-level defaults merge UNDER what the request already carries:
    /// a header or query param the stream (or auth) set wins over the
    /// source-level default of the same name.
    fn apply_defaults(&self, request: &mut reqwest::Request) -> Result<(), SourceError> {
        for (name, value) in &self.default_headers {
            if !request.headers().contains_key(name) {
                request.headers_mut().insert(name.clone(), value.clone());
            }
        }
        if !self.default_params.is_empty() {
            let url = request.url_mut();
            let existing: std::collections::BTreeSet<String> =
                url.query_pairs().map(|(k, _)| k.into_owned()).collect();
            let missing: Vec<&(String, String)> = self
                .default_params
                .iter()
                .filter(|(k, _)| !existing.contains(k))
                .collect();
            if !missing.is_empty() {
                let mut pairs = url.query_pairs_mut();
                for (k, v) in missing {
                    pairs.append_pair(k, v);
                }
            }
        }
        Ok(())
    }
}

pub(crate) fn retry_after(response: &reqwest::Response) -> Option<Duration> {
    response
        .headers()
        .get(reqwest::header::RETRY_AFTER)
        .and_then(|v| v.to_str().ok())
        .and_then(|s| s.parse::<u64>().ok())
        .map(Duration::from_secs)
}

pub(crate) fn classify_reqwest(error: reqwest::Error) -> SourceError {
    // Connection-level problems are the textbook transient failure.
    SourceError::transient(error)
}

/// Classify an undeclared HTTP error status from its parts: 429 is rate-limited
/// (carrying any Retry-After), a 5xx status is transient, everything else is fatal.
/// Taken from parts because the response body may already be consumed by the time an
/// undeclared error status is classified.
pub(crate) fn classify_status(
    status: reqwest::StatusCode,
    retry_after: Option<Duration>,
) -> SourceError {
    if status == reqwest::StatusCode::TOO_MANY_REQUESTS {
        return SourceError::RateLimited {
            retry_after,
            source: "HTTP 429 from API".into(),
        };
    }
    if status.is_server_error() {
        return SourceError::transient(format!("HTTP {status}"));
    }
    SourceError::fatal(format!("HTTP {status}"))
}
