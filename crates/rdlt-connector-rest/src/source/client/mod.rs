//! HTTP execution: request build, auth attachment, classification, pacing,
//! bounded Retry-After waits (contract RS3 — the source classifies, the
//! ENGINE retries; in-source waits are bounded, never free loops).

pub mod auth;
pub mod secret;

use std::time::{Duration, Instant};

use rdlt_connector::SourceError;

pub use auth::AuthProvider;
pub use secret::Secret;

/// One configured HTTP client for a source document: reqwest + auth +
/// source-level defaults + pacing state.
#[derive(Debug)]
pub struct RestClient {
    http: reqwest::Client,
    auth: AuthProvider,
    /// Source-level headers, merged UNDER per-stream headers.
    pub default_headers: Vec<(String, String)>,
    /// Source-level params, merged UNDER per-stream params.
    pub default_params: Vec<(String, String)>,
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

    pub fn http(&self) -> &reqwest::Client {
        &self.http
    }

    /// Pacing floor (RS3): at least `min_interval` between request sends.
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

    /// Send with auth + defaults + pacing; classify the outcome. Retry-After
    /// on 429/503 is honored IN-SOURCE up to the cap (one wait, one retry per
    /// occurrence); beyond the cap the classified error surfaces to the
    /// engine's budget.
    pub async fn send(
        &self,
        build: impl Fn(&reqwest::Client) -> reqwest::RequestBuilder,
    ) -> Result<reqwest::Response, SourceError> {
        // Bounded by construction (RS3): at most ONE Retry-After wait and at
        // most one auth re-fetch per send — never a free loop; persistent
        // rate limiting surfaces to the engine's budget with the header value.
        let mut rate_limit_waited = false;
        let mut auth_retried = false;
        loop {
            self.pace().await;
            let mut request = build(&self.http);
            for (name, value) in &self.default_headers {
                request = request.header(name, value);
            }
            if !self.default_params.is_empty() {
                request = request.query(&self.default_params);
            }
            let request = self.auth.attach(request).await?;
            let response = request.send().await.map_err(classify_reqwest)?;
            let status = response.status();
            if status.is_success() {
                return Ok(response);
            }
            // 401 once-through PER SEND: a refreshable credential re-fetches
            // and the loop retries exactly once; a second 401 is fatal (RS3).
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
            return Err(classify_status(status, &response));
        }
    }
}

fn retry_after(response: &reqwest::Response) -> Option<Duration> {
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

pub(crate) fn classify_status(
    status: reqwest::StatusCode,
    response: &reqwest::Response,
) -> SourceError {
    if status == reqwest::StatusCode::TOO_MANY_REQUESTS {
        return SourceError::RateLimited {
            retry_after: retry_after(response),
            source: "HTTP 429 from API".into(),
        };
    }
    if status.is_server_error() {
        return SourceError::transient(format!("HTTP {status}"));
    }
    SourceError::fatal(format!("HTTP {status}"))
}
