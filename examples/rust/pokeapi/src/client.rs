//! The API's HTTP client: one request in flight, each bounded in time, and each failure of the
//! kind the engine decides a retry by.

use std::time::{Duration, SystemTime};

use rdlt_connector::prelude::*;
use reqwest::header::{HeaderValue, RETRY_AFTER};
use reqwest::{StatusCode, Url};
use serde::Deserialize;
use serde::de::DeserializeOwned;
use tokio::sync::Mutex;

/// What the client tells the API it is.
const USER_AGENT: &str = "rdlt-example-pokeapi";

/// The longest a request may take, its body read.
const TIMEOUT: Duration = Duration::from_secs(30);

/// The wait a 429 asks for where it says none.
const DEFAULT_RETRY_AFTER: Duration = Duration::from_secs(1);

/// One page of a list, as `/pokemon?offset=&limit=` answers it.
#[derive(Debug, Deserialize)]
pub(crate) struct Page {
    /// The next page's URL, or none on the last page.
    pub(crate) next: Option<String>,
    pub(crate) results: Vec<Entry>,
}

/// One entry of a list: a name and the URL of its detail.
#[derive(Debug, Deserialize)]
pub(crate) struct Entry {
    pub(crate) name: String,
    pub(crate) url: String,
}

impl Entry {
    /// The id the entry's URL ends in, a run of digits, as `/type/12/` does.
    pub(crate) fn id(&self) -> Result<u64> {
        let last = self.url.trim_end_matches('/').rsplit('/').next();
        let digits = last.filter(|id| !id.is_empty() && id.bytes().all(|b| b.is_ascii_digit()));
        digits.and_then(|id| id.parse().ok()).ok_or_else(|| {
            ConnectorError::data(format!("entry {} has no id in its url", self.name))
        })
    }
}

#[derive(Debug)]
pub(crate) struct Client {
    http: reqwest::Client,
    base: Url,
    /// Held across each request, so the API is asked one thing at a time by every stream.
    turn: Mutex<()>,
}

impl Client {
    /// A client of the API at `base_url`.
    pub(crate) fn new(base_url: &str) -> Result<Self> {
        let mut base = Url::parse(base_url).config("base_url is not a URL")?;
        if !matches!(base.scheme(), "http" | "https") {
            return Err(ConnectorError::config("base_url is not an HTTP URL"));
        }
        // A path is joined beneath the root only where the root ends in a slash.
        if !base.path().ends_with('/') {
            base.set_path(&format!("{}/", base.path()));
        }
        let http = reqwest::Client::builder()
            .user_agent(USER_AGENT)
            .timeout(TIMEOUT)
            .build()
            .internal("building the HTTP client")?;
        let turn = Mutex::new(());
        Ok(Self { http, base, turn })
    }

    /// The page of `resource`'s list holding `limit` entries from `offset`.
    pub(crate) async fn page(&self, resource: &str, offset: u64, limit: u64) -> Result<Page> {
        self.get(&format!("{resource}?offset={offset}&limit={limit}"))
            .await
    }

    /// The JSON at `path`, beneath the API's root.
    pub(crate) async fn get<T: DeserializeOwned>(&self, path: &str) -> Result<T> {
        let url = self.base.join(path).config("base_url joins no path")?;
        let _turn = self.turn.lock().await;
        let response = self
            .http
            .get(url)
            .send()
            .await
            .transient(format!("GET {path}"))?;
        let status = response.status();
        if !status.is_success() {
            return Err(failure(status, response.headers().get(RETRY_AFTER), path));
        }
        let body = response
            .bytes()
            .await
            .transient(format!("reading {path}"))?;
        serde_json::from_slice(&body).data(format!("{path} answered another shape"))
    }
}

/// What a request for `path` the API answered with `status` failed with.
fn failure(status: StatusCode, retry_after: Option<&HeaderValue>, path: &str) -> ConnectorError {
    let message = format!("GET {path} answered {status}");
    match status {
        StatusCode::TOO_MANY_REQUESTS => {
            let wait = retry_after.and_then(|value| asked(value.to_str().ok()?));
            ConnectorError::rate_limited(message, Some(wait.unwrap_or(DEFAULT_RETRY_AFTER)))
        }
        StatusCode::UNAUTHORIZED | StatusCode::FORBIDDEN => {
            ConnectorError::new(ConnectorErrorKind::Auth, message)
        }
        _ if status.is_server_error() => {
            ConnectorError::new(ConnectorErrorKind::Transient, message)
        }
        _ => ConnectorError::config(message),
    }
}

/// The wait a `Retry-After` of `value` asks for: a count of seconds, or an HTTP date, of which one
/// already past asks for none; none where it is neither.
fn asked(value: &str) -> Option<Duration> {
    if let Ok(seconds) = value.parse() {
        return Some(Duration::from_secs(seconds));
    }
    let date = httpdate::parse_http_date(value).ok()?;
    Some(date.duration_since(SystemTime::now()).unwrap_or_default())
}
