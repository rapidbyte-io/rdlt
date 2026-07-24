//! Declarative REST source configuration: base URL, auth, pagination
//! strategy, incremental cursors, response actions, parent-child linkage,
//! per-column type hints — everything in one YAML/JSON document a platform
//! can render and validate — configs are DATA, with no callbacks.
//!
//! Evolution is ADDITIVE: every older config spelling parses unchanged; superseded
//! fields remain as documented aliases.

use std::collections::BTreeMap;

use rdlt_connector::core::LogicalType;
use serde::{Deserialize, Serialize};

use rdlt_connector::Secret;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
#[non_exhaustive]
pub struct RestConfig {
    /// e.g. `https://api.example.com`
    pub base_url: String,
    // auth_compat: accepts BOTH the natural `auth: {bearer: {token: ...}}`
    // singleton-map form (YAML and JSON) AND the older YAML tagged form
    // `auth: !bearer`, so old spellings parse unchanged.
    #[serde(default, with = "auth_compat")]
    #[schemars(with = "Auth")]
    pub auth: Auth,
    /// Source-level default headers, merged UNDER per-stream headers.
    ///
    /// These are plain strings and appear VERBATIM in this struct's derived
    /// `Debug` output — a credential put here is NOT redacted. Put credentials
    /// under `auth:` instead, where they are `Secret`-wrapped and masked;
    /// `validate` rejects the `authorization`/`x-api-key` header names here for
    /// exactly this reason.
    #[serde(default)]
    pub headers: BTreeMap<String, String>,
    /// Source-level default query params, merged UNDER per-stream params.
    ///
    /// Plain strings, printed VERBATIM in derived `Debug` like [`headers`](Self::headers).
    /// A credential belongs under `auth:` (`Secret`-wrapped, masked), e.g. the
    /// `api_key` scheme with `location: query` — never hard-coded here.
    #[serde(default)]
    pub params: BTreeMap<String, String>,
    /// Parent-child fan-out cap (streams themselves read sequentially).
    #[serde(default = "default_max_concurrency")]
    pub max_concurrency: u32,
    /// Pacing floor between request sends, milliseconds.
    #[serde(default)]
    pub min_request_interval_ms: u64,
    /// Max in-source Retry-After wait; beyond it the rate-limit error
    /// surfaces to the engine's retry budget.
    #[serde(default = "default_retry_after_cap")]
    pub retry_after_cap_secs: u64,
    /// Loop guard: a stream exceeding this many pages is a typed error.
    #[serde(default = "default_max_pages")]
    pub max_pages: u64,
    pub streams: Vec<RestStream>,
}

fn default_max_concurrency() -> u32 {
    1
}

/// Header names that almost always mean a credential was hard-coded into a
/// plain (Debug-printable) `headers:` map instead of the `Secret`-wrapped
/// `auth:` block. Returns the rejection message when `name` is one of them,
/// case-insensitively.
fn reserved_auth_header(name: &str) -> Option<String> {
    const RESERVED: [&str; 2] = ["authorization", "x-api-key"];
    let lower = name.to_ascii_lowercase();
    RESERVED.contains(&lower.as_str()).then(|| {
        format!(
            "header `{name}` carries a credential — put it under `auth:` \
             (Secret-wrapped and masked), not in a plain `headers:` map that \
             renders verbatim in Debug output"
        )
    })
}

/// Auth field (de)serialization: singleton-map form in and out, PLUS the older
/// YAML tagged spelling (`auth: !bearer`) on the way in — the only YAML form the
/// original plain externally-tagged enum accepted.
mod auth_compat {
    use serde::de::Error as _;
    use serde::{Deserialize, Deserializer, Serializer};

    use super::Auth;

    pub fn serialize<S: Serializer>(auth: &Auth, serializer: S) -> Result<S::Ok, S::Error> {
        serde_yaml::with::singleton_map::serialize(auth, serializer)
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(deserializer: D) -> Result<Auth, D::Error> {
        // Buffer into a format-agnostic value first: YAML keeps `!tags` as
        // Value::Tagged; JSON and singleton-map YAML land as mappings.
        let value = serde_yaml::Value::deserialize(deserializer)?;
        if matches!(value, serde_yaml::Value::Tagged(_)) {
            Auth::deserialize(value).map_err(D::Error::custom)
        } else {
            serde_yaml::with::singleton_map::deserialize(value).map_err(D::Error::custom)
        }
    }
}
fn default_retry_after_cap() -> u64 {
    300
}
fn default_max_pages() -> u64 {
    10_000
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum Auth {
    #[default]
    None,
    /// `Authorization: Bearer <token>`
    Bearer {
        token: Secret,
    },
    /// Arbitrary header.
    Header {
        name: String,
        value: Secret,
    },
    Basic {
        username: String,
        password: Secret,
    },
    /// A named credential in a header or query parameter.
    ApiKey {
        name: String,
        key: Secret,
        #[serde(default)]
        location: ApiKeyLocation,
    },
    /// OAuth2 client-credentials grant: lazy token fetch, cached with an
    /// expiry margin, single-flight refresh, ONE 401 re-fetch then fatal.
    #[serde(rename = "oauth2_client_credentials")]
    OAuth2ClientCredentials {
        token_url: String,
        client_id: String,
        client_secret: Secret,
        #[serde(default)]
        scopes: Vec<String>,
        #[serde(default)]
        audience: Option<String>,
        #[serde(default = "default_expiry_margin")]
        expiry_margin_secs: u64,
    },
}

fn default_expiry_margin() -> u64 {
    60
}

#[derive(
    Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema,
)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum ApiKeyLocation {
    #[default]
    Header,
    Query,
}

#[derive(
    Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema,
)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum HttpMethod {
    #[default]
    Get,
    Post,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
#[non_exhaustive]
pub struct RestStream {
    /// Stream name (becomes the root table name).
    pub name: String,
    /// Path joined onto `base_url`, e.g. `/issues`. May carry `{placeholder}`
    /// tokens when a `parent` block declares them.
    pub path: String,
    /// HTTP method; `post` requires/permits `body`.
    #[serde(default)]
    pub method: HttpMethod,
    /// JSON body template (POST only). Pagination params for POST-cursor
    /// APIs are set INTO this body under their declared names.
    #[serde(default)]
    pub body: Option<serde_json::Value>,
    /// Extra query parameters sent with every request.
    #[serde(default)]
    pub params: BTreeMap<String, String>,
    /// Per-stream headers, merged OVER source-level headers.
    #[serde(default)]
    pub headers: BTreeMap<String, String>,
    /// Selector for the records array: dot paths + `[*]` wildcards + `[N]`
    /// indices (e.g. `data.items[*]`). Omitted = the body IS the array
    /// (that case streams bytes straight to the shredder — the perf path).
    #[serde(default)]
    pub records_path: Option<String>,
    #[serde(default)]
    pub pagination: Pagination,
    /// Incremental block (supersedes the flat aliases below).
    #[serde(default)]
    pub incremental: Option<Incremental>,
    /// Legacy ALIAS for `incremental.cursor_field`.
    #[serde(default)]
    pub cursor_field: Option<String>,
    /// Legacy ALIAS for `incremental.start_param`.
    #[serde(default)]
    pub cursor_param: Option<String>,
    /// Declared handling for specific responses; anything undeclared keeps
    /// the typed-error posture. First match wins.
    #[serde(default)]
    pub response_actions: Vec<ResponseAction>,
    /// Parent-child linkage: this stream fans out per parent record.
    #[serde(default)]
    pub parent: Option<Parent>,
    #[serde(default)]
    pub primary_key: Option<Vec<String>>,
    /// Per-column logical types overriding inference, e.g. `created_at: timestamp_tz`.
    #[serde(default)]
    pub type_hints: BTreeMap<String, HintType>,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(rename_all = "snake_case", tag = "type")]
#[non_exhaustive]
pub enum Pagination {
    /// Single request.
    #[default]
    None,
    /// `?<page_param>=N`, starting at `start`, until a page returns no
    /// records (or the declared total is reached).
    Page {
        #[serde(default = "default_page_param")]
        page_param: String,
        #[serde(default = "default_page_start")]
        start: u64,
        /// Optional stop: selector to the total-page count in the response.
        #[serde(default)]
        total_pages_path: Option<String>,
        /// Optional stop: selector to the total-record count.
        #[serde(default)]
        total_count_path: Option<String>,
    },
    /// `?<offset_param>=N&<limit_param>=page_size` until a short page (or
    /// the declared total is reached).
    Offset {
        #[serde(default = "default_offset_param")]
        offset_param: String,
        #[serde(default = "default_limit_param")]
        limit_param: String,
        page_size: u64,
        #[serde(default)]
        total_count_path: Option<String>,
    },
    /// A cursor in the response body feeds the next request's param;
    /// terminates when the cursor is absent or null. The config spelling stays
    /// `type: cursor`; the variant is named for its body-cursor semantics.
    #[serde(rename = "cursor")]
    BodyCursor {
        /// Selector to the cursor value in the response body.
        cursor_path: String,
        cursor_param: String,
    },
    /// A cursor in a response HEADER feeds the next request's param;
    /// terminates when the header is absent.
    HeaderCursor {
        header: String,
        cursor_param: String,
    },
    /// The response body carries the next page's URL (absolute or relative);
    /// terminates when absent or null.
    NextUrl { next_url_path: String },
    /// RFC5988 `Link: <url>; rel="next"`; terminates when no next link.
    LinkHeader,
}

impl Pagination {
    /// Every selector path this pagination family carries, each labeled for
    /// error messages — the one home for the variant knowledge that config
    /// validation (which checks they parse) and paginator construction both
    /// reach for.
    pub(crate) fn selector_paths(&self) -> Vec<(&'static str, &str)> {
        match self {
            Pagination::Page {
                total_pages_path,
                total_count_path,
                ..
            } => {
                let mut paths = Vec::new();
                if let Some(p) = total_pages_path {
                    paths.push(("pagination.total_pages_path", p.as_str()));
                }
                if let Some(p) = total_count_path {
                    paths.push(("pagination.total_count_path", p.as_str()));
                }
                paths
            }
            Pagination::Offset {
                total_count_path, ..
            } => total_count_path
                .as_deref()
                .map(|p| ("pagination.total_count_path", p))
                .into_iter()
                .collect(),
            Pagination::BodyCursor { cursor_path, .. } => {
                vec![("pagination.cursor_path", cursor_path.as_str())]
            }
            Pagination::NextUrl { next_url_path } => {
                vec![("pagination.next_url_path", next_url_path.as_str())]
            }
            Pagination::None | Pagination::HeaderCursor { .. } | Pagination::LinkHeader => vec![],
        }
    }
}

fn default_page_param() -> String {
    "page".into()
}
fn default_page_start() -> u64 {
    1
}
fn default_offset_param() -> String {
    "offset".into()
}
fn default_limit_param() -> String {
    "limit".into()
}

/// Incremental cursor binding (S2 mechanics unchanged: max-observed value,
/// checkpoint after rows, resume via the engine's committed cursor).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
#[non_exhaustive]
pub struct Incremental {
    /// Record field carrying the cursor value.
    pub cursor_field: String,
    /// Request parameter carrying the resume value ("only records after").
    #[serde(default)]
    pub start_param: Option<String>,
    /// Optional closed-window upper bound parameter. Requires `end_value`.
    #[serde(default)]
    pub end_param: Option<String>,
    /// Explicit value for `end_param` (closed windows take explicit values;
    /// "now" is never synthesized).
    #[serde(default)]
    pub end_value: Option<String>,
    /// First-run lower bound when no cursor is committed yet.
    #[serde(default)]
    pub initial_value: Option<String>,
}

/// Declared response handling — an ALLOW-list over the typed-error default.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
#[non_exhaustive]
pub struct ResponseAction {
    /// HTTP status this action matches (absent = any status).
    #[serde(default)]
    pub status: Option<u16>,
    /// Substring match over the first 64KiB of the body (absent = any body).
    #[serde(default)]
    pub content_contains: Option<String>,
    /// What to do when this action matches. The config key stays `action`.
    #[serde(rename = "action")]
    pub kind: ActionKind,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum ActionKind {
    /// Treat as an empty page (pagination still terminates per its family).
    Ignore,
    /// Clean end of the stream.
    EndStream,
    /// Explicit fatal (documents intent).
    Error,
}

/// Parent-child linkage: `{placeholder}` tokens in path/params/body resolve
/// from each parent record.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
#[non_exhaustive]
pub struct Parent {
    /// The parent stream's name (must be a declared stream).
    pub stream: String,
    /// placeholder token → parent record field (dot path).
    pub placeholders: BTreeMap<String, String>,
    /// Parent fields embedded into child records as `_parent_<name>`.
    #[serde(default)]
    pub include: Vec<String>,
}

/// Human-friendly hint names in YAML, mapped onto the engine's logical types.
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum HintType {
    Bool,
    Int64,
    Float64,
    Utf8,
    TimestampTz,
    Date,
    Time,
    Uuid,
    Json,
}

impl From<HintType> for LogicalType {
    fn from(hint: HintType) -> Self {
        match hint {
            HintType::Bool => LogicalType::Bool,
            HintType::Int64 => LogicalType::Int64,
            HintType::Float64 => LogicalType::Float64,
            HintType::Utf8 => LogicalType::Utf8,
            HintType::TimestampTz => LogicalType::TimestampTz,
            HintType::Date => LogicalType::Date,
            HintType::Time => LogicalType::Time,
            HintType::Uuid => LogicalType::Uuid,
            HintType::Json => LogicalType::Json,
        }
    }
}

impl RestStream {
    /// The effective incremental configuration: the block, or the legacy flat
    /// aliases assembled into one (validation forbids mixing them).
    pub fn effective_incremental(&self) -> Option<Incremental> {
        if let Some(inc) = &self.incremental {
            return Some(inc.clone());
        }
        self.cursor_field.as_ref().map(|field| Incremental {
            cursor_field: field.clone(),
            start_param: self.cursor_param.clone(),
            end_param: None,
            end_value: None,
            initial_value: None,
        })
    }
}

impl RestConfig {
    pub fn from_yaml(yaml: &str) -> Result<Self, ConfigError> {
        let config: RestConfig = serde_yaml::from_str(yaml)?;
        config.validate()?;
        Ok(config)
    }

    /// JSON text form — same document shape and validation as YAML.
    pub fn from_json(json: &str) -> Result<Self, ConfigError> {
        let config: RestConfig = serde_json::from_str(json)?;
        config.validate()?;
        Ok(config)
    }

    /// The embedder entry point: a platform holding connector configs as
    /// JSON documents (validated against the connector's declared config
    /// schema, `ConnectorSpec`) passes the `serde_json::Value` directly —
    /// no string round-trip, same validation as every other entry point.
    pub fn from_value(value: serde_json::Value) -> Result<Self, ConfigError> {
        let config: RestConfig = serde_json::from_value(value)?;
        config.validate()?;
        Ok(config)
    }

    pub(crate) fn validate(&self) -> Result<(), ConfigError> {
        if self.streams.is_empty() {
            return invalid("at least one stream is required".into());
        }
        if self.max_concurrency == 0 {
            return invalid("max_concurrency must be at least 1".into());
        }
        validate_headers(&self.headers, None)?;
        let names: Vec<&str> = self.streams.iter().map(|s| s.name.as_str()).collect();
        for stream in &self.streams {
            validate_headers(&stream.headers, Some(&stream.name))?;
            validate_stream_aliases(stream)?;
            validate_selectors(stream)?;
            validate_response_actions(stream)?;
            validate_parent(self, &names, stream)?;
        }
        Ok(())
    }
}

fn invalid(msg: String) -> Result<(), ConfigError> {
    Err(ConfigError::Invalid(msg))
}

/// Reject credential-bearing header names toward `auth:`. Source-level headers
/// (`stream` is `None`) additionally get their name/value parse-checked, since
/// they are parsed once at client construction; per-stream headers are attached
/// per request, where reqwest surfaces a malformed one at build time.
fn validate_headers(
    headers: &BTreeMap<String, String>,
    stream: Option<&str>,
) -> Result<(), ConfigError> {
    for (name, value) in headers {
        if stream.is_none() {
            if name.parse::<reqwest::header::HeaderName>().is_err() {
                return invalid(format!("header `{name}`: not a valid HTTP header name"));
            }
            if value.parse::<reqwest::header::HeaderValue>().is_err() {
                return invalid(format!(
                    "header `{name}`: value is not a valid HTTP header value"
                ));
            }
        }
        if let Some(msg) = reserved_auth_header(name) {
            return match stream {
                Some(name) => invalid(format!("stream `{name}`: {msg}")),
                None => invalid(msg),
            };
        }
    }
    Ok(())
}

/// Legacy cursor aliases (set together, never mixed with the block), the
/// incremental block's own consistency, and the POST-only `body`.
fn validate_stream_aliases(stream: &RestStream) -> Result<(), ConfigError> {
    let name = &stream.name;
    if stream.cursor_field.is_some() != stream.cursor_param.is_some() {
        return invalid(format!(
            "stream `{name}`: cursor_field and cursor_param must be set together"
        ));
    }
    if stream.incremental.is_some() && stream.cursor_field.is_some() {
        return invalid(format!(
            "stream `{name}`: use either the `incremental` block or the \
             legacy cursor_field/cursor_param aliases, not both"
        ));
    }
    if let Some(inc) = &stream.incremental {
        if inc.end_param.is_some() != inc.end_value.is_some() {
            return invalid(format!(
                "stream `{name}`: incremental.end_param and end_value must be \
                 set together (closed windows take explicit values)"
            ));
        }
        if inc.cursor_field.trim().is_empty() {
            return invalid(format!(
                "stream `{name}`: incremental.cursor_field must not be empty"
            ));
        }
    }
    if stream.body.is_some() && stream.method != HttpMethod::Post {
        return invalid(format!("stream `{name}`: `body` requires `method: post`"));
    }
    Ok(())
}

/// Selector syntax, validated eagerly at parse time: the stream's
/// `records_path` and every selector its pagination family carries, plus the
/// Page family's mutually exclusive total stops.
fn validate_selectors(stream: &RestStream) -> Result<(), ConfigError> {
    let name = &stream.name;
    let record_path = stream.records_path.as_deref().map(|p| ("records_path", p));
    for (label, selector) in record_path
        .into_iter()
        .chain(stream.pagination.selector_paths())
    {
        if let Err(e) = super::read::extract::Selector::parse(selector) {
            return invalid(format!("stream `{name}`: {label}: {e}"));
        }
    }
    if let Pagination::Page {
        total_pages_path: Some(_),
        total_count_path: Some(_),
        ..
    } = &stream.pagination
    {
        return invalid(format!(
            "stream `{name}`: pagination declares both total_pages_path and \
             total_count_path — pick one stop condition"
        ));
    }
    Ok(())
}

/// Response actions: each needs at least one matcher, and a declared status
/// must be a real HTTP status (a typo like `42` would otherwise be silently
/// dead).
fn validate_response_actions(stream: &RestStream) -> Result<(), ConfigError> {
    let name = &stream.name;
    for (i, action) in stream.response_actions.iter().enumerate() {
        if action.status.is_none() && action.content_contains.is_none() {
            return invalid(format!(
                "stream `{name}`: response_actions[{i}] needs `status` and/or \
                 `content_contains` — an unconditional action would swallow \
                 every response"
            ));
        }
        if let Some(status) = action.status
            && !(100..=599).contains(&status)
        {
            return invalid(format!(
                "stream `{name}`: response_actions[{i}]: status {status} is not \
                 an HTTP status (100–599)"
            ));
        }
    }
    Ok(())
}

/// Parent-child linkage: a declared, non-self, non-nested parent stream, with
/// every placeholder actually referenced; and, absent a parent, no stray
/// `{placeholder}` tokens in the path.
fn validate_parent(
    config: &RestConfig,
    names: &[&str],
    stream: &RestStream,
) -> Result<(), ConfigError> {
    let name = &stream.name;
    let Some(parent) = &stream.parent else {
        // Placeholders in the path REQUIRE a parent block.
        if stream.path.contains('{') && stream.path.contains('}') {
            return invalid(format!(
                "stream `{name}`: path contains `{{placeholder}}` tokens but no \
                 `parent` block declares them"
            ));
        }
        return Ok(());
    };
    if parent.stream == *name {
        return invalid(format!("stream `{name}`: parent.stream is itself"));
    }
    if !names.contains(&parent.stream.as_str()) {
        return invalid(format!(
            "stream `{name}`: parent.stream `{}` is not a declared stream",
            parent.stream
        ));
    }
    let parent_decl = config
        .streams
        .iter()
        .find(|s| s.name == parent.stream)
        .expect("parent.stream membership just checked against declared names");
    if parent_decl.parent.is_some() {
        return invalid(format!(
            "stream `{name}`: parent.stream `{}` is itself a child — \
             one level of nesting is supported",
            parent.stream
        ));
    }
    if parent.placeholders.is_empty() {
        return invalid(format!(
            "stream `{name}`: parent.placeholders must not be empty"
        ));
    }
    for token in parent.placeholders.keys() {
        let referenced = stream.path.contains(&format!("{{{token}}}"))
            || stream
                .params
                .values()
                .any(|v| v.contains(&format!("{{{token}}}")))
            || stream
                .body
                .as_ref()
                .is_some_and(|b| b.to_string().contains(&format!("{{{token}}}")));
        if !referenced {
            return invalid(format!(
                "stream `{name}`: placeholder `{{{token}}}` is declared but \
                 never used in path, params, or body"
            ));
        }
    }
    Ok(())
}

#[derive(Debug, thiserror::Error)]
pub enum ConfigError {
    #[error("invalid REST source YAML: {0}")]
    Yaml(#[from] serde_yaml::Error),
    #[error("invalid REST source JSON: {0}")]
    Json(#[from] serde_json::Error),
    #[error("invalid REST source config: {0}")]
    Invalid(String),
}

#[cfg(test)]
mod tests {
    use super::*;

    fn err(config: serde_json::Value) -> String {
        RestConfig::from_value(config)
            .expect_err("must reject")
            .to_string()
    }

    #[test]
    fn credential_header_names_are_rejected_toward_auth() {
        // Source-level, per-stream, and case-insensitively — each steered to `auth:`.
        for header in ["Authorization", "authorization", "X-API-Key", "x-api-key"] {
            let source_level = serde_json::json!({
                "base_url": "https://x",
                "headers": {header: "Bearer leaked"},
                "streams": [{"name": "s", "path": "/s"}],
            });
            let msg = err(source_level);
            assert!(msg.contains("auth:"), "{header} source-level: {msg}");

            let per_stream = serde_json::json!({
                "base_url": "https://x",
                "streams": [{"name": "s", "path": "/s", "headers": {header: "leaked"}}],
            });
            let msg = err(per_stream);
            assert!(msg.contains("auth:"), "{header} per-stream: {msg}");
        }
    }

    #[test]
    fn ordinary_headers_still_accepted() {
        RestConfig::from_value(serde_json::json!({
            "base_url": "https://x",
            "headers": {"user-agent": "rdlt", "x-shared": "1"},
            "streams": [{"name": "s", "path": "/s", "headers": {"x-stream": "2"}}],
        }))
        .expect("non-credential headers pass validation");
    }
}
