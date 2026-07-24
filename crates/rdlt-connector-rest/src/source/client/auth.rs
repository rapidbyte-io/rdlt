//! Credential attachment + lifecycle. Schemes: none, bearer, arbitrary
//! header, basic, api_key (header|query), OAuth2 client-credentials (lazy
//! single-flight token cache, expiry margin, ONE 401 re-fetch then fatal —
//! a post-refresh 401 means wrong credentials, never a retry loop). Secrets
//! attach via [`Secret::reveal`] at the request only.

use std::time::{Duration, Instant};

use rdlt_connector::SourceError;

use crate::source::config::{ApiKeyLocation, Auth};
use rdlt_connector::Secret;

/// Runtime auth state built from the config `Auth`.
#[derive(Debug)]
pub struct AuthProvider {
    scheme: Auth,
    /// OAuth2 token cache: single-flight via the async mutex.
    token: tokio::sync::Mutex<Option<CachedToken>>,
}

#[derive(Debug, Clone)]
struct CachedToken {
    access_token: Secret,
    expires_at: Option<Instant>,
}

impl AuthProvider {
    pub fn new(scheme: Auth) -> Self {
        Self {
            scheme,
            token: tokio::sync::Mutex::new(None),
        }
    }

    /// Attach credentials to a request (fetching/refreshing OAuth2 tokens as
    /// needed).
    pub async fn attach(
        &self,
        request: reqwest::RequestBuilder,
    ) -> Result<reqwest::RequestBuilder, SourceError> {
        match &self.scheme {
            Auth::None => Ok(request),
            Auth::Bearer { token } => Ok(request.bearer_auth(token.reveal())),
            Auth::Header { name, value } => Ok(request.header(name, value.reveal())),
            Auth::Basic { username, password } => {
                Ok(request.basic_auth(username, Some(password.reveal())))
            }
            Auth::ApiKey {
                name,
                key,
                location,
            } => Ok(match location {
                ApiKeyLocation::Header => request.header(name, key.reveal()),
                ApiKeyLocation::Query => request.query(&[(name, key.reveal())]),
            }),
            Auth::Oauth2ClientCredentials { .. } => {
                let token = self.current_token().await?;
                Ok(request.bearer_auth(token.reveal()))
            }
        }
    }

    /// 401 hook: refreshable schemes drop their cache and report "retry
    /// once"; everything else reports "no, the 401 is final".
    pub async fn on_unauthorized(&self) -> Result<bool, SourceError> {
        if !matches!(&self.scheme, Auth::Oauth2ClientCredentials { .. }) {
            return Ok(false);
        }
        let mut guard = self.token.lock().await;
        if guard.is_none() {
            // Already dropped by a concurrent 401: don't retry twice.
            return Ok(false);
        }
        *guard = None;
        Ok(true)
    }

    async fn current_token(&self) -> Result<Secret, SourceError> {
        let Auth::Oauth2ClientCredentials {
            token_url,
            client_id,
            client_secret,
            scopes,
            audience,
            expiry_margin_secs,
        } = &self.scheme
        else {
            unreachable!("current_token is only called for oauth2");
        };
        let mut guard = self.token.lock().await;
        if let Some(cached) = guard.as_ref()
            && cached.expires_at.is_none_or(|at| Instant::now() < at)
        {
            return Ok(cached.access_token.clone());
        }
        // Single-flight by construction: the mutex is held across the fetch.
        let mut form: Vec<(&str, String)> = vec![
            ("grant_type", "client_credentials".into()),
            ("client_id", client_id.clone()),
            ("client_secret", client_secret.reveal().to_owned()),
        ];
        if !scopes.is_empty() {
            form.push(("scope", scopes.join(" ")));
        }
        if let Some(audience) = audience {
            form.push(("audience", audience.clone()));
        }
        // The token endpoint is fetched with a fresh reqwest client — it does NOT
        // go through this source's `RestClient`, so it gets no pacing or default
        // headers. It reuses only the shared error classification: 5xx/network =
        // transient (engine budget), 4xx = fatal.
        let response = reqwest::Client::new()
            .post(token_url)
            .form(&form)
            .send()
            .await
            .map_err(super::classify_reqwest)?;
        let status = response.status();
        if !status.is_success() {
            if status.is_server_error() {
                return Err(SourceError::transient(format!(
                    "oauth2 token endpoint: HTTP {status}"
                )));
            }
            return Err(SourceError::fatal(format!(
                "oauth2 token endpoint: HTTP {status} — check client credentials"
            )));
        }
        let body: serde_json::Value = response
            .json()
            .await
            .map_err(|e| SourceError::fatal(format!("oauth2 token response: {e}")))?;
        let access_token = body
            .get("access_token")
            .and_then(|v| v.as_str())
            .ok_or_else(|| {
                SourceError::fatal("oauth2 token response: missing `access_token` field")
            })?;
        let expires_at = body.get("expires_in").and_then(|v| v.as_u64()).map(|secs| {
            let margin = Duration::from_secs(*expiry_margin_secs);
            let life = Duration::from_secs(secs);
            Instant::now() + life.saturating_sub(margin)
        });
        let token = CachedToken {
            access_token: Secret::new(access_token),
            expires_at,
        };
        let out = token.access_token.clone();
        *guard = Some(token);
        Ok(out)
    }
}
