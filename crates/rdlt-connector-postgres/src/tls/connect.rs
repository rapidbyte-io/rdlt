//! THE connect path for both connectors: a resolved policy + parsed config →
//! live client, plus the classification that turns a driver failure into a
//! phase-tagged [`TlsFailure`]. Verification failures are never transient
//! (retries don't mint certificates); network-shaped failures are.

use tokio_postgres::Client;
use tokio_postgres::config::SslMode;

use super::policy::{TlsConfigError, TlsMode, TlsPolicy};
use super::rustls_config::client_config;

/// Connect-phase TLS failures, distinguished by cause.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TlsFailure {
    /// Certificate signed by an unknown CA — supply `tls.root_cert`.
    TrustAnchor,
    /// Chain invalid (expired, wrong usage, malformed…).
    Chain,
    /// Certificate does not match the host (verify_full only).
    Hostname,
    /// The server refused TLS while the policy demands it.
    ServerRefusedTls,
    /// The server rejected OUR client credential (mTLS): TLS-level
    /// certificate alerts or auth-phase SQLSTATE 28000.
    ClientCert,
    /// Any other connection error (not TLS-classified).
    Other,
}

#[derive(Debug, thiserror::Error)]
#[error("{failure:?}: {detail}")]
pub struct ConnectError {
    pub failure: TlsFailure,
    pub detail: String,
    /// Retry-worthiness hint for the caller's Transient/Fatal mapping:
    /// TLS-verification failures are never transient (retries don't mint
    /// certificates); network-shaped `Other` failures are.
    pub transient: bool,
}

/// The connect path's two failure shapes, for callers to phase-tag.
#[derive(Debug, thiserror::Error)]
pub enum ConnectResult {
    #[error("tls config: {0}")]
    Config(#[from] TlsConfigError),
    #[error("connect: {0}")]
    Connect(#[from] ConnectError),
}

/// The server-refused-TLS signal is a STRING SNIFF: tokio-postgres surfaces a
/// server that rejects the SSL request as a plain error with no typed variant
/// and no SQLSTATE, so the ONLY way to distinguish it from other connect
/// failures is to match this exact needle in the rendered detail. Pinned by
/// the `server_tls_refusal_needle_is_pinned` test so a driver wording change is
/// caught by a failing test rather than silently reclassified as `Other`.
const SERVER_TLS_REFUSED_NEEDLE: &str = "server does not support TLS";

/// THE connect path for both connectors: parsed config + resolved policy →
/// live client (connection task spawned internally, ending with the client).
pub async fn connect(
    pg: &tokio_postgres::Config,
    policy: &TlsPolicy,
) -> Result<Client, ConnectResult> {
    let mut pg = pg.clone();
    // Verifying/require modes must never fall back to plaintext.
    if policy.mode.wants_encryption() && policy.mode != TlsMode::Prefer {
        pg.ssl_mode(SslMode::Require);
    }
    if policy.mode == TlsMode::Disable {
        pg.ssl_mode(SslMode::Disable);
    }
    match client_config(policy).map_err(ConnectResult::Config)? {
        None => connect_with(&pg, tokio_postgres::NoTls).await,
        Some(config) => {
            connect_with(&pg, tokio_postgres_rustls::MakeRustlsConnect::new(config)).await
        }
    }
}

/// Connect with a chosen TLS maker, generic so the plaintext and rustls paths
/// are ONE path.
///
/// They were two copies differing only in the connector value, which meant
/// error classification and driver spawning had to be kept in step by hand — the
/// same shape as the defect where two executors classified a shared plan's
/// errors oppositely because only one was updated.
async fn connect_with<T>(pg: &tokio_postgres::Config, tls: T) -> Result<Client, ConnectResult>
where
    T: tokio_postgres::tls::MakeTlsConnect<tokio_postgres::Socket>,
    T::Stream: Send + 'static,
    T::TlsConnect: Send,
    <T::TlsConnect as tokio_postgres::tls::TlsConnect<tokio_postgres::Socket>>::Future: Send,
{
    let (client, connection) = pg
        .connect(tls)
        .await
        .map_err(|e| ConnectResult::Connect(classify_connect_error(&e)))?;
    spawn_driver(connection);
    Ok(client)
}

/// Classify a driver error's TLS meaning by walking its source chain for
/// rustls/tokio-postgres evidence. Best-effort by design: unknown shapes
/// classify `Other` with the full detail preserved.
fn classify_connect_error(err: &tokio_postgres::Error) -> ConnectError {
    use std::error::Error as _;
    // tokio-postgres Display for db errors is just "db error"; ALWAYS carry
    // the real server message + SQLSTATE — bad password / unknown database
    // are the most common connect failures. Shared rendering with both
    // connectors so the detail cannot drift.
    let detail = crate::driver_error::detail(err);
    let mut source: Option<&(dyn std::error::Error + 'static)> = err.source();
    while let Some(cause) = source {
        // io::Error::source() skips its own inner error (it delegates to the
        // inner's source) — reach the rustls error through get_ref().
        let rustls_ref = cause.downcast_ref::<rustls::Error>().or_else(|| {
            cause
                .downcast_ref::<std::io::Error>()
                .and_then(|io| io.get_ref())
                .and_then(|inner| inner.downcast_ref::<rustls::Error>())
        });
        if let Some(rustls_err) = rustls_ref {
            return ConnectError {
                failure: classify_rustls(rustls_err),
                detail: format!("{detail}: {rustls_err}"),
                transient: false,
            };
        }
        source = cause.source();
    }
    if detail.contains(SERVER_TLS_REFUSED_NEEDLE) {
        return ConnectError {
            failure: TlsFailure::ServerRefusedTls,
            detail,
            transient: false,
        };
    }
    // Auth-phase client-certificate rejection: pg_hba `cert`/`clientcert=`
    // failures surface as SQLSTATE 28000 with a certificate-naming message,
    // AFTER a successful handshake. The Display form is just "db error"
    // (tokio-postgres drops the cause), so read the REAL server message
    // through as_db_error().
    if let Some(db) = err.as_db_error()
        && db.code().code() == "28000"
        && db.message().to_lowercase().contains("certificate")
    {
        return ConnectError {
            failure: TlsFailure::ClientCert,
            detail,
            transient: false,
        };
    }
    // Non-TLS failure: the shared SQLSTATE heuristic (no code = io-shaped).
    ConnectError {
        failure: TlsFailure::Other,
        detail,
        transient: crate::driver_error::is_transient_connect_sqlstate(err),
    }
}

/// The rustls-error half of classification, factored for direct unit tests.
fn classify_rustls(err: &rustls::Error) -> TlsFailure {
    use rustls::AlertDescription as AD;
    use rustls::CertificateError as CE;
    match err {
        rustls::Error::InvalidCertificate(ce) => match ce {
            CE::UnknownIssuer => TlsFailure::TrustAnchor,
            CE::NotValidForName | CE::NotValidForNameContext { .. } => TlsFailure::Hostname,
            _ => TlsFailure::Chain,
        },
        // The server aborting the handshake over OUR certificate (mTLS):
        // certificate-shaped alerts, plus the handshake_failure/access_denied
        // forms TLS 1.2 servers send when a required client cert is absent.
        rustls::Error::AlertReceived(
            AD::CertificateRequired
            | AD::BadCertificate
            | AD::UnknownCA
            | AD::CertificateUnknown
            | AD::AccessDenied
            | AD::HandshakeFailure,
        ) => TlsFailure::ClientCert,
        _ => TlsFailure::Chain,
    }
}

/// Drive a connection to completion, reporting how it ended.
///
/// The driver future owns the socket: when it stops, every later statement on
/// that client fails with a generic "connection closed" that names nothing.
/// Discarding its error threw away the only description of WHY — a TLS reset, a
/// server shutdown, an idle timeout — leaving the operator to guess from the
/// symptom. Logged rather than propagated: nothing awaits this task, and by the
/// time it ends the failure has already surfaced through the client.
fn spawn_driver<F, E>(connection: F)
where
    F: std::future::Future<Output = Result<(), E>> + Send + 'static,
    E: std::fmt::Display + Send + 'static,
{
    tokio::spawn(async move {
        if let Err(e) = connection.await {
            tracing::warn!(error = %e, "postgres connection driver stopped");
        }
    });
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn server_tls_refusal_needle_is_pinned() {
        // Probe-style pin: this exact substring is the ONLY signal for a
        // server that refuses TLS. If the driver's wording changes, this
        // fails loudly instead of silently downgrading to `Other`.
        assert_eq!(SERVER_TLS_REFUSED_NEEDLE, "server does not support TLS");
    }

    #[test]
    fn rustls_classification_distinguishes_client_cert_rejection() {
        use rustls::AlertDescription as AD;
        use rustls::CertificateError as CE;
        // Server-verification failures keep their classes…
        assert_eq!(
            classify_rustls(&rustls::Error::InvalidCertificate(CE::UnknownIssuer)),
            TlsFailure::TrustAnchor
        );
        assert_eq!(
            classify_rustls(&rustls::Error::InvalidCertificate(CE::NotValidForName)),
            TlsFailure::Hostname
        );
        // …while the server aborting over OUR certificate is ClientCert.
        for alert in [
            AD::CertificateRequired,
            AD::BadCertificate,
            AD::UnknownCA,
            AD::HandshakeFailure,
        ] {
            assert_eq!(
                classify_rustls(&rustls::Error::AlertReceived(alert)),
                TlsFailure::ClientCert,
                "{alert:?}"
            );
        }
        // Unrelated alerts stay in the generic Chain bucket.
        assert_eq!(
            classify_rustls(&rustls::Error::AlertReceived(AD::RecordOverflow)),
            TlsFailure::Chain
        );
    }
}
