//! TLS policy for BOTH Postgres connectors.
//!
//! One policy type, one connect path — the source and destination call the
//! same code, so their TLS behavior cannot drift. Modes carry libpq
//! semantics: `require` encrypts WITHOUT validating the certificate (the
//! ecosystem's long-standing meaning — documented loudly), `verify_full` is
//! the production recommendation.

use std::sync::Arc;

use rustls::RootCertStore;
use serde::{Deserialize, Serialize};
use tokio_postgres::Client;
use tokio_postgres::config::SslMode;

use crate::tls_verify::{AcceptAnyCert, ChainOnly, provider};

#[derive(
    Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema,
)]
#[serde(rename_all = "snake_case")]
pub enum TlsMode {
    /// Plaintext only.
    Disable,
    /// Opportunistic: encrypted when the server offers TLS, plaintext
    /// otherwise. Never validates (it exists for opportunistic encryption).
    #[default]
    Prefer,
    /// Encrypted, certificate NOT validated (libpq semantics — use
    /// verify_full in production).
    Require,
    /// Encrypted + certificate chain verified; hostname NOT checked.
    VerifyCa,
    /// Encrypted + chain + hostname — the production recommendation.
    VerifyFull,
}

impl TlsMode {
    fn wants_encryption(self) -> bool {
        !matches!(self, TlsMode::Disable)
    }
}

/// A trust root: a filesystem path to a PEM bundle, or the PEM text inline
/// (config strings starting with `-----BEGIN` are treated as inline).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(transparent)]
pub struct RootCert(pub String);

/// The per-connection TLS posture shared by source and destination.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct TlsPolicy {
    #[serde(default)]
    pub mode: TlsMode,
    #[serde(default)]
    pub root_cert: Option<RootCert>,
    /// Client certificate for mutual TLS. Path or inline PEM; requires
    /// `client_key`.
    #[serde(default)]
    pub client_cert: Option<RootCert>,
    /// Private key matching `client_cert` (PKCS#8/RSA/SEC1, unencrypted).
    #[serde(default)]
    pub client_key: Option<RootCert>,
}

/// Config-shaped TLS failures (open phase).
#[derive(Debug, thiserror::Error)]
pub enum TlsConfigError {
    #[error(
        "tls.mode `{block:?}` contradicts the conn string's sslmode `{conn}` — \
         silently out-ranking an explicit sslmode is how plaintext surprises \
         happen; align them or drop one"
    )]
    Contradiction { conn: &'static str, block: TlsMode },
    #[error("tls.root_cert `{path}`: {detail}")]
    RootCert { path: String, detail: String },
    #[error(
        "tls.mode `{0:?}` verifies certificates but no trust root resolved \
         (no tls.root_cert and the platform trust store is empty/unavailable)"
    )]
    NoRoots(TlsMode),
    #[error("tls client credential `{input}`: {detail}")]
    ClientCredential { input: String, detail: String },
    #[error(
        "conn parameter `{param}={conn_value}` conflicts with the tls block's \
         {param_field} `{block_value}` — align them or drop one"
    )]
    ConnParamConflict {
        param: &'static str,
        param_field: &'static str,
        conn_value: String,
        block_value: String,
    },
    #[error("unsupported connection parameter `{param}`: {hint}")]
    UnsupportedParam { param: String, hint: String },
    #[error("conn string does not parse: {0}")]
    ConnSyntax(String),
}

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

/// A fully parsed connection: the driver config (TLS trio stripped,
/// `application_name` defaulted) + the effective TLS policy.
#[derive(Debug)]
pub struct ParsedConn {
    pub pg: tokio_postgres::Config,
    pub policy: TlsPolicy,
}

/// THE parse gate for both connectors: extract libpq's TLS parameter trio,
/// hand the
/// remainder to the driver, translate extractions into the policy with the
/// same agree-or-error rule sslmode has, and make sure no rejection is ever
/// a bare parse error.
pub fn parse_conn(conn: &str, block: Option<&TlsPolicy>) -> Result<ParsedConn, TlsConfigError> {
    let extracted = extract_tls_params(conn);
    if let Some((param, value)) = &extracted.bad_escape {
        return Err(TlsConfigError::ConnSyntax(format!(
            "malformed percent-escape in `{param}` value `{value}`"
        )));
    }
    let mut pg: tokio_postgres::Config = extracted.remainder.parse().map_err(|e| {
        // A driver rejection must name the parameter when one is the
        // cause — scan OUR key list for anything outside the driver's set.
        for key in &extracted.seen_keys {
            if !DRIVER_PARAMS.contains(&key.as_str()) {
                return TlsConfigError::UnsupportedParam {
                    param: key.clone(),
                    hint: param_hint(key),
                };
            }
        }
        TlsConfigError::ConnSyntax(format!("{e}"))
    })?;
    let mut policy = resolve_policy(&pg, block)?;
    // A conn-string `sslmode=verify-*` (libpq spelling the driver rejects)
    // sets the mode; a block may keep or STRENGTHEN it, never weaken it —
    // the same never-silently-reversed rule as the other sslmode values.
    if let Some(conn_mode) = extracted.sslmode_verify {
        let strength = |m: TlsMode| match m {
            TlsMode::Disable => 0,
            TlsMode::Prefer => 1,
            TlsMode::Require => 2,
            TlsMode::VerifyCa => 3,
            TlsMode::VerifyFull => 4,
        };
        match block {
            None => policy.mode = conn_mode,
            Some(b) if strength(b.mode) >= strength(conn_mode) => {}
            Some(b) => {
                return Err(TlsConfigError::Contradiction {
                    conn: if conn_mode == TlsMode::VerifyCa {
                        "verify-ca"
                    } else {
                        "verify-full"
                    },
                    block: b.mode,
                });
            }
        }
    }

    // Merge the trio: conn value + absent block field fills in; agreeing
    // duplicates pass; disagreement is a typed conflict.
    let merge = |param: &'static str,
                 param_field: &'static str,
                 conn_value: Option<String>,
                 field: &mut Option<RootCert>|
     -> Result<(), TlsConfigError> {
        let Some(conn_value) = conn_value else {
            return Ok(());
        };
        // `sslrootcert=system` (libpq 16+) = the platform store — our
        // native-roots default, i.e. an EMPTY root_cert.
        if param == "sslrootcert" && conn_value == "system" {
            if let Some(RootCert(existing)) = field {
                return Err(TlsConfigError::ConnParamConflict {
                    param,
                    param_field,
                    conn_value,
                    block_value: existing.clone(),
                });
            }
            return Ok(());
        }
        match field {
            Some(RootCert(existing)) if *existing != conn_value => {
                Err(TlsConfigError::ConnParamConflict {
                    param,
                    param_field,
                    conn_value,
                    block_value: existing.clone(),
                })
            }
            Some(_) => Ok(()),
            None => {
                *field = Some(RootCert(conn_value));
                Ok(())
            }
        }
    };
    merge(
        "sslrootcert",
        "root_cert",
        extracted.sslrootcert,
        &mut policy.root_cert,
    )?;
    merge(
        "sslcert",
        "client_cert",
        extracted.sslcert,
        &mut policy.client_cert,
    )?;
    merge(
        "sslkey",
        "client_key",
        extracted.sslkey,
        &mut policy.client_key,
    )?;
    // The both-or-neither rule holds ACROSS sources of the values.
    validate_credentials(&policy)?;

    // Identify ourselves unless the user chose a name.
    if pg.get_application_name().is_none() {
        pg.application_name("rdlt");
    }
    Ok(ParsedConn { pg, policy })
}

/// The driver's accepted parameter set (tokio-postgres 0.7) — used ONLY to
/// name the offending key when the driver rejects a string.
const DRIVER_PARAMS: &[&str] = &[
    "user",
    "password",
    "dbname",
    "options",
    "application_name",
    "sslmode",
    "host",
    "hostaddr",
    "port",
    "connect_timeout",
    "tcp_user_timeout",
    "keepalives",
    "keepalives_idle",
    "keepalives_interval",
    "keepalives_retries",
    "target_session_attrs",
    "channel_binding",
    "load_balance_hosts",
];

fn param_hint(param: &str) -> String {
    match param {
        "sslpassword" => "encrypted client keys are unsupported — provide an \
                          unencrypted PKCS#8/RSA/SEC1 key via tls.client_key"
            .into(),
        "gssencmode" | "krbsrvname" | "gsslib" => "GSS/Kerberos transport is not supported".into(),
        "requiressl" => "use sslmode= (requiressl is the pre-7.2 libpq spelling)".into(),
        "sslcrl" | "sslcrldir" => "certificate revocation lists are not supported".into(),
        "service" => "libpq service files are not read — inline the parameters".into(),
        other => format!("`{other}` has no rdlt equivalent; see the supported parameter list"),
    }
}

/// The extraction half: strip the TLS trio from both libpq forms, remember
/// every key seen (for named rejections).
struct ExtractedConn {
    remainder: String,
    /// First malformed percent-escape seen in an EXTRACTED value:
    /// (param, raw value) — surfaced as a typed error.
    bad_escape: Option<(String, String)>,
    /// Keys seen and KEPT for the driver (extracted ones are excluded —
    /// they cannot be the cause of a driver rejection).
    seen_keys: Vec<String>,
    sslrootcert: Option<String>,
    sslcert: Option<String>,
    sslkey: Option<String>,
    /// `sslmode=verify-ca|verify-full` — libpq spellings the driver does
    /// not accept; translated into the policy mode.
    sslmode_verify: Option<TlsMode>,
}

fn extract_tls_params(conn: &str) -> ExtractedConn {
    let mut out = ExtractedConn {
        remainder: String::new(),
        bad_escape: None,
        seen_keys: Vec::new(),
        sslrootcert: None,
        sslcert: None,
        sslkey: None,
        sslmode_verify: None,
    };
    let capture = |key: &str, value: String, out: &mut ExtractedConn| -> bool {
        match key {
            "sslrootcert" => out.sslrootcert = Some(value),
            "sslcert" => out.sslcert = Some(value),
            "sslkey" => out.sslkey = Some(value),
            "sslmode" if value == "verify-ca" => out.sslmode_verify = Some(TlsMode::VerifyCa),
            "sslmode" if value == "verify-full" => out.sslmode_verify = Some(TlsMode::VerifyFull),
            _ => return false,
        }
        true
    };
    if conn.contains("://") {
        // URL form: rewrite the query string, percent-decoding captured values.
        let (base, query) = match conn.split_once('?') {
            Some((b, q)) => (b, q),
            None => (conn, ""),
        };
        let mut kept: Vec<&str> = Vec::new();
        for pair in query.split('&').filter(|p| !p.is_empty()) {
            let (key, value) = pair.split_once('=').unwrap_or((pair, ""));
            let decoded = match percent_decode(value) {
                Ok(v) => v,
                Err(()) => {
                    if out.bad_escape.is_none() {
                        out.bad_escape = Some((key.to_string(), value.to_string()));
                    }
                    value.to_string()
                }
            };
            if !capture(key, decoded, &mut out) {
                out.seen_keys.push(key.to_string());
                kept.push(pair);
            }
        }
        out.remainder = if kept.is_empty() {
            base.to_string()
        } else {
            format!("{base}?{}", kept.join("&"))
        };
    } else {
        // key=value form: quote-aware scan (libpq allows `key = 'a value'`).
        let bytes = conn.as_bytes();
        let mut kept_spans: Vec<(usize, usize)> = Vec::new();
        let mut i = 0usize;
        while i < bytes.len() {
            while i < bytes.len() && bytes[i].is_ascii_whitespace() {
                i += 1;
            }
            if i >= bytes.len() {
                break;
            }
            let start = i;
            while i < bytes.len() && bytes[i] != b'=' && !bytes[i].is_ascii_whitespace() {
                i += 1;
            }
            let key = conn[start..i].to_string();
            while i < bytes.len() && bytes[i].is_ascii_whitespace() {
                i += 1;
            }
            if i < bytes.len() && bytes[i] == b'=' {
                i += 1;
                while i < bytes.len() && bytes[i].is_ascii_whitespace() {
                    i += 1;
                }
                let value = if i < bytes.len() && bytes[i] == b'\'' {
                    i += 1;
                    let vstart = i;
                    while i < bytes.len() && bytes[i] != b'\'' {
                        i += if bytes[i] == b'\\' { 2 } else { 1 };
                    }
                    let v = conn[vstart..i.min(bytes.len())].replace("\\'", "'");
                    i = (i + 1).min(bytes.len());
                    v
                } else {
                    let vstart = i;
                    while i < bytes.len() && !bytes[i].is_ascii_whitespace() {
                        i += 1;
                    }
                    conn[vstart..i].to_string()
                };
                if !capture(&key, value, &mut out) {
                    out.seen_keys.push(key);
                    kept_spans.push((start, i));
                }
            } else {
                // Not a k=v token (malformed) — keep it for the driver's
                // syntax error to describe.
                kept_spans.push((start, i));
            }
        }
        out.remainder = kept_spans
            .iter()
            .map(|&(s, e)| &conn[s..e])
            .collect::<Vec<_>>()
            .join(" ");
    }
    out
}

/// Strict: a `%` not followed by two hex digits is an error, never a silent
/// literal passthrough.
fn percent_decode(value: &str) -> Result<String, ()> {
    let bytes = value.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%' {
            let (Some(h), Some(l)) = (
                bytes.get(i + 1).and_then(|b| (*b as char).to_digit(16)),
                bytes.get(i + 2).and_then(|b| (*b as char).to_digit(16)),
            ) else {
                return Err(());
            };
            out.push((h * 16 + l) as u8);
            i += 3;
            continue;
        }
        out.push(bytes[i]);
        i += 1;
    }
    Ok(String::from_utf8_lossy(&out).into_owned())
}

/// Resolve the effective policy from the parsed conn string and the optional
/// config block. The block wins ONLY when consistent: an explicit conn
/// `sslmode` may be refined (require → verify_*) but never
/// silently reversed.
pub fn resolve_policy(
    conn: &tokio_postgres::Config,
    block: Option<&TlsPolicy>,
) -> Result<TlsPolicy, TlsConfigError> {
    let conn_mode = conn.get_ssl_mode();
    let Some(block) = block else {
        return Ok(TlsPolicy {
            mode: match conn_mode {
                SslMode::Disable => TlsMode::Disable,
                SslMode::Require => TlsMode::Require,
                _ => TlsMode::Prefer,
            },
            ..TlsPolicy::default()
        });
    };
    let contradiction = match conn_mode {
        // Explicit plaintext vs a block DEMANDING encryption. `prefer`
        // tolerates plaintext by its own semantics: a block whose mode
        // defaulted to prefer must compose with disable.
        SslMode::Disable => matches!(
            block.mode,
            TlsMode::Require | TlsMode::VerifyCa | TlsMode::VerifyFull
        ),
        // Explicit encryption vs a block demanding plaintext.
        SslMode::Require => block.mode == TlsMode::Disable,
        // Prefer (the unsignaled default) composes with anything.
        _ => false,
    };
    if contradiction {
        return Err(TlsConfigError::Contradiction {
            conn: match conn_mode {
                SslMode::Disable => "disable",
                SslMode::Require => "require",
                _ => "prefer",
            },
            block: block.mode,
        });
    }
    Ok(block.clone())
}

/// Client-credential shape rules, enforced BEFORE any connection:
/// both-or-neither, and never with plaintext.
pub(crate) fn validate_credentials(policy: &TlsPolicy) -> Result<(), TlsConfigError> {
    match (&policy.client_cert, &policy.client_key) {
        (Some(_), None) => Err(TlsConfigError::ClientCredential {
            input: "client_cert".into(),
            detail: "client_key is missing — a certificate cannot authenticate without \
                     its private key"
                .into(),
        }),
        (None, Some(_)) => Err(TlsConfigError::ClientCredential {
            input: "client_key".into(),
            detail: "client_cert is missing — a private key alone is not a credential".into(),
        }),
        (Some(_), Some(_)) if policy.mode == TlsMode::Disable => {
            Err(TlsConfigError::ClientCredential {
                input: "client_cert".into(),
                detail: "tls.mode is `disable` — a client certificate cannot be presented \
                         over plaintext; enable TLS or drop the credential"
                    .into(),
            })
        }
        _ => Ok(()),
    }
}

/// Resolve a `RootCert`-shaped input (path or inline PEM) to labeled bytes.
fn pem_bytes(source: &RootCert, kind: &str) -> Result<(String, Vec<u8>), TlsConfigError> {
    let RootCert(source) = source;
    if source.trim_start().starts_with("-----BEGIN") {
        Ok((format!("<inline {kind} pem>"), source.clone().into_bytes()))
    } else {
        let bytes = std::fs::read(source).map_err(|e| TlsConfigError::ClientCredential {
            input: source.clone(),
            detail: format!("unreadable: {e}"),
        })?;
        Ok((source.clone(), bytes))
    }
}

/// Load the client credential when configured: certificate chain + private
/// key, with typed errors naming the offending input.
fn client_credential(
    policy: &TlsPolicy,
) -> Result<
    Option<(
        Vec<rustls::pki_types::CertificateDer<'static>>,
        rustls::pki_types::PrivateKeyDer<'static>,
    )>,
    TlsConfigError,
> {
    validate_credentials(policy)?;
    let (Some(cert), Some(key)) = (&policy.client_cert, &policy.client_key) else {
        return Ok(None);
    };
    let (cert_label, cert_bytes) = pem_bytes(cert, "client cert")?;
    let mut cursor = std::io::Cursor::new(cert_bytes);
    let chain: Vec<_> = rustls_pemfile::certs(&mut cursor)
        .collect::<Result<_, _>>()
        .map_err(|e| TlsConfigError::ClientCredential {
            input: cert_label.clone(),
            detail: format!("PEM parse error: {e}"),
        })?;
    if chain.is_empty() {
        return Err(TlsConfigError::ClientCredential {
            input: cert_label,
            detail: "no certificates found in PEM input".into(),
        });
    }
    let (key_label, key_bytes) = pem_bytes(key, "client key")?;
    let mut cursor = std::io::Cursor::new(key_bytes);
    let key = rustls_pemfile::private_key(&mut cursor)
        .map_err(|e| TlsConfigError::ClientCredential {
            input: key_label.clone(),
            detail: format!("PEM parse error: {e}"),
        })?
        .ok_or_else(|| TlsConfigError::ClientCredential {
            input: key_label,
            detail: "no private key found in PEM input (encrypted keys are \
                     unsupported — provide an unencrypted PKCS#8/RSA/SEC1 key)"
                .into(),
        })?;
    Ok(Some((chain, key)))
}

/// Load the trust store for a verifying mode: the custom root when given,
/// else the platform store. Typed errors name what failed.
fn root_store(policy: &TlsPolicy) -> Result<RootCertStore, TlsConfigError> {
    let mut store = RootCertStore::empty();
    match &policy.root_cert {
        Some(RootCert(source)) => {
            let (label, pem_bytes): (String, Vec<u8>) =
                if source.trim_start().starts_with("-----BEGIN") {
                    ("<inline pem>".into(), source.clone().into_bytes())
                } else {
                    (
                        source.clone(),
                        std::fs::read(source).map_err(|e| TlsConfigError::RootCert {
                            path: source.clone(),
                            detail: format!("unreadable: {e}"),
                        })?,
                    )
                };
            let mut cursor = std::io::Cursor::new(pem_bytes);
            let mut added = 0usize;
            for item in rustls_pemfile::certs(&mut cursor) {
                let cert = item.map_err(|e| TlsConfigError::RootCert {
                    path: label.clone(),
                    detail: format!("PEM parse error: {e}"),
                })?;
                store.add(cert).map_err(|e| TlsConfigError::RootCert {
                    path: label.clone(),
                    detail: format!("not a usable CA certificate: {e}"),
                })?;
                added += 1;
            }
            if added == 0 {
                return Err(TlsConfigError::RootCert {
                    path: label,
                    detail: "no certificates found in PEM input".into(),
                });
            }
        }
        None => {
            let native = rustls_native_certs::load_native_certs();
            for cert in native.certs {
                let _ = store.add(cert); // tolerate individual store oddities
            }
            if store.is_empty() {
                return Err(TlsConfigError::NoRoots(policy.mode));
            }
        }
    }
    Ok(store)
}

fn builder()
-> Result<rustls::ConfigBuilder<rustls::ClientConfig, rustls::WantsVerifier>, TlsConfigError> {
    rustls::ClientConfig::builder_with_provider(provider())
        .with_safe_default_protocol_versions()
        .map_err(|e| TlsConfigError::RootCert {
            path: "<crypto provider>".into(),
            detail: e.to_string(),
        })
}

fn client_config(policy: &TlsPolicy) -> Result<Option<rustls::ClientConfig>, TlsConfigError> {
    // A mismatched cert/key pair fails HERE (rustls checks consistency at
    // config construction) — a config error before any connection.
    let credential = client_credential(policy)?;
    let auth = move |builder: rustls::ConfigBuilder<
        rustls::ClientConfig,
        rustls::client::WantsClientCert,
    >|
          -> Result<rustls::ClientConfig, TlsConfigError> {
        match credential {
            Some((chain, key)) => builder.with_client_auth_cert(chain, key).map_err(|e| {
                TlsConfigError::ClientCredential {
                    input: "client_cert/client_key".into(),
                    detail: format!("rejected by TLS stack (mismatched pair?): {e}"),
                }
            }),
            None => Ok(builder.with_no_client_auth()),
        }
    };
    let config = match policy.mode {
        TlsMode::Disable => return Ok(None),
        TlsMode::Prefer | TlsMode::Require => auth(
            builder()?
                .dangerous()
                .with_custom_certificate_verifier(Arc::new(AcceptAnyCert::new())),
        )?,
        TlsMode::VerifyCa => {
            let store = root_store(policy)?;
            auth(
                builder()?
                    .dangerous()
                    .with_custom_certificate_verifier(Arc::new(ChainOnly::new(store).map_err(
                        |e| TlsConfigError::RootCert {
                            path: "<trust store>".into(),
                            detail: format!("building verifier: {e}"),
                        },
                    )?)),
            )?
        }
        TlsMode::VerifyFull => {
            let store = root_store(policy)?;
            auth(builder()?.with_root_certificates(store))?
        }
    };
    Ok(Some(config))
}

/// Classify a driver error's TLS meaning by walking its source chain for
/// rustls/tokio-postgres evidence. Best-effort by design: unknown shapes
/// classify `Other` with the full detail preserved.
pub fn classify_connect_error(err: &tokio_postgres::Error) -> ConnectError {
    use std::error::Error as _;
    // tokio-postgres Display for db errors is just "db error"; ALWAYS carry
    // the real server message + SQLSTATE — bad password / unknown database
    // are the most common connect failures.
    let detail = match err.as_db_error() {
        Some(db) => format!("{err}: {} (SQLSTATE {})", db.message(), db.code().code()),
        None => format!("{err}"),
    };
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
    if detail.contains("server does not support TLS") {
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
    // Non-TLS failure: the SQLSTATE heuristic (no code = io-shaped).
    let transient = match err.code() {
        None => true,
        Some(state) => matches!(&state.code()[..2], "08" | "53" | "57" | "40"),
    };
    ConnectError {
        failure: TlsFailure::Other,
        detail,
        transient,
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
        None => {
            let (client, connection) = pg
                .connect(tokio_postgres::NoTls)
                .await
                .map_err(|e| ConnectResult::Connect(classify_connect_error(&e)))?;
            tokio::spawn(async move {
                let _ = connection.await;
            });
            Ok(client)
        }
        Some(config) => {
            let connector = tokio_postgres_rustls::MakeRustlsConnect::new(config);
            let (client, connection) = pg
                .connect(connector)
                .await
                .map_err(|e| ConnectResult::Connect(classify_connect_error(&e)))?;
            tokio::spawn(async move {
                let _ = connection.await;
            });
            Ok(client)
        }
    }
}

/// The connect path's two failure shapes, for callers to phase-tag.
#[derive(Debug, thiserror::Error)]
pub enum ConnectResult {
    #[error("tls config: {0}")]
    Config(#[from] TlsConfigError),
    #[error("connect: {0}")]
    Connect(#[from] ConnectError),
}

#[cfg(test)]
mod tests {
    use super::*;

    fn conn(s: &str) -> tokio_postgres::Config {
        s.parse().expect("conn parses")
    }

    #[test]
    fn policy_resolution_and_contradictions() {
        // Conn-only: sslmode maps straight through.
        assert_eq!(
            resolve_policy(&conn("host=h sslmode=require"), None)
                .unwrap()
                .mode,
            TlsMode::Require
        );
        assert_eq!(
            resolve_policy(&conn("host=h"), None).unwrap().mode,
            TlsMode::Prefer
        );
        // Block refines require → verify_full: allowed.
        let block = TlsPolicy {
            mode: TlsMode::VerifyFull,
            root_cert: None,
            ..TlsPolicy::default()
        };
        assert_eq!(
            resolve_policy(&conn("host=h sslmode=require"), Some(&block))
                .unwrap()
                .mode,
            TlsMode::VerifyFull
        );
        // Contradictions: disable vs encryption, require vs disable.
        assert!(resolve_policy(&conn("host=h sslmode=disable"), Some(&block)).is_err());
        let disable = TlsPolicy {
            mode: TlsMode::Disable,
            root_cert: None,
            ..TlsPolicy::default()
        };
        assert!(resolve_policy(&conn("host=h sslmode=require"), Some(&disable)).is_err());
        // Prefer composes with anything.
        assert!(resolve_policy(&conn("host=h sslmode=prefer"), Some(&block)).is_ok());
        // A block whose mode is prefer (the DEFAULT — e.g. a block that only
        // sets root_cert) tolerates plaintext by its own semantics and must
        // compose with conn sslmode=disable.
        let prefer_block = TlsPolicy {
            root_cert: Some(RootCert("/some/ca.pem".into())),
            ..TlsPolicy::default()
        };
        let resolved = resolve_policy(&conn("host=h sslmode=disable"), Some(&prefer_block))
            .expect("prefer block composes with disable");
        assert_eq!(resolved.mode, TlsMode::Prefer);
    }

    #[test]
    fn malformed_percent_escapes_are_typed_errors() {
        // Never a silent literal passthrough.
        for bad in [
            "postgresql://u@h/db?sslrootcert=%2",
            "postgresql://u@h/db?sslkey=bad%zz&sslcert=/c.pem",
        ] {
            let err = parse_conn(bad, None).unwrap_err();
            let msg = err.to_string();
            assert!(msg.contains("percent-escape"), "{bad}: {msg}");
        }
        // Valid escapes still decode.
        parse_conn("postgresql://u@h/db?sslrootcert=%2Fca.pem", None).expect("valid escape");
    }

    #[test]
    fn root_errors_are_typed_and_name_the_input() {
        let missing = TlsPolicy {
            mode: TlsMode::VerifyFull,
            root_cert: Some(RootCert("/nonexistent/ca.pem".into())),
            ..TlsPolicy::default()
        };
        let err = root_store(&missing).unwrap_err();
        assert!(err.to_string().contains("/nonexistent/ca.pem"), "{err}");

        let garbage = TlsPolicy {
            mode: TlsMode::VerifyFull,
            root_cert: Some(RootCert("-----BEGIN CERTIFICATE-----\ngarbage".into())),
            ..TlsPolicy::default()
        };
        let err = root_store(&garbage).unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("inline"), "{msg}");
    }

    #[test]
    fn rcgen_root_loads_inline_and_from_path() {
        let ca = rcgen::CertificateParams::new(Vec::<String>::new())
            .expect("params")
            .self_signed(&rcgen::KeyPair::generate().expect("key"))
            .expect("ca");
        let pem = ca.pem();
        let inline = TlsPolicy {
            mode: TlsMode::VerifyFull,
            root_cert: Some(RootCert(pem.clone())),
            ..TlsPolicy::default()
        };
        assert_eq!(root_store(&inline).expect("inline").len(), 1);

        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("ca.pem");
        std::fs::write(&path, pem).expect("write");
        let from_path = TlsPolicy {
            mode: TlsMode::VerifyCa,
            root_cert: Some(RootCert(path.display().to_string())),
            ..TlsPolicy::default()
        };
        assert_eq!(root_store(&from_path).expect("path").len(), 1);
    }

    // ---- client credentials ----

    fn client_pair() -> (String, String) {
        let key = rcgen::KeyPair::generate().expect("key");
        let cert = rcgen::CertificateParams::new(Vec::<String>::new())
            .expect("params")
            .self_signed(&key)
            .expect("cert");
        (cert.pem(), key.serialize_pem())
    }

    fn policy(mode: TlsMode, cert: Option<&str>, key: Option<&str>) -> TlsPolicy {
        TlsPolicy {
            mode,
            root_cert: None,
            client_cert: cert.map(|c| RootCert(c.into())),
            client_key: key.map(|k| RootCert(k.into())),
        }
    }

    #[test]
    fn credential_shape_rules_are_typed_and_early() {
        let (cert, key) = client_pair();
        // Cert without key / key without cert: name the missing counterpart.
        let err = validate_credentials(&policy(TlsMode::Require, Some(&cert), None)).unwrap_err();
        assert!(err.to_string().contains("client_key is missing"), "{err}");
        let err = validate_credentials(&policy(TlsMode::Require, None, Some(&key))).unwrap_err();
        assert!(err.to_string().contains("client_cert is missing"), "{err}");
        // Credential over plaintext: contradiction.
        let err =
            validate_credentials(&policy(TlsMode::Disable, Some(&cert), Some(&key))).unwrap_err();
        assert!(err.to_string().contains("disable"), "{err}");
        // Complete pair with TLS active: fine, and the handshake config builds.
        let ok = policy(TlsMode::Require, Some(&cert), Some(&key));
        validate_credentials(&ok).expect("valid shape");
        assert!(client_config(&ok).expect("config builds").is_some());
    }

    #[test]
    fn credential_material_errors_name_the_input() {
        let (cert, _) = client_pair();
        // Unreadable key path.
        let err = client_credential(&policy(
            TlsMode::Require,
            Some(&cert),
            Some("/nonexistent/client.key"),
        ))
        .unwrap_err();
        assert!(err.to_string().contains("/nonexistent/client.key"), "{err}");
        // A PEM with no key in it (e.g. a certificate pasted as the key) —
        // the message names the encrypted-key limitation too.
        let err =
            client_credential(&policy(TlsMode::Require, Some(&cert), Some(&cert))).unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("no private key") && msg.contains("encrypted"),
            "{msg}"
        );
        // Garbage cert PEM.
        let (_, key) = client_pair();
        let err = client_credential(&policy(
            TlsMode::Require,
            Some("-----BEGIN CERTIFICATE-----\ngarbage"),
            Some(&key),
        ))
        .unwrap_err();
        assert!(err.to_string().contains("inline client cert"), "{err}");
    }

    #[test]
    fn credentials_compose_with_every_verifying_mode() {
        // Adding a credential must not change what any mode means —
        // the config still BUILDS under all four TLS-active modes.
        let (cert, key) = client_pair();
        let ca = rcgen::CertificateParams::new(Vec::<String>::new())
            .expect("params")
            .self_signed(&rcgen::KeyPair::generate().expect("key"))
            .expect("ca");
        for mode in [
            TlsMode::Prefer,
            TlsMode::Require,
            TlsMode::VerifyCa,
            TlsMode::VerifyFull,
        ] {
            let mut p = policy(mode, Some(&cert), Some(&key));
            p.root_cert = Some(RootCert(ca.pem()));
            assert!(
                client_config(&p).expect("config").is_some(),
                "mode {mode:?}"
            );
        }
    }

    // ---- conn-string portability ----

    #[test]
    fn tls_trio_extracts_from_both_libpq_forms() {
        // URL form, percent-encoded path.
        let parsed = parse_conn(
            "postgresql://u@h:5432/db?sslmode=verify-full&sslrootcert=%2Fetc%2Fca.pem",
            None,
        )
        .expect("url form");
        assert_eq!(
            parsed.policy.root_cert,
            Some(RootCert("/etc/ca.pem".into()))
        );
        // libpq's verify-full spelling (which the driver itself rejects)
        // translates into the policy mode.
        assert_eq!(parsed.policy.mode, TlsMode::VerifyFull);
        // …and a weaker block mode cannot silently reverse it.
        let weaker = TlsPolicy {
            mode: TlsMode::Require,
            ..TlsPolicy::default()
        };
        assert!(
            parse_conn(
                "postgresql://u@h/db?sslmode=verify-full&sslrootcert=/ca.pem",
                Some(&weaker),
            )
            .is_err(),
            "block must not weaken conn verify-full"
        );

        // key=value form with spaces and a quoted path; full trio.
        let parsed = parse_conn(
            "host=h user=u sslrootcert = '/my ca/ca.pem' sslcert=/c.pem sslkey=/k.pem",
            None,
        )
        .expect("kv form");
        assert_eq!(
            parsed.policy.root_cert,
            Some(RootCert("/my ca/ca.pem".into()))
        );
        assert_eq!(parsed.policy.client_cert, Some(RootCert("/c.pem".into())));
        assert_eq!(parsed.policy.client_key, Some(RootCert("/k.pem".into())));
        // The remainder reached the driver intact.
        assert_eq!(parsed.pg.get_user(), Some("u"));

        // `system` selects the platform store (no explicit root).
        let parsed = parse_conn("host=h sslrootcert=system", None).expect("system");
        assert_eq!(parsed.policy.root_cert, None);
    }

    #[test]
    fn conn_and_block_values_must_agree() {
        let block = TlsPolicy {
            root_cert: Some(RootCert("/etc/other.pem".into())),
            ..TlsPolicy::default()
        };
        // Disagreement: typed, names both sides.
        let err = parse_conn("host=h sslrootcert=/etc/ca.pem", Some(&block)).unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("sslrootcert") && msg.contains("/etc/other.pem"),
            "{msg}"
        );
        // Agreement: accepted.
        parse_conn("host=h sslrootcert=/etc/other.pem", Some(&block)).expect("agreeing dup");
        // Split credential across sources: cert in string, key in block.
        let block = TlsPolicy {
            client_key: Some(RootCert("/k.pem".into())),
            ..TlsPolicy::default()
        };
        let parsed = parse_conn("host=h sslcert=/c.pem", Some(&block)).expect("split");
        assert_eq!(parsed.policy.client_cert, Some(RootCert("/c.pem".into())));
        // …and the both-or-neither rule still bites across sources.
        let err = parse_conn("host=h sslcert=/c.pem", None).unwrap_err();
        assert!(err.to_string().contains("client_key is missing"), "{err}");
    }

    #[test]
    fn sslrootcert_system_selects_the_platform_store() {
        // libpq 16+ `sslrootcert=system` = platform trust store: the policy
        // resolves to a verifying mode with NO explicit root.
        let parsed = parse_conn(
            "postgresql://u@h/d?sslmode=verify-full&sslrootcert=system",
            None,
        )
        .expect("system root parses");
        assert_eq!(parsed.policy.mode, TlsMode::VerifyFull);
        assert!(
            parsed.policy.root_cert.is_none(),
            "system = platform store = no explicit root in the policy"
        );
    }

    #[test]
    fn rejected_parameters_are_named_never_bare() {
        // Every unsupported parameter is NAMED, with a hint where one exists.
        for (conn, param, hint_word) in [
            ("host=h sslpassword=secret", "sslpassword", "encrypted"),
            ("host=h gssencmode=require", "gssencmode", "GSS"),
            ("host=h requiressl=1", "requiressl", "sslmode"),
            ("host=h sslcrl=/crl.pem", "sslcrl", "revocation"),
            (
                "postgresql://u@h/db?connect_timeout=5&some_future_param=1",
                "some_future_param",
                "no rdlt equivalent",
            ),
        ] {
            let err = parse_conn(conn, None).unwrap_err();
            let msg = err.to_string();
            assert!(
                msg.contains(param) && msg.contains(hint_word),
                "{conn}: {msg}"
            );
        }
        // Pure syntax garbage still gets the driver's description, prefixed.
        let err = parse_conn("host=h port=notaport", None).unwrap_err();
        assert!(err.to_string().contains("does not parse"), "{err}");
    }

    #[test]
    fn application_name_defaults_and_yields() {
        let parsed = parse_conn("host=h user=u", None).expect("parses");
        assert_eq!(parsed.pg.get_application_name(), Some("rdlt"));
        let parsed = parse_conn("host=h user=u application_name=mine", None).expect("parses");
        assert_eq!(parsed.pg.get_application_name(), Some("mine"));
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
