//! TLS policy for BOTH Postgres connectors (feature 006 US1, contract:
//! `specs/006-postgres-completeness/contracts/tls-policy.md`).
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
    /// Client certificate for mutual TLS (feature 007, contract
    /// tls-client-auth.md). Path or inline PEM; requires `client_key`.
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
}

/// Connect-phase TLS failures, distinguished per the contract.
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
    /// The server rejected OUR client credential (mTLS, feature 007):
    /// TLS-level certificate alerts or auth-phase SQLSTATE 28000.
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

/// Resolve the effective policy from the parsed conn string and the optional
/// config block. The block wins ONLY when consistent (contract rule): an
/// explicit conn `sslmode` may be refined (require → verify_*) but never
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
        // Explicit plaintext vs a block demanding encryption.
        SslMode::Disable => block.mode.wants_encryption(),
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
    validate_credentials(block)?;
    Ok(block.clone())
}

/// Client-credential shape rules (contract tls-client-auth.md C2), enforced
/// BEFORE any connection: both-or-neither, and never with plaintext.
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
/// key, with typed errors naming the offending input (contract C2/C4).
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
    // config construction) — a config error before any connection (C4).
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
    let detail = format!("{err}");
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
    // Auth-phase client-certificate rejection (contract tls-client-auth C3):
    // pg_hba `cert`/`clientcert=` failures surface as SQLSTATE 28000 with a
    // certificate-naming message, AFTER a successful handshake. The Display
    // form is just "db error" (tokio-postgres drops the cause — 006 finding),
    // so read the REAL server message through as_db_error().
    if let Some(db) = err.as_db_error()
        && db.code().code() == "28000"
        && db.message().to_lowercase().contains("certificate")
    {
        return ConnectError {
            failure: TlsFailure::ClientCert,
            detail: format!("{detail}: {}", db.message()),
            transient: false,
        };
    }
    // Non-TLS failure: the 005 SQLSTATE heuristic (no code = io-shaped).
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

    // ---- feature 007: client credentials (contract tls-client-auth.md) ----

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
        // C5: adding a credential must not change what any mode means —
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

    #[test]
    fn rustls_classification_distinguishes_client_cert_rejection() {
        use rustls::AlertDescription as AD;
        use rustls::CertificateError as CE;
        // Server-verification failures keep their 006 classes…
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
