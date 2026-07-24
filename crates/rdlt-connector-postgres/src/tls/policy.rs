//! The config-shaped TLS vocabulary: the policy types, the config-error enum,
//! and the agree-or-error resolution between a conn-string `sslmode` and the
//! `tls:` block. No network, no rustls — only the posture and its rules.

use serde::{Deserialize, Serialize};
use tokio_postgres::config::SslMode;

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
    pub(crate) fn wants_encryption(self) -> bool {
        !matches!(self, TlsMode::Disable)
    }
}

/// A PEM input — trust root, client certificate, or client key: a filesystem
/// path to a PEM file, or the PEM text inline (config strings starting with
/// `-----BEGIN` are treated as inline).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(transparent)]
pub struct PemSource(pub String);

/// The per-connection TLS posture shared by source and destination.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct TlsPolicy {
    #[serde(default)]
    pub mode: TlsMode,
    #[serde(default)]
    pub root_cert: Option<PemSource>,
    /// Client certificate for mutual TLS. Path or inline PEM; requires
    /// `client_key`.
    #[serde(default)]
    pub client_cert: Option<PemSource>,
    /// Private key matching `client_cert` (PKCS#8/RSA/SEC1, unencrypted).
    #[serde(default)]
    pub client_key: Option<PemSource>,
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
    /// TLS setup failure naming its subject — a PEM input
    /// (`root_cert `path``), the crypto provider, or verifier
    /// construction. One variant because every arm is "the TLS stack
    /// could not be built from this input".
    #[error("tls {subject}: {detail}")]
    Setup { subject: String, detail: String },
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

/// Resolve the effective policy from the parsed conn string and the optional
/// config block. The block wins ONLY when consistent: an explicit conn
/// `sslmode` may be refined (require → verify_*) but never
/// silently reversed.
pub(crate) fn resolve_policy(
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
            root_cert: Some(PemSource("/some/ca.pem".into())),
            ..TlsPolicy::default()
        };
        let resolved = resolve_policy(&conn("host=h sslmode=disable"), Some(&prefer_block))
            .expect("prefer block composes with disable");
        assert_eq!(resolved.mode, TlsMode::Prefer);
    }

    #[test]
    fn credential_shape_rules_are_typed_and_early() {
        let cert = PemSource("cert".into());
        let key = PemSource("key".into());
        let policy = |mode, c: Option<&PemSource>, k: Option<&PemSource>| TlsPolicy {
            mode,
            root_cert: None,
            client_cert: c.cloned(),
            client_key: k.cloned(),
        };
        // Cert without key / key without cert: name the missing counterpart.
        let err = validate_credentials(&policy(TlsMode::Require, Some(&cert), None)).unwrap_err();
        assert!(err.to_string().contains("client_key is missing"), "{err}");
        let err = validate_credentials(&policy(TlsMode::Require, None, Some(&key))).unwrap_err();
        assert!(err.to_string().contains("client_cert is missing"), "{err}");
        // Credential over plaintext: contradiction.
        let err =
            validate_credentials(&policy(TlsMode::Disable, Some(&cert), Some(&key))).unwrap_err();
        assert!(err.to_string().contains("disable"), "{err}");
        // Complete pair with TLS active: fine.
        validate_credentials(&policy(TlsMode::Require, Some(&cert), Some(&key)))
            .expect("valid shape");
    }
}
