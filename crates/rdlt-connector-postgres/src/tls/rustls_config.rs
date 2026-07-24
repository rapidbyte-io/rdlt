//! Turning a resolved [`TlsPolicy`] into a rustls `ClientConfig`: trust store
//! loading, client-credential loading, and the per-mode verifier wiring. Every
//! material failure is a typed [`TlsConfigError`] naming the offending input,
//! and construction happens BEFORE any connection so a mismatched cert/key
//! pair fails as a config error, not mid-handshake.

use std::sync::Arc;

use rustls::RootCertStore;

use super::policy::{PemSource, TlsConfigError, TlsMode, TlsPolicy, validate_credentials};
use crate::tls_verify::{AcceptAnyCert, ChainOnly, provider};

/// Resolve a `PemSource`-shaped input (path or inline PEM) to labeled bytes.
fn pem_bytes(source: &PemSource, kind: &str) -> Result<(String, Vec<u8>), TlsConfigError> {
    let PemSource(source) = source;
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
        Some(PemSource(source)) => {
            let (label, pem_bytes): (String, Vec<u8>) =
                if source.trim_start().starts_with("-----BEGIN") {
                    ("<inline pem>".into(), source.clone().into_bytes())
                } else {
                    (
                        source.clone(),
                        std::fs::read(source).map_err(|e| TlsConfigError::Setup {
                            subject: format!("root_cert `{source}`"),
                            detail: format!("unreadable: {e}"),
                        })?,
                    )
                };
            let mut cursor = std::io::Cursor::new(pem_bytes);
            let mut added = 0usize;
            for item in rustls_pemfile::certs(&mut cursor) {
                let cert = item.map_err(|e| TlsConfigError::Setup {
                    subject: format!("root_cert `{label}`"),
                    detail: format!("PEM parse error: {e}"),
                })?;
                store.add(cert).map_err(|e| TlsConfigError::Setup {
                    subject: format!("root_cert `{label}`"),
                    detail: format!("not a usable CA certificate: {e}"),
                })?;
                added += 1;
            }
            if added == 0 {
                return Err(TlsConfigError::Setup {
                    subject: format!("root_cert `{label}`"),
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
        .map_err(|e| TlsConfigError::Setup {
            subject: "crypto provider".into(),
            detail: e.to_string(),
        })
}

/// Build the rustls `ClientConfig` for a policy — `None` for plaintext
/// (`disable`). A mismatched cert/key pair fails HERE (rustls checks
/// consistency at config construction), a config error before any connection.
pub(crate) fn client_config(
    policy: &TlsPolicy,
) -> Result<Option<rustls::ClientConfig>, TlsConfigError> {
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
                        |e| TlsConfigError::Setup {
                            subject: "trust store".into(),
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

#[cfg(test)]
mod tests {
    use super::*;

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
            client_cert: cert.map(|c| PemSource(c.into())),
            client_key: key.map(|k| PemSource(k.into())),
        }
    }

    #[test]
    fn root_errors_are_typed_and_name_the_input() {
        let missing = TlsPolicy {
            mode: TlsMode::VerifyFull,
            root_cert: Some(PemSource("/nonexistent/ca.pem".into())),
            ..TlsPolicy::default()
        };
        let err = root_store(&missing).unwrap_err();
        assert!(err.to_string().contains("/nonexistent/ca.pem"), "{err}");

        let garbage = TlsPolicy {
            mode: TlsMode::VerifyFull,
            root_cert: Some(PemSource("-----BEGIN CERTIFICATE-----\ngarbage".into())),
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
            root_cert: Some(PemSource(pem.clone())),
            ..TlsPolicy::default()
        };
        assert_eq!(root_store(&inline).expect("inline").len(), 1);

        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("ca.pem");
        std::fs::write(&path, pem).expect("write");
        let from_path = TlsPolicy {
            mode: TlsMode::VerifyCa,
            root_cert: Some(PemSource(path.display().to_string())),
            ..TlsPolicy::default()
        };
        assert_eq!(root_store(&from_path).expect("path").len(), 1);
    }

    #[test]
    fn complete_credential_pair_builds_config() {
        let (cert, key) = client_pair();
        // A valid shape with TLS active builds the handshake config.
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
            p.root_cert = Some(PemSource(ca.pem()));
            assert!(
                client_config(&p).expect("config").is_some(),
                "mode {mode:?}"
            );
        }
    }
}
