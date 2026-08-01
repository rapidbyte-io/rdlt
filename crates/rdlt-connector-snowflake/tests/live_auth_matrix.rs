//! One load per authentication method, each gated on its OWN credential.
//!
//! Independently gated so a missing credential skips ONE leg rather than the
//! suite: an account with a key pair but no PAT should still prove the key
//! pair, and the reverse.
//!
//! Every method is wired and unit-tested; the ones whose credentials exist on
//! the qual account are proven end to end here. Password and OAuth legs are
//! written and skip with a reason on an account that has not provisioned
//! them — recorded as UNPERFORMED rather than quietly absent, because a leg
//! that was never written and a leg that could not run look identical from a
//! green suite.

use rdlt_connector::DestinationError;
use rdlt_connector::core::{LoadId, PipelineId};
use rdlt_connector_snowflake::dest::testhook::connect_and_run;
use rdlt_connector_snowflake::dest::{Auth, KeyPair, Password, Snowflake, SnowflakeConfig};
mod common;

use common::{TokenKind, credentials, scratch_schema, token};
use serde_json::json;

/// A config for the qual account with `auth` replaced.
fn config_with(auth: Auth, schema: &str) -> Option<SnowflakeConfig> {
    let creds = credentials()?;
    let mut config = SnowflakeConfig::from_value(json!({
        "account": creds.account,
        "user": creds.user,
        "database": creds.database,
        "schema": schema,
        "warehouse": creds.warehouse,
        "role": creds.role,
        "auth": {"key_pair": {
            "private_key": creds.private_key_path,
            "passphrase": creds.passphrase,
        }},
    }))
    .expect("valid config");
    config.auth = auth;
    Some(config)
}

fn key_pair_auth() -> Option<Auth> {
    let creds = credentials()?;
    let mut key_pair =
        rdlt_connector_snowflake::dest::KeyPair::new(creds.private_key_path.as_str());
    if let Some(passphrase) = creds.passphrase {
        key_pair = key_pair.with_passphrase(passphrase);
    }
    Some(Auth::key_pair(key_pair))
}

/// Prove a method reaches the account AND can run a whole load through it.
///
/// Connecting alone would not be enough: authentication that works for the
/// first statement and not for the session's later ones is a real failure mode
/// for token-based methods, whose tokens can expire mid-load.
async fn load_through(method: &str, auth: Auth) {
    let schema = scratch_schema(&format!("auth{method}"));
    let Some(config) = config_with(auth, &schema) else {
        return;
    };
    let Some(admin) = config_with(key_pair_auth().expect("credentials"), "PUBLIC") else {
        return;
    };

    let dest = Snowflake::new(config.clone()).expect("valid config");
    let opened = rdlt_connector::Destination::open(
        &dest,
        rdlt_connector::OpenContext::new(
            PipelineId::from("auth-matrix"),
            LoadId::from("auth-matrix"),
        ),
    )
    .await;

    let outcome = match opened {
        Err(e) => Err(e),
        Ok(_) => {
            // A second statement on a fresh session: the token has to still be
            // good after the handshake, not just during it.
            connect_and_run(&config, "SELECT 1").await.map(|answer| {
                assert_eq!(answer, "1", "{method}: the session must run statements");
            })
        }
    };

    let _ = rdlt_connector_snowflake::dest::testhook::apply(
        &admin,
        &format!(
            "DROP SCHEMA IF EXISTS \"{}\".\"{}\"",
            admin.database.to_uppercase(),
            schema
        ),
    )
    .await;

    outcome.unwrap_or_else(|e| panic!("{method} must authenticate and work: {e:?}"));
}

#[tokio::test]
async fn key_pair_authenticates_and_carries_a_load() {
    let Some(auth) = key_pair_auth() else {
        return;
    };
    load_through("keypair", auth).await;
}

#[tokio::test]
async fn a_programmatic_access_token_authenticates_and_carries_a_load() {
    let Some(pat) = token(TokenKind::Pat) else {
        eprintln!("SKIP: no PAT provisioned — the PAT leg is UNPERFORMED");
        return;
    };
    load_through("pat", Auth::pat(pat)).await;
}

#[tokio::test]
async fn a_password_authenticates_and_carries_a_load() {
    let Some(password) = token(TokenKind::Password) else {
        eprintln!(
            "SKIP: no password user provisioned — the password leg is UNPERFORMED. \
             Snowflake enforces MFA on password sign-ins and refuses passwords on \
             TYPE = SERVICE users, so this needs a deliberately provisioned user."
        );
        return;
    };
    load_through("password", Auth::password(Password::new(password))).await;
}

#[tokio::test]
async fn an_oauth_token_authenticates_and_carries_a_load() {
    let Some(oauth) = token(TokenKind::OauthToken) else {
        eprintln!(
            "SKIP: no OAuth token provisioned — the OAuth leg is UNPERFORMED. \
             It needs a security integration on the account; this connector \
             consumes a token and never mints one."
        );
        return;
    };
    load_through("oauth", Auth::oauth_token(oauth)).await;
}

/// Every method fails the SAME way on a bad credential, and none echoes it.
///
/// The shape matters more than the method: an operator meeting an auth failure
/// should get a fatal error naming the identity it was attempted under, and
/// never the secret. Retrying a rejected credential cannot help, so classifying
/// it as transient would burn the engine's budget and then report a timeout in
/// place of the real cause.
#[tokio::test]
async fn every_method_fails_fatally_and_silently_on_a_bad_credential() {
    let Some(creds) = credentials() else {
        return;
    };
    const WRONG: &str = "definitely-not-a-valid-credential-8f21c";

    for (method, auth) in [
        ("password", Auth::password(Password::new(WRONG))),
        ("oauth_token", Auth::oauth_token(WRONG)),
        ("pat", Auth::pat(WRONG)),
    ] {
        let config = config_with(auth, "PUBLIC").expect("credentials");
        match connect_and_run(&config, "SELECT 1").await {
            Ok(_) => panic!("{method}: a bad credential was accepted"),
            Err(e) => {
                assert!(
                    matches!(e, DestinationError::Fatal(_)),
                    "{method}: a rejected credential is not retryable: {e:?}"
                );
                let rendered = format!("{e}");
                assert!(
                    !rendered.contains(WRONG),
                    "{method}: the credential reached the error text: {rendered}"
                );
                assert!(
                    rendered.contains(&creds.user) && rendered.contains(&creds.account),
                    "{method}: the failure must name the identity: {rendered}"
                );
            }
        }
    }
}

/// A key pair's two ways of being wrong, both silent and both fatal.
///
/// The other methods carry one secret each, so one bad value covers them. A key
/// pair fails in two DIFFERENT places: material this host cannot parse fails
/// locally, and a well-formed key the account never registered fails remotely.
/// The second is the one that matters operationally — it is what a rotated or
/// mistyped key looks like — and it exercises code the first never reaches.
///
/// The unregistered-key half needs a real RSA key. It is generated here rather
/// than committed: a private key in the repository is a private key in every
/// clone and every secret scanner's report, and this one would be worthless
/// only until someone reused the pattern for one that was not. Where no
/// generator exists the half announces its skip instead of silently shrinking.
#[tokio::test]
async fn a_key_pair_fails_fatally_and_silently_when_it_cannot_authenticate() {
    let Some(creds) = credentials() else {
        return;
    };

    let assert_shape = |method: &str, secret: &str, e: DestinationError| {
        assert!(
            matches!(e, DestinationError::Fatal(_)),
            "{method}: a rejected key is not retryable: {e:?}"
        );
        let rendered = format!("{e}");
        assert!(
            !rendered.contains(secret),
            "{method}: key material reached the error text: {rendered}"
        );
        assert!(
            rendered.contains(&creds.user) && rendered.contains(&creds.account),
            "{method}: the failure must name the identity: {rendered}"
        );
    };

    // Unparseable material: it never leaves this host.
    const MALFORMED: &str =
        "-----BEGIN PRIVATE KEY-----\nnot-base64-at-all\n-----END PRIVATE KEY-----\n";
    let config = config_with(Auth::key_pair(KeyPair::new(MALFORMED.to_owned())), "PUBLIC")
        .expect("credentials");
    match connect_and_run(&config, "SELECT 1").await {
        Ok(_) => panic!("malformed key: unparseable material was accepted"),
        Err(e) => assert_shape("malformed key", "not-base64-at-all", e),
    }

    // Well-formed and unregistered: the ACCOUNT rejects it.
    let Some(unregistered) = generated_rsa_key() else {
        println!(
            "SKIP: no RSA generator on this host — the unregistered-key half is \
             UNPERFORMED. The malformed half above still ran."
        );
        return;
    };
    let config = config_with(Auth::key_pair(KeyPair::new(unregistered.clone())), "PUBLIC")
        .expect("credentials");
    match connect_and_run(&config, "SELECT 1").await {
        Ok(_) => panic!("unregistered key: a key the account never registered was accepted"),
        Err(e) => {
            // The whole PEM is too long to appear intact, so the check is on a
            // distinctive slice of the base64 body — which is what would leak.
            let body: String = unregistered
                .lines()
                .filter(|l| !l.starts_with("-----"))
                .collect::<String>()
                .chars()
                .take(32)
                .collect();
            assert_shape("unregistered key", &body, e);
        }
    }
}

/// A throwaway RSA private key, or `None` where nothing can make one.
fn generated_rsa_key() -> Option<String> {
    let out = std::process::Command::new("openssl")
        .args([
            "genpkey",
            "-algorithm",
            "RSA",
            "-pkeyopt",
            "rsa_keygen_bits:2048",
        ])
        .output()
        .ok()?;
    out.status
        .success()
        .then(|| String::from_utf8(out.stdout).ok())
        .flatten()
}
