use std::fmt::Write as _;

use rdlt_connector::{ConnectorError, ConnectorErrorKind};
use rdlt_host::{Redactions, SecretKind, SecretReference, SecretResolver as _};

use super::Canary;
use crate::world::{FORMS, Told, quoting};

/// Every text an error holds: its own and each of its causes', as `Display` and `Debug` write them.
fn texts(error: &(dyn std::error::Error + 'static)) -> String {
    let mut texts = format!("{error} {error:?}");
    let mut cause = error.source();
    while let Some(error) = cause {
        write!(texts, " {error} {error:?}").ok();
        cause = error.source();
    }
    texts
}

/// What a connector of `canary`'s world is told.
fn told(canary: &Canary) -> Told {
    let value = |name| canary.value(name).expect("a secret of the canary");
    Told {
        canary: value("canary"),
        lines: value("lines"),
        token: value("token"),
    }
}

#[test]
fn a_secret_is_found_in_a_text_that_holds_it_and_no_other() {
    let canary = Canary::of("oracle-7");
    let told = told(&canary);
    for secret in [&told.canary, &told.lines, &told.token] {
        assert!(
            canary.found(&format!("said {secret} once")).is_some(),
            "{secret:?}"
        );
    }
    assert!(canary.found("second-line-of-oracle-7 alone").is_some());
    for clean in [
        "no world named oracle-7",
        "told *** as a cause",
        "{",
        "}",
        "rdlt-canary-",
    ] {
        assert_eq!(canary.found(clean), None, "{clean}");
    }
}

#[tokio::test]
async fn the_vault_resolves_the_canarys_secrets_and_nothing_else() {
    let canary = Canary::of("oracle-7");
    let vault = canary.vault();
    let resolved = |kind, name: &str| SecretReference {
        kind,
        name: name.to_owned(),
    };
    for name in ["canary", "lines", "token"] {
        let secret = vault
            .resolve(&resolved(SecretKind::Named, name))
            .await
            .expect("a secret of the canary resolves");
        assert_eq!(Some(secret.expose().clone()), canary.value(name));
    }
    for (kind, name) in [
        (SecretKind::Named, "other"),
        (SecretKind::Env, "canary"),
        (SecretKind::File, "/canary"),
    ] {
        assert!(
            vault.resolve(&resolved(kind, name)).await.is_err(),
            "{name}"
        );
    }
    assert_eq!(format!("{vault:?}"), "Vault(***)");
}

#[test]
fn every_form_a_connector_quotes_its_secrets_in_is_scrubbed_by_its_host() {
    let canary = Canary::of("oracle-7");
    let told = told(&canary);
    let redactions = Redactions::new();
    for secret in [&told.canary, &told.lines, &told.token] {
        redactions.add(secret);
    }
    for turn in 0..FORMS {
        let (message, cause) = quoting(&told, turn, "injected fault at Read".to_owned());
        let mut error = ConnectorError::new(ConnectorErrorKind::Transient, message);
        if let Some(cause) = cause {
            error = error.with_source(std::io::Error::other(cause));
        }
        let said = texts(&error);
        assert!(
            canary.found(&said).is_some(),
            "form {turn} says no secret: {said}"
        );
        let kept = texts(&error.received(&|text| redactions.scrubbed(text)));
        assert_eq!(canary.found(&kept), None, "form {turn}: {kept}");
    }
}
