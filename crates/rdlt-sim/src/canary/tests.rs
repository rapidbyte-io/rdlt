use std::panic::{AssertUnwindSafe, catch_unwind};

use rdlt_connector::{ConnectorError, ConnectorErrorKind, ConnectorId};
use rdlt_host::{ProviderError, Redactions, SecretKind, SecretReference, SecretResolver as _};

use super::{Canary, texts};
use crate::rng::SplitMix64;
use crate::world::{FORMS, Told, World, quoting};

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
fn no_configurations_schema_holds_what_its_connector_was_told() {
    let schemas = [
        schemars::schema_for!(crate::source::SimSourceConfig),
        schemars::schema_for!(crate::changes::SimChangeSourceConfig),
        schemars::schema_for!(crate::destination::SimDestinationConfig),
    ];
    for schema in schemas.map(serde_json::to_value) {
        let schema = schema.expect("a schema serializes");
        let properties = schema["properties"]
            .as_object()
            .expect("a schema has properties");
        assert_eq!(properties.keys().collect::<Vec<_>>(), ["world"], "{schema}");
    }
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

/// An error whose own text is clean and whose cause says `cause`.
fn caused_by(cause: &str) -> ProviderError {
    ProviderError::NotFound {
        id: ConnectorId::parse("sim-source").expect("a valid id"),
        source: Some(std::io::Error::other(cause.to_owned())),
    }
}

#[test]
fn an_answer_whose_cause_holds_a_secret_is_a_violation() {
    let canary = Canary::of("oracle-7");
    let registered = World::register("answered-oracle-7", &mut SplitMix64::new(1));
    let world = registered.world();
    canary.answered(world, "reset", &caused_by("a lookup of ***"));
    assert_eq!(world.violations(), Vec::<String>::new());
    canary.answered(
        world,
        "reset",
        &caused_by("a lookup of rdlt-canary-oracle-7"),
    );
    let violations = world.violations();
    assert_eq!(violations.len(), 1, "{violations:#?}");
    assert!(
        violations[0].contains("rdlt-canary-oracle-7"),
        "{violations:#?}"
    );
}

#[test]
fn a_placement_whose_cause_holds_a_secret_fails() {
    let canary = Canary::of("oracle-7");
    canary.placed("source", &caused_by("a lookup of ***"));
    let placed = catch_unwind(AssertUnwindSafe(|| {
        canary.placed("source", &caused_by("a lookup of second-line-of-oracle-7"));
    }));
    let message = placed.expect_err("a secret in a placement's error fails");
    let message = message
        .downcast_ref::<String>()
        .expect("the failure says why");
    assert!(message.contains("second-line-of-oracle-7"), "{message}");
}

#[test]
fn a_secret_logged_where_an_operator_reads_it_is_a_leak() {
    let canary = Canary::of("oracle-7");
    let (logged, guard) = canary.watching();
    tracing::warn!(target: "rdlt_host", line = "said rdlt-canary-oracle-7 \t", "a connector wrote output");
    tracing::info!(target: "hyper", "second-line-of-oracle-7");
    tracing::debug!(target: "h2", "rdlt-canary-token-oracle-7 in a frame");
    tracing::warn!(target: "rdlt_host", line = "said ***", "a connector wrote output");
    let failed = caused_by("a lookup of rdlt-canary-token-oracle-7");
    tracing::warn!(target: "rdlt_engine", error = &failed as &dyn std::error::Error, "placing failed");
    drop(guard);
    tracing::warn!(target: "rdlt_host", "rdlt-canary-oracle-7, once nothing watches");
    let leaks = logged.leaks();
    assert_eq!(leaks.len(), 3, "{leaks:#?}");
    assert!(leaks[0].contains("rdlt-canary-oracle-7"), "{leaks:#?}");
    assert!(leaks[1].contains("second-line-of-oracle-7"), "{leaks:#?}");
    assert!(
        leaks[2].contains("rdlt-canary-token-oracle-7"),
        "{leaks:#?}"
    );
}
