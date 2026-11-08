use rdlt::{SecretFault, SecretKind, SecretReference, SecretResolver as _};

use super::PythonSecrets;
use crate::{embedded, runtime};

fn store(py: pyo3::Python<'_>) -> PythonSecrets {
    let code = c"lambda name: {'pg_dsn': 'postgres://u:p@h/db', 'number': 7}.get(name)";
    PythonSecrets::new(py.eval(code, None, None).unwrap().unbind())
}

fn named(name: &str) -> SecretReference {
    SecretReference {
        kind: SecretKind::Named,
        name: name.to_owned(),
    }
}

#[test]
fn a_named_secret_resolves_through_the_operator_s_callable() {
    embedded::with(|py| {
        let store = store(py);
        let resolved = runtime::wait(py, async move { store.resolve(&named("pg_dsn")).await });
        let secret = resolved.unwrap().expect("resolved");
        assert_eq!(secret.expose(), "postgres://u:p@h/db");
    });
}

#[test]
fn none_text_and_other_kinds_are_faults_that_quote_nothing() {
    embedded::with(|py| {
        for (reference, expected) in [
            (named("absent"), "Missing"),
            (named("number"), "NotText"),
            (
                SecretReference {
                    kind: SecretKind::Env,
                    name: "HOME".to_owned(),
                },
                "Unsupported",
            ),
        ] {
            let store = store(py);
            let fault = runtime::wait(py, async move { store.resolve(&reference).await })
                .unwrap()
                .expect_err("a fault");
            let variant = match fault {
                SecretFault::Missing => "Missing",
                SecretFault::NotText => "NotText",
                SecretFault::Unsupported => "Unsupported",
                other => panic!("unexpected fault {other:?}"),
            };
            assert_eq!(variant, expected);
        }
    });
}

#[test]
fn a_store_shows_nothing_of_what_it_serves() {
    embedded::with(|py| {
        let store = py
            .eval(c"{'name': 'hunter2-secret'}.get", None, None)
            .unwrap()
            .unbind();
        assert_eq!(format!("{:?}", PythonSecrets::new(store)), "PythonSecrets");
    });
}
