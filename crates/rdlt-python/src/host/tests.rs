use std::ffi::CString;
use std::sync::Arc;

use pyo3::types::{PyAnyMethods as _, PyTypeMethods as _};
use rdlt_connector_reference::{MemoryDestination, MemorySource};

use super::{NativeHost, Settings};
use crate::embedded;
use crate::pipeline::NativePipeline;
use crate::pipeline::tests::memory_document;

/// `rdlt.Host`'s attributes, as a namespace holds them, allowing `allow_env`, with `build_wait`
/// written as Python.
fn settings_waiting(py: pyo3::Python<'_>, allow_env: &str, build_wait: &str) -> Settings {
    let code = format!(
        "__import__('types').SimpleNamespace(connectors=(), writable=(), readable=(), \
         always_readable=(), allow_network=False, build_wait={build_wait}, \
         allow_env=({allow_env:?},), allow_files=(), secret_store=None, remote=None, \
         trust_binaries=False)"
    );
    let code = CString::new(code).unwrap();
    py.eval(&code, None, None)
        .unwrap()
        .extract()
        .expect("the settings extract")
}

fn settings(py: pyo3::Python<'_>, allow_env: &str) -> Settings {
    settings_waiting(py, allow_env, "None")
}

#[test]
fn an_environment_secret_resolves_only_where_the_host_allows_its_variable() {
    embedded::with(|py| {
        // Set for this test's process alone: nextest runs each test in a process of its own.
        py.run(
            c"import os; os.environ['RDLT_TEST_STORE'] = 'from-env'",
            None,
            None,
        )
        .unwrap();
        let mut document: serde_json::Value =
            serde_json::from_str(&memory_document("secret", 1)).unwrap();
        document["destination"]["config"]["store"] = "${env:RDLT_TEST_STORE}".into();
        for (allowed, builds) in [("RDLT_TEST_STORE", true), ("OTHER", false)] {
            let host = NativeHost::new(settings(py, allowed)).host(py).unwrap();
            let host = host
                .source::<MemorySource>()
                .destination::<MemoryDestination>();
            let builder = rdlt::Pipeline::from_text(&document.to_string(), "test").unwrap();
            let built = NativePipeline::built(py, builder, Arc::new(host))
                .and_then(|pipeline| pipeline.check(py));
            match (built, builds) {
                (Ok(()), true) => {}
                (Err(error), false) => {
                    assert_eq!(
                        error.get_type(py).name().unwrap().to_string(),
                        "ConfigError"
                    );
                }
                (other, _) => panic!("{allowed}: {other:?}"),
            }
        }
    });
}

#[test]
fn every_build_of_a_host_shares_its_facade_host() {
    embedded::with(|py| {
        let native = NativeHost::new(settings(py, "OTHER"));
        let first = native.shared(py).unwrap();
        let second = native.shared(py).unwrap();
        assert!(Arc::ptr_eq(&first, &second));
    });
}

#[test]
fn a_build_wait_reaches_the_facade_s_host() {
    embedded::with(|py| {
        let wait = "__import__('datetime').timedelta(seconds=30)";
        let host = NativeHost::new(settings_waiting(py, "OTHER", wait))
            .host(py)
            .unwrap();
        assert!(format!("{host:?}").contains("build_wait: 30s"), "{host:?}");
    });
}
