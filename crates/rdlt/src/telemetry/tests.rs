use rdlt_testkit::tls::Pki;

use super::{Settings, commit_seconds, endpoint_of, environment, providers};
use crate::codes;

/// Drops the providers from a blocking thread, as their flush waits on threads of their own.
async fn shut_down(telemetry: super::Telemetry) {
    tokio::time::timeout(
        std::time::Duration::from_secs(60),
        tokio::task::spawn_blocking(move || drop(telemetry)),
    )
    .await
    .expect("it shuts down")
    .expect("it ends");
}

fn shown(error: &crate::Error) -> String {
    let report = error.report();
    format!("{error} {error:?} {} {:?}", report.message, report.causes)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_endpoint_that_does_not_parse_is_never_echoed() {
    let settings = Settings {
        endpoint: Some("http://user:CANARY-token@[bad".to_owned()),
        ..Settings::default()
    };
    let error = providers(&settings).expect_err("the endpoint is refused");
    assert_eq!(error.code(), Some(codes::TELEMETRY_UNAVAILABLE));
    let shown = shown(&error);
    assert!(
        !shown.contains("CANARY") && !shown.contains("user:"),
        "{shown}"
    );
    assert!(shown.contains("OTEL_EXPORTER_OTLP_"), "{shown}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_certificate_file_that_cannot_be_read_is_named_by_its_variable_and_not_its_path() {
    let settings = Settings {
        endpoint: Some("https://collector.invalid:4317".to_owned()),
        traces_certificate: Some("/nonexistent/CANARY-path.pem".to_owned()),
        ..Settings::default()
    };
    let error = providers(&settings).expect_err("the file is missing");
    let shown = shown(&error);
    assert!(shown.contains("OTEL_EXPORTER_OTLP_CERTIFICATE"), "{shown}");
    assert!(!shown.contains("CANARY"), "{shown}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_https_collector_is_verified_by_the_system_roots_or_by_the_certificate_named() {
    let system = Settings {
        endpoint: Some("https://collector.invalid:4317".to_owned()),
        ..Settings::default()
    };
    shut_down(providers(&system).expect("built over the system roots")).await;
    let pki = Pki::new("collector-ca");
    let named = Settings {
        endpoint: Some("https://collector.invalid:4317".to_owned()),
        traces_certificate: Some(pki.ca().display().to_string()),
        metrics_certificate: Some(pki.ca().display().to_string()),
    };
    shut_down(providers(&named).expect("built over the certificate named")).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_plaintext_or_unix_collector_is_built_with_no_tls_and_so_needs_no_ca() {
    // A certificate that cannot be read would be refused if TLS were set up for the endpoint.
    for endpoint in [
        "http://collector.invalid:4318",
        "unix:/tmp/rdlt-collector.sock",
    ] {
        let settings = Settings {
            endpoint: Some(endpoint.to_owned()),
            traces_certificate: Some("/nonexistent/ca.pem".to_owned()),
            metrics_certificate: Some("/nonexistent/ca.pem".to_owned()),
        };
        shut_down(providers(&settings).unwrap_or_else(|error| panic!("{endpoint}: {error}"))).await;
    }
}

#[test]
fn only_an_endpoint_that_is_not_plaintext_is_verified() {
    for (endpoint, plaintext) in [
        (None, true),
        (Some("http://collector:4317"), true),
        (Some("HTTP://collector:4317"), true),
        (Some("unix:/run/otel.sock"), true),
        (Some("https://collector:4317"), false),
        (Some("collector:4317"), false),
    ] {
        assert_eq!(super::plaintext(endpoint), plaintext, "{endpoint:?}");
    }
}

#[test]
fn without_a_runtime_installing_is_an_error_and_not_a_panic() {
    let error = providers(&Settings::default()).expect_err("there is no runtime");
    assert_eq!(error.code(), Some(codes::TELEMETRY_UNAVAILABLE));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_providers_have_a_debug_form_that_names_nothing_of_them() {
    let telemetry = providers(&Settings::default()).expect("built");
    assert_eq!(format!("{telemetry:?}"), "Telemetry { .. }");
    shut_down(telemetry).await;
}

/// A set of variables, as a lookup.
fn vars<'a>(set: &'a [(&'a str, &'a str)]) -> impl Fn(&str) -> Option<String> + 'a {
    move |name| {
        set.iter()
            .find(|(key, _)| *key == name)
            .map(|(_, value)| (*value).to_owned())
    }
}

#[test]
fn a_signal_s_certificate_comes_before_the_general_one_and_an_empty_one_is_none() {
    let settings = Settings::from_vars(&vars(&[
        ("OTEL_EXPORTER_OTLP_CERTIFICATE", "/etc/general.pem"),
        ("OTEL_EXPORTER_OTLP_TRACES_CERTIFICATE", "/etc/traces.pem"),
    ]));
    assert_eq!(
        settings.traces_certificate.as_deref(),
        Some("/etc/traces.pem")
    );
    assert_eq!(
        settings.metrics_certificate.as_deref(),
        Some("/etc/general.pem")
    );
    assert_eq!(settings.endpoint, None);
    let empty = Settings::from_vars(&vars(&[("OTEL_EXPORTER_OTLP_CERTIFICATE", "")]));
    assert_eq!(empty.traces_certificate, None);
    assert_eq!(empty.metrics_certificate, None);
    assert!(Settings::from_vars(&vars(&[])).traces_certificate.is_none());
}

#[test]
fn a_signal_s_endpoint_comes_before_the_general_one_and_an_empty_one_is_none() {
    let none = Settings::default();
    let set = [
        ("OTEL_EXPORTER_OTLP_ENDPOINT", "https://general:4317"),
        ("OTEL_EXPORTER_OTLP_METRICS_ENDPOINT", "http://metrics:4317"),
    ];
    assert_eq!(
        endpoint_of(&none, "TRACES", &vars(&set)).as_deref(),
        Some("https://general:4317")
    );
    assert_eq!(
        endpoint_of(&none, "METRICS", &vars(&set)).as_deref(),
        Some("http://metrics:4317")
    );
    let named = Settings {
        endpoint: Some("unix:/run/otel.sock".to_owned()),
        ..Settings::default()
    };
    assert_eq!(
        endpoint_of(&named, "TRACES", &vars(&set)).as_deref(),
        Some("unix:/run/otel.sock")
    );
    assert_eq!(
        endpoint_of(
            &none,
            "TRACES",
            &vars(&[("OTEL_EXPORTER_OTLP_ENDPOINT", "")])
        ),
        None
    );
}

#[test]
fn only_the_histograms_of_a_commit_s_time_get_the_sub_second_buckets() {
    for name in ["rdlt_commit_seconds", "rdlt_commit_flush_seconds"] {
        assert!(commit_seconds(name), "{name}");
    }
    for name in ["rdlt_commits_total", "rdlt_other_seconds", "commit_seconds"] {
        assert!(!commit_seconds(name), "{name}");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn dropping_the_providers_shuts_them_down() {
    let telemetry = providers(&Settings::default()).expect("built");
    let (tracer, meter) = (telemetry.tracer.clone(), telemetry.meter.clone());
    shut_down(telemetry).await;
    assert!(tracer.shutdown().is_err(), "the tracer was shut down");
    assert!(meter.shutdown().is_err(), "the meter was shut down");
}

#[test]
fn the_process_environment_gives_a_variable_it_has_and_none_it_lacks() {
    assert!(environment("PATH").is_some_and(|path| !path.is_empty()));
    assert_eq!(environment("RDLT_NO_SUCH_VARIABLE_ANYWHERE"), None);
}
