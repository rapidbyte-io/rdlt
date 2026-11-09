//! Export of the facade's traces and metrics over the OpenTelemetry protocol, configured by the
//! standard `OTEL_*` variables.

#[cfg(test)]
mod tests;

use std::io::IsTerminal as _;

use opentelemetry::metrics::MeterProvider as _;
use opentelemetry::trace::{TraceContextExt as _, TracerProvider as _};
use opentelemetry_otlp::tonic_types::transport::{Certificate, ClientTlsConfig};
use opentelemetry_otlp::{WithExportConfig as _, WithTonicConfig as _};
use opentelemetry_sdk::metrics::{Aggregation, SdkMeterProvider, Stream};
use opentelemetry_sdk::trace::SdkTracerProvider;
use rdlt_host::TraceParent;
use tracing_opentelemetry::OpenTelemetrySpanExt as _;
use tracing_subscriber::Layer as _;
use tracing_subscriber::filter::FilterExt as _;
use tracing_subscriber::layer::SubscriberExt as _;
use tracing_subscriber::util::SubscriberInitExt as _;

use crate::{Error, codes};

/// Seconds: the edges of the histograms of a commit's time, which the SDK's defaults would put
/// in one bucket.
const SECONDS: [f64; 12] = [
    0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0,
];

/// Targets whose spans and events the export leaves out, so exporting is not itself exported.
const OWN_TRAFFIC: [&str; 7] = [
    "h2",
    "hyper",
    "hyper_util",
    "tonic",
    "tower",
    "opentelemetry",
    "opentelemetry_sdk",
];

/// The installed export: dropping it flushes what is buffered and shuts the exporters down.
///
/// The exporters flush from threads of their own, which a drop on a thread of a single-threaded
/// runtime waits for; drop it from a blocking thread or after the runtime has ended its work.
#[must_use = "dropping it ends the export"]
pub struct Telemetry {
    tracer: SdkTracerProvider,
    meter: SdkMeterProvider,
}

impl std::fmt::Debug for Telemetry {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.debug_struct("Telemetry").finish_non_exhaustive()
    }
}

impl Drop for Telemetry {
    fn drop(&mut self) {
        // What a collector does not take at shutdown is lost; the run is not.
        self.tracer.shutdown().ok();
        self.meter.shutdown().ok();
    }
}

/// What the `OTEL_*` variables set that the exporters' own reading of them does not cover.
#[derive(Debug, Default)]
struct Settings {
    /// The endpoint to use in place of those the variables name.
    endpoint: Option<String>,
    /// The file of the certificate that verifies the traces collector, by its path.
    traces_certificate: Option<String>,
    /// The file of the certificate that verifies the metrics collector, by its path.
    metrics_certificate: Option<String>,
}

impl Settings {
    /// The settings `vars` holds, a variable's value or none.
    fn from_vars(vars: &dyn Fn(&str) -> Option<String>) -> Self {
        let read = |specific: &str| {
            vars(specific)
                .or_else(|| vars("OTEL_EXPORTER_OTLP_CERTIFICATE"))
                .filter(|path| !path.is_empty())
        };
        Self {
            endpoint: None,
            traces_certificate: read("OTEL_EXPORTER_OTLP_TRACES_CERTIFICATE"),
            metrics_certificate: read("OTEL_EXPORTER_OTLP_METRICS_CERTIFICATE"),
        }
    }
}

/// Installs export of traces and metrics over the OpenTelemetry protocol, as the `OTEL_*`
/// variables configure it: the process's `tracing` subscriber, its `metrics` recorder, and the
/// trace context each connector call sends.
///
/// `RUST_LOG`, where set, filters both what is exported and what is written to standard error;
/// where it is not, `info` is exported and `stderr_level`, a `tracing` level, is written, in
/// colour only on a terminal.
///
/// A collector at an `https` endpoint is verified against the operating system's roots, or
/// against the certificate `OTEL_EXPORTER_OTLP_CERTIFICATE` names. A failed export is logged by
/// the exporters through `tracing`, and never stalls a run.
///
/// No span or metric attribute holds a configuration value; connector text in them is shown as
/// the host shows it. No error or log names the value of an endpoint, which may hold
/// credentials.
///
/// # Errors
///
/// `telemetry_unavailable` where there is no tokio runtime to export from, an exporter cannot be
/// built, or a subscriber or recorder is already installed.
pub fn otlp(stderr_level: &str) -> Result<Telemetry, Error> {
    install(&Settings::from_vars(&environment), stderr_level)
}

/// `RUST_LOG`'s filter, else one at `level`.
fn filter_at(level: &str) -> tracing_subscriber::EnvFilter {
    tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new(level))
}

fn install(settings: &Settings, stderr_level: &str) -> Result<Telemetry, Error> {
    let telemetry = providers(settings)?;
    let own_traffic = tracing_subscriber::filter::Targets::new()
        .with_default(tracing::Level::TRACE)
        .with_targets(OWN_TRAFFIC.map(|target| (target, tracing::level_filters::LevelFilter::OFF)));
    let exporting = tracing_opentelemetry::layer()
        .with_tracer(telemetry.tracer.tracer("rdlt"))
        .with_tracked_inactivity(false)
        .with_filter(filter_at("info").and(own_traffic));
    let writing = tracing_subscriber::fmt::layer()
        .with_writer(std::io::stderr)
        .with_ansi(std::io::stderr().is_terminal())
        .with_filter(filter_at(stderr_level));
    tracing_subscriber::registry()
        .with(exporting)
        .with(writing)
        .try_init()
        .map_err(|error| unavailable("the tracing subscriber", error))?;
    let recorder =
        metrics_exporter_opentelemetry::Recorder::with_meter(telemetry.meter.meter("rdlt"));
    metrics::set_global_recorder(recorder)
        .map_err(|error| unavailable("the metrics recorder", error))?;
    rdlt_host::trace::propagate(current);
    Ok(telemetry)
}

/// The tracer and meter providers over exporters that `settings` and the `OTEL_*` variables
/// configure.
fn providers(settings: &Settings) -> Result<Telemetry, Error> {
    if tokio::runtime::Handle::try_current().is_err() {
        return Err(Error::internal(
            codes::TELEMETRY_UNAVAILABLE,
            "export needs a tokio runtime to run on",
            Said("there is no tokio runtime"),
        ));
    }
    // TLS is set up only for a collector reached over it: that loads the system's roots, which a
    // plaintext or unix socket collector needs none of, and a unix socket takes no TLS at all.
    let mut spans = opentelemetry_otlp::SpanExporter::builder().with_tonic();
    if !plaintext(endpoint_of(settings, "TRACES", &environment).as_deref()) {
        spans = spans.with_tls_config(tls(settings.traces_certificate.as_deref())?);
    }
    let mut points = opentelemetry_otlp::MetricExporter::builder().with_tonic();
    if !plaintext(endpoint_of(settings, "METRICS", &environment).as_deref()) {
        points = points.with_tls_config(tls(settings.metrics_certificate.as_deref())?);
    }
    if let Some(endpoint) = &settings.endpoint {
        spans = spans.with_endpoint(endpoint);
        points = points.with_endpoint(endpoint);
    }
    let spans = spans.build().map_err(|_| refused("the span exporter"))?;
    let tracer = SdkTracerProvider::builder()
        .with_batch_exporter(spans)
        .build();
    let points = points.build().map_err(|_| refused("the metric exporter"))?;
    let meter = SdkMeterProvider::builder()
        .with_periodic_exporter(points)
        .with_view(|instrument: &opentelemetry_sdk::metrics::Instrument| {
            commit_seconds(instrument.name())
                .then(|| {
                    Stream::builder()
                        .with_aggregation(Aggregation::ExplicitBucketHistogram {
                            boundaries: SECONDS.to_vec(),
                            record_min_max: true,
                        })
                        .build()
                        .ok()
                })
                .flatten()
        })
        .build();
    Ok(Telemetry { tracer, meter })
}

/// The TLS a collector is verified with: the certificate the file at `path` holds, or the
/// operating system's roots.
fn tls(path: Option<&str>) -> Result<ClientTlsConfig, Error> {
    let Some(path) = path else {
        return Ok(ClientTlsConfig::new().with_enabled_roots());
    };
    let pem = std::fs::read(path).map_err(|_| {
        Error::internal(
            codes::TELEMETRY_UNAVAILABLE,
            "the file OTEL_EXPORTER_OTLP_CERTIFICATE names cannot be read",
            Said("the certificate file was not read"),
        )
    })?;
    Ok(ClientTlsConfig::new().ca_certificate(Certificate::from_pem(pem)))
}

/// Whether the instrument `name` is a histogram of a commit's time.
fn commit_seconds(name: &str) -> bool {
    name.starts_with("rdlt_commit") && name.ends_with("_seconds")
}

/// Whether a collector at `endpoint`, as given or else the SDK's default, is reached without TLS.
fn plaintext(endpoint: Option<&str>) -> bool {
    endpoint.is_none_or(|endpoint| {
        let scheme = |scheme: &str| {
            endpoint
                .get(..scheme.len())
                .is_some_and(|start| start.eq_ignore_ascii_case(scheme))
        };
        scheme("http://") || scheme("unix:")
    })
}

/// The endpoint a signal's exporter is given, from `settings`, else the signal's variable, else
/// the general one, and only ever inspected for its scheme.
fn endpoint_of(
    settings: &Settings,
    signal: &str,
    vars: &dyn Fn(&str) -> Option<String>,
) -> Option<String> {
    settings.endpoint.clone().or_else(|| {
        vars(&format!("OTEL_EXPORTER_OTLP_{signal}_ENDPOINT"))
            .or_else(|| vars("OTEL_EXPORTER_OTLP_ENDPOINT"))
            .filter(|endpoint| !endpoint.is_empty())
    })
}

/// The process's environment.
fn environment(name: &str) -> Option<String> {
    std::env::var(name).ok()
}

/// A failure of installing `what`, for `cause`.
fn unavailable(what: &str, cause: impl std::error::Error + Send + Sync + 'static) -> Error {
    Error::internal(
        codes::TELEMETRY_UNAVAILABLE,
        format!("installing {what} failed: {cause}"),
        cause,
    )
}

/// A failure of building `what` from the `OTEL_*` variables, saying neither what they hold nor
/// what the exporter said of them, since an endpoint may carry credentials.
fn refused(what: &str) -> Error {
    Error::internal(
        codes::TELEMETRY_UNAVAILABLE,
        format!("installing {what} failed: its OTEL_EXPORTER_OTLP_* settings were refused"),
        Said("the exporter refused its settings"),
    )
}

/// A cause that is a fixed sentence.
#[derive(Debug, thiserror::Error)]
#[error("{0}")]
struct Said(&'static str);

/// The W3C context of the calling task's span, where it is a sampled or recorded OpenTelemetry
/// span.
fn current() -> Option<TraceParent> {
    let context = tracing::Span::current().context();
    let span = context.span();
    let found = span.span_context();
    if !found.is_valid() {
        return None;
    }
    TraceParent::new(
        found.trace_id().to_bytes(),
        found.span_id().to_bytes(),
        found.trace_flags().to_u8(),
    )
}
