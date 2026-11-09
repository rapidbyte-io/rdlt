//! Secrets in store and file references and in mask keys, made to fail at every boundary: no
//! value appears, in any form a scrub or an encoding could leave it in, in events, reports,
//! errors, tracing output or an OpenTelemetry export.
//!
//! Each place is shown able to fail too: a value the host was not told is a secret reaches it by
//! the same paths, and is found there. Connector text reaches the export only as the stream and
//! partition names its spans carry, which the host refuses where a name holds a secret.

use std::path::Path;
use std::sync::{Arc, LazyLock};
use std::time::Duration;

use opentelemetry::trace::TracerProvider as _;
use opentelemetry_sdk::trace::{InMemorySpanExporter, SdkTracerProvider};
use parking_lot::Mutex;
use rdlt::{
    BoxFuture, Connector, Host, MaskStep, MaskWith, Pipeline, PipelineBuilder, Placement, Retry,
    Secret, SecretFault, SecretKind, SecretReference, SecretResolver, Side, Stream, TransformStep,
};
use rdlt_connector::{ConnectContext, SourceConnector, Streams};
use rdlt_connector_reference::{MemoryDestination, MemorySource};
use rdlt_testkit::failing::FailingSource;
use rdlt_testkit::secrets::leaks;
use tracing_subscriber::layer::SubscriberExt as _;

use crate::support::{example, within};

const MEMORY: &str = "io.rapidbyte.memory";
const FAILING: &str = "io.rapidbyte.failing";

/// The store's secrets, each a kind of text a scrub could miss.
const STORED: [(&str, &str); 5] = [
    ("plain", "CANARY-plain-7c1f0e"),
    ("quoted", "CANARY\"quoted\\by\"JSON"),
    ("wide", "CANARY-päss-名-🦀"),
    ("unseen", "CANARY-ctl-\u{1}-zero-width-\u{200b}-77"),
    ("mask", "CANARY-mask-key-55aa"),
];

/// What the file secret holds: two lines, each long enough to be found alone.
const FILED: &str = "CANARY-file-91b2\nCANARY-file-line-two-5e";

/// A value written out in a configuration: the host cannot know it is a secret, so nothing
/// scrubs it, and it reaches every place a scrubbed secret would.
const UNTOLD: &str = "CANARY-untold-0b7d";

/// A value written out where nothing may quote it: a configuration's field, or a row a query
/// fails on.
const UNQUOTED: &str = "CANARY-unquoted-a41c";

/// Every secret the host is told of, and each line of the secret of several lines.
fn secrets() -> impl Iterator<Item = &'static str> {
    let stored = STORED.iter().map(|(_, value)| *value);
    stored.chain([FILED]).chain(FILED.lines())
}

#[derive(Debug)]
struct Stored;

impl SecretResolver for Stored {
    fn resolve<'a>(
        &'a self,
        reference: &'a SecretReference,
    ) -> BoxFuture<'a, Result<Secret<String>, SecretFault>> {
        Box::pin(async move {
            let found = STORED
                .iter()
                .find(|(name, _)| reference.kind == SecretKind::Named && *name == reference.name);
            found
                .map(|(_, value)| Secret::new((*value).to_owned()))
                .ok_or(SecretFault::Missing)
        })
    }
}

/// A writer the tracing output is kept in.
#[derive(Clone, Default)]
struct Kept(Arc<Mutex<Vec<u8>>>);

impl std::io::Write for Kept {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        self.0.lock().extend_from_slice(bytes);
        Ok(bytes.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

/// The process's one tracing subscriber, writing to a [`Kept`] and exporting to memory: a run's
/// tasks move between threads, so a per-thread default would see none of them.
struct Watch {
    kept: Kept,
    exporter: InMemorySpanExporter,
    provider: SdkTracerProvider,
}

/// The watch, installed by the first test of the process to ask; each test reads all it holds.
static WATCH: LazyLock<Watch> = LazyLock::new(|| {
    let kept = Kept::default();
    let exporter = InMemorySpanExporter::default();
    let provider = SdkTracerProvider::builder()
        .with_simple_exporter(exporter.clone())
        .build();
    let writer = kept.clone();
    let subscriber = tracing_subscriber::registry()
        .with(
            tracing_subscriber::fmt::layer()
                .with_writer(move || writer.clone())
                .with_ansi(false),
        )
        .with(tracing_opentelemetry::layer().with_tracer(provider.tracer("scan")));
    tracing::subscriber::set_global_default(subscriber).expect("no other subscriber");
    Watch {
        kept,
        exporter,
        provider,
    }
});

/// What was said at each place the scan watches.
#[derive(Default)]
struct Said {
    events: Vec<String>,
    reports: Vec<String>,
    errors: Vec<String>,
}

impl Said {
    fn error(&mut self, error: &rdlt::Error) {
        let mut said = vec![format!("{error} {error:?}")];
        let mut cause = std::error::Error::source(error);
        while let Some(found) = cause {
            said.push(format!("{found} {found:?}"));
            cause = found.source();
        }
        self.errors.push(said.join(" "));
        self.reports.push(json(&error.report()));
    }

    fn event(&mut self, event: &rdlt::Event) {
        self.events.push(json(event));
    }

    fn report(&mut self, report: &rdlt::Report) {
        self.reports.push(json(report));
    }

    /// Each place the scan reads and what it holds now: what was said, the tracing output and the
    /// OpenTelemetry export.
    fn places(&self) -> [(&'static str, String); 5] {
        let watch = &*WATCH;
        watch.provider.force_flush().expect("the export flushes");
        let traced = String::from_utf8_lossy(&watch.kept.0.lock()).into_owned();
        let spans = watch
            .exporter
            .get_finished_spans()
            .expect("the export reads");
        [
            ("events", self.events.join("\n")),
            ("reports", self.reports.join("\n")),
            ("errors", self.errors.join("\n")),
            ("tracing output", traced),
            ("OpenTelemetry export", format!("{spans:?}")),
        ]
    }
}

/// `value` as JSON, as an embedder writes it out.
fn json(value: &impl serde::Serialize) -> String {
    serde_json::to_string(value).expect("what the facade says serializes")
}

/// How a scenario must end.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Ends {
    Refused,
    Failed,
    Succeeded,
}

/// Where a scenario's source runs.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Runs {
    Spawned,
    InProcess,
}

struct Scenario {
    pipeline: PipelineBuilder,
    runs: Runs,
    ends: Ends,
    /// The code its error must have, as the host's and not its connector's, where it must.
    coded: Option<&'static str>,
}

impl Scenario {
    fn new(pipeline: PipelineBuilder, runs: Runs, ends: Ends) -> Self {
        Self {
            pipeline,
            runs,
            ends,
            coded: None,
        }
    }

    /// Asserts `error`, which the scenario's pipeline met, has the code it must.
    fn coded(&self, error: &rdlt::Error) {
        if let Some(code) = self.coded {
            let coded = (error.code().map(rdlt::Code::as_str), error.connector_code());
            assert_eq!(coded, (Some(code), None), "{error:?}");
        }
    }
}

/// What `scenario` said, at the places its pipeline reaches, on the host that places its source
/// where it must run.
async fn ran(scenario: Scenario, spawned: &Host, in_process: &Host, said: &mut Said) {
    let name = scenario.pipeline.document().pipeline.clone();
    let host = match scenario.runs {
        Runs::Spawned => spawned,
        Runs::InProcess => in_process,
    };
    let built = within(scenario.pipeline.clone().build(host)).await;
    let pipeline = match built {
        Ok(pipeline) => pipeline,
        Err(error) => {
            assert_eq!(scenario.ends, Ends::Refused, "{name}: {error:?}");
            scenario.coded(&error);
            said.error(&error);
            return;
        }
    };
    assert_ne!(scenario.ends, Ends::Refused, "{name} built");
    let placement = &pipeline.source().placement;
    let placed = match scenario.runs {
        Runs::Spawned => matches!(placement, Placement::Process { .. }),
        Runs::InProcess => *placement == Placement::InProcess,
    };
    assert!(placed, "{name}: its source runs {placement:?}");
    let run = pipeline.start();
    let mut events = run.events();
    let outcome = within(run).await;
    while let Some(item) = within(events.next()).await {
        said.event(&item.expect("no scenario outgrows its ring"));
    }
    said.report(&outcome.report);
    match &outcome.error {
        Some(error) => {
            assert_eq!(scenario.ends, Ends::Failed, "{name}: {error:?}");
            scenario.coded(error);
            said.error(error);
        }
        None => assert_eq!(scenario.ends, Ends::Succeeded, "{name} succeeded"),
    }
}

/// A source configured with `config`, of the connector named `id`.
fn side(id: &str, config: serde_json::Value) -> Side {
    Side::new(Connector::new(id)).config(config)
}

/// A pipeline of `source` into memory, retried once after a moment where its error says so.
fn into_memory(name: &str, source: Side, stream: &str) -> PipelineBuilder {
    let destination = side(MEMORY, serde_json::json!({ "store": name }));
    Pipeline::builder(name, source, destination)
        .stream(Stream::new(stream))
        .retry(
            Retry::new()
                .max_attempts(2)
                .initial(Duration::from_millis(1))
                .max_delay(Duration::from_millis(1)),
        )
}

/// The failing source of `config`, a mapping its `fail` and `token` are added to, read into
/// memory as every stream it offers.
fn failing(name: &str, fail: &str, token: &str, config: serde_json::Value) -> PipelineBuilder {
    let mut config = config;
    config["fail"] = fail.into();
    config["token"] = token.into();
    into_memory(name, side(FAILING, config), "*")
}

/// A pipeline of `rows` of `users` from the memory source into memory, through `transform`.
fn memory_rows(name: &str, rows: &serde_json::Value, transform: Vec<TransformStep>) -> Scenario {
    let source = side(MEMORY, serde_json::json!({ "streams": { "users": rows } }));
    let mut stream = Stream::new("users");
    for step in transform {
        stream = stream.transform(step);
    }
    let destination = side(MEMORY, serde_json::json!({ "store": name }));
    let pipeline = Pipeline::builder(name, source, destination).stream(stream);
    Scenario::new(pipeline, Runs::InProcess, Ends::Succeeded)
}

fn hmac_mask(column: &str) -> TransformStep {
    TransformStep::Mask(MaskStep::new([column], MaskWith::HmacSha256).key("${secret:mask}"))
}

/// The failing source's scenarios: a check and a read that say a secret, a read tried again, a
/// panic, a stream and a partition named by a secret, each in process and spawned.
fn failures(file: &str) -> Vec<Scenario> {
    use Ends::{Failed, Refused};
    use Runs::{InProcess, Spawned};
    let plain = "${secret:plain}";
    let named = serde_json::json!({ "stream": plain });
    let partitioned = serde_json::json!({ "partition": plain });
    let failures = [
        ("echoed", "check", file, serde_json::json!({}), Refused),
        (
            "read",
            "read",
            "${secret:quoted}",
            serde_json::json!({}),
            Failed,
        ),
        (
            "unseen",
            "read",
            "${secret:unseen}",
            serde_json::json!({}),
            Failed,
        ),
        (
            "retried",
            "transient",
            "${secret:wide}",
            serde_json::json!({}),
            Failed,
        ),
        (
            "panic",
            "panic",
            "${secret:wide}",
            serde_json::json!({}),
            Failed,
        ),
        ("named", "never", "", named, Refused),
        ("partitioned", "never", "", partitioned, Failed),
    ];
    let mut scenarios = Vec::new();
    for (name, fail, token, config, ends) in failures {
        let named = matches!(name, "named" | "partitioned");
        for (runs, at) in [(Spawned, "spawned"), (InProcess, "in-process")] {
            let pipeline = failing(&format!("{name}-{at}"), fail, token, config.clone());
            let coded = named.then_some(rdlt_host::NAMED_BY_SECRET);
            scenarios.push(Scenario {
                coded,
                ..Scenario::new(pipeline, runs, ends)
            });
        }
    }
    scenarios
}

/// Every boundary a secret could leak by, each failing the way it must: the failing source's,
/// configurations the connector refuses, a SQL error on a row holding a secret, and a mask by a
/// secret key.
fn scenarios(file: &str) -> Vec<Scenario> {
    let mut scenarios = failures(file);
    let refused = [
        ("refused-memory", MEMORY, "streams", Runs::InProcess),
        ("refused-spawned", FAILING, "fail", Runs::Spawned),
    ];
    for (name, id, field, runs) in refused {
        let source = side(id, serde_json::json!({ field: "${secret:plain}" }));
        let pipeline = into_memory(name, source, "*");
        scenarios.push(Scenario::new(pipeline, runs, Ends::Refused));
    }
    let addresses = serde_json::json!([{ "email": "a@example.com" }]);
    scenarios.push(memory_rows("masked", &addresses, vec![hmac_mask("email")]));
    if cfg!(feature = "sql") {
        let rows = serde_json::json!([{ "name": "${secret:plain}", "email": "a@example.com" }]);
        scenarios.push(Scenario {
            ends: Ends::Failed,
            ..memory_rows("sql", &rows, vec![hmac_mask("email"), cast()])
        });
    }
    scenarios
}

/// A query that fails on a row whose `name` is no integer.
fn cast() -> TransformStep {
    TransformStep::Sql("SELECT CAST(name AS BIGINT) AS n FROM input".to_owned())
}

/// The host of the scenarios, with the failing source spawned from `connectors`, or also
/// registered in process, where the placement by reference runs it instead.
fn host(files: &Path, connectors: &Path, in_process: bool) -> Host {
    let host = Host::new()
        .trust_binaries()
        .connectors(connectors)
        .secret_store(Stored)
        .allow_files([files])
        .source::<MemorySource>()
        .destination::<MemoryDestination>();
    if in_process {
        host.source::<FailingSource>()
    } else {
        host
    }
}

/// The two hosts of the scenarios, the private directory holding the file secret and the
/// connectors' directory they read, and the reference a configuration makes to the file secret.
struct Hosts {
    spawned: Host,
    in_process: Host,
    file: String,
    _directories: (tempfile::TempDir, tempfile::TempDir),
}

/// The hosts, the failing connector copied in, as a build's output has more than one name.
fn hosts() -> Hosts {
    use std::os::unix::fs::PermissionsExt as _;
    LazyLock::force(&WATCH);
    let files = tempfile::tempdir().expect("a temporary directory");
    let token = files.path().join("token");
    std::fs::write(&token, FILED).expect("the secret writes");
    let private = std::fs::Permissions::from_mode(0o600);
    std::fs::set_permissions(&token, private).expect("the secret is private");
    let connectors = tempfile::tempdir().expect("a temporary directory");
    let copy = connectors.path().join("rdlt-connector-failing");
    std::fs::copy(example("failing_connector"), copy).expect("the connector copies");
    Hosts {
        spawned: host(files.path(), connectors.path(), false),
        in_process: host(files.path(), connectors.path(), true),
        file: format!("${{file:{}}}", token.display()),
        _directories: (files, connectors),
    }
}

/// What `scenarios` said, each run on `hosts`.
async fn said(hosts: &Hosts, scenarios: Vec<Scenario>) -> [(&'static str, String); 5] {
    let mut said = Said::default();
    for scenario in scenarios {
        ran(scenario, &hosts.spawned, &hosts.in_process, &mut said).await;
    }
    said.places()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn no_secret_leaves_by_any_boundary() {
    let hosts = hosts();
    let places = said(&hosts, scenarios(&hosts.file)).await;
    // Each place holds what the connectors said, scrubbed: the scan reads what reaches it.
    let words = [
        ("events", "the read failed holding ***"),
        ("reports", "the read panicked holding ***"),
        ("errors", "the check refused ***"),
        ("tracing output", "the read panicked holding ***"),
        ("OpenTelemetry export", "rdlt.stream"),
    ];
    for ((place, text), (named, words)) in places.iter().zip(words) {
        assert_eq!(*place, named);
        assert!(text.contains(words), "the {place} holds {words}: {text}");
    }
    for (place, text) in places {
        let found = leaks(&text, secrets());
        assert_eq!(found, Vec::<String>::new(), "{place}: {text}");
    }
}

/// A value the host was not told is a secret takes the path of each boundary unscrubbed, so the
/// scan above finds what reaches each place it reads.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_secret_that_reaches_a_place_is_found_there() {
    let hosts = hosts();
    let told = |name: &str, fail: &str, config: serde_json::Value, ends: Ends| {
        Scenario::new(failing(name, fail, UNTOLD, config), Runs::Spawned, ends)
    };
    let named = serde_json::json!({ "stream": UNTOLD });
    let scenarios = vec![
        told("untold-read", "read", serde_json::json!({}), Ends::Failed),
        told(
            "untold-transient",
            "transient",
            serde_json::json!({}),
            Ends::Failed,
        ),
        told("untold-panic", "panic", serde_json::json!({}), Ends::Failed),
        told("untold-named", "never", named, Ends::Succeeded),
    ];
    for (place, text) in said(&hosts, scenarios).await {
        assert!(
            leaks(&text, [UNTOLD]).contains(&UNTOLD.to_owned()),
            "the scan of the {place} finds what reaches it: {text}"
        );
    }
}

/// A configuration the connector refuses and a query that fails on a row are said without the
/// value they refuse, a secret or not: the scan above holds them to more than the scrub.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_refusal_never_quotes_the_value_it_refuses() {
    let hosts = hosts();
    let refused = [
        ("unquoted-memory", MEMORY, "streams", Runs::InProcess),
        ("unquoted-spawned", FAILING, "fail", Runs::Spawned),
    ];
    let mut scenarios: Vec<Scenario> = refused
        .into_iter()
        .map(|(name, id, field, runs)| {
            let source = side(id, serde_json::json!({ field: UNQUOTED }));
            Scenario::new(into_memory(name, source, "*"), runs, Ends::Refused)
        })
        .collect();
    if cfg!(feature = "sql") {
        let rows = serde_json::json!([{ "name": UNQUOTED }]);
        scenarios.push(Scenario {
            ends: Ends::Failed,
            ..memory_rows("unquoted-sql", &rows, vec![cast()])
        });
    }
    for (place, text) in said(&hosts, scenarios).await {
        assert_eq!(
            leaks(&text, [UNQUOTED]),
            Vec::<String>::new(),
            "{place}: {text}"
        );
    }
}

/// A source that panics as it connects, saying the configuration it was sent.
struct PanicsConnecting;

impl SourceConnector for PanicsConnecting {
    const ID: &'static str = "io.test.panics-connecting";
    const VERSION: &'static str = "1.0.0";
    type Config = std::collections::BTreeMap<String, String>;

    async fn connect(config: Self::Config, _: &ConnectContext) -> rdlt_connector::Result<Self> {
        panic!("connecting with {config:?}");
    }

    async fn check(&self) -> rdlt_connector::Result<()> {
        Ok(())
    }

    fn streams(&self) -> Streams<Self> {
        Streams::new()
    }
}

/// A connector placed in process that panics as it parses its configuration unwinds out of the
/// build with the secrets it was sent scrubbed from the panic's message.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_connect_that_panics_unwinds_out_of_the_build_scrubbed() {
    let config = serde_json::json!({ "token": "${secret:plain}" });
    let source = side("io.test.panics-connecting", config);
    let pipeline = into_memory("panics-connecting", source, "*");
    let building = tokio::spawn(async move {
        let host = Host::new()
            .secret_store(Stored)
            .source::<PanicsConnecting>()
            .destination::<MemoryDestination>();
        pipeline.build(&host).await.map(drop)
    });
    let joined = within(building).await;
    let payload = joined.expect_err("the connect panics").into_panic();
    let said = payload.downcast_ref::<String>().expect("a panic's message");
    assert!(said.contains("connecting with"), "{said}");
    assert_eq!(leaks(said, secrets()), Vec::<String>::new(), "{said}");
}
