//! A pipeline run in a process of its own with its write-ahead log in an S3 bucket, which the
//! container tests crash at the engine's durability steps and run again.
//!
//! `log_crash <dir> <log-store.json>` loads two partitions of fifty messages from a log that
//! forgets what it committed, its consumer group kept in `<dir>`, into SQLite at `<dir>/out.db`,
//! committing every sixteen rows and keeping its log in the store the file names. The store's
//! credentials are references to `RDLT_S3_ACCESS_KEY_ID` and `RDLT_S3_SECRET_ACCESS_KEY`, the
//! only variables it resolves. `FAILPOINTS` crashes it where it names. It exits 0 where the run
//! succeeded, 1 where it failed, and 2 where it could not begin.

#![forbid(unsafe_code)]

use std::io::Write as _;
use std::num::NonZeroUsize;
use std::path::{Path, PathBuf};
use std::process::ExitCode;
use std::sync::Arc;
use std::time::Duration;

use rdlt_connector::{
    ConnectContext, PipelineId, ReadMode, StreamName, destination_factory, source_factory,
};
use rdlt_connector_reference::{LogSource, SqliteDestination};
use rdlt_engine::{
    CommitPolicy, Cores, Engine, EngineConfig, PipelinePlan, RetryPolicy, RunStatus, StreamPlan,
    SystemClock, SystemEnv, WriteMode,
};
use rdlt_host::EnvSecrets;
use rdlt_log_store::LogStoreConfig;
use serde_json::json;

/// The cores the run takes: its runtime's two workers and two compute threads.
const CORES: Cores = Cores::new(
    NonZeroUsize::new(4).expect("four is not zero"),
    NonZeroUsize::new(2).expect("two is not zero"),
);

/// The variables the store's credentials are read from, and no other.
const CREDENTIALS: [&str; 2] = ["RDLT_S3_ACCESS_KEY_ID", "RDLT_S3_SECRET_ACCESS_KEY"];

fn main() -> ExitCode {
    let _failpoints = fail::FailScenario::setup();
    let mut args = std::env::args().skip(1);
    let (Some(dir), Some(store)) = (args.next(), args.next()) else {
        writeln!(std::io::stderr(), "usage: log_crash <dir> <log-store.json>").ok();
        return ExitCode::from(2);
    };
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(CORES.workers().get())
        .enable_all()
        .build()
        .expect("a runtime starts");
    match runtime.block_on(run(Path::new(&dir), Path::new(&store))) {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            writeln!(std::io::stderr(), "log_crash: {error}").ok();
            ExitCode::FAILURE
        }
    }
}

async fn run(dir: &Path, store: &Path) -> Result<(), String> {
    let text = std::fs::read(store).map_err(|error| error.to_string())?;
    let document = serde_json::from_slice(&text).map_err(|error| error.to_string())?;
    let config = LogStoreConfig::parse(&document).map_err(|error| error.to_string())?;
    let secrets = EnvSecrets::allowing(CREDENTIALS);
    let wal = config
        .open(Arc::new(secrets), Arc::new(SystemClock))
        .await
        .map_err(|error| format!("{error}: {}", error.code()))?;
    let source = source_factory::<LogSource>()
        .connect(
            json!({
                "seed": 1, "group_path": dir.join("events.group"),
                "streams": [{ "name": "events", "partitions": 2, "messages": 50,
                              "batch_rows": 8, "replayable": false }],
            }),
            ConnectContext::new(),
        )
        .await
        .map_err(|error| error.to_string())?;
    let destination = destination_factory::<SqliteDestination>()
        .connect(
            json!({ "path": PathBuf::from(dir).join("out.db") }),
            ConnectContext::new(),
        )
        .await
        .map_err(|error| error.to_string())?;
    let env = SystemEnv::try_new(CORES)
        .map_err(|e| e.to_string())?
        .with_wal(wal);
    let outcome = Engine::new(engine()?, Arc::new(env))
        .run(plan()?, Arc::from(source), Arc::from(destination))
        .await;
    match outcome.report.status {
        RunStatus::Succeeded => Ok(()),
        status => Err(format!("the run ended {status:?}: {:?}", outcome.error)),
    }
}

/// The pipeline: one stream read incrementally and appended, keeping a log.
fn plan() -> Result<PipelinePlan, String> {
    let name = StreamName::new("events").map_err(|error| error.to_string())?;
    let stream = StreamPlan::new(name)
        .read(ReadMode::Incremental)
        .write(WriteMode::Append);
    let pipeline = PipelineId::parse("crash").map_err(|error| error.to_string())?;
    let plan = PipelinePlan::new(pipeline, [stream]).map_err(|error| error.to_string())?;
    Ok(plan.with_wal(true))
}

/// Commits of sixteen rows, and retries enough to ride out the store's slow moments.
fn engine() -> Result<EngineConfig, String> {
    let commit = CommitPolicy::new(None, Some(16), None).map_err(|error| error.to_string())?;
    let retry = RetryPolicy::default()
        .max_attempts(10)
        .initial(Duration::from_millis(10))
        .max_delay(Duration::from_millis(200));
    EngineConfig::builder()
        .commit(commit)
        .retry(retry)
        .build()
        .map_err(|error| error.to_string())
}
