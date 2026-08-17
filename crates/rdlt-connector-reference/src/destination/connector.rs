//! The sdk `DestinationConnector`: one directory, one session at a
//! time. `connect` creates the directory and takes the session lease
//! before handing out a [`session::Session`].

use std::path::{Path, PathBuf};

use async_trait::async_trait;
use fs4::fs_std::FileExt as _;
use rdlt_connector_sdk::config::schema_of;
use rdlt_connector_sdk::destination::DestinationConnector;
use rdlt_connector_sdk::spi::destination::{Capabilities, OpenContext};
use rdlt_connector_sdk::spi::error::DestinationError;

use super::{config, session, store};

/// The connector: one directory. `Clone` is cheap (a path) and lets a
/// test host that reuses one destination across engine runs hand each
/// run its own handle — the engine's crash sweep clones per attempt.
#[derive(Debug, Clone)]
pub struct Reference {
    dir: PathBuf,
}

#[async_trait]
impl DestinationConnector for Reference {
    const NAME: &'static str = "io.rapidbyte.reference";
    const VERSION: &'static str = env!("CARGO_PKG_VERSION");
    type Config = config::Config;
    type Backend = session::Session;

    fn assemble(config: config::Config) -> Result<Self, config::Error> {
        Ok(Self {
            dir: PathBuf::from(config.path),
        })
    }

    fn config_schema() -> Option<serde_json::Value> {
        Some(schema_of::<config::Config>())
    }

    fn capabilities(&self) -> Capabilities {
        // Truthful: arrow's json writer renders structs, lists, json
        // and decimals into the parts. Merge stays undeclared — jsonl
        // parts are append-only files with no upsert machinery.
        Capabilities::default()
            .with_structs(true)
            .with_scalar_lists(true)
            .with_json_type(true)
            .with_decimal(true)
    }

    async fn connect(&self, context: &OpenContext) -> Result<session::Session, DestinationError> {
        // The open contract — a crashed predecessor's staging invisible
        // and reclaimable — holds by construction: staging lives in the
        // dead session's memory, and nothing staged ever touches disk.
        std::fs::create_dir_all(&self.dir).map_err(|error| {
            DestinationError::transient(format!(
                "reference destination: create {}: {error}",
                self.dir.display()
            ))
        })?;
        let lease = lease(&self.dir)?;
        Ok(session::Session::new(
            self.dir.clone(),
            context.load_id.clone(),
            lease,
        ))
    }
}

/// Take the session lease, refusing typed when another session holds
/// it. Fatal, not transient: the holder may be a hung run, and retrying
/// against it forever is exactly the double-fired-cron scenario the
/// lease exists to surface.
fn lease(dir: &Path) -> Result<std::fs::File, DestinationError> {
    let path = dir.join(store::LEASE_FILE);
    let framed = |verb: &str, error: std::io::Error| {
        DestinationError::transient(format!(
            "reference destination: {verb} {}: {error}",
            path.display()
        ))
    };
    let file = std::fs::OpenOptions::new()
        .create(true)
        .truncate(false)
        .write(true)
        .open(&path)
        .map_err(|error| framed("open", error))?;
    if !file
        .try_lock_exclusive()
        .map_err(|error| framed("lock", error))?
    {
        return Err(DestinationError::fatal(format!(
            "reference destination: another session holds the lease at {} — one session \
             per output directory",
            path.display()
        )));
    }
    Ok(file)
}
