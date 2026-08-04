//! The destination connector: config in, [`Load`] sessions out.

use async_trait::async_trait;
use rdlt_connector_sdk::destination::DestinationConnector;
use rdlt_connector_sdk::spi::core::naming::IdentRules;
use rdlt_connector_sdk::spi::{DestinationCapabilities, DestinationError, OpenContext};

use super::config::{Config, ConfigError, config_schema};
use super::layout::scope_of;
use super::load::Load;
use super::stage::writer_props;
use crate::location::Location;

/// The LOCAL-protocol crash points the ENGINE's sweep drives against
/// [`super::ParquetDir`] — exported so the sweep iterates exactly this
/// list; a point in the code but not the sweep is a protocol edge
/// nobody ever crashes at. These spellings are frozen.
pub const FAIL_POINTS: &[&str] = &[
    "pq.replace.truncate",
    "pq.manifest.write",
    "pq.staged.sync",
    "pq.part.rename",
    "pq.dir.fsync",
    "pq.state.write",
    "pq.receipt.write",
];

/// The S3-protocol crash points — they cannot fire on a local store,
/// so the crate's own container-gated sweep owns them.
pub const S3_FAIL_POINTS: &[&str] = &[
    "file.stage.put",
    "file.finalize.copy",
    "file.finalize.delete",
];

/// The file destination.
#[derive(Debug, Clone)]
pub struct File {
    config: Config,
}

#[async_trait]
impl DestinationConnector for File {
    const NAME: &'static str = "file";
    const VERSION: &'static str = env!("CARGO_PKG_VERSION");

    type Config = Config;
    type Backend = Load;

    fn assemble(config: Config) -> Result<Self, ConfigError> {
        Ok(Self { config })
    }

    fn config_schema() -> Option<serde_json::Value> {
        Some(config_schema())
    }

    fn capabilities(&self) -> DestinationCapabilities {
        // Files have no key semantics, so merge is honestly false;
        // structs and scalar lists serialize natively in both formats;
        // Json lands as text, so json_type is honestly false.
        DestinationCapabilities::default()
            .with_merge(false)
            .with_structs(true)
            .with_scalar_lists(true)
            .with_json_type(false)
            .with_decimal(true)
            .with_ident_rules(IdentRules::default())
    }

    async fn connect(&self, context: &OpenContext) -> Result<Load, DestinationError> {
        // Writer properties resolve FIRST: the translation is pure and
        // can fail (the parquet library bounds level ranges the config
        // gate cannot see), and a refusal must not leave a freshly
        // created output directory behind as its only trace.
        let props = writer_props(&self.config.parquet_options())?;
        let location = Location::for_dest(&self.config.path, self.config.location.as_ref())?;
        Load::open(
            location,
            self.config.format,
            self.config.partition_by.clone(),
            props,
            scope_of(context.pipeline.as_str()),
            context.load_id.clone(),
            super::load::PartsWiring {
                options: self.config.part_options(),
                events: context.part_events.clone(),
            },
        )
        .await
    }
}

/// Seams the tests need and nothing else may use. Not a public API.
#[doc(hidden)]
pub mod testhook {
    use rdlt_connector_sdk::spi::DestinationError;

    use crate::location::Location;

    /// Count rows over the ownership listing, both protocols.
    pub async fn count_rows_async(
        config: &super::super::Config,
        table: &str,
    ) -> Result<u64, DestinationError> {
        let location = Location::for_dest(&config.path, config.location.as_ref())?;
        super::super::inspect::count_rows_async(&location, table).await
    }

    /// The synchronous local-only form with its frozen refusal.
    pub fn count_rows(config: &super::super::Config, table: &str) -> Result<u64, DestinationError> {
        let location = Location::for_dest(&config.path, config.location.as_ref())?;
        super::super::inspect::count_rows(&location, table)
    }

    /// Drive the lease's conditional-doc verbs (`create_doc_exclusive`,
    /// `read_doc_versioned`, `replace_doc_if`, `delete_doc`) through a
    /// real `Location`, end to end. `CreateDoc`/`DocVersion` are
    /// private to `crate::location`, so the round-trip runs and
    /// asserts entirely inside `Location::probe_conditional_docs` —
    /// this wrapper only opens the location and forwards the plain
    /// `Result`. Used by the live S3 probe in `tests/cases/test_s3.rs`
    /// (037 US2 T5) to pin the verbs against a real store, not just
    /// the raw client the sibling probe drives directly.
    pub async fn probe_conditional_docs(
        config: &super::super::Config,
        name: &str,
    ) -> Result<(), DestinationError> {
        let location = Location::for_dest(&config.path, config.location.as_ref())?;
        location.probe_conditional_docs(name).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Both registries carry their frozen spellings — the engine's
    /// sweep binds to the first list, the crate's S3 sweep to the
    /// second.
    #[test]
    fn the_registries_are_the_frozen_spellings() {
        assert_eq!(
            FAIL_POINTS,
            &[
                "pq.replace.truncate",
                "pq.manifest.write",
                "pq.staged.sync",
                "pq.part.rename",
                "pq.dir.fsync",
                "pq.state.write",
                "pq.receipt.write",
            ]
        );
        assert_eq!(
            S3_FAIL_POINTS,
            &[
                "file.stage.put",
                "file.finalize.copy",
                "file.finalize.delete"
            ]
        );
    }

    /// The capability declaration is the frozen truth the host plans
    /// from.
    #[test]
    fn capabilities_declare_the_frozen_truth() {
        let file = File::assemble(Config::new("out")).expect("assembles");
        let caps = file.capabilities();
        assert!(!caps.merge);
        assert!(caps.structs);
        assert!(caps.scalar_lists);
        assert!(!caps.json_type);
        assert!(caps.decimal);
    }
}
