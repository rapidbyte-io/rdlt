//! The framework's certification: the example connector, authored
//! purely on the SDK, passes the SAME conformance kits every shipping
//! connector answers to — plus the choreography refusal the framework
//! itself owns.

use rdlt_connector::core::TableName;
use rdlt_connector::{Destination, Source};
use rdlt_connector_sdk::config::Document;
use rdlt_connector_sdk::source::SourceConnector;
use rdlt_connector_sdk::{destination, source};
use rdlt_testkit::{ProbeError, TableProbe, assert_conformant, verify_destination, verify_source};

use super::example::{SharedStore, Sink, Ticker, TickerConfig, visible_rows};

/// The kit's one required capability: counting reader-VISIBLE rows.
struct StoreProbe {
    store: SharedStore,
}

#[async_trait::async_trait]
impl TableProbe for StoreProbe {
    async fn count(&self, table: &TableName) -> Result<u64, ProbeError> {
        Ok(visible_rows(&self.store, table.as_str()))
    }
}

/// The source kit: deterministic reads, checkpoints, the resume law,
/// cancellation — certified against the framework shell, not against a
/// hand-written SPI impl.
#[tokio::test]
async fn a_framework_source_passes_the_conformance_kit() {
    let config = TickerConfig::from_yaml("count: 6").expect("valid");
    let connector = Ticker::assemble(config).expect("assemble");
    let shell = source::shell(connector);
    assert_eq!(shell.spec().name, "ticker");
    assert_conformant(verify_source(&shell).await.expecting_no_skips());
}

/// The destination kit: fresh state, idempotent DDL, staging
/// invisibility, crashed-session teardown, idempotent re-commit, atomic
/// state — certified against the framework session choreography over
/// the example backend. The probe shares the connector's storage handle
/// through the example's test constructor.
#[tokio::test]
async fn a_framework_destination_passes_the_conformance_kit() {
    let store = SharedStore::default();
    let connector = Sink::with_store(std::sync::Arc::clone(&store));
    let shell = destination::shell(connector);
    assert_eq!(shell.spec().name, "sink");
    let probe = StoreProbe { store };
    assert_conformant(verify_destination(&shell, &probe).await);
}

/// The framework's own refusal: a write to a table this session never
/// ensured is a fatal contract violation, enforced by the SDK session —
/// no backend gets the chance to half-apply it.
#[tokio::test]
async fn the_session_refuses_a_write_before_ensure() {
    use rdlt_connector::core::{LoadId, PipelineId};
    let shell = destination::shell(Sink::with_store(SharedStore::default()));
    let mut session = shell
        .open(rdlt_connector::OpenContext::new(
            PipelineId::from("p"),
            LoadId::from("l"),
        ))
        .await
        .expect("open");
    let batch = rdlt_testkit::batch_of(&[1]);
    let refused = session
        .write(&TableName::new("never_ensured"), batch)
        .await
        .expect_err("the choreography must refuse");
    let rendered = refused.to_string();
    assert!(
        rendered.contains("write before ensure_table for `never_ensured`"),
        "{rendered}"
    );
}
