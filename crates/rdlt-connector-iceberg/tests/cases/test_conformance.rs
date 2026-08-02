//! The sdk conformance kit over the live fixture — "certified = passes
//! conformance", through the same Shell every embedder gets. NEW over
//! generation 1, which predated the kit.

use rdlt_connector_iceberg::destination::Shell;
use rdlt_connector_sdk::spi::core::TableName;
use rdlt_testkit::{TableProbe, assert_conformant, verify_destination};

use super::common::{CatalogFixture, WAREHOUSE};

struct LiveProbe {
    fixture: CatalogFixture,
    namespace: String,
}

#[async_trait::async_trait]
impl TableProbe for LiveProbe {
    async fn count(&self, table: &TableName) -> u64 {
        // Total records off the newest snapshot summary — the
        // catalog's own count, independent of the crate. Errors read
        // as 0; the kit's nonzero clauses keep a broken probe from
        // certifying anything.
        self.fixture
            .snapshot_summaries(&self.namespace, table.as_str())
            .await
            .last()
            .and_then(|s| s.get("total-records"))
            .and_then(|v| v.parse().ok())
            .unwrap_or(0)
    }
}

#[tokio::test]
async fn the_destination_is_conformant_against_the_live_fixture() {
    let Some(fixture) = CatalogFixture::start().await else {
        return;
    };
    let namespace = "conf_v2";
    let shell = Shell::from_value(fixture.doc(namespace)).expect("valid");
    let _ = WAREHOUSE; // the probe reads through the fixture's oracle
    let probe = LiveProbe {
        fixture,
        namespace: namespace.into(),
    };
    assert_conformant(verify_destination(&shell, &probe).await);
}
