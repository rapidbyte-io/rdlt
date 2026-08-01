//! Live probe: a rejected credential at the REST catalog must classify
//! FATAL (fail fast), never transient (which would burn the whole engine
//! retry budget on an unwinnable retry). The catalog client carries the
//! HTTP status only as an error context entry, so classification parses
//! that entry from the rendered error; this probe pins the end-to-end
//! path against a real catalog answering a real 401. Skip-not-fail
//! without a container runtime.

mod common;

use common::CatalogFixture;
use rdlt_connector::core::{LoadId, PipelineId};
use rdlt_connector::{Destination, DestinationError, OpenContext};
use rdlt_connector_iceberg::{AuthOptions, IcebergConfig, IcebergDest};

#[tokio::test]
async fn live_auth_rejection_classifies_fatal() {
    let Some(fixture) = CatalogFixture::start().await else {
        return;
    };
    let config = IcebergConfig::new(
        fixture.catalog_uri.clone(),
        common::WAREHOUSE,
        AuthOptions::bearer("not-a-valid-token"),
        "auth_probe",
    );
    let dest = IcebergDest::from_config(config).expect("config parses");
    let result = dest
        .open(OpenContext::new(
            PipelineId::new("auth-probe"),
            LoadId::new("load-1"),
        ))
        .await;
    match result {
        Err(DestinationError::Fatal { .. }) => {}
        Err(other) => panic!("rejected credential must be FATAL, got a retryable class: {other}"),
        Ok(_) => panic!("catalog accepted a garbage bearer token"),
    }
}
