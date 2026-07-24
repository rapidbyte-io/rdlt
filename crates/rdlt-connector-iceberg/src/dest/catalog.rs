//! REST-catalog construction from the config document. The catalog-load
//! property vocabulary (the `CAT_*` keys) and the secret reveal live HERE,
//! so the audit surface for credential material is one function.

use std::collections::HashMap;
use std::sync::Arc;

use iceberg::{Catalog, CatalogBuilder};
use iceberg_catalog_rest::RestCatalogBuilder;
use iceberg_storage_opendal::OpenDalResolvingStorageFactory;
use rdlt_connector::DestError;

use super::config::IcebergConfig;
use super::errors::classify;

/// REST-catalog load-property keys (iceberg-rust `RestCatalogBuilder::load`).
/// Centralized so the catalog-construction wiring reads against one vocabulary
/// rather than scattered string literals.
const CAT_URI: &str = "uri";
const CAT_WAREHOUSE: &str = "warehouse";
const CAT_CREDENTIAL: &str = "credential";
const CAT_SCOPE: &str = "scope";
const CAT_OAUTH2_SERVER_URI: &str = "oauth2-server-uri";
const CAT_TOKEN: &str = "token";
const CAT_S3_ENDPOINT: &str = "s3.endpoint";
const CAT_S3_REGION: &str = "s3.region";
const CAT_S3_ACCESS_KEY_ID: &str = "s3.access-key-id";
const CAT_S3_SECRET_ACCESS_KEY: &str = "s3.secret-access-key";
const CAT_S3_PATH_STYLE_ACCESS: &str = "s3.path-style-access";

/// Translate the config document into the catalog's load properties. Every
/// `Secret::reveal` in this crate is concentrated here: revealed material
/// leaves only as opaque map values consumed immediately by [`connect`], so
/// this is the single function to audit for credential handling. The storage
/// override (family S3 spelling) maps to FileIO props that take precedence
/// over the catalog's vended defaults; caller-supplied `props` win last.
fn catalog_props(config: &IcebergConfig) -> HashMap<String, String> {
    let mut props: HashMap<String, String> = HashMap::from([
        (CAT_URI.to_string(), config.catalog.uri.clone()),
        (CAT_WAREHOUSE.to_string(), config.catalog.warehouse.clone()),
    ]);
    if let Some(oauth) = &config.catalog.auth.oauth2_client_credentials {
        props.insert(
            CAT_CREDENTIAL.into(),
            format!("{}:{}", oauth.client_id, oauth.client_secret.reveal()),
        );
        if !oauth.scopes.is_empty() {
            props.insert(CAT_SCOPE.into(), oauth.scopes.join(" "));
        }
        if let Some(url) = &oauth.token_url {
            props.insert(CAT_OAUTH2_SERVER_URI.into(), url.clone());
        }
    }
    if let Some(bearer) = &config.catalog.auth.bearer {
        props.insert(CAT_TOKEN.into(), bearer.token.reveal().to_owned());
    }
    if let Some(s3) = config.storage.as_ref().and_then(|s| s.s3.as_ref()) {
        if let Some(endpoint) = &s3.endpoint {
            props.insert(CAT_S3_ENDPOINT.into(), endpoint.clone());
        }
        if let Some(region) = &s3.region {
            props.insert(CAT_S3_REGION.into(), region.clone());
        }
        props.insert(
            CAT_S3_ACCESS_KEY_ID.into(),
            s3.access_key.reveal().to_owned(),
        );
        props.insert(
            CAT_S3_SECRET_ACCESS_KEY.into(),
            s3.secret_key.reveal().to_owned(),
        );
        props.insert(CAT_S3_PATH_STYLE_ACCESS.into(), s3.path_style.to_string());
    }
    for (key, value) in &config.catalog.props {
        props.insert(key.clone(), value.clone());
    }
    props
}

/// Build the REST catalog from the config document.
pub(crate) async fn connect(config: &IcebergConfig) -> Result<Arc<dyn Catalog>, DestError> {
    let catalog = RestCatalogBuilder::default()
        .with_storage_factory(Arc::new(OpenDalResolvingStorageFactory::new()))
        .load(config.catalog.warehouse.clone(), catalog_props(config))
        .await
        .map_err(|e| {
            classify(
                &format!(
                    "catalog `{}` warehouse `{}`",
                    config.catalog.uri, config.catalog.warehouse
                ),
                e,
            )
        })?;
    Ok(Arc::new(catalog))
}
