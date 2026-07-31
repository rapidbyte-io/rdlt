//! Destination handle + builder: connection string, dataset, TLS posture.

use rdlt_connector::DestinationError;
use tokio_postgres::Client;

#[derive(Debug, Clone)]
pub struct Postgres {
    pub(super) conn_string: String,
    pub(super) schema: String,
    pub(super) tls: Option<crate::tls::TlsPolicy>,
    pub(super) options: DestinationOptions,
}

impl Postgres {
    /// `conn_string`: e.g. `host=localhost user=postgres password=pw dbname=raw`.
    pub fn connect(conn_string: impl Into<String>) -> Self {
        Self {
            conn_string: conn_string.into(),
            schema: "public".into(),
            tls: None,
            options: DestinationOptions::default(),
        }
    }

    /// Target schema (dataset); created if missing.
    pub fn dataset(mut self, schema: impl Into<String>) -> Self {
        self.schema = schema.into();
        self
    }

    /// TLS posture — the SAME policy type the source uses; the two directions
    /// share one connect path.
    pub fn tls(mut self, policy: crate::tls::TlsPolicy) -> Self {
        self.tls = Some(policy);
        self
    }

    pub(super) async fn client(&self) -> Result<Client, DestinationError> {
        let crate::tls::ParsedConn { pg, policy } =
            crate::tls::parse_conn(&self.conn_string, self.tls.as_ref())
                .map_err(|e| DestinationError::fatal(e.to_string()))?;
        match crate::tls::connect(&pg, &policy).await {
            Ok(client) => Ok(client),
            Err(crate::tls::ConnectResult::Config(e)) => {
                Err(DestinationError::fatal(e.to_string()))
            }
            Err(crate::tls::ConnectResult::Connect(e)) if e.transient => {
                Err(DestinationError::transient(e.to_string()))
            }
            Err(crate::tls::ConnectResult::Connect(e)) => {
                Err(DestinationError::fatal(e.to_string()))
            }
        }
    }
}

// ---- Destination options ----
//
// The vocabulary + validation live in rdlt-connector-sqlcore (shared with
// every SQL destination). Re-exported here under their bare sqlcore names —
// the same spelling duckdb uses, so a config type reads identically whichever
// SQL destination consumes it.

pub use rdlt_connector_sqlcore::{
    AbsentPolicy, DedupSort, DestinationOptions, MergeStrategy, Scd2Options, SortOrder,
    TableOptions,
};

impl Postgres {
    /// Strategy/hard-delete/SCD2 options. Validated here — errors name the
    /// field.
    pub fn options(mut self, options: DestinationOptions) -> Result<Self, DestinationError> {
        options.validate().map_err(DestinationError::fatal)?;
        self.options = options;
        Ok(self)
    }
}
