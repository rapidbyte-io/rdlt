//! Destination handle + builder: connection string, dataset, TLS posture.
//! (Feature 008 T001: relocated verbatim from the old single-file module.)

use rdlt_connector::DestError;
use tokio_postgres::Client;

#[derive(Debug, Clone)]
pub struct Postgres {
    pub(super) conn_string: String,
    pub(super) schema: String,
    pub(super) tls: Option<crate::tls::TlsPolicy>,
    pub(super) options: PgDestOptions,
}

impl Postgres {
    /// `conn_string`: e.g. `host=localhost user=postgres password=pw dbname=raw`.
    pub fn connect(conn_string: impl Into<String>) -> Self {
        Self {
            conn_string: conn_string.into(),
            schema: "public".into(),
            tls: None,
            options: PgDestOptions::default(),
        }
    }

    /// Target schema (dataset); created if missing.
    pub fn dataset(mut self, schema: impl Into<String>) -> Self {
        self.schema = schema.into();
        self
    }

    /// TLS posture (feature 006, contract tls-policy.md) — the SAME policy
    /// type the source uses; the two directions share one connect path.
    pub fn tls(mut self, policy: crate::tls::TlsPolicy) -> Self {
        self.tls = Some(policy);
        self
    }

    pub(super) async fn client(&self) -> Result<Client, DestError> {
        let crate::tls::ParsedConn { pg, policy } =
            crate::tls::parse_conn(&self.conn_string, self.tls.as_ref())
                .map_err(|e| DestError::fatal(e.to_string()))?;
        match crate::tls::connect(&pg, &policy).await {
            Ok(client) => Ok(client),
            Err(crate::tls::ConnectResult::Config(e)) => Err(DestError::fatal(e.to_string())),
            Err(crate::tls::ConnectResult::Connect(e)) if e.transient => {
                Err(DestError::transient(e.to_string()))
            }
            Err(crate::tls::ConnectResult::Connect(e)) => Err(DestError::fatal(e.to_string())),
        }
    }
}

// ---- Destination options (features 008/010/011) ----
//
// Feature 013: the vocabulary + validation MOVED to rdlt-connector-sqlcore
// (shared with every SQL destination, contract SM5). Re-exported here at the
// original paths; the Pg* names stay as aliases so no consumer changes.

pub use rdlt_connector_sqlcore::{AbsentPolicy, DedupSort, MergeStrategy, Scd2Options, SortOrder};

/// Alias of the shared [`rdlt_connector_sqlcore::DestOptions`] (feature 013).
pub type PgDestOptions = rdlt_connector_sqlcore::DestOptions;
/// Alias of the shared [`rdlt_connector_sqlcore::TableOptions`] (feature 013).
pub type PgTableOptions = rdlt_connector_sqlcore::TableOptions;

impl Postgres {
    /// Strategy/hard-delete/SCD2 options (feature 008). Validated here —
    /// errors name the field.
    pub fn options(mut self, options: PgDestOptions) -> Result<Self, DestError> {
        options.validate().map_err(DestError::fatal)?;
        self.options = options;
        Ok(self)
    }
}
