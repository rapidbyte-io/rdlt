//! Destination handle + builder: connection string, dataset, TLS posture.
//! (Feature 008 T001: relocated verbatim from the old single-file module.)

use rdlt_connector::DestError;
use tokio_postgres::Client;

#[derive(Debug, Clone)]
pub struct Postgres {
    pub(super) conn_string: String,
    pub(super) schema: String,
    pub(super) tls: Option<crate::tls::TlsPolicy>,
}

impl Postgres {
    /// `conn_string`: e.g. `host=localhost user=postgres password=pw dbname=raw`.
    pub fn connect(conn_string: impl Into<String>) -> Self {
        Self {
            conn_string: conn_string.into(),
            schema: "public".into(),
            tls: None,
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
