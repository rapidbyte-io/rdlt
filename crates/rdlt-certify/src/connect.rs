//! The source and destination clauses' connector factories over the wire: each connection is a
//! fresh one to the target, handshaken, with nothing between the clause and the protocol to redial
//! or retry what the connector did.

use rdlt_connector::testing::{Clause, ClauseResult, Outcome, Report};
use rdlt_connector::{
    BoxFuture, ConnectContext, ConnectorError, ConnectorSpec, Destination, DestinationFactory,
    Role, Source, SourceFactory,
};
use rdlt_host::{RemoteDestination, RemoteSource};

use crate::target::Target;

/// A factory of connections to `target`, as a connector factory makes connectors.
pub(crate) struct Factory<'a> {
    target: &'a Target,
    spec: ConnectorSpec,
}

/// Why a role cannot be certified at all.
pub(crate) enum Unmet {
    /// The connector does not serve the role.
    Unserved(ConnectorError),
    /// The first connection failed.
    Failed(ConnectorError),
}

impl Unmet {
    /// A report of `families`' clauses, each skipped or failed as this says.
    pub(crate) fn report(&self, target: &Target, families: &[&[Clause]]) -> Report {
        let outcome = match self {
            Self::Unserved(error) => Outcome::Skipped(error.to_string()),
            Self::Failed(error) => Outcome::Failed(format!("connect failed: {error}")),
        };
        Report {
            connector: target.describe(),
            results: families
                .iter()
                .flat_map(|clauses| clauses.iter())
                .map(|clause| ClauseResult {
                    clause: *clause,
                    outcome: outcome.clone(),
                })
                .collect(),
        }
    }
}

/// The code of the error a connector refuses a role it does not serve with.
pub(crate) const UNSERVED: &str = "role";

impl<'a> Factory<'a> {
    /// A factory for `target` as `role`, whose spec a first connection, with `config`, answers.
    pub(crate) async fn new(
        target: &'a Target,
        role: Role,
        config: &serde_json::Value,
    ) -> Result<Self, Unmet> {
        let connection = target.connect(role, config).await.map_err(|error| {
            if error.code() == Some(UNSERVED) {
                Unmet::Unserved(error)
            } else {
                Unmet::Failed(error)
            }
        })?;
        let spec = connection.connector_spec(role).map_err(Unmet::Failed)?;
        Ok(Self { target, spec })
    }
}

impl SourceFactory for Factory<'_> {
    fn spec(&self) -> &ConnectorSpec {
        &self.spec
    }

    fn connect(
        &self,
        config: serde_json::Value,
        _context: ConnectContext,
    ) -> BoxFuture<'_, rdlt_connector::Result<Box<dyn Source>>> {
        Box::pin(async move {
            let connection = self.target.connect(Role::Source, &config).await?;
            Ok(Box::new(RemoteSource::new(connection)) as Box<dyn Source>)
        })
    }
}

impl DestinationFactory for Factory<'_> {
    fn spec(&self) -> &ConnectorSpec {
        &self.spec
    }

    fn connect(
        &self,
        config: serde_json::Value,
        _context: ConnectContext,
    ) -> BoxFuture<'_, rdlt_connector::Result<Box<dyn Destination>>> {
        Box::pin(async move {
            let connection = self.target.connect(Role::Destination, &config).await?;
            Ok(Box::new(RemoteDestination::new(connection)?) as Box<dyn Destination>)
        })
    }
}
