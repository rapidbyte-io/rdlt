//! The failing source, and a destination whose sessions never open, served as one connector
//! binary, for the tests of how each failure exits.

#![forbid(unsafe_code)]

use std::process::ExitCode;

use rdlt_connector::serve::Served;
use rdlt_connector::{
    BoxFuture, Capabilities, ConnectContext, ConnectorError, ConnectorId, ConnectorSpec,
    Destination, DestinationFactory, OpenContext, OpenedSession, destination_factory,
    source_factory,
};
use rdlt_connector_reference::MemoryDestination;
use rdlt_testkit::failing::FailingSource;

/// The memory destination under the failing connector's id, connecting and checking as it does,
/// and refusing every session, as a store that goes down after its check does.
struct Refused {
    spec: ConnectorSpec,
    inner: Box<dyn DestinationFactory>,
}

struct Refusing(Box<dyn Destination>);

impl Destination for Refusing {
    fn capabilities(&self) -> &Capabilities {
        self.0.capabilities()
    }

    fn check(&self) -> BoxFuture<'_, rdlt_connector::Result<()>> {
        self.0.check()
    }

    fn open<'a>(
        &'a self,
        _context: &'a OpenContext,
    ) -> BoxFuture<'a, rdlt_connector::Result<OpenedSession>> {
        Box::pin(async { Err(ConnectorError::internal("the store went down")) })
    }
}

impl DestinationFactory for Refused {
    fn spec(&self) -> &ConnectorSpec {
        &self.spec
    }

    fn connect(
        &self,
        config: serde_json::Value,
        context: ConnectContext,
    ) -> BoxFuture<'_, rdlt_connector::Result<Box<dyn Destination>>> {
        Box::pin(async move {
            let connected = self.inner.connect(config, context).await?;
            Ok(Box::new(Refusing(connected)) as Box<dyn Destination>)
        })
    }
}

fn refused() -> Box<dyn DestinationFactory> {
    let inner = destination_factory::<MemoryDestination>();
    let id = ConnectorId::parse("io.rapidbyte.failing").expect("a valid id");
    let spec = ConnectorSpec {
        id,
        ..inner.spec().clone()
    };
    Box::new(Refused { spec, inner })
}

fn main() -> ExitCode {
    Served::serve(|| {
        Served::new()
            .with_source(source_factory::<FailingSource>())
            .with_destination(refused())
    })
}
