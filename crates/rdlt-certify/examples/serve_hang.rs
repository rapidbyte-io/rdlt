//! A connector whose destination never answers its configuration, as one dialing a store that
//! never answers does, for the tests of what bounds a certification.

#![forbid(unsafe_code)]

use std::process::ExitCode;

use rdlt_connector::serve::Served;
use rdlt_connector::{
    BoxFuture, ConnectContext, ConnectorSpec, Destination, DestinationFactory, destination_factory,
};
use rdlt_connector_reference::MemoryDestination;

struct Hang(Box<dyn DestinationFactory>);

impl DestinationFactory for Hang {
    fn spec(&self) -> &ConnectorSpec {
        self.0.spec()
    }

    fn connect(
        &self,
        _config: serde_json::Value,
        _context: ConnectContext,
    ) -> BoxFuture<'_, rdlt_connector::Result<Box<dyn Destination>>> {
        Box::pin(std::future::pending())
    }
}

fn main() -> ExitCode {
    let hang = Hang(destination_factory::<MemoryDestination>());
    Served::new().with_destination(Box::new(hang)).serve()
}
