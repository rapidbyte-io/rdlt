//! The memory destination, except that it crashes as it commits: it writes its last words to
//! standard error and exits with status 3.

#![forbid(unsafe_code)]

use std::io::Write as _;
use std::process::ExitCode;
use std::sync::Arc;

use rdlt_connector::serve::Served;
use rdlt_connector::{
    BoxFuture, Capabilities, CommitMeta, ConnectContext, ConnectorSpec, Destination,
    DestinationFactory, DestinationSession, DestinationWriter, OpenContext, OpenedSession, Receipt,
    Result, TableChange, TableRef, destination_factory,
};
use rdlt_connector_reference::MemoryDestination;

struct Crashing(Box<dyn DestinationFactory>);

impl DestinationFactory for Crashing {
    fn spec(&self) -> &ConnectorSpec {
        self.0.spec()
    }

    fn connect(
        &self,
        config: serde_json::Value,
        context: ConnectContext,
    ) -> BoxFuture<'_, Result<Box<dyn Destination>>> {
        Box::pin(async move {
            let inner = self.0.connect(config, context).await?;
            Ok(Box::new(Connected(Arc::from(inner))) as Box<dyn Destination>)
        })
    }
}

struct Connected(Arc<dyn Destination>);

impl Destination for Connected {
    fn capabilities(&self) -> &Capabilities {
        self.0.capabilities()
    }

    fn check(&self) -> BoxFuture<'_, Result<()>> {
        self.0.check()
    }

    fn open<'a>(&'a self, context: &'a OpenContext) -> BoxFuture<'a, Result<OpenedSession>> {
        Box::pin(async move {
            let opened = self.0.open(context).await?;
            Ok(OpenedSession {
                session: Box::new(Session(opened.session)),
                ..opened
            })
        })
    }
}

struct Session(Box<dyn DestinationSession>);

impl DestinationSession for Session {
    fn apply_schema<'a>(&'a mut self, change: &'a TableChange) -> BoxFuture<'a, Result<()>> {
        self.0.apply_schema(change)
    }

    fn writer<'a>(
        &'a mut self,
        table: &'a TableRef,
    ) -> BoxFuture<'a, Result<Box<dyn DestinationWriter>>> {
        self.0.writer(table)
    }

    fn commit<'a>(&'a mut self, _meta: &'a CommitMeta) -> BoxFuture<'a, Result<Receipt>> {
        let mut stderr = std::io::stderr();
        writeln!(stderr, "crashing in the commit").ok();
        stderr.flush().ok();
        std::process::exit(3)
    }

    fn close(self: Box<Self>) -> BoxFuture<'static, Result<()>> {
        self.0.close()
    }
}

fn main() -> ExitCode {
    Served::new()
        .with_destination(Box::new(Crashing(
            destination_factory::<MemoryDestination>(),
        )))
        .serve()
}
