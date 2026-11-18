//! The generator source, saying what its configuration's `said` holds on its standard output and
//! its standard error as it connects, for the tests that nothing a certified connector says shows
//! a secret.

#![forbid(unsafe_code)]

use std::io::Write as _;
use std::process::ExitCode;

use rdlt_connector::serve::Served;
use rdlt_connector::{
    BoxFuture, ConnectContext, ConnectorSpec, Source, SourceFactory, source_factory,
};
use rdlt_connector_reference::GeneratorSource;

/// The generator source, which says what it was told before it connects.
struct Saying(Box<dyn SourceFactory>);

impl SourceFactory for Saying {
    fn spec(&self) -> &ConnectorSpec {
        self.0.spec()
    }

    fn connect(
        &self,
        mut config: serde_json::Value,
        context: ConnectContext,
    ) -> BoxFuture<'_, rdlt_connector::Result<Box<dyn Source>>> {
        let said = config
            .as_object_mut()
            .and_then(|fields| fields.remove("said"));
        if let Some(said) = said {
            writeln!(std::io::stdout(), "connecting with {said}").ok();
            writeln!(std::io::stderr(), "connecting with {said:?}").ok();
        }
        self.0.connect(config, context)
    }
}

fn main() -> ExitCode {
    Served::serve(|| {
        Served::new().with_source(Box::new(Saying(source_factory::<GeneratorSource>())))
    })
}
