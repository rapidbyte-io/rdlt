//! How a played connector is installed: as a binary its connectors are built into, or as
//! `rdlt connector install` installs a Python connector, a directory connector whose launcher is a
//! script naming the tree's own interpreter and whose connectors are described at run time.

#[cfg(test)]
mod tests;

use std::os::unix::ffi::OsStrExt as _;
use std::os::unix::fs::PermissionsExt as _;
use std::path::Path;

use rdlt_connector::{
    ConnectorId, DestinationConnector, DestinationFactory, SourceConnector, SourceFactory,
    destination_factory, destination_factory_from, parse_config, source_factory,
    source_factory_from,
};
use rdlt_host::ConnectorRef;

use crate::changes::SimChangeSource;
use crate::destination::SimDestination;
use crate::network::Serving;
use crate::rng::SplitMix64;
use crate::seed::Seed;
use crate::source::SimSource;

/// What the installation's draw mixes into the seed: "install" in ASCII.
const INSTALL: u64 = 0x0069_6e73_7461_6c6c;

/// How a played connector is installed.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Installed {
    /// A binary, its connectors built in, which a reference names by its path.
    Binary,
    /// A directory connector in the connector directory, which a reference names by its id alone.
    Directory,
}

impl Installed {
    /// How `seed`'s played connectors are installed, each way for half the seeds; drawn apart from
    /// the processes, so every seed's processes play as they always have.
    pub(super) fn drawn(seed: Seed) -> Self {
        if SplitMix64::new(seed.value() ^ INSTALL).chance(500) {
            Self::Directory
        } else {
            Self::Binary
        }
    }

    /// Installs the connector `id` names in `connectors`, a connector directory the user's alone,
    /// and says the reference that names it.
    pub(super) fn install(self, connectors: &Path, id: ConnectorId) -> ConnectorRef {
        match self {
            Self::Binary => {
                let binary = connectors.join("rdlt-connector-sim");
                written(&binary, b"rdlt-sim played connector\n");
                ConnectorRef::new(id).path(binary)
            }
            Self::Directory => {
                // Named by the id's last segment, as the host looks a connector up.
                let last = id.as_str().rsplit('.').next().unwrap_or(id.as_str());
                let tree = connectors.join(format!("rdlt-connector-{last}"));
                let bin = tree.join("bin");
                std::fs::create_dir_all(&bin).expect("the tree is made");
                for directory in [&tree, &bin] {
                    std::fs::set_permissions(directory, std::fs::Permissions::from_mode(0o700))
                        .expect("the tree is the user's alone");
                }
                let interpreter = bin.join("python");
                written(&interpreter, b"rdlt-sim played interpreter\n");
                let mut launcher = b"#!".to_vec();
                launcher.extend_from_slice(interpreter.as_os_str().as_bytes());
                launcher.extend_from_slice(b" -I\n");
                written(&bin.join("rdlt-connector"), &launcher);
                ConnectorRef::new(id)
            }
        }
    }

    /// The factories each process of the connector serves: of the source `serving` names, and
    /// of the simulated destination.
    pub(super) fn factories(
        self,
        serving: Serving,
    ) -> (Box<dyn SourceFactory>, Box<dyn DestinationFactory>) {
        match (self, serving) {
            (Self::Binary, _) => (serving.factory(), destination_factory::<SimDestination>()),
            (Self::Directory, Serving::Rows) => (
                described_source::<SimSource>(),
                described_destination::<SimDestination>(),
            ),
            (Self::Directory, Serving::Changes) => (
                described_source::<SimChangeSource>(),
                described_destination::<SimDestination>(),
            ),
        }
    }
}

/// Writes `contents` to `path`, a program the user's alone, which nothing runs.
fn written(path: &Path, contents: &[u8]) {
    std::fs::write(path, contents).expect("the program is written");
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o700))
        .expect("the program is the user's alone");
}

/// The factory of source `C` described at run time, as one written in another language is: the
/// spec its built factory names, and `C` connected from the whole configuration.
fn described_source<C: SourceConnector>() -> Box<dyn SourceFactory> {
    let spec = source_factory::<C>().spec().clone();
    source_factory_from::<C, _>(spec, |config, context| {
        Box::pin(async move { C::connect(parse_config(&config)?, &context).await })
    })
}

/// The factory of destination `C` described at run time, as [`described_source`] describes a
/// source.
fn described_destination<C: DestinationConnector>() -> Box<dyn DestinationFactory> {
    let spec = destination_factory::<C>().spec().clone();
    destination_factory_from::<C, _>(spec, |config, context| {
        Box::pin(async move { C::connect(parse_config(&config)?, &context).await })
    })
}
