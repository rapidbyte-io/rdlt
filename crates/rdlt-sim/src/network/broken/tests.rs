use rdlt_connector::ConnectorId;
use rdlt_host::ConnectorRef;
use rdlt_wire::Limits;

use super::{ID, catalogue, port};
use crate::canary::Canary;
use crate::network::{BROKEN, Networked, Peers, Serving, options, run_networked};
use crate::rng::SplitMix64;
use crate::seed::Seed;

/// A network with the broken connectors' host and nothing hostile beside it.
const BROKEN_ONLY: Peers = Peers {
    hostile: false,
    framing: true,
};

#[test]
fn every_broken_connector_listens_on_its_port_over_mutual_tls() {
    let dialed = run_networked(
        Seed::new(5),
        Serving::Rows,
        BROKEN_ONLY,
        |_env, net| async move {
            let mut rng = SplitMix64::new(7);
            let placing = Networked::new(
                net,
                &options(&mut rng, Limits::default()),
                &Canary::of("broken-ports"),
            );
            let id = ConnectorId::parse(ID).expect("a valid id");
            let mut dialed = Vec::new();
            for index in 0..catalogue().len() {
                let endpoint = format!("grpcs://{BROKEN}:{}", port(index));
                let reference = ConnectorRef::new(id.clone()).endpoint(endpoint);
                dialed.push(
                    placing
                        .remote
                        .wire(&reference)
                        .await
                        .map(drop)
                        .map_err(|error| error.to_string()),
                );
            }
            dialed
        },
    );
    let failed: Vec<_> = dialed
        .iter()
        .enumerate()
        .filter(|(_, dialed)| dialed.is_err())
        .collect();
    assert!(failed.is_empty(), "{failed:#?}");
    assert_eq!(dialed.len(), 20);
}
