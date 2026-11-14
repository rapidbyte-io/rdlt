//! A read asked to stop while a request of its is in flight ends at once, not with its page.

use std::time::Duration;

use rdlt::{Connector, Host, Pipeline, Side, StopMode, Stream};
use rdlt_connector_reference::SqliteDestination;
use serde_json::json;

use crate::server::{Answer, Api};

#[tokio::test]
async fn a_read_stops_while_its_request_is_held() {
    // The detail of the third pokemon is held far longer than the test waits for the run.
    let api = Api::answering(
        5,
        vec![Answer::held("/api/v2/pokemon/3", Duration::from_secs(60))],
    );
    let directory = tempfile::tempdir().unwrap();
    let host = Host::new()
        .source::<pokeapi::PokeApi>()
        .destination::<SqliteDestination>();
    let source = Side::new(Connector::new("io.example.pokeapi"));
    let source = source.config(json!({ "base_url": api.url }));
    let database = directory.path().join("pokeapi.db");
    let destination = Side::new(Connector::new("io.rapidbyte.sqlite"));
    let destination = destination.config(json!({ "path": database }));
    let pipeline = Pipeline::builder("pokeapi", source, destination)
        .stream(Stream::new("pokemon"))
        .build(&host)
        .await
        .expect("the pipeline builds");
    let run = pipeline.start();
    let control = run.control();
    let stopping = async {
        while api.holding() == 0 {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        control.stop(StopMode::AfterCommit);
    };
    let ended = async { tokio::join!(run, stopping).0 };
    let outcome = tokio::time::timeout(Duration::from_secs(20), ended).await;
    let outcome = outcome.expect("the read ends once it is asked to stop, not with its request");
    assert!(outcome.error.is_none(), "{:?}", outcome.error);
}
