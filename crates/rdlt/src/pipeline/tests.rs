use std::path::PathBuf;

use super::Placement;

#[test]
fn a_placement_says_where_its_connector_runs_and_an_endpoint_only_by_its_host_and_port() {
    assert_eq!(
        Placement::of(rdlt_host::Placement::InProcess),
        Placement::InProcess
    );
    let path = PathBuf::from("/opt/rdlt/bin/rdlt-connector-sqlite");
    let spawned = rdlt_host::Placement::Process { path: path.clone() };
    assert_eq!(Placement::of(spawned), Placement::Process { path });
    let listening = rdlt_host::Placement::Remote {
        endpoint: "grpcs://db.example:7443".to_owned(),
    };
    let shown = Placement::Remote {
        endpoint: "db.example:7443".to_owned(),
    };
    assert_eq!(Placement::of(listening), shown);
    let unusable = rdlt_host::Placement::Remote {
        endpoint: "grpcs://user:CANARY@db.example:7443".to_owned(),
    };
    let Placement::Remote { endpoint } = Placement::of(unusable) else {
        panic!("a remote placement");
    };
    assert!(!endpoint.contains("CANARY"), "{endpoint}");
}

#[test]
fn a_builder_sets_each_part_of_its_document() {
    use std::time::Duration;

    use crate::{
        Commit, Connector, Defaults, Document, Pipeline, ReadMode, Resources, Retry, Side, Stream,
        Until,
    };

    let side = || Side::new(Connector::new("a.b"));
    let builder = Pipeline::builder("p", side(), side())
        .stream(Stream::new("s"))
        .defaults(Defaults::new().read(ReadMode::Cdc))
        .commit(Commit::new().rows(5))
        .resources(Resources::new().writers(2))
        .retry(Retry::new().initial(Duration::from_secs(1)))
        .until(Until::Forever);
    let mut document = Document::new("p", side(), side());
    document.streams = vec![Stream::new("s")];
    document.defaults = Defaults::new().read(ReadMode::Cdc);
    document.commit = Some(Commit::new().rows(5));
    document.resources = Resources::new().writers(2);
    document.retry = Some(Retry::new().initial(Duration::from_secs(1)));
    document.until = Until::Forever;
    assert_eq!(builder.document(), &document);
    assert_eq!(
        Pipeline::from_document(document.clone()).document(),
        &document
    );
}
