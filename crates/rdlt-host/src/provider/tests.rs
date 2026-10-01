use super::{ConnectorRef, Digest};

#[test]
fn a_digest_shows_as_lowercase_hex() {
    let mut bytes = [0; 32];
    bytes[0] = 0xab;
    bytes[31] = 0x0f;
    let digest = Digest(bytes);
    let hex = format!("ab{}0f", "00".repeat(30));
    assert_eq!(digest.to_string(), hex);
    assert_eq!(format!("{digest:?}"), format!("Digest({hex})"));
}

#[test]
fn a_reference_shows_its_endpoint_by_host_and_port_and_never_one_that_is_refused() {
    let id = rdlt_connector::ConnectorId::parse("io.example.sink").expect("a valid id");
    let reference = ConnectorRef::new(id);
    let shown = |endpoint: &str| format!("{:?}", reference.clone().endpoint(endpoint));
    let placed = shown("grpcs://connector.example:7443/");
    assert!(placed.contains("connector.example:7443"), "{placed}");
    for refused in [
        "grpcs://svc:hunter2@connector.example:7443",
        "grpcs://connector.example:7443/?token=hunter2",
        "hunter2",
    ] {
        let shown = shown(refused);
        assert!(!shown.contains("hunter2"), "{shown}");
        assert!(shown.contains("io.example.sink"), "{shown}");
    }
    let unplaced = format!("{reference:?}");
    assert!(unplaced.contains("endpoint: None"), "{unplaced}");
}
