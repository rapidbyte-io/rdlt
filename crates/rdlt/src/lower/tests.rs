use rdlt_connector::{ConnectorId, Role};
use rdlt_engine::{Code, ErrorKind};
use rdlt_host::{ConnectorRef, Digest};

use super::reference;
use crate::{Access, Connector, Side};

#[test]
fn a_side_names_its_connector_s_requirements_and_access() {
    let digest = "0a".repeat(32);
    let connector = Connector::new("io.rapidbyte.sqlite")
        .version("^1.2")
        .digest(&digest)
        .endpoint("grpcs://db.example:7443");
    let access = Access::new().read("/in").write("/out").network();
    let side = Side::new(connector).access(access);
    let named = reference(&side, Role::Destination).expect("a reference");
    let expected = ConnectorRef::new(ConnectorId::parse("io.rapidbyte.sqlite").unwrap())
        .version(semver::VersionReq::parse("^1.2").unwrap())
        .digest(Digest([0x0a; 32]))
        .endpoint("grpcs://db.example:7443")
        .read("/in")
        .write("/out")
        .network();
    assert_eq!(named, expected);
    let bare = reference(
        &Side::new(Connector::new("io.rapidbyte.memory")),
        Role::Source,
    );
    let id = ConnectorId::parse("io.rapidbyte.memory").unwrap();
    assert_eq!(bare.expect("a reference"), ConnectorRef::new(id));
}

#[test]
fn a_digest_is_sixty_four_hex_digits() {
    let upper = "AB".repeat(32);
    let named = reference(
        &Side::new(Connector::new("a.b").digest(&upper)),
        Role::Source,
    );
    assert_eq!(
        named.expect("upper-case hex reads").digest,
        Some(Digest([0xab; 32]))
    );
    for wrong in [
        "0".repeat(63),
        "0".repeat(65),
        "0".repeat(66),
        format!("+f{}", "0".repeat(62)),
        format!("0g{}", "0".repeat(62)),
        format!("é{}", "0".repeat(62)),
    ] {
        let refused = reference(
            &Side::new(Connector::new("a.b").digest(wrong.clone())),
            Role::Source,
        );
        let refused = refused.expect_err("not a digest");
        assert_eq!(
            refused.code(),
            Some(Code::new("reference_invalid")),
            "{wrong}"
        );
    }
}

#[test]
fn a_reference_that_is_not_one_is_refused_as_a_mistake_of_its_author() {
    for side in [
        Side::new(Connector::new("not an id")),
        Side::new(Connector::new("a.b").version("one point two")),
    ] {
        let refused = reference(&side, Role::Destination).expect_err("refused");
        assert_eq!(refused.code(), Some(Code::new("reference_invalid")));
        assert_eq!(refused.kind(), ErrorKind::Config);
        assert!(refused.to_string().contains("destination"), "{refused}");
        assert!(std::error::Error::source(&refused).is_some());
    }
}
