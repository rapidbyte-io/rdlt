use super::{Violation, binaries, surfaces};

const ALONE: &str = "\
rdlt-connector-reference v0.0.0 (/repo/crates/rdlt-connector-reference)|
rdlt-connector v0.0.0 (/repo/crates/rdlt-connector)|default,macros,serve,sqlgen,wire
rdlt-wire v0.0.0 (/repo/crates/rdlt-wire)|serve,tls
serde v1.0.229|default,derive,std
";

fn surface(of: &str, feature: &str) -> Violation {
    Violation::Surface {
        package: "rdlt-connector-reference".to_owned(),
        of: of.to_owned(),
        feature: feature.to_owned(),
    }
}

#[test]
fn a_build_alone_with_no_test_or_certification_feature_passes() {
    assert_eq!(surfaces("rdlt-connector-reference", ALONE), []);
    assert_eq!(surfaces("rdlt-connector-reference", ""), []);
}

#[test]
fn each_surface_a_build_turns_on_is_reported_once() {
    let unified = "\
rdlt-connector-reference v0.0.0 (/repo)|certify,test-connectors
rdlt-connector v0.0.0 (/repo)|certify,default,serve,testing
rdlt-connector v0.0.0 (/repo)|certify,default,serve,testing
rdlt-engine v0.0.0 (/repo)|failpoints
certify v1.0.0|certify,testing
no features line
";
    assert_eq!(
        surfaces("rdlt-connector-reference", unified),
        [
            surface("rdlt-connector", "certify"),
            surface("rdlt-connector", "testing"),
            surface("rdlt-connector-reference", "certify"),
            surface("rdlt-connector-reference", "test-connectors"),
            surface("rdlt-engine", "failpoints"),
        ]
    );
}

#[test]
fn the_binaries_built_unasked_are_those_shipped_and_no_other() {
    let ships = ["rdlt-connector-files", "rdlt-connector-sqlite"];
    let built = |names: &[&str]| -> Vec<String> { names.iter().map(|n| (*n).to_owned()).collect() };
    assert_eq!(binaries("reference", &ships, &built(&ships)), []);
    let extra = built(&[
        "rdlt-connector-files",
        "rdlt-connector-memory",
        "rdlt-connector-sqlite",
    ]);
    assert_eq!(
        binaries("reference", &ships, &extra),
        [Violation::Binary {
            package: "reference".to_owned(),
            binary: "rdlt-connector-memory".to_owned(),
        }]
    );
    assert_eq!(
        binaries("reference", &ships, &built(&["rdlt-connector-files"])),
        [Violation::Missing {
            package: "reference".to_owned(),
            binary: "rdlt-connector-sqlite".to_owned(),
        }]
    );
}
