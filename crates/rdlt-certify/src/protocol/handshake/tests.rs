use super::unknown;

#[test]
fn an_unknown_feature_is_named_after_the_time_beside_no_host_s() {
    let name = unknown();
    let time = name
        .strip_prefix("x.")
        .expect("named apart from every host's feature");
    assert!(!time.is_empty(), "{name}");
    assert!(time.bytes().all(|byte| byte.is_ascii_hexdigit()), "{name}");
}
