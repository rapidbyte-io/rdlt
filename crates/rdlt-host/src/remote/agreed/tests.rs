use super::version;

#[test]
fn a_version_is_a_short_string_of_its_own_characters() {
    for kept in ["0.1.0", "1.2.3-rc.1+build.5", "2026.10"] {
        assert!(version(kept), "{kept}");
    }
    let long = "1".repeat(65);
    for refused in ["", "1.0 beta", "1.0\n", "v1\u{202e}", long.as_str()] {
        assert!(!version(refused), "{refused:?}");
    }
}
