use super::version;

#[test]
fn a_version_pep_440_writes_is_taken_and_a_file_s_name_is_not() {
    for taken in [
        "1",
        "1.0",
        "2.11.7",
        "1!2.0",
        "v1.0",
        "1.0a1",
        "1.0-alpha.1",
        "1.0rc",
        "1.0c2",
        "1.0.post1",
        "1.0-1",
        "1.0r",
        "1.0.dev3",
        "1!2.0.post1.dev3+local.7",
        "1.0+ubuntu-1_x",
        "2024.1.POST1",
    ] {
        assert!(version(taken), "{taken} is refused");
    }
    for refused in [
        "", "v", ".1", "1.", "1..0", "1.tar.gz", "1.zip", "1.whl", "1.0+", "1.0+a.", "1.0*",
        "1.0,2", "1!", "1.0 ", "1.0dev3x", "a1",
    ] {
        assert!(!version(refused), "{refused} is taken");
    }
}
