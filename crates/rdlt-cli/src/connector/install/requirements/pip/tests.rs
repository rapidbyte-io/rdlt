use super::{reads_option, reads_requirement, shell_words};

/// A digest, as a hash names one.
const HASH: &str = "sha256:8f4ab0e8ea1d1e1ad4c8b0e5f2b7f9d9a3a8c1d4e7f6a5b4c3d2e1f0a9b8c7d6";

#[test]
fn a_requirement_reads_as_written_only_where_its_options_are_its_hashes() {
    let hashes = [HASH, HASH];
    let read = format!(r#"a[x]==1.0 ; os_name == "nt" --hash={HASH} --hash={HASH}"#);
    assert!(reads_requirement(&read, &hashes));
    for misread in [
        format!("a==1.0 ; --no-binary=planted --hash={HASH} --hash={HASH}"),
        format!("a==1.0 -x --hash={HASH} --hash={HASH}"),
        format!("a==1.0 --hash={HASH}"),
        format!("a==1.0 --hash={HASH} --hash={HASH} --pre"),
        format!("a==1.0 #x --hash={HASH} --hash={HASH}"),
        format!("a==${{X}} --hash={HASH} --hash={HASH}"),
        format!("a==1.0\u{1c} --hash={HASH} --hash={HASH}"),
        "a==1.0".to_owned(),
    ] {
        assert!(!reads_requirement(&misread, &hashes), "{misread:?}");
    }
}

#[test]
fn an_option_reads_as_written_only_where_a_shell_splits_it_into_it_and_its_value() {
    assert!(reads_option(
        "--find-links '/a b/w'",
        "--find-links",
        "/a b/w"
    ));
    let url = "https://x.example/simple#y";
    assert!(reads_option(
        &format!("--index-url '{url}'"),
        "--index-url",
        url
    ));
    for (line, value) in [
        ("--find-links '/a'b'", "/a'b"),
        ("--find-links '/a #b'", "/a #b"),
        ("--find-links '/${X}'", "/${X}"),
        ("--find-links '/a' '/b'", "/a"),
        ("#--find-links '/a'", "/a"),
    ] {
        assert!(!reads_option(line, "--find-links", value), "{line:?}");
    }
}

#[test]
fn words_are_split_as_python_s_shlex_splits_them() {
    let split = shell_words(r#"a "b\"c\d" 'e f' g\ h  i"#).unwrap();
    assert_eq!(split, ["a", r#"b"c\d"#, "e f", "g h", "i"]);
    for open in ["'a", "\"a", "a\\"] {
        assert_eq!(shell_words(open), None, "{open:?}");
    }
}
