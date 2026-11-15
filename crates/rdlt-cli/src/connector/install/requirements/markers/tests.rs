use super::{NESTING, marked};

#[test]
fn markers_pep_508_writes_are_written_again_from_what_was_read() {
    for (read, written) in [
        ("sys_platform == 'linux'", r#"sys_platform == "linux""#),
        (
            "python_full_version>='3.11'and platform_machine!=\"aarch64\"",
            r#"python_full_version >= "3.11" and platform_machine != "aarch64""#,
        ),
        (
            "(os_name == 'posix' or os_name == 'nt') and extra == 'a,b'",
            r#"( os_name == "posix" or os_name == "nt" ) and extra == "a,b""#,
        ),
        (
            "'linux' in sys_platform and python_version not in '3.1*'",
            r#""linux" in sys_platform and python_version not in "3.1*""#,
        ),
        (
            "python_version ~= '3.11' or python_version === '3.14'",
            r#"python_version ~= "3.11" or python_version === "3.14""#,
        ),
        (
            "implementation_version < '3.12.0rc1+local'",
            r#"implementation_version < "3.12.0rc1+local""#,
        ),
    ] {
        assert_eq!(marked(read).as_deref(), Some(written), "{read:?}");
    }
}

#[test]
fn what_pep_508_does_not_write_or_could_read_as_an_option_is_refused() {
    let deep = format!(
        "{}os_name == 'nt'{}",
        "(".repeat(NESTING + 1),
        ")".repeat(NESTING + 1)
    );
    for refused in [
        "",
        "--no-binary=planted",
        "os_name == '--no-binary'",
        "os_name == '-x'",
        "os_name == 'a b'",
        "os_name == 'a'b'",
        "os_name == 'a\"'",
        "os_name == 'nt",
        "os_name == nt",
        "home == 'x'",
        "os.name == 'nt'",
        "os_name = 'nt'",
        "os_name => 'nt'",
        "os_name 'nt'",
        "os_name == 'nt' or",
        "os_name == 'nt' and and os_name == 'nt'",
        "(os_name == 'nt'",
        "os_name == 'nt')",
        "os_name not 'nt'",
        "os_name == '${X}'",
        "os_name == 'a;b'",
        "os_name == 'a#b'",
        deep.as_str(),
    ] {
        assert_eq!(marked(refused), None, "{refused:?}");
    }
    let nested = format!(
        "{}os_name == 'nt'{}",
        "(".repeat(NESTING),
        ")".repeat(NESTING)
    );
    assert!(
        marked(&nested).is_some(),
        "markers nested as deep as they may be"
    );
}
