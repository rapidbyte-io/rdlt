use std::path::Path;

use super::{file_name, reference};

#[test]
fn a_wheel_s_file_name_is_taken_and_any_other_is_not() {
    for taken in [
        "tickets-0.1.0-py3-none-any.whl",
        "rdlt-0.0.0-cp311-abi3-manylinux_2_28_x86_64.whl",
        "Rdlt_Connector.Tickets-1!2.0.post1+local.7-1build-cp311.cp312-abi3-macosx_11_0_arm64.whl",
        "torch-2.0.0+cu118-cp311-cp311-linux_x86_64.whl",
    ] {
        assert!(file_name(taken), "{taken} is refused");
    }
    for refused in [
        ".whl",
        "..whl",
        "...whl",
        "x.whl",
        "tickets-0.1.0.whl",
        "tickets-0.1.0-py3-none.whl",
        "tickets-0.1.0-x-y-py3-none-any.whl",
        "tickets-0.1.0-py3-none-any.WHL",
        "tickets-0.1.0-py3-none-any.whl.zip",
        "tickets-0.1.0-py3-none-any.tar.gz",
        ".tickets-0.1.0-py3-none-any.whl",
        "tic kets-0.1.0-py3-none-any.whl",
        "tickets-0.1.0.tar.gz-py3-none-any.whl",
        "tickets-1.0/x-py3-none-any.whl",
        "tickets-0.1.0-build-py3-none-any.whl",
        "tickets-0.1.0--none-any.whl",
        "tickets-0.1.0-py3-n%41ne-any.whl",
    ] {
        assert!(!file_name(refused), "{refused} is taken");
    }
}

#[test]
fn a_wheel_is_named_to_pip_by_its_copy_s_absolute_path_quoted() {
    let named = Path::new("/operator/tickets-0.1.0+local-py3-none-any.whl");
    let held = Path::new("/tmp/a b/./zo\u{eb}%//tickets-0.1.0+local-py3-none-any.whl");
    assert_eq!(
        reference("tickets", held, named).unwrap(),
        "tickets @ file:///tmp/a%20b/zo%C3%AB%25/tickets-0.1.0%2Blocal-py3-none-any.whl"
    );
    for (name, held) in [
        ("tickets", "tmp/tickets-0.1.0-py3-none-any.whl"),
        ("tickets", "/tmp/../tickets-0.1.0-py3-none-any.whl"),
        ("tickets", "/tmp/..whl"),
        ("tickets", "/tmp/tickets.tar.gz"),
        ("tic kets", "/tmp/tickets-0.1.0-py3-none-any.whl"),
        ("-r", "/tmp/tickets-0.1.0-py3-none-any.whl"),
    ] {
        let refused = reference(name, Path::new(held), named).unwrap_err();
        assert_eq!(refused.code(), "wheel", "{held}");
        assert!(refused.to_string().contains("/operator/"), "{refused}");
    }
}
