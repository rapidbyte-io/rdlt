use std::path::Path;

use super::{file_name, reference};
use crate::connector::install::tests::{NO_WHEELS, WHEELS};

#[test]
fn a_wheel_s_file_name_is_taken_and_any_other_is_not() {
    for taken in WHEELS {
        assert!(file_name(taken), "{taken} is refused");
    }
    for refused in NO_WHEELS {
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
