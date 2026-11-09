use std::cell::Cell;

use super::Served;

#[test]
fn arguments_given_are_read_in_place_of_the_process_s_and_build_nothing_when_refused() {
    let built = Cell::new(false);
    let refused = Served::serve_args(["--no-such-flag".to_owned()], || {
        built.set(true);
        Served::new()
    });
    let failed = refused.expect_err("an unknown argument is refused");
    assert!(failed.to_string().contains("--no-such-flag"));
    assert!(
        !built.get(),
        "nothing is built before the arguments are read"
    );
}
