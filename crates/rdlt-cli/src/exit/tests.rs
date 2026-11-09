use rdlt::ErrorKind;

use super::Exit;

#[test]
fn every_error_kind_exits_with_its_code_from_the_table() {
    for (kind, code) in [
        (ErrorKind::Config, 2),
        (ErrorKind::Schema, 3),
        (ErrorKind::Source, 4),
        (ErrorKind::Destination, 5),
        (ErrorKind::Transform, 6),
        (ErrorKind::Fenced, 7),
        (ErrorKind::Memory, 8),
        (ErrorKind::Internal, 70),
        (ErrorKind::Cancelled, 70),
    ] {
        assert_eq!(Exit::of(kind).code(), code, "{kind:?}");
    }
    let codes: Vec<u8> = Exit::TABLE.iter().map(|(exit, _)| exit.code()).collect();
    assert_eq!(
        codes,
        [0, 1, 2, 3, 4, 5, 6, 7, 8, 64, 70, 74, 130, 143, 129, 131]
    );
}

#[test]
fn a_signal_exits_with_its_status_and_a_status_no_exit_code_holds_is_a_defect() {
    for status in [129, 130, 131, 143] {
        assert_eq!(i32::from(Exit::Signalled(status).code()), status);
    }
    assert_eq!(Exit::Signalled(256).code(), Exit::Internal.code());
}
