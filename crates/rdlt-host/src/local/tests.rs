use std::os::unix::fs::PermissionsExt as _;

use super::{Binary, programs};

#[test]
fn a_program_with_a_second_name_is_held_as_linked_a_script_and_its_interpreter_alike() {
    let root = tempfile::tempdir().expect("a temporary directory");
    let interpreter = root.path().join("sh");
    std::fs::copy("/bin/sh", &interpreter).expect("the shell copies");
    let script = root.path().join("rdlt-connector-script");
    let line = format!("#!{}\nexit 3\n", interpreter.display());
    std::fs::write(&script, line).expect("the script writes");
    std::fs::set_permissions(&script, std::fs::Permissions::from_mode(0o755))
        .expect("the script is executable");
    let linked = || {
        let binary = Binary::at(&script).expect("the script opens");
        let programs = programs(&binary).expect("its programs");
        programs
            .iter()
            .map(|program| program.linked)
            .collect::<Vec<_>>()
    };
    assert_eq!(linked(), [false, false]);
    std::fs::hard_link(&interpreter, root.path().join("another-sh")).expect("a second name");
    assert_eq!(linked(), [false, true]);
    std::fs::hard_link(&script, root.path().join("another-script")).expect("a second name");
    assert_eq!(linked(), [true, true]);
}
