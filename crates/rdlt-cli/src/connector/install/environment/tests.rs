use std::os::unix::fs::PermissionsExt as _;
use std::path::{Path, PathBuf};

use rdlt_connector::text::CUT;

use super::{installed, shown};
use crate::connector::install::InstallError;
use crate::connector::install::wheel::Wheel;
use crate::output::LINE_BYTES;

#[tokio::test]
async fn what_a_child_prints_is_shown_a_line_at_a_time_and_the_end_of_a_long_line_dropped() {
    let long = "x".repeat(LINE_BYTES * 2 + 10);
    let printed = format!("plain\ncoloured \u{1b}[31mred\n{long}\nafter\nunended");
    let mut written = Vec::new();
    shown(Some(printed.as_bytes()), &mut written).await;
    let written = String::from_utf8(written).unwrap();
    let lines: Vec<&str> = written.lines().collect();
    let cut = format!("{}{CUT}", "x".repeat(LINE_BYTES - CUT.len()));
    let expected = ["plain", "coloured \\u{1b}[31mred", cut.as_str(), "after", "unended"];
    assert_eq!(lines, expected);
    assert!(written.ends_with('\n'), "every line is ended: {written:?}");
}

/// An environment in `dir` whose interpreter takes any install and exits with `checking` when it
/// checks the packages installed.
fn environment(dir: &Path, checking: i32) -> PathBuf {
    let environment = dir.join("environment");
    std::fs::create_dir_all(environment.join("bin")).unwrap();
    let python = environment.join("bin/python");
    let script = format!("#!/bin/sh\nif [ \"$2\" = -S ]; then exit {checking}; fi\nexit 0\n");
    std::fs::write(&python, script).unwrap();
    std::fs::set_permissions(&python, std::fs::Permissions::from_mode(0o755)).unwrap();
    environment
}

/// What installing, with a requirements file, into an environment whose check exits with
/// `checking` answers.
async fn checked(checking: i32) -> Result<(), InstallError> {
    let dir = tempfile::tempdir().unwrap();
    let requirements = dir.path().join("requirements.txt");
    std::fs::write(&requirements, "").unwrap();
    let wheel = Wheel {
        name: "tickets".to_owned(),
        uri: "file:///tickets-0.1.0-py3-none-any.whl".to_owned(),
        sha256: "00".to_owned(),
        entry_points: Vec::new(),
    };
    let environment = environment(dir.path(), checking);
    installed(&environment, &wheel, Some(&requirements)).await
}

#[tokio::test]
async fn a_check_that_could_not_run_is_told_from_one_that_found_a_package_missing() {
    checked(0).await.expect("nothing is missing");
    let unrun = checked(100).await.unwrap_err();
    assert!(
        matches!(unrun, InstallError::CheckUnrun { .. }),
        "{unrun:?}"
    );
    assert_eq!(unrun.code(), "pip_failed");
    let missing = checked(1).await.unwrap_err();
    assert!(
        matches!(&missing, InstallError::PipFailed { what } if what.contains("leave out")),
        "{missing:?}"
    );
}
