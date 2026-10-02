use std::ffi::OsString;
use std::path::PathBuf;

use super::{Bubblewrap, PROGRAM};
use crate::local::process::{PROGRAM_FD, SOCKET_FD};
use crate::local::sandbox::{Confined, Grants, NetworkGrant, Sandbox, SandboxError, Stops};

fn confined<'a>(
    grants: &'a Grants,
    env: &'a [(OsString, OsString)],
    args: &'a [OsString],
) -> Confined<'a> {
    Confined {
        program: PROGRAM_FD,
        args,
        env,
        socket: SOCKET_FD,
        grants,
    }
}

/// The arguments that confine a connector with `grants`, as text.
fn arguments(grants: &Grants) -> Vec<String> {
    let env = [(OsString::from("KEPT"), OsString::from("a value"))];
    let args = [OsString::from("--rdlt-fd=3")];
    let arguments = Bubblewrap::confinement(&confined(grants, &env, &args)).expect("arguments");
    arguments
        .into_iter()
        .map(|argument| argument.into_string().expect("text"))
        .collect()
}

/// Whether `arguments` hold `sequence` in order and adjacent.
fn holds(arguments: &[String], sequence: &[&str]) -> bool {
    arguments.windows(sequence.len()).any(|window| {
        window
            .iter()
            .map(String::as_str)
            .eq(sequence.iter().copied())
    })
}

#[test]
fn a_connector_is_confined_to_what_it_is_granted_and_run_from_its_descriptor() {
    let arguments = arguments(&Grants::default());
    for alone in [
        "--unshare-all",
        "--die-with-parent",
        "--new-session",
        "--clearenv",
    ] {
        assert!(
            arguments.iter().any(|argument| argument == alone),
            "{alone}"
        );
    }
    assert!(!arguments.iter().any(|argument| argument == "--share-net"));
    assert!(holds(&arguments, &["--setenv", "KEPT", "a value"]));
    assert!(holds(&arguments, &["--proc", "/proc"]));
    assert!(holds(&arguments, &["--dev", "/dev"]));
    assert!(holds(&arguments, &["--tmpfs", "/tmp"]));
    assert!(holds(
        &arguments,
        &["--ro-bind-fd", &PROGRAM_FD.to_string(), PROGRAM]
    ));
    // The program and its arguments end the command, and nothing follows them.
    assert!(arguments.ends_with(&[
        "--".to_owned(),
        PROGRAM.to_owned(),
        "--rdlt-fd=3".to_owned()
    ]));
    // Nothing of the host is bound to be written, and no home directory at all.
    assert!(!arguments.iter().any(|argument| argument == "--bind"));
    assert!(
        !arguments
            .iter()
            .any(|argument| argument.starts_with("/home"))
    );
    assert!(!arguments.iter().any(|argument| argument == "--dev-bind"));
}

#[test]
fn what_is_granted_is_bound_and_nothing_else_of_the_hosts() {
    let granted = tempfile::tempdir().expect("a temporary directory");
    let (read, write) = (granted.path().join("read"), granted.path().join("write"));
    std::fs::create_dir(&read).expect("a directory");
    std::fs::create_dir(&write).expect("a directory");
    let grants = Grants {
        read: vec![read.clone()],
        write: vec![write.clone()],
        network: NetworkGrant::Granted,
    };
    let arguments = arguments(&grants);
    let (read, write) = (read.to_str().expect("text"), write.to_str().expect("text"));
    assert!(holds(&arguments, &["--ro-bind", read, read]));
    assert!(holds(&arguments, &["--bind", write, write]));
    assert!(holds(&arguments, &["--unshare-all", "--share-net"]));
}

#[test]
fn a_grant_that_is_no_absolute_path_to_something_there_is_refused() {
    let absent = PathBuf::from("/nonexistent/granted");
    for path in [PathBuf::from("relative"), PathBuf::from(""), absent] {
        for write in [false, true] {
            let mut grants = Grants::default();
            if write {
                grants.write.push(path.clone());
            } else {
                grants.read.push(path.clone());
            }
            let refused = Bubblewrap::confinement(&confined(&grants, &[], &[]));
            assert_eq!(
                refused,
                Err(SandboxError::Grant { path: path.clone() }),
                "{write}"
            );
        }
    }
}

#[test]
fn a_launcher_that_is_not_there_or_makes_no_sandbox_is_refused_with_its_reason() {
    let grants = Grants::default();
    let asked = confined(&grants, &[], &[]);
    let launcher = |path: &str| Bubblewrap::at(path).launcher(&asked).map(|_| ());
    let refused = launcher("/nonexistent/bwrap").expect_err("it is missing");
    assert_eq!(
        refused.code(),
        if cfg!(target_os = "linux") {
            "sandbox_missing"
        } else {
            "sandbox_unsupported"
        }
    );
    // Never looked for by name, on `PATH` or in the working directory.
    for relative in ["bwrap", "./bwrap", ""] {
        let refused = launcher(relative).expect_err("it is no absolute path");
        assert!(matches!(
            refused,
            SandboxError::Missing { .. } | SandboxError::Unsupported
        ));
    }
    #[cfg(target_os = "linux")]
    {
        // Something that runs and makes no sandbox, as bubblewrap does where it may not.
        let refused = launcher("/usr/bin/false").expect_err("it makes no sandbox");
        assert!(
            matches!(&refused, SandboxError::Unavailable { path, .. } if path.as_os_str() == "/usr/bin/false")
        );
        assert_eq!(refused.code(), "sandbox_unavailable");
    }
}

#[test]
fn whether_a_launcher_makes_a_sandbox_is_tried_once() {
    let sandbox = Bubblewrap::at("/nonexistent/bwrap");
    let first = sandbox.usable();
    assert!(first.is_err());
    assert_eq!(sandbox.usable(), first);
    assert!(sandbox.usable.get().is_some());
}

#[cfg(target_os = "linux")]
#[test]
fn bubblewrap_where_it_runs_stops_its_connector_by_the_end_of_its_input() {
    let sandbox = Bubblewrap::new();
    let grants = Grants::default();
    match sandbox.launcher(&confined(&grants, &[], &[])) {
        Ok(launcher) => {
            assert_eq!(launcher.stops, Stops::ByInputEnd);
            assert_eq!(launcher.command.get_program(), "/usr/bin/bwrap");
            // The launcher's own environment is empty too.
            assert_eq!(launcher.command.get_envs().count(), 0);
        }
        Err(unusable) => {
            use std::io::Write as _;
            writeln!(std::io::stderr(), "skipped: {unusable}").ok();
        }
    }
}
