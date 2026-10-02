use std::ffi::OsString;
use std::path::Path;

use super::{Bubblewrap, PROGRAM};
use crate::local::process::{GRANTS_FD, PROGRAM_FD, SOCKET_FD};
use crate::local::sandbox::{Bind, Confined, NetworkGrant, Sandbox, SandboxError};

fn confined<'a>(
    binds: &'a [Bind<'a>],
    network: NetworkGrant,
    env: &'a [(OsString, OsString)],
    args: &'a [OsString],
) -> Confined<'a> {
    Confined {
        program: PROGRAM_FD,
        args,
        env,
        socket: SOCKET_FD,
        binds,
        network,
    }
}

/// The arguments that confine a connector granted `binds` and `network`, as text.
fn arguments(binds: &[Bind<'_>], network: NetworkGrant) -> Vec<String> {
    let env = [(OsString::from("KEPT"), OsString::from("a value"))];
    let args = [OsString::from("--rdlt-fd=3")];
    let arguments = Bubblewrap::confinement(&confined(binds, network, &env, &args));
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
    let arguments = arguments(&[], NetworkGrant::Denied);
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
    // Each system directory as it is on the host: bound read only, or the link it is.
    for system in super::SYSTEM {
        match std::fs::symlink_metadata(system) {
            Ok(found) if found.is_symlink() => {
                let target = std::fs::read_link(system).expect("a link");
                let target = target.to_str().expect("text");
                assert!(
                    holds(&arguments, &["--symlink", target, system]),
                    "{system}"
                );
            }
            Ok(_) => assert!(
                holds(&arguments, &["--ro-bind", system, system]),
                "{system}"
            ),
            Err(_) => assert!(!arguments.iter().any(|argument| argument == system)),
        }
    }
    assert!(holds(&arguments, &["--proc", "/proc"]));
    assert!(holds(&arguments, &["--dev", "/dev"]));
    assert!(holds(&arguments, &["--tmpfs", "/tmp"]));
    assert!(holds(
        &arguments,
        &["--ro-bind-fd", &PROGRAM_FD.to_string(), PROGRAM]
    ));
    // Its own user namespace, in which it may make none.
    assert!(holds(&arguments, &["--unshare-user", "--disable-userns"]));
    // Neither the program nor its arguments are among what the descriptor carries.
    assert!(
        !arguments
            .iter()
            .any(|argument| argument == "--" || argument == "--rdlt-fd=3")
    );
    // Nothing of the host is bound to be written, and no home directory at all.
    assert!(
        !arguments
            .iter()
            .any(|argument| argument == "--bind" || argument == "--bind-fd")
    );
    assert!(
        !arguments
            .iter()
            .any(|argument| argument.starts_with("/home"))
    );
    assert!(!arguments.iter().any(|argument| argument == "--dev-bind"));
}

#[test]
fn what_is_granted_is_bound_from_its_descriptor_and_nothing_else_of_the_hosts() {
    let binds = [
        Bind {
            fd: GRANTS_FD,
            at: Path::new("/granted/read"),
            write: false,
        },
        Bind {
            fd: GRANTS_FD + 1,
            at: Path::new("/granted/write"),
            write: true,
        },
    ];
    let arguments = arguments(&binds, NetworkGrant::Granted);
    let (read, write) = (GRANTS_FD.to_string(), (GRANTS_FD + 1).to_string());
    assert!(holds(&arguments, &["--ro-bind-fd", &read, "/granted/read"]));
    assert!(holds(&arguments, &["--bind-fd", &write, "/granted/write"]));
    // Never by a path, which bubblewrap would resolve again.
    for by_path in ["--bind", "--ro-bind"] {
        assert!(!holds(&arguments, &[by_path, "/granted/read"]), "{by_path}");
        assert!(
            !holds(&arguments, &[by_path, "/granted/write"]),
            "{by_path}"
        );
    }
    assert!(holds(&arguments, &["--unshare-all", "--share-net"]));
}

#[test]
fn a_launcher_that_is_not_there_or_makes_no_sandbox_is_refused_with_its_reason() {
    let asked = confined(&[], NetworkGrant::Denied, &[], &[]);
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
    match sandbox.launcher(&confined(&[], NetworkGrant::Denied, &[], &[])) {
        Ok(launcher) => {
            assert_eq!(launcher.stops, crate::local::sandbox::Stops::ByInputEnd);
            let program = launcher
                .command
                .get_program()
                .to_string_lossy()
                .into_owned();
            assert!(program.starts_with("/proc/self/fd/"), "{program}");
            // The launcher's own environment is empty too.
            assert_eq!(launcher.command.get_envs().count(), 0);
        }
        Err(unusable) => rdlt_testkit::process::without_sandbox(&unusable),
    }
}

#[cfg(target_os = "linux")]
#[test]
fn no_argument_and_no_value_of_the_environment_is_on_the_launchers_command_line() {
    let sandbox = Bubblewrap::new();
    if let Err(unusable) = sandbox.usable() {
        rdlt_testkit::process::without_sandbox(&unusable);
        return;
    }
    let env = [(OsString::from("SECRET"), OsString::from("hunter2-in-env"))];
    let args = [OsString::from("--rdlt-fd=3")];
    let launcher = sandbox
        .launcher(&confined(&[], NetworkGrant::Denied, &env, &args))
        .expect("a launcher");
    let line: Vec<String> = std::iter::once(launcher.command.get_program())
        .chain(launcher.command.get_args())
        .map(|argument| argument.to_string_lossy().into_owned())
        .collect();
    assert!(line[0].starts_with("/proc/self/fd/"), "{line:?}");
    assert_eq!(&line[1..], ["--args", "5", "--", PROGRAM, "--rdlt-fd=3"]);
    assert_eq!(launcher.command.get_envs().count(), 0);
    // What it is given instead: every argument, each ended by a NUL, in a file of its own.
    let [(file, at)] = <[_; 1]>::try_from(launcher.given).expect("one descriptor");
    assert_eq!(at, 5);
    let mut read = String::new();
    std::io::Read::read_to_string(&mut std::fs::File::from(file), &mut read).expect("it reads");
    assert!(
        read.contains("--setenv\0SECRET\0hunter2-in-env\0"),
        "{read:?}"
    );
    assert!(read.ends_with('\0'));
    assert_eq!(launcher.held.len(), 1);
}

#[cfg(target_os = "linux")]
#[test]
fn a_launcher_another_user_may_change_is_refused_and_every_launcher_is_guarded() {
    use std::os::unix::fs::PermissionsExt as _;
    let root = tempfile::tempdir().expect("a temporary directory");
    let copy = root.path().join("bwrap");
    std::fs::copy("/usr/bin/bwrap", &copy).ok();
    if !copy.exists() {
        rdlt_testkit::process::without_sandbox(&"no /usr/bin/bwrap to copy");
        return;
    }
    std::fs::set_permissions(&copy, std::fs::Permissions::from_mode(0o775)).expect("a mode");
    let refused = Bubblewrap::at(&copy)
        .usable()
        .expect_err("another user may write it");
    assert_eq!(refused.code(), "sandbox_launcher_shared");
    // Whoever may write the launcher decides what confines every connector.
    assert_eq!(Bubblewrap::at(&copy).programs(), [copy]);
}

#[cfg(target_os = "linux")]
#[test]
fn the_launcher_runs_from_a_descriptor_above_every_one_it_is_given() {
    let sandbox = Bubblewrap::new();
    if let Err(unusable) = sandbox.usable() {
        rdlt_testkit::process::without_sandbox(&unusable);
        return;
    }
    let at = Path::new("/granted");
    let binds: Vec<Bind<'_>> = (GRANTS_FD..GRANTS_FD + 6)
        .map(|fd| Bind {
            fd,
            at,
            write: false,
        })
        .collect();
    let launcher = sandbox
        .launcher(&confined(&binds, NetworkGrant::Denied, &[], &[]))
        .expect("a launcher");
    let program = launcher
        .command
        .get_program()
        .to_string_lossy()
        .into_owned();
    let executed: i32 = program
        .strip_prefix("/proc/self/fd/")
        .and_then(|fd| fd.parse().ok())
        .expect("a descriptor");
    assert!(executed > GRANTS_FD + 5, "{executed}");
}

#[cfg(target_os = "linux")]
#[test]
fn a_step_the_system_refuses_is_kept_as_the_cause_not_as_text() {
    let sandbox = Bubblewrap::new();
    if let Err(unusable) = sandbox.usable() {
        rdlt_testkit::process::without_sandbox(&unusable);
        return;
    }
    // A value the launcher's file of arguments cannot hold.
    let env = [(OsString::from("HELD"), OsString::from("a\0b"))];
    let refused = sandbox
        .launcher(&confined(&[], NetworkGrant::Denied, &env, &[]))
        .expect_err("refused");
    assert_eq!(refused.code(), "sandbox_failed");
    let cause = std::error::Error::source(&refused).expect("a cause");
    let os = cause
        .downcast_ref::<crate::local::sandbox::OsError>()
        .expect("the system's error, kept whole");
    assert_eq!(os.io().kind(), std::io::ErrorKind::InvalidInput);
}
