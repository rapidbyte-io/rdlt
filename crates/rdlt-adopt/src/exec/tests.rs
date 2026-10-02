use std::os::fd::AsRawFd as _;
use std::process::{Command, Stdio};

use super::{inheriting_below, mark_from};

/// The descriptors a shell started by `command` holds, as it lists them.
fn held(command: &mut Command) -> Vec<i32> {
    let output = command
        .args(["-c", "ls /dev/fd"])
        .stdout(Stdio::piped())
        .output()
        .expect("the shell runs");
    let listed = String::from_utf8_lossy(&output.stdout).into_owned();
    let mut held: Vec<i32> = listed
        .split_whitespace()
        .filter_map(|fd| fd.parse().ok())
        .collect();
    held.sort_unstable();
    held
}

/// A file opened without close-on-exec, as a library that knows nothing of the flag opens one,
/// at a number above any a shell opens for itself.
fn inheritable() -> std::os::fd::OwnedFd {
    use nix::fcntl::{FcntlArg, FdFlag, fcntl};
    let file = std::fs::File::open("/dev/null").expect("it opens");
    let high = fcntl(&file, FcntlArg::F_DUPFD(40)).expect("it is copied high");
    #[expect(unsafe_code, reason = "the copy fcntl made is owned here alone")]
    // SAFETY: `high` was just returned by `F_DUPFD`, and nothing else owns it.
    let high = unsafe { std::os::fd::FromRawFd::from_raw_fd(high) };
    fcntl(&high, FcntlArg::F_SETFD(FdFlag::empty())).expect("the flag clears");
    high
}

#[test]
fn a_child_inherits_no_descriptor_from_the_number_up() {
    let open = inheritable();
    let fd = open.as_raw_fd();
    let mut plain = Command::new("/bin/sh");
    assert!(
        held(&mut plain).contains(&fd),
        "the file is inherited without the hook"
    );
    let mut marked = Command::new("/bin/sh");
    inheriting_below(&mut marked, 3);
    let kept = held(&mut marked);
    assert!(!kept.contains(&fd), "{kept:?}");
    // The parent's own descriptor is as it was: inheritable, and open.
    let flags = nix::fcntl::fcntl(&open, nix::fcntl::FcntlArg::F_GETFD).expect("it is open");
    assert_eq!(flags & libc::FD_CLOEXEC, 0);
}

#[test]
fn a_descriptor_below_the_number_is_inherited_still() {
    let open = inheritable();
    let fd = open.as_raw_fd();
    let mut marked = Command::new("/bin/sh");
    inheriting_below(&mut marked, fd + 1);
    assert!(held(&mut marked).contains(&fd));
}

#[test]
fn marking_one_by_one_marks_what_marking_at_once_does() {
    let open = inheritable();
    let fd = open.as_raw_fd();
    let mut marked = Command::new("/bin/sh");
    #[expect(
        unsafe_code,
        reason = "the fallback runs in the child as the hook does"
    )]
    // SAFETY: as `inheriting_below`'s: only async-signal-safe calls, no allocation.
    unsafe {
        std::os::unix::process::CommandExt::pre_exec(&mut marked, || {
            super::marked_one_by_one(3);
            Ok(())
        });
    }
    assert!(!held(&mut marked).contains(&fd));
    // And in this process too, where it can be watched: then the flag is put back.
    mark_from(fd);
    let flags = nix::fcntl::fcntl(&open, nix::fcntl::FcntlArg::F_GETFD).expect("it is open");
    assert_ne!(flags & libc::FD_CLOEXEC, 0);
}
