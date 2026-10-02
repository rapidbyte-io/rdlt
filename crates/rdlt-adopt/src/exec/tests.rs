use std::cell::RefCell;
use std::os::fd::{AsRawFd as _, OwnedFd, RawFd};
use std::process::{Command, Stdio};

use super::{Marking, ONE_BY_ONE_CAP, inheriting_only, mark_except, mark_range, marked_one_by_one};

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
/// at `at` or the first number above it that is free, above any a shell opens for itself.
fn inheritable(at: RawFd) -> OwnedFd {
    inheritable_within_limit(at).expect("it is copied high")
}

/// As [`inheritable`], where the process's limit lets a descriptor be numbered `at`.
fn inheritable_within_limit(at: RawFd) -> Option<OwnedFd> {
    use nix::fcntl::{FcntlArg, FdFlag, fcntl};
    let file = std::fs::File::open("/dev/null").expect("it opens");
    let high = fcntl(&file, FcntlArg::F_DUPFD(at)).ok()?;
    #[expect(unsafe_code, reason = "the copy fcntl made is owned here alone")]
    // SAFETY: `high` was just returned by `F_DUPFD`, and nothing else owns it.
    let high: OwnedFd = unsafe { std::os::fd::FromRawFd::from_raw_fd(high) };
    fcntl(&high, FcntlArg::F_SETFD(FdFlag::empty())).expect("the flag clears");
    Some(high)
}

/// Whether `fd` is close-on-exec.
fn close_on_exec(fd: &OwnedFd) -> bool {
    let flags = nix::fcntl::fcntl(fd, nix::fcntl::FcntlArg::F_GETFD).expect("it is open");
    flags & libc::FD_CLOEXEC != 0
}

/// Three inheritable files, numbered in order: one below a given one, the given one, and one
/// above it.
fn three() -> (OwnedFd, OwnedFd, OwnedFd) {
    let below = inheritable(40);
    let given = inheritable(50);
    let above = inheritable(60);
    assert!(below.as_raw_fd() < given.as_raw_fd() && given.as_raw_fd() < above.as_raw_fd());
    (below, given, above)
}

#[test]
fn a_child_inherits_the_descriptors_it_is_given_and_none_below_or_above_them() {
    let (below, given, above) = three();
    let all = [below.as_raw_fd(), given.as_raw_fd(), above.as_raw_fd()];
    let plain = held(&mut Command::new("/bin/sh"));
    assert!(
        all.iter().all(|fd| plain.contains(fd)),
        "the files are inherited without the hook: {plain:?}"
    );
    // Only Linux marks a range at once: elsewhere a spawn that needs it fails.
    let markings: &[Marking] = if cfg!(target_os = "linux") {
        &[Marking::AtOnce, Marking::OrOneByOne]
    } else {
        &[Marking::OrOneByOne]
    };
    for &marking in markings {
        let mut marked = Command::new("/bin/sh");
        inheriting_only(&mut marked, &[given.as_raw_fd(), 1], marking);
        let kept = held(&mut marked);
        assert!(kept.contains(&given.as_raw_fd()), "{marking:?}: {kept:?}");
        assert!(!kept.contains(&below.as_raw_fd()), "{marking:?}: {kept:?}");
        assert!(!kept.contains(&above.as_raw_fd()), "{marking:?}: {kept:?}");
        // The standard streams are not marked.
        assert!([0, 1, 2].iter().all(|fd| kept.contains(fd)), "{kept:?}");
    }
    // The parent's own descriptors are as they were: inheritable, and open.
    assert!(!close_on_exec(&below) && !close_on_exec(&above));
}

#[test]
fn marking_one_by_one_in_a_child_leaves_what_marking_at_once_does() {
    let (below, given, above) = three();
    let kept = [given.as_raw_fd()];
    let mut marked = Command::new("/bin/sh");
    #[expect(
        unsafe_code,
        reason = "the fallback runs in the child as the hook does"
    )]
    // SAFETY: as `inheriting_only`'s: plain system calls, no lock and no allocation.
    unsafe {
        std::os::unix::process::CommandExt::pre_exec(&mut marked, move || {
            mark_except(&kept, Marking::OrOneByOne, |_, _| false)
        });
    }
    let held = held(&mut marked);
    assert!(held.contains(&given.as_raw_fd()), "{held:?}");
    assert!(!held.contains(&below.as_raw_fd()), "{held:?}");
    assert!(!held.contains(&above.as_raw_fd()), "{held:?}");
}

thread_local! {
    /// The ranges a test's marking was asked for.
    static ASKED: RefCell<Vec<(RawFd, RawFd)>> = const { RefCell::new(Vec::new()) };
}

/// Records each range it is asked to mark, and marks none.
fn recorded(first: RawFd, last: RawFd) -> bool {
    ASKED.with_borrow_mut(|asked| asked.push((first, last)));
    true
}

#[test]
fn every_descriptor_from_three_up_but_those_given_is_marked() {
    let max = RawFd::MAX;
    for (kept, ranges) in [
        (vec![], vec![(3, max)]),
        (vec![3], vec![(4, max)]),
        (vec![3, 4, 5, 10], vec![(6, 9), (11, max)]),
        (vec![7], vec![(3, 6), (8, max)]),
        (vec![4, max], vec![(3, 3), (5, max - 1)]),
        // A standard stream among those given changes nothing.
        (vec![0, 1, 2, 7], vec![(3, 6), (8, max)]),
    ] {
        ASKED.with_borrow_mut(Vec::clear);
        mark_except(&kept, Marking::AtOnce, recorded).expect("marked");
        assert_eq!(ASKED.with_borrow(Clone::clone), ranges, "{kept:?}");
    }
}

#[test]
fn where_a_range_cannot_be_marked_at_once_the_spawn_fails_or_each_is_marked_as_asked() {
    let open = inheritable(40);
    let fd = open.as_raw_fd();
    let refused = mark_range(fd, fd, Marking::AtOnce, |_, _| false);
    assert!(refused.is_err());
    assert!(!close_on_exec(&open), "nothing was marked");
    mark_range(fd, fd, Marking::OrOneByOne, |_, _| false).expect("marked one by one");
    assert!(close_on_exec(&open));
    let refused = mark_except(&[], Marking::AtOnce, |_, _| false);
    assert!(refused.is_err());
}

#[test]
fn the_loop_marks_nothing_from_its_cap_up() {
    let Some(beyond) = inheritable_within_limit(ONE_BY_ONE_CAP) else {
        eprintln!("the soft limit is within the cap: no descriptor above it to leave");
        return;
    };
    let at = beyond.as_raw_fd();
    marked_one_by_one(at, at);
    assert!(!close_on_exec(&beyond), "{at} is above the cap");
    // A descriptor marked already stays marked.
    let marked = std::fs::File::open("/dev/null").expect("it opens");
    let fd = marked.as_raw_fd();
    marked_one_by_one(fd, fd);
    let flags = nix::fcntl::fcntl(&marked, nix::fcntl::FcntlArg::F_GETFD).expect("it is open");
    assert_eq!(flags, libc::FD_CLOEXEC);
    // Below the cap, a descriptor above the lowest common limit is marked too.
    for at in [40, 5000] {
        let within = inheritable_within_limit(at).expect("within the limit");
        marked_one_by_one(within.as_raw_fd(), within.as_raw_fd());
        assert!(close_on_exec(&within), "{at}");
    }
}

#[cfg(target_os = "linux")]
#[test]
fn this_kernel_marks_a_range_at_once() {
    assert!(super::marks_at_once());
}
