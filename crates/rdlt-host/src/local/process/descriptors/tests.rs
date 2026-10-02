use std::os::fd::AsRawFd as _;

use command_fds::FdMapping;

use super::mappings;

#[cfg(target_os = "linux")]
#[test]
fn a_descriptors_flags_say_whether_it_is_closed_on_exec() {
    use super::closed_on_exec;
    assert!(closed_on_exec("pos:\t0\nflags:\t02100000\nmnt_id:\t29\n"));
    assert!(closed_on_exec("flags:\t02000000\n"));
    assert!(!closed_on_exec("pos:\t0\nflags:\t0100000\nmnt_id:\t29\n"));
    assert!(!closed_on_exec("flags:\t00\n"));
    // What cannot be read is taken to be inherited.
    for unread in ["", "pos:\t0\n", "flags:\n", "flags:\tx\n", "flags:\t9\n"] {
        assert!(!closed_on_exec(unread), "{unread:?}");
    }
}

/// The descriptors a child is given beside `given`.
fn covered(given: Vec<FdMapping>) -> Vec<i32> {
    let taken: Vec<i32> = given.iter().map(|mapping| mapping.child_fd).collect();
    let mappings = mappings(given).expect("the descriptors are listed");
    let children = mappings.iter().map(|mapping| mapping.child_fd);
    children.filter(|fd| !taken.contains(fd)).collect()
}

#[cfg(target_os = "linux")]
#[test]
fn a_descriptor_a_child_would_inherit_is_covered_and_one_closed_on_exec_is_not() {
    let closed = std::fs::File::open("/dev/null").expect("it opens");
    let inherited = std::fs::File::open("/dev/zero").expect("it opens");
    assert!(!covered(Vec::new()).contains(&inherited.as_raw_fd()));
    rustix::io::fcntl_setfd(&inherited, rustix::io::FdFlags::empty()).expect("the flag clears");
    let covers = covered(Vec::new());
    assert!(covers.contains(&inherited.as_raw_fd()), "{covers:?}");
    assert!(!covers.contains(&closed.as_raw_fd()), "{covers:?}");
    // The standard streams are the command's own to give.
    assert!(covers.iter().all(|fd| *fd > 2), "{covers:?}");
    // What the connector is given at a descriptor replaces what the host holds there.
    let given = FdMapping {
        parent_fd: closed.try_clone().expect("it is copied").into(),
        child_fd: inherited.as_raw_fd(),
    };
    assert!(!covered(vec![given]).contains(&inherited.as_raw_fd()));
    // Each cover is the null device, a descriptor of its own.
    let covering = mappings(Vec::new()).expect("the descriptors are listed");
    for mapping in covering {
        let target = std::fs::read_link(format!("/proc/self/fd/{}", mapping.parent_fd.as_raw_fd()));
        assert_eq!(
            target.expect("it is open"),
            std::path::Path::new("/dev/null")
        );
        assert_ne!(mapping.parent_fd.as_raw_fd(), mapping.child_fd);
    }
}

#[test]
fn what_is_given_is_given_as_it_was() {
    let file = std::fs::File::open("/dev/null").expect("it opens");
    let given = FdMapping {
        parent_fd: file.try_clone().expect("it is copied").into(),
        child_fd: 3,
    };
    let raw = given.parent_fd.as_raw_fd();
    let mappings = mappings(vec![given]).expect("the descriptors are listed");
    assert_eq!(
        (mappings[0].parent_fd.as_raw_fd(), mappings[0].child_fd),
        (raw, 3)
    );
}
