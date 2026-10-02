use std::path::{Path, PathBuf};

use super::{Chain, Claim, Guarded, Lease, Leases, Opened, Program, Roots};
use crate::local::sandbox::{Grants, SandboxError};

fn dir(root: &Path, name: &str) -> PathBuf {
    let path = root.join(name);
    std::fs::create_dir_all(&path).expect("a directory");
    path
}

fn file(path: &Path) -> PathBuf {
    std::fs::write(path, "x").expect("it writes");
    path.to_owned()
}

/// What a test asks a placement to hold: grants, the roots they may lie in, what is guarded,
/// and the programs it runs.
#[derive(Default)]
struct Asked {
    grants: Grants,
    shared_reads: Vec<PathBuf>,
    roots: Roots,
    guarded: Guarded,
    programs: Vec<PathBuf>,
}

impl Asked {
    /// Grants under `root` alone, which may be written.
    fn under(root: &Path) -> Self {
        Self {
            roots: Roots {
                read: Vec::new(),
                write: vec![root.to_owned()],
            },
            ..Self::default()
        }
    }

    fn writing(mut self, path: &Path) -> Self {
        self.grants.write.push(path.to_owned());
        self
    }

    fn reading(mut self, path: &Path) -> Self {
        self.grants.read.push(path.to_owned());
        self
    }

    fn shared(mut self) -> Self {
        self.grants.shared = true;
        self
    }

    fn running(mut self, program: &Path) -> Self {
        self.programs.push(program.to_owned());
        self
    }

    fn taken(&self, leases: &Leases) -> Result<Lease, SandboxError> {
        let programs: Vec<Program> = self
            .programs
            .iter()
            .map(|path| Program {
                path: path.clone(),
                chain: Opened::at(path).expect("the program is there").chain,
            })
            .collect();
        leases.take(&Claim {
            grants: &self.grants,
            shared_reads: &self.shared_reads,
            roots: &self.roots,
            guarded: &self.guarded,
            programs: &programs,
        })
    }
}

/// The code of the error `asked` is refused with.
fn refused(leases: &Leases, asked: &Asked) -> &'static str {
    asked.taken(leases).expect_err("refused").code()
}

#[test]
fn grants_of_two_connectors_that_do_not_overlap_are_both_held() {
    let root = tempfile::tempdir().expect("a temporary directory");
    let (a, b) = (dir(root.path(), "a"), dir(root.path(), "b"));
    let leases = Leases::default();
    let under = || Asked::under(root.path());
    let held_a = under().writing(&a).taken(&leases).expect("a's grant");
    let held_b = under().writing(&b).taken(&leases).expect("b's grant");
    // A read of what another writes is refused; reads of what nobody writes overlap freely.
    assert_eq!(
        refused(&leases, &under().reading(root.path())),
        "grant_overlap"
    );
    drop((held_a, held_b));
    let first = under().reading(root.path()).taken(&leases).expect("a read");
    let second = under()
        .reading(root.path())
        .taken(&leases)
        .expect("another");
    drop((first, second));
}

#[test]
fn a_grant_overlapping_one_held_is_refused_until_it_is_released() {
    let root = tempfile::tempdir().expect("a temporary directory");
    let (a, inner) = (dir(root.path(), "a"), dir(root.path(), "a/inner"));
    let leases = Leases::default();
    let under = || Asked::under(root.path());
    let held = under().writing(&a).taken(&leases).expect("a's grant");
    for (asked, overlapping) in [
        (under().writing(&a), &a),
        (under().writing(&inner), &inner),
        (under().writing(root.path()), &root.path().to_owned()),
        (under().reading(&inner), &inner),
    ] {
        let refused = asked.taken(&leases).expect_err("it overlaps");
        let path = overlapping.clone();
        assert_eq!(refused, SandboxError::Overlap { path });
        assert_eq!(refused.code(), "grant_overlap");
    }
    drop(held);
    let _again = under()
        .writing(root.path())
        .taken(&leases)
        .expect("released, it is free");
}

#[test]
fn grants_that_are_both_shared_may_overlap_and_no_other() {
    let root = tempfile::tempdir().expect("a temporary directory");
    let a = dir(root.path(), "a");
    let leases = Leases::default();
    let under = || Asked::under(root.path());
    let _shared = under().writing(&a).shared().taken(&leases).expect("shared");
    let _also = under().writing(&a).shared().taken(&leases).expect("shared");
    assert_eq!(refused(&leases, &under().writing(&a)), "grant_overlap");
    // Every clone holds the same leases, as every provider of the process does.
    assert_eq!(
        refused(&leases.clone(), &under().writing(&a)),
        "grant_overlap"
    );
}

#[test]
fn the_process_has_one_registry_of_leases() {
    let root = tempfile::tempdir().expect("a temporary directory");
    let held = Asked::under(root.path())
        .writing(root.path())
        .taken(&Leases::process())
        .expect("held");
    let again = Asked::under(root.path()).writing(root.path());
    assert_eq!(refused(&Leases::process(), &again), "grant_overlap");
    drop(held);
}

#[test]
fn a_grant_or_a_root_that_is_relative_or_absent_is_refused() {
    let root = tempfile::tempdir().expect("a temporary directory");
    let leases = Leases::default();
    for path in ["relative", "/nonexistent/granted", ""] {
        let path = Path::new(path);
        let asked = Asked::under(root.path()).writing(path);
        assert_eq!(
            refused(&leases, &asked),
            "sandbox_grant",
            "{}",
            path.display()
        );
        assert_eq!(refused(&leases, &Asked::under(path)), "sandbox_grant");
        let mut shared = Asked::under(root.path());
        shared.shared_reads.push(path.to_owned());
        assert_eq!(refused(&leases, &shared), "sandbox_grant");
    }
}

#[test]
fn a_grant_lies_within_a_root_its_operator_named_that_lets_it_do_what_it_asks() {
    let root = tempfile::tempdir().expect("a temporary directory");
    let (readable, writable, other) = (
        dir(root.path(), "readable"),
        dir(root.path(), "writable"),
        dir(root.path(), "other"),
    );
    let leases = Leases::default();
    let roots = || Asked {
        roots: Roots {
            read: vec![readable.clone()],
            write: vec![writable.clone()],
        },
        ..Asked::default()
    };
    let inner = dir(&writable, "inner");
    for asked in [
        roots().reading(&readable),
        roots().reading(&dir(&readable, "inner")),
        // What may be written may be read.
        roots().reading(&inner),
        roots().writing(&inner),
    ] {
        drop(asked.taken(&leases).expect("within"));
    }
    for asked in [
        roots().reading(&other),
        roots().reading(root.path()),
        roots().writing(&readable),
        roots().writing(&other),
        // Where no root is named, nothing is granted.
        Asked::default().reading(&readable),
        Asked::default().writing(&inner),
    ] {
        assert_eq!(refused(&leases, &asked), "grant_outside");
    }
    let refused = roots().writing(&other).taken(&leases).expect_err("outside");
    assert_eq!(refused, SandboxError::Outside { path: other });
}

#[test]
fn a_root_that_may_be_written_holds_nothing_that_decides_what_the_host_runs_or_keeps() {
    let root = tempfile::tempdir().expect("a temporary directory");
    let held = dir(root.path(), "held");
    let leases = Leases::default();
    let guarding = |directories: Vec<PathBuf>, files: Vec<PathBuf>| Asked {
        guarded: Guarded { directories, files },
        ..Asked::under(&held)
    };
    let binary = file(&dir(&held, "bin").join("rdlt-connector-x"));
    let outside = file(&root.path().join("program"));
    for (asked, what) in [
        (
            guarding(vec![dir(&held, "connectors")], vec![]),
            "a directory within",
        ),
        (
            guarding(vec![root.path().to_owned()], vec![]),
            "a directory above",
        ),
        (guarding(vec![held.clone()], vec![]), "the directory itself"),
        (
            guarding(vec![held.join("absent/state")], vec![]),
            "an absent directory",
        ),
        (
            guarding(vec![], vec![file(&held.join("bwrap"))]),
            "a launcher",
        ),
        (
            guarding(vec![], vec![held.join("absent")]),
            "an absent launcher",
        ),
        (guarding(vec![], vec![]).running(&binary), "a binary within"),
        (
            guarding(vec![], vec![]).running(&held.join("bin")),
            "a program's directory",
        ),
    ] {
        let refused = asked.taken(&leases).expect_err(what);
        assert_eq!(
            refused,
            SandboxError::Guarded { path: held.clone() },
            "{what}"
        );
        assert_eq!(refused.code(), "grant_root_guarded");
    }
    // Beside it, and in roots that may only be read, they are no root's.
    let beside = dir(root.path(), "beside");
    let unguarded = guarding(vec![beside.join("absent"), dir(&beside, "state")], vec![]);
    drop(unguarded.running(&outside).taken(&leases).expect("beside"));
    let reading = Asked {
        roots: Roots {
            read: vec![held.clone()],
            write: Vec::new(),
        },
        guarded: Guarded {
            directories: vec![held.join("connectors")],
            files: vec![held.join("bwrap")],
        },
        ..Asked::default()
    };
    drop(reading.running(&binary).taken(&leases).expect("read only"));
}

#[test]
fn a_grant_to_write_what_every_connector_reads_is_refused() {
    let root = tempfile::tempdir().expect("a temporary directory");
    let (read, inner) = (dir(root.path(), "read"), dir(root.path(), "read/inner"));
    let leases = Leases::default();
    for (written, by_all) in [(&inner, &read), (&read, &inner), (&read, &read)] {
        let mut asked = Asked::under(root.path()).writing(written);
        asked.shared_reads.push(by_all.clone());
        assert_eq!(refused(&leases, &asked), "grant_overlap");
    }
    let mut beside = Asked::under(root.path()).writing(&dir(root.path(), "beside"));
    beside.shared_reads.push(read);
    drop(beside.taken(&leases).expect("beside what all read"));
}

#[test]
fn a_file_linked_twice_is_one_grant_whichever_name_grants_it() {
    let root = tempfile::tempdir().expect("a temporary directory");
    let (a, b) = (dir(root.path(), "a"), dir(root.path(), "b"));
    let first = file(&a.join("table"));
    let second = b.join("table");
    std::fs::hard_link(&first, &second).expect("a second link");
    let leases = Leases::default();
    let _held = Asked::under(root.path())
        .writing(&first)
        .taken(&leases)
        .expect("held");
    let asked = Asked::under(root.path()).writing(&second);
    assert_eq!(refused(&leases, &asked), "grant_overlap");
}

#[test]
fn no_grant_may_write_a_program_another_placement_runs_nor_a_placement_run_one_written() {
    let root = tempfile::tempdir().expect("a temporary directory");
    let (bin, data) = (dir(root.path(), "bin"), dir(root.path(), "data"));
    let program = file(&bin.join("rdlt-connector-x"));
    let leases = Leases::default();
    // A placement runs its program; another's grant over it is refused, shared or not.
    let running = Asked::default()
        .running(&program)
        .taken(&leases)
        .expect("runs");
    for asked in [
        Asked::under(root.path()).writing(&bin),
        Asked::under(root.path()).writing(&bin).shared(),
    ] {
        let refused = asked.taken(&leases).expect_err("it covers");
        assert_eq!(refused, SandboxError::Covers { path: bin.clone() });
        assert_eq!(refused.code(), "grant_covers");
    }
    drop(
        Asked::under(root.path())
            .reading(&bin)
            .taken(&leases)
            .expect("a read"),
    );
    drop(running);
    // A placement whose program lies where a grant held now may write is refused.
    let writing = Asked::under(root.path()).writing(&data).taken(&leases);
    let writing = writing.expect("held");
    let exposed = file(&data.join("rdlt-connector-y"));
    let refused = Asked::default()
        .running(&exposed)
        .taken(&leases)
        .expect_err("exposed");
    assert_eq!(refused, SandboxError::Exposed { path: exposed });
    assert_eq!(refused.code(), "program_exposed");
    drop(
        Asked::default()
            .running(&program)
            .taken(&leases)
            .expect("elsewhere"),
    );
    drop(writing);
}

#[cfg(target_os = "linux")]
#[test]
fn a_grant_through_a_link_is_held_and_bound_as_what_it_led_to_when_it_was_taken() {
    use std::os::unix::fs::MetadataExt as _;
    let root = tempfile::tempdir().expect("a temporary directory");
    let (a, b, c) = (
        dir(root.path(), "a"),
        dir(root.path(), "b"),
        dir(root.path(), "c"),
    );
    let link = a.join("link");
    std::os::unix::fs::symlink(&b, &link).expect("a link");
    let leases = Leases::default();
    let held = Asked::under(root.path())
        .writing(&link)
        .taken(&leases)
        .expect("held as b");
    // What it led to is held: b, by any name, and not a, where the link is.
    assert_eq!(
        refused(&leases, &Asked::under(root.path()).writing(&b)),
        "grant_overlap"
    );
    drop(
        Asked::under(root.path())
            .writing(&a)
            .taken(&leases)
            .expect("a is free"),
    );
    // Retargeted, the link changes nothing of what is bound.
    std::fs::remove_file(&link).expect("unlinked");
    std::os::unix::fs::symlink(&c, &link).expect("retargeted");
    let [bound] = held.bound.as_slice() else {
        panic!("{:?}", held.bound);
    };
    assert!(bound.write);
    assert_eq!(bound.at, link);
    let identity = |metadata: std::fs::Metadata| (metadata.dev(), metadata.ino());
    let bound = identity(bound.file.metadata().expect("it is open"));
    assert_eq!(bound, identity(std::fs::metadata(&b).expect("b is there")));
}

#[test]
fn what_is_bound_reads_first_every_connectors_reads_before_its_own() {
    let root = tempfile::tempdir().expect("a temporary directory");
    let (by_all, read, write) = (
        dir(root.path(), "all"),
        dir(root.path(), "read"),
        dir(root.path(), "write"),
    );
    let mut asked = Asked::under(root.path()).writing(&write).reading(&read);
    asked.shared_reads.push(by_all.clone());
    let held = asked.taken(&Leases::default()).expect("held");
    let bound: Vec<(PathBuf, bool)> = held
        .bound
        .iter()
        .map(|bound| (bound.at.clone(), bound.write))
        .collect();
    assert_eq!(bound, [(by_all, false), (read, false), (write, true)]);
}

#[test]
fn a_chain_knows_what_lies_within_what() {
    let root = tempfile::tempdir().expect("a temporary directory");
    let inner = dir(root.path(), "a/inner");
    let chain = |path: &Path| Opened::at(path).expect("there").chain;
    let (top, a, inner, beside) = (
        chain(root.path()),
        chain(&root.path().join("a")),
        chain(&inner),
        chain(&dir(root.path(), "b")),
    );
    assert!(inner.within(&a) && inner.within(&top) && a.within(&a));
    assert!(!a.within(&inner) && !beside.within(&a));
    assert!(a.overlaps(&inner) && inner.overlaps(&a) && !beside.overlaps(&a));
    assert_eq!(inner.parent(), Some(a));
    assert_eq!(chain(Path::new("/")).parent(), None::<Chain>);
}
