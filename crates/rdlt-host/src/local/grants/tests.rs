use std::path::{Path, PathBuf};

use super::{Leases, guarding};
use crate::local::sandbox::{Grants, SandboxError};

fn dir(root: &Path, name: &str) -> PathBuf {
    let path = root.join(name);
    std::fs::create_dir_all(&path).expect("a directory");
    std::fs::canonicalize(path).expect("it resolves")
}

fn writing(paths: &[&Path], shared: bool) -> Grants {
    Grants {
        write: paths.iter().map(|path| path.to_path_buf()).collect(),
        shared,
        ..Grants::default()
    }
}

fn reading(paths: &[&Path]) -> Grants {
    Grants {
        read: paths.iter().map(|path| path.to_path_buf()).collect(),
        ..Grants::default()
    }
}

#[test]
fn grants_of_two_connectors_that_do_not_overlap_are_both_held() {
    let root = tempfile::tempdir().expect("a temporary directory");
    let (a, b) = (dir(root.path(), "a"), dir(root.path(), "b"));
    let leases = Leases::default();
    let held_a = leases.take(&writing(&[&a], false)).expect("a's grant");
    let held_b = leases.take(&writing(&[&b], false)).expect("b's grant");
    // A read of what another writes is refused; reads of what nobody writes overlap freely.
    assert!(leases.take(&reading(&[root.path()])).is_err());
    drop((held_a, held_b));
    let first = leases.take(&reading(&[root.path()])).expect("a read");
    let second = leases.take(&reading(&[root.path()])).expect("another read");
    drop((first, second));
}

#[test]
fn a_grant_overlapping_one_held_is_refused_until_it_is_released() {
    let root = tempfile::tempdir().expect("a temporary directory");
    let (a, inner) = (dir(root.path(), "a"), dir(root.path(), "a/inner"));
    let leases = Leases::default();
    let held = leases.take(&writing(&[&a], false)).expect("a's grant");
    for (grants, overlapping) in [
        (writing(&[&a], false), &a),
        (writing(&[&inner], false), &inner),
        (
            writing(&[root.path()], false),
            &root.path().canonicalize().expect("it resolves"),
        ),
        (reading(&[&inner]), &inner),
    ] {
        let refused = leases.take(&grants).expect_err("it overlaps");
        assert_eq!(
            refused,
            SandboxError::Overlap {
                path: overlapping.clone()
            }
        );
        assert_eq!(refused.code(), "grant_overlap");
    }
    drop(held);
    let _again = leases
        .take(&writing(&[root.path()], false))
        .expect("released, it is free");
}

#[test]
fn grants_that_are_both_shared_may_overlap_and_no_other() {
    let root = tempfile::tempdir().expect("a temporary directory");
    let a = dir(root.path(), "a");
    let leases = Leases::default();
    let _shared = leases.take(&writing(&[&a], true)).expect("a shared grant");
    let _also = leases
        .take(&writing(&[&a], true))
        .expect("another shared grant");
    assert!(leases.take(&writing(&[&a], false)).is_err());
    // A clone of the provider holds the same leases.
    assert!(leases.clone().take(&writing(&[&a], false)).is_err());
}

#[test]
fn a_grant_of_a_path_that_is_relative_or_absent_is_refused() {
    let leases = Leases::default();
    for path in ["relative", "/nonexistent/granted"] {
        let refused = leases
            .take(&writing(&[Path::new(path)], false))
            .expect_err("refused");
        assert_eq!(refused.code(), "sandbox_grant", "{path}");
    }
}

#[test]
fn a_write_grant_may_not_hold_a_program_the_host_runs_or_touch_a_connector_directory() {
    let root = tempfile::tempdir().expect("a temporary directory");
    let (bin, data, inner) = (
        dir(root.path(), "bin"),
        dir(root.path(), "data"),
        dir(root.path(), "bin/inner"),
    );
    let program = bin.join("rdlt-connector-x");
    std::fs::write(&program, "x").expect("it writes");
    let launcher = data.join("bwrap");
    std::fs::write(&launcher, "x").expect("it writes");
    let leases = Leases::default();
    let directories = [bin.clone()];
    for (written, programs) in [
        (&bin, vec![program.as_path()]),
        (&inner, vec![]),
        (&root.path().canonicalize().expect("it resolves"), vec![]),
        (&data, vec![launcher.as_path()]),
    ] {
        let lease = leases.take(&writing(&[written], true)).expect("held");
        let refused = guarding(&lease, &programs, &directories).expect_err("it covers");
        assert_eq!(refused.code(), "grant_covers", "{}", written.display());
    }
    let lease = leases.take(&writing(&[&data], true)).expect("held");
    guarding(&lease, &[program.as_path()], &directories).expect("data holds no program");
}
