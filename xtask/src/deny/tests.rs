use std::ffi::OsString;
use std::path::Path;

use super::runs;
use crate::workspaces::MANIFESTS;

// Each workspace is checked as its lockfile has it, against the repository's one configuration;
// only the main workspace, which builds the bans' wrappers, reports a wrapper it never meets.
#[test]
fn every_workspace_is_checked_as_locked_against_one_configuration() {
    let root = Path::new("/repository");
    let runs = runs(root);
    assert_eq!(runs.len(), MANIFESTS.len());
    for (run, manifest) in runs.iter().zip(MANIFESTS) {
        let mut expected: Vec<OsString> = vec![
            "deny".into(),
            "--locked".into(),
            "--manifest-path".into(),
            root.join(manifest).into_os_string(),
            "--config".into(),
            root.join("deny.toml").into_os_string(),
            "check".into(),
        ];
        if *manifest == "fuzz/Cargo.toml" {
            expected.extend(["--allow".into(), "unused-wrapper".into()]);
        }
        assert_eq!(run, &expected, "{manifest}");
    }
}

#[test]
fn the_fuzzing_workspace_is_among_those_checked() {
    assert!(MANIFESTS.contains(&"Cargo.toml") && MANIFESTS.contains(&"fuzz/Cargo.toml"));
}
