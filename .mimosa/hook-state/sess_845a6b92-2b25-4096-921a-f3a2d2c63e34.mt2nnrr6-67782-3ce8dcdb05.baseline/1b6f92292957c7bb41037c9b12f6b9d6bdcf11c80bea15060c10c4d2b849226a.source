//! Shared support: the checked-in registry under `benches/`, as the harness
//! itself resolves it.

use rdlt_bench::paths::Paths;

/// The repo's `Paths`, anchored at this crate's manifest — two levels below
/// the repo root — with the default target directory.
pub(crate) fn repo_paths() -> Paths {
    let repo = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .ancestors()
        .nth(2)
        .expect("crates/rdlt-bench sits two levels below the repo root")
        .to_path_buf();
    let target = repo.join("target");
    Paths::rooted(repo, target)
}
