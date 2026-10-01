# ADR 0048: The supply chain and CI

Status: accepted, 2026-10-01.

## Context

ADR 0037 puts the build, its tools and CI among what must not be turned against the engine: a
contributor's change, a dependency's update or a tool's release can each bring code that runs
with a maintainer's access or decides whether a change merges. The review behind H2 found five
places where a stated assurance rested on less than it said:

- "One audited `unsafe` module" (ADR 0017) rested, inside the crate that held the module and in
  every test, example, bench and fuzz target, on a pattern over `cargo xtask lint`'s own reading
  of the text. Text the lexer misread, a directory it skipped by name, and a file brought in with
  `include!` were not read.
- "Every tool is pinned" meant a version label. Nothing held a version to the bytes installed,
  six tools were whatever prebuilt binary an installer picked that day, and CI's tool installer
  was the newest release.
- "Actions are pinned" meant a commit SHA, with nothing checking that the SHA was the release the
  comment beside it named.
- Advisories were checked for one of two lockfiles, and only when a change was pushed.
- The lint job's token was in the environment of every build script it ran, each checkout left
  it in git's configuration, and the tool installer exported it to every later step.

## Decision

- **The compiler holds `unsafe` code to one crate.**
  - The audited module is a crate of its own, `rdlt-adopt`: two files, `src/lib.rs` and its
    tests. It allows `unsafe` code with `#[expect(unsafe_code)]` on the one statement and the
    one function that need it, under the workspace's `deny`.
  - Every other target's root file carries `#![forbid(unsafe_code)]`: libraries, binaries,
    examples, integration tests, benches and fuzz targets, `rdlt-connector`'s library included.
    The compiler holds a forbid through every module, `#[path]`, included file and local macro
    of that target, and refuses an `allow` or `expect` beneath it.
  - `cargo xtask lint` checks the arrangement and no longer looks for the keyword:
    - target roots come from `cargo metadata`, for the workspace and the fuzzing crate, not from
      the shape of paths;
    - a root's crate attributes are read with `syn`, as the compiler reads them, so text in a
      string, a comment or a nested module is not the attribute. A root that cannot be read
      fails;
    - the audited crate holds exactly its audited files, whatever a new file's extension or
      directory, and they may name no `include!`, `include_str!`, `include_bytes!` or `#[path]`;
    - a lockfile git tracks beside no listed manifest fails, so a new workspace is listed
      before it is unchecked.
  - Workspace-wide `forbid` in `[workspace.lints]` was not used: a package inherits that table
    whole or restates it, so the audited crate would restate every lint, and the fuzzing crate
    has no such table.
  - The lexer, which the comment and style rules still use, ends a raw C string where the
    compiler does. The walker skips two paths, the fuzzing build's output and the generated
    protocol file, not every directory with their names.
  - Miri, coverage and mutation testing follow the code to `rdlt-adopt`.
- **Tools are locked to bytes.**
  - `mise.lock` holds, for Linux x64 and macOS arm64, each tool's release archive and its
    checksum. A locked install takes only what the lockfile holds and fails on other bytes.
  - Every tool installs a release archive. The `cargo:` backend resolves a download at install
    time and locks nothing, so the six tools that used it install their projects' archives, and
    cargo-binstall is gone. `cargo-fuzz` and `cargo-mutants` publish no macOS arm64 archive and
    are Linux-only.
  - `cargo xtask tools`, in `just lint`, fails on a tool `mise.toml` names that the lockfile
    does not hold, at that version, to a download and a digest on each platform. Where a release
    publishes no digest, the checksum is computed from the archive and added by hand
    (CONTRIBUTING.md).
  - CI installs with `--locked`, from a mise binary of a fixed version whose SHA-256 the
    workflow states for each runner platform. The tool caches have new keys.
  - Locked mode is not a project setting: mise would apply it to a contributor's own tools. A
    plain `mise install` still verifies the lockfile's checksums.
  - Renovate proposes a release seven days after it is published.
- **An action's SHA is checked against its version.** `pinact run --check --verify-comment`
  resolves the version in each pin's comment and fails when the SHA is not that release's commit.
- **Every lockfile is checked for advisories, every day.** `cargo xtask deny` runs cargo-deny
  with `--locked` for each workspace xtask lists, the fuzzing crate's included, in `just lint`
  and in a job of the nightly workflow. A lockfile behind its manifest fails.
- **Workflows hold the least they need.**
  - The token can read the repository and nothing else, in both workflows.
  - No checkout persists it. The tool installer is given none: it installs from the lockfile's
    URLs.
  - `just lint` is `just checks`, which builds and runs the workspace's and its dependencies'
    code with no credential, and `just pins`, the one step CI gives the token, to stay within
    GitHub's rate limit.
  - Workflows run on `pull_request`, `push`, a schedule and by hand. None references a secret,
    and none runs a fork's code with more than a read-only token.

## Consequences

- A new target needs `#![forbid(unsafe_code)]` in its root, and the lint says so. New `unsafe`
  code means a change to `rdlt-adopt`, where a reviewer expects it, or a second audited crate
  and a new record.
- The lint's `unsafe` check is as strong as the compiler's lint: code a dependency's macro
  expands is that dependency's, and is not reported. Dependencies are outside this guarantee, as
  they were; cargo-deny and review cover them.
- A workspace added outside `crates/`, `fuzz/` and `xtask/` is caught by its lockfile, not by
  the walker: it is listed in xtask before its sources are linted.
- Updating a tool is two steps, the version and `mise lock`, and a third for a release without
  digests. Renovate's change to `mise.toml` alone fails CI until the lockfile follows.
- A locked archive is trusted as of the day it was locked: the lock detects a release replaced
  afterwards, not one that was hostile when locked. The release-age wait narrows that.
- Updating mise in CI is a manual change of a version and two checksums.
- The nightly advisory job builds xtask, about two minutes.
- Each job logs the installer's warning that it has no token.
- Restricting which actions the repository may run is a repository setting, outside the tree,
  and left to the owner.
