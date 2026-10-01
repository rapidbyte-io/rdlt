# ADR 0048: The supply chain and CI

Status: accepted, 2026-10-01.

## Context

ADR 0037 makes the engine and the host the trusted computing base. What builds them is part of
that base: a contributor's change, a dependency's update, a tool's release and a workflow's
action each bring code that runs with a maintainer's access, or decides whether a change merges.

The repository made four statements about them, each weaker than it read:

- `unsafe` code was said to be in one audited module (ADR 0017). The module sat in
  `rdlt-connector`, so that crate's root could not forbid `unsafe` code, and tests, examples,
  benches and fuzz targets had no root that did. There the only check was a pattern over
  `cargo xtask lint`'s own reading of the text, which skipped directories by name and read no
  file brought in with `include!`.
- Tools and actions were said to be pinned. A tool was pinned to a version label and an action
  to a commit, with nothing holding the label to bytes or the commit to the release named
  beside it.
- Dependencies were checked against advisories, for one of the repository's two lockfiles, and
  only when someone pushed a change.
- CI's token was read-only, and was in the environment of every build script the lint ran.

This record decides what holds each statement, and says where a check ends.

## Decision

- **`unsafe` code is in one crate, held by the compiler and by the lint together.**
  - The audited code is a crate of its own, `rdlt-adopt`: two files, `src/lib.rs` and its
    tests, compiled on Unix only. It allows `unsafe` code with `#[expect(unsafe_code)]` on the
    one statement and the one function that need it, under the workspace's `deny`.
  - `rdlt_adopt::adopt` is a safe function, so that a crate forbidding `unsafe` code can call
    it. It checks what it can: the descriptor is open, a socket, not a standard stream, not
    close-on-exec, and taken once. That nothing else in the process owns the descriptor is left
    to its caller, which calls it first thing in a connector's `main`. The dependency rule
    (`cargo xtask deps`) lets `rdlt-connector` alone use the crate, in tests as well.
  - **What the compiler holds.** Every other target's root file carries
    `#![forbid(unsafe_code)]`: libraries, binaries, examples, integration tests, benches and
    fuzz targets, `rdlt-connector`'s library included. The compiler then refuses `unsafe` code
    in everything it compiles for that target, through modules, `#[path]` and included files,
    and refuses an `allow` or `expect` beneath the forbid. It does not look inside a macro's
    definition, and does not report what a macro of another crate expands to.
  - **What the lint holds.** `cargo xtask lint` reads the tokens of every Rust file under
    `crates/`, `fuzz/` and `xtask/`, the generated protocol file included, and outside the
    audited crate reports:
    - `unsafe` wherever it is a token: in code, in a `macro_rules!` body, in `quote!`, as a raw
      identifier. Strings and comments are not tokens;
    - `include!`, and a `#[path]` that is not a string naming a `.rs` file at or beneath its
      own directory, so that no code is compiled from a file the lint did not read;
    - a target root, as `cargo metadata` lists them for each workspace, whose crate attributes
      do not include the forbid. They are read with `syn`, as the compiler reads them;
    - a manifest git tracks, or would, that is no listed workspace's and no package's of one,
      so a new workspace is listed before its targets go unchecked.
  - In the audited crate the lint reports any file git tracks, or would, beyond the audited
    ones, and any `include!`, `include_str!`, `include_bytes!` or `#[path]` in them.
  - Neither check is enough alone. Without the forbid, the lint is a pattern again. Without the
    lint, a macro's body is unchecked.
  - Workspace-wide `forbid` in `[workspace.lints]` was not used: a package inherits that table
    whole or restates it, so the audited crate would restate every lint, and the fuzzing crate
    has no such table.
  - The lexer, which the comment and style rules use, ends a raw C string where the compiler
    does. The walker skips one path, the fuzzing build's output, not every directory with its
    name.
  - Miri, coverage and mutation testing follow the code to `rdlt-adopt`.
- **Tools are locked to bytes.**
  - `mise.lock` holds, for Linux x64 and macOS arm64, each tool's release archive and its
    checksum. A locked install takes only what the lockfile holds and fails on other bytes.
  - Every tool installs a release archive built for its platform. The `cargo:` backend resolves
    a download at install time and locks nothing, so the tools that used it install their
    projects' archives, and cargo-binstall is gone. `cargo-fuzz` and `cargo-mutants` publish no
    macOS arm64 archive and are Linux-only.
  - `cargo xtask tools`, in `just lint`, fails on a tool `mise.toml` names that the lockfile
    does not hold at that version, and on any platform in the lockfile without a download and
    a digest. Where a release publishes no digest, the checksum is computed from the archive
    and added by hand (CONTRIBUTING.md).
  - CI installs with `--locked`, from a mise binary of a fixed version whose SHA-256 the
    workflow states for each runner platform. The tool caches have new keys.
  - Locked mode is not a project setting: mise would apply it to a contributor's own tools. A
    plain `mise install` still verifies the lockfile's checksums.
- **An action's SHA is checked against its version.** `pinact run --check --verify-comment`
  resolves the version in each pin's comment and fails when the SHA is not that release's commit.
- **Every lockfile is checked for advisories, every day.**
  - `cargo xtask deny` runs cargo-deny with `--locked` for each workspace xtask lists, the
    fuzzing crate's included, in `just lint` and in a job of the nightly workflow.
  - A lockfile behind its manifests fails. For the workspace's own, `just checks` and
    `just deny` first run `cargo metadata --locked`: any other cargo command, running xtask
    included, would bring the lockfile up to date instead of failing.
- **Updates wait.** Renovate proposes a release seven days after it is published: crates,
  tools, actions, and the mise release CI installs, which a custom manager tracks. Lock file
  maintenance is off, because Renovate cannot apply the wait to what cargo resolves, so
  transitive crates are updated by hand.
- **Workflows hold the least they need.**
  - The token can read the repository and nothing else, in both workflows.
  - No checkout persists it. The tool installer is given none: it installs from the lockfile's
    URLs.
  - `just lint` is `just checks`, which builds and runs the workspace's and its dependencies'
    code, and `just pins`. In CI the `lint` job runs the first with no credential. A job of its
    own, `pins`, runs the second and is the only place the token goes, to stay within GitHub's
    rate limit: it installs `just` and `pinact` from the lockfile and builds nothing, so no
    build script, proc macro or program of the workspace runs where the token is.
  - Workflows run on `pull_request`, `push`, a schedule and by hand. None references a secret,
    and none runs a fork's code with more than a read-only token.

## Consequences

- A new target needs `#![forbid(unsafe_code)]` in its root, and the lint says so. New `unsafe`
  code means a change to `rdlt-adopt`, where a reviewer expects it, or a second audited crate
  and a new record.
- What neither check sees: `unsafe` code in a dependency, and in what a dependency's macro
  expands to, which are the dependency's and are covered by cargo-deny and review; and a proc
  macro of the workspace that assembles the keyword from text rather than writing it. The
  second needs a deliberate change to `rdlt-connector-macros`, which review covers.
- A source directory added beside `crates/`, `fuzz/` and `xtask/` is not walked until it is
  listed in xtask; its manifest fails the lint until its workspace is.
- The word `unsafe` cannot be an identifier, a macro argument or a macro's fragment outside the
  audited crate, and `include!` cannot be used for code. Data is included with `include_str!`
  and `include_bytes!`.
- Updating a tool is the version, then `mise lock`, and a checksum by hand for a release
  without digests. Renovate's mise manager is documented to update `mise.lock` with the
  version; where it does not, CI fails until the lockfile follows.
- A locked archive is trusted as of the day it was locked: the lock detects a release replaced
  afterwards, not one that was hostile when locked. The wait narrows that.
- Updating mise in CI is a version, which Renovate proposes, and two checksums by hand; CI fails
  until they match.
- Transitive crates go stale unless someone runs `cargo update`. The nightly advisory check says
  when one must be updated.
- The `pins` job's recipe and tools come from the pull request, as the whole workflow does under
  `pull_request`. What bounds it is the token: read-only, for a public repository.
- `pins` is a new check: the repository's ruleset has to require it beside `lint`.
- The nightly advisory job builds xtask, about two minutes.
- Each job logs the installer's warning that it has no token.
- Restricting which actions the repository may run is a repository setting, outside the tree,
  and left to the owner.
