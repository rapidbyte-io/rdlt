#!/usr/bin/env sh
# Distribution gate (FR-017): every git dependency in this workspace is either
# absent, or recorded in tools/allowed-git-deps.toml together with the crates it
# stops us publishing.
#
# There are two ways a git dependency can appear, and they fail in OPPOSITE
# directions. Both are checked here; the first is the dangerous one.
#
#   MODE A -- `git` AND `version` on the same dependency.
#     `cargo package` and `cargo publish` SUCCEED. Cargo deletes the git source
#     and keeps the version, so the crate that reaches users is built against
#     the registry, not against the fork. Nothing warns. Measured on cargo
#     1.97.1: the packaged Cargo.toml held exactly `[dependencies.<dep>]` +
#     `version = "1.1"`, the packaged Cargo.lock recorded
#     `source = "registry+https://github.com/rust-lang/crates.io-index"` with
#     the UPSTREAM checksum, and `cargo publish --dry-run` ran through the
#     verification build to `Uploading`, stopping only because of --dry-run.
#     This is silent corruption, so it is a hard failure here and it is NOT
#     allowlistable.
#
#   MODE B -- `git` with no `version`.
#     `cargo package` and `cargo publish` REFUSE before building anything:
#     "all dependencies must have a version requirement specified when
#     packaging" / "...when publishing". Loud, early, nothing uploaded. This is
#     the SAFE way to pin a fork. It is allowlistable, and the allowlist entry
#     must state which crates it makes unpublishable.
#
#   MODE C -- a `[patch.*]` or `[replace]` entry with a git source.
#     Redirects THIS workspace's builds only. It is dropped from published
#     crates entirely, so consumers silently get the unpatched dependency.
#     Hard failure; not allowlistable.
#
# The blast radius is computed from what SURVIVES into the packaged manifest,
# which is not the same as the dependency graph:
#   - normal and build edges always survive        -> they propagate the block
#   - dev edges survive only if they carry `version`
#   - a dev edge with a path and no `version` is emptied out of the packaged
#     manifest, so it propagates NOTHING (verified: a crate whose only git
#     dependency sat in a versionless [dev-dependencies] packaged cleanly)
#   - crates with `publish = false` were never publishable and are excluded
#
# SCOPE: the main workspace only — the root Cargo.toml and every member it
# names. `fuzz/` is deliberately excluded: it declares its own `[workspace]`
# (fuzz/Cargo.toml:21, libFuzzer needs nightly) and carries `publish = false`
# (fuzz/Cargo.toml:4), so nothing in it can reach a registry and a git
# dependency there has no distribution consequence to record. That is what
# FR-017 is about; scanning it would only produce findings no one can act on.
#
# No network, no cargo invocation, no build: manifests are read directly, so
# this stays a sub-second step in `make lint`. python3 only (stdlib tomllib).
set -eu

TOOLS_DIR=$(cd "$(dirname "$0")" && pwd)
REPO_ROOT=$(cd "$TOOLS_DIR/.." && pwd)

if ! command -v python3 >/dev/null 2>&1; then
    echo "git-deps: python3 not installed (needed to read the manifests)" >&2
    exit 1
fi
# tomllib landed in 3.11. Say which tool is too old rather than letting an
# ImportError traceback stand in for a gate result — a check that dies is not a
# check that passed, and it must not read like one.
if ! python3 -c 'import sys; sys.exit(0 if sys.version_info >= (3, 11) else 1)'; then
    echo "git-deps: python3 $(python3 -c 'import platform;print(platform.python_version())') is too old — 3.11+ needed for tomllib" >&2
    exit 1
fi

python3 - "$REPO_ROOT" <<'PY'
import glob, pathlib, sys, tomllib

REPO = pathlib.Path(sys.argv[1])
ALLOWLIST = REPO / "tools" / "allowed-git-deps.toml"
ALLOWLIST_REL = "tools/allowed-git-deps.toml"

DEP_TABLES = {
    "dependencies": "normal",
    "dev-dependencies": "dev",
    "build-dependencies": "build",
}


def load(path):
    with open(path, "rb") as f:
        return tomllib.load(f)


def rel(path):
    return str(pathlib.Path(path).relative_to(REPO))


root_doc = load(REPO / "Cargo.toml")
ws = root_doc.get("workspace", {})
ws_deps = ws.get("dependencies", {})

# ---------------------------------------------------------------- manifests --
excluded = {str((REPO / e).resolve()) for e in ws.get("exclude", [])}
manifests = []
for pattern in ws.get("members", []):
    for hit in sorted(glob.glob(str(REPO / pattern / "Cargo.toml"))):
        if str(pathlib.Path(hit).parent.resolve()) not in excluded:
            manifests.append(pathlib.Path(hit))
if "package" in root_doc:
    manifests.append(REPO / "Cargo.toml")

packages = {}   # name -> {"doc":…, "manifest":…, "publishable":bool}
for manifest in manifests:
    doc = load(manifest)
    pkg = doc.get("package")
    if not pkg or "name" not in pkg:
        continue
    publish = pkg.get("publish")
    if isinstance(publish, dict) and publish.get("workspace"):
        publish = ws.get("package", {}).get("publish")
    publishable = not (publish is False or (isinstance(publish, list) and not publish))
    packages[pkg["name"]] = {
        "doc": doc,
        "manifest": manifest,
        "publishable": publishable,
    }


def dep_tables(doc, manifest_is_root):
    """(label, kind, table) for every dependency table in one manifest."""
    for name, kind in DEP_TABLES.items():
        if isinstance(doc.get(name), dict):
            yield name, kind, doc[name]
    for target, sub in (doc.get("target") or {}).items():
        if not isinstance(sub, dict):
            continue
        for name, kind in DEP_TABLES.items():
            if isinstance(sub.get(name), dict):
                yield f'target."{target}".{name}', kind, sub[name]
    if manifest_is_root and isinstance(ws_deps, dict) and ws_deps:
        # Declaration only; its packaging consequence is decided by the members
        # that inherit it, handled at their use sites.
        yield "workspace.dependencies", None, ws_deps


def normalise(spec):
    return spec if isinstance(spec, dict) else {"version": spec}


# ------------------------------------------------------------- git use sites --
# A member entry that says `workspace = true` takes the ROOT declaration, and
# ONLY the root declaration. Cargo 1.97.1 silently ignores a `version` the
# member adds alongside it -- verified: with `{ workspace = true, version =
# "1.1" }` on a git dependency, `cargo metadata` still reported `req = "*"`,
# cargo wrote 0 bytes to stderr, and `cargo package` still refused for want of
# a version. So a member's own keys are never read as the version here.
sites = []          # every place a git dependency is USED
internal_edges = [] # (consumer, dependency, kind, effective spec)

for manifest in manifests:
    doc = load(manifest)
    is_root = manifest == REPO / "Cargo.toml"
    owner = (doc.get("package") or {}).get("name")
    for label, kind, table in dep_tables(doc, is_root):
        for key, raw in table.items():
            spec = normalise(raw)
            inherited = spec.get("workspace") is True
            if inherited:
                effective = normalise(ws_deps.get(key, {}))
                decl_manifest, decl_table = "Cargo.toml", "workspace.dependencies"
            else:
                effective = spec
                decl_manifest, decl_table = rel(manifest), label

            if label == "workspace.dependencies":
                continue  # declarations are judged through their use sites

            depname = effective.get("package", spec.get("package", key))
            if depname in packages and owner:
                internal_edges.append((owner, depname, kind, effective))
            if "git" in effective and owner:
                sites.append({
                    "package": owner,
                    "dep": key,
                    "real_name": depname,
                    "use_manifest": rel(manifest),
                    "use_table": label,
                    "decl_manifest": decl_manifest,
                    "decl_table": decl_table,
                    "inherited": inherited,
                    "kind": kind,
                    "spec": effective,
                })

# A git declaration in [workspace.dependencies] that no member inherits is still
# a declaration, and still has to be accounted for.
used = {(s["decl_manifest"], s["decl_table"], s["dep"]) for s in sites}
for key, raw in (ws_deps or {}).items():
    spec = normalise(raw)
    if "git" in spec and ("Cargo.toml", "workspace.dependencies", key) not in used:
        sites.append({
            "package": None, "dep": key,
            "real_name": spec.get("package", key),
            "use_manifest": "Cargo.toml", "use_table": "workspace.dependencies",
            "decl_manifest": "Cargo.toml", "decl_table": "workspace.dependencies",
            "inherited": False, "kind": None, "spec": spec,
        })


def survives_packaging(kind, spec):
    """Does this edge appear in the manifest cargo uploads?"""
    if kind in ("normal", "build"):
        return True
    if kind == "dev":
        return "version" in spec
    return False


def blast_radius(seed_sites):
    affected = {s["package"] for s in seed_sites
                if s["package"] and survives_packaging(s["kind"], s["spec"])}
    changed = True
    while changed:
        changed = False
        for consumer, dep, kind, spec in internal_edges:
            if dep in affected and consumer not in affected \
                    and survives_packaging(kind, spec):
                affected.add(consumer)
                changed = True
    return sorted(n for n in affected if packages.get(n, {}).get("publishable", True))


# ----------------------------------------------------------------- allowlist --
allows = []
if ALLOWLIST.exists():
    allows = load(ALLOWLIST).get("allow", [])
REQUIRED = ("dependency", "manifest", "table", "git", "rev",
            "reason", "exit", "unpublishable")

failures = []
notes = []
matched_allows = set()

# One DECLARATION can have several use sites — a `[workspace.dependencies]`
# entry taken by three members is one thing to fix, one allowlist entry, and one
# blast radius covering all three. Group before judging, or the same declaration
# reports three times with three partial radii, at most one of which could ever
# match the record.
groups = {}
for s in sites:
    groups.setdefault((s["decl_manifest"], s["decl_table"], s["dep"]), []).append(s)

for (decl_manifest, decl_table, dep), group in sorted(groups.items()):
    site = group[0]
    where = f"{decl_manifest} [{decl_table}] {dep}"
    via = ""
    users = [g for g in group if g["inherited"] and g["package"]]
    if users:
        via = "".join(f'\n  (used by {g["package"]} at {g["use_manifest"]} '
                      f'[{g["use_table"]}] as `{{ workspace = true }}`)'
                      for g in sorted(users, key=lambda g: g["package"]))
    radius = blast_radius(group)
    radius_txt = ", ".join(radius) if radius else "(none)"
    spec = site["spec"]

    # Bind any allowlist entry to this site FIRST, by (dependency, manifest,
    # table), before deciding the mode. A site that has flipped from MODE B to
    # MODE A must report ONE finding — the flip — and not also accuse its own
    # allowlist entry of being dead, which would send the reader looking for a
    # dependency that is still right there.
    entry = next((a for a in allows
                  if a.get("dependency") == dep
                  and a.get("manifest") == decl_manifest
                  and a.get("table") == decl_table), None)
    if entry is not None:
        matched_allows.add(id(entry))

    # ---- MODE A: git + version. Never allowlistable.
    if "version" in spec:
        failures.append(f"""\
MODE A -- publishes SILENTLY against the registry
  {where}
  carries BOTH `git` and `version = "{spec['version']}"`.{via}

  What cargo actually does: `cargo package` and `cargo publish` SUCCEED. Cargo
  deletes the git source and keeps only the version, so the .crate that reaches
  users declares `{site["real_name"]} = "{spec['version']}"` from crates.io and
  its lockfile names the registry source with the UPSTREAM checksum. The
  published crate compiles, installs, and then behaves like upstream -- the
  reason the fork exists is simply not in it. Nothing warns, at any point.

  Crates whose published form would resolve `{site["real_name"]}` from the
  registry instead of the fork:
      {radius_txt}

  This is the only arrangement here that ships the wrong code without saying
  so, so there is no allowlist for it. Pick what you actually want:
    - the fork      -> delete `version = "{spec['version']}"`. Publishing then
                       fails loudly instead of lying, and you record the
                       consequence in {ALLOWLIST_REL}.
    - upstream      -> delete `git` (and any `rev`/`branch`/`tag`).""")
        continue

    # ---- MODE B: git, no version. Allowlistable, must be recorded.
    if entry is None:
        failures.append(f"""\
MODE B -- publishing is blocked, and the block is unrecorded
  {where}
  is a git dependency with no `version` key, and has no entry in
  {ALLOWLIST_REL}.{via}

  What cargo actually does: `cargo package` and `cargo publish` REFUSE, before
  building anything, with "all dependencies must have a version requirement
  specified when packaging" (and "...when publishing" from `cargo publish`).
  Nothing is uploaded and nothing silently changes meaning. This is the SAFE
  form of a fork pin. The check is not saying it is wrong -- it is saying it is
  unrecorded.

  While it stands, these crates cannot be published to crates.io:
      {radius_txt}
  (every crate reaching it over a normal, build, or versioned dev edge; a dev
  edge with no `version` is stripped at package time and carries no block;
  `publish = false` crates are excluded.)

  DO NOT silence this by adding `version = ...`. That converts it to MODE A,
  which publishes successfully against upstream and ships the wrong code.
  Record it instead, in {ALLOWLIST_REL}:

    [[allow]]
    dependency = "{dep}"
    manifest = "{decl_manifest}"
    table = "{decl_table}"
    git = "{spec.get("git", "")}"
    rev = "{spec.get("rev", "<exact 40-char revision>")}"
    reason = "<why the fork is required, in one sentence>"
    exit = "<the named event that retires it>"
    unpublishable = [{", ".join(f'"{c}"' for c in radius)}]""")
        continue

    missing = [k for k in REQUIRED if not entry.get(k) and entry.get(k) != []]
    if missing:
        failures.append(f"""\
ALLOWLIST INCOMPLETE
  The entry in {ALLOWLIST_REL} for
  {where}
  is missing: {", ".join(missing)}.
  Every field is required: the entry is the record that FR-017 asks for, and a
  record with a hole in it is not one.""")
        continue

    if entry["git"] != spec.get("git"):
        failures.append(f"""\
ALLOWLIST STALE -- git URL
  {where} points at {spec.get("git")}
  but {ALLOWLIST_REL} records {entry["git"]}.
  Update the record, or point the manifest back where it was recorded.""")
        continue

    for moving in ("branch", "tag"):
        if moving in spec:
            failures.append(f"""\
MOVING GIT REFERENCE
  {where}
  pins `{moving} = "{spec[moving]}"`. A branch or tag can be moved under the
  build, so two checkouts of this commit can compile different code, and the
  revision recorded in {ALLOWLIST_REL} stops meaning anything. Pin
  `rev = "<40-char revision>"` instead — allowlisted git dependencies are
  required to name an exact revision.""")
            break
    else:
        if entry["rev"] != spec.get("rev"):
            failures.append(f"""\
ALLOWLIST STALE -- revision
  {where} is pinned at rev {spec.get("rev")}
  but {ALLOWLIST_REL} records {entry["rev"]}.
  A fork revision is what people will be asked to trust; moving it without
  touching the record makes the record fiction. Update both together.""")
            continue

        declared = sorted(entry["unpublishable"])
        if declared != radius:
            failures.append(f"""\
ALLOWLIST STALE -- blast radius
  {where}
  makes these crates unpublishable:
      computed: {", ".join(radius) if radius else "(none)"}
      recorded: {", ".join(declared) if declared else "(none)"}
  The graph moved since the record was written. Cargo strips a versionless dev
  edge from the packaged manifest, so only normal, build, and versioned-dev
  edges carry the block; `publish = false` crates never counted. Re-read the
  computed list, satisfy yourself it is what you meant, and record it.""")
            continue

        notes.append(f'{dep} @ {spec.get("rev", "")[:12]} '
                     f'({decl_manifest} [{decl_table}]) '
                     f'-> unpublishable: {radius_txt}')

# ---- MODE C: [patch] / [replace] with a git source.
seen_for_patch = []
for manifest in manifests + [REPO / "Cargo.toml"]:
    if manifest in seen_for_patch:
        continue
    seen_for_patch.append(manifest)
    doc = load(manifest)
    entries = []
    for registry, table in (doc.get("patch") or {}).items():
        if isinstance(table, dict):
            entries += [(f"patch.{registry}", k, normalise(v)) for k, v in table.items()]
    for k, v in (doc.get("replace") or {}).items():
        entries.append(("replace", k, normalise(v)))
    for label, key, spec in entries:
        if "git" not in spec:
            continue
        failures.append(f"""\
MODE C -- redirects this workspace only
  {rel(manifest)} [{label}] {key} points at a git source.

  A `[{label}]` entry rewrites builds INSIDE this workspace and is dropped from
  every published crate. So the tree here builds against the fork, the crate
  users install builds against whatever the registry holds, and the two differ
  with nothing to show for it -- the same silent divergence as MODE A, reached
  by a different route, and invisible in the dependent crates' own manifests.

  Express the fork as a plain git dependency without a `version` key (MODE B)
  and record it in {ALLOWLIST_REL}, where the distribution cost is stated.""")

# ---- allowlist entries matching nothing.
for entry in allows:
    if id(entry) not in matched_allows:
        failures.append(f"""\
ALLOWLIST DEAD ENTRY
  {ALLOWLIST_REL} allows `{entry.get("dependency")}` in
  {entry.get("manifest")} [{entry.get("table")}], but no such git dependency
  exists any more. If the fork is gone, delete the entry and say so where the
  feature's dispositions are recorded; a standing permission for a thing that
  no longer exists will be read as one that does.""")

if failures:
    print("git-deps: FAIL\n", file=sys.stderr)
    for f in failures:
        print(f + "\n", file=sys.stderr)
    print(f"git-deps: {len(failures)} finding(s). "
          f"Full rules: tools/check-git-deps.sh header.", file=sys.stderr)
    sys.exit(1)

if notes:
    plural = "y" if len(notes) == 1 else "ies"
    print(f"git-deps: ok — {len(manifests)} manifests, "
          f"{len(notes)} recorded git dependenc{plural}:")
    for n in notes:
        print(f"  {n}")
else:
    print(f"git-deps: ok — {len(manifests)} manifests, no git dependencies")
PY
