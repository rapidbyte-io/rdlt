# Detection demonstration — US3 (FR-015, GI3)

Ten crash-point registries across six crates now verify against their own
sources. Observed, with real exit codes captured from the command itself.

Before this story, every one of the six sweeps checked `fired == registry` — the
registry against itself. All three cases below passed silently.

## Case 1 — a point renamed in the source only

Registry shrunk to `["duck.append"]`, the arming site renamed to `"duck.REMOVED"`.

```text
exit 100
armed in .../rdlt-connector-duckdb/src but absent from the registry:
  ["duck.REMOVED"] — an instrumented boundary the sweep will never visit
```

**Caught**, by direction 1: everything armed must be declared.

## Case 2 — the PURE silent shrink, and what actually catches it

Both sides made consistent at one name: the registry entry deleted AND the
arming site repointed, so nothing in the crate mentions `duck.tx.commit`.

| check | exit | reading |
|---|---|---|
| `the_registry_matches_the_sources` | **0 — passes** | Correct, and documented: with both sides consistent there is nothing for a two-direction check to compare. |
| `scanner_selfcheck` | **100 — fails** | `rdlt-connector-duckdb: scanner found 1 distinct names, expected 2: ["duck.append"]` |

This is the case the whole story is about, and it is worth being precise about
which mechanism catches it. The registry assertion **cannot** — its doc comment
says so — because after such a deletion neither the code nor the list mentions the
point. What catches it is the **independently committed site count**: a number
recorded by reading the sources before the scanner existed, which the scanner must
reproduce.

Had the count not been committed, this feature would have shipped a check that
looked like it closed the silent-shrink hole while leaving it open — the exact
failure mode it exists to remove.

## Case 3 — a point added to the source but not the registry

`crash_point!("duck.brand_new", …)` inserted, registry untouched.

```text
exit 100
armed in .../rdlt-connector-duckdb/src but absent from the registry:
  ["duck.brand_new"] — an instrumented boundary the sweep will never visit
```

**Caught**, by direction 1.

## Case 4 — the check's own vacuity

`scanning_nowhere_against_a_real_registry_fails` (a `#[should_panic]` test in
`rdlt-testkit`) points the scanner at a directory with no arming calls and a
non-empty registry:

```text
no crash-point sites found under .../src/memory while the registry declares 1 —
either the scan path is wrong or an arming spelling is unrecognised;
recognised spellings are ["crash_point!(", "crash_at("]
```

**Caught.** This is the one way the new check could itself pass while verifying
nothing: a mistyped path, a moved module, or an unrecognised arming spelling
yields an empty set, and an empty set trivially satisfies "everything armed is
declared".

## All six crates, unmodified

```text
6 tests run: 6 passed, 975 skipped
  rdlt-connector-snowflake::crash_sweep       the_registry_matches_the_sources
  rdlt-connector-rest::sweep                  the_registry_matches_the_sources
  rdlt-connector-iceberg::sweep               the_registry_matches_the_sources
  rdlt-connector-duckdb::sweep                the_registry_matches_the_sources
  rdlt-connector-file::sweep                  the_registry_matches_the_sources
  rdlt-connector-postgres::dest_crash_sweep   the_registry_matches_the_sources
```

Ten registries, 37 declared names, all agreeing with their sources.

## Case 5 — a registry entry that arms nothing

`"duck.phantom"` added to the registry, no arming site anywhere.

```text
exit 100
armed nowhere in .../rdlt-connector-duckdb/src: ["duck.phantom"] — a name the
sweep iterates that no code can ever fire
```

**Caught**, by direction 2. This closes the opposite hole from Case 3: a sweep
iterating a name nothing can fire spends cells proving nothing, and its
armed-fire pin would fail confusingly rather than naming the cause.

## Three design changes measurement forced

Recorded because both wrong designs would have caused harm.

**Set equality does not survive this workspace.** Three postgres points are armed
INDIRECTLY — the macro takes a variable and the name's literal sits at the
constructor supplying it (`CrashSite { label: … }`, `Push::Crash(…)`, and a
labelled struct in `source/cdc/read.rs`). A set-equality scanner reports those
three as missing, and the plausible reading of that gap is "the registry is too
big". Shrinking it would have removed three points from the sweep matrix while
every assertion passed.

**One assertion per crate, not per registry.** Several crates declare more than
one registry over the same sources — file and postgres have three each. Checking
one registry against a scope containing its siblings reports every sibling's
points as undeclared, and that false failure invites widening the registry being
checked.

**Counting occurrences does not work; excluding declaration blocks does.** The
first implementation of direction 2 required each declared name to appear at least
twice — once in the declaration, once where it arms. That silently assumes the
declaration lives inside the scanned tree. Every connector satisfies it; the
ENGINE does not, because `ENGINE_POINTS` is declared in its TEST file. Migrating
the engine to the shared helper failed immediately, naming all seven of its points
as armed nowhere.

The fix is to locate registry declarations by their shape
(`pub const NAME: &[&str] = &[ … ];`), blank them out, and harvest the literals
that remain. That works whether a registry is declared inside the scanned tree or
outside it, and it states the non-circularity directly rather than encoding it in
an arithmetic threshold that means different things in different crates.

Worth noting what caught this: migrating the engine — the one crate that already
had a working check — is what exposed the flaw. Had the engine been left alone as
"already correct", the shared helper would have shipped with a rule that happened
to hold for six crates by coincidence of where they declare things.
