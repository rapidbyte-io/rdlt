# Detection demonstration — US2 (FR-015, GI2)

## The orphaned suites

`crates/rdlt-connector-file/tests/e2e_copy.rs` and `e2e_duckdb.rs` were reachable
from **no** gate path. `TARGET=e2e` existed to run them; `check` never invoked it,
and neither did `deep`. Two suites that looked like coverage in the tree and
provided none.

```text
$ make test TARGET=e2e
2 tests run: 2 passed, 0 skipped     exit 0
```

They pass — and had been passing all along, unobserved. `check` now invokes the
target, between the workspace tests and the sweeps.

## The enumeration is mechanical, not hand-kept

`evidence/suite-reachability.md` lists all **107** test binaries in the repository
against the gate path that reaches each, derived by listing `crates/*/tests/*.rs`
rather than by maintaining a list. A hand-kept list is the thing that goes stale
and produces exactly the false confidence this story is about.

**Zero unreachable-and-unexplained.** One binary is deliberately not run:

```text
rdlt-connector-snowflake  crash_sweep
  NOT RUN by any gate; type-checked by lint [ADDED]. 101.5 min, live creds.
```

That is an exemption with its reason and its measured cost, which GI2 requires —
as distinct from being orphaned, which is what it was before.

## What detection looks like here

A binary added to the repository and reached by nothing shows up in the
enumeration as having no gate path. The check is the enumeration itself being
derived from the filesystem: a new file appears in it automatically, whereas a
hand-written list would simply not mention it.

This is the one story whose detection is structural rather than an assertion, and
worth being honest about: it depends on someone reading the enumeration when the
set of suites changes. The stronger form — a test that fails when a binary is
unreachable — would need the gate to know its own target graph, which is a larger
change than this feature's scope. Recorded as the weaker guarantee it is.
