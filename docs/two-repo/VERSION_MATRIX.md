# The two-repo version matrix

The engine repository (this one) and the connectors repository
(`rapidbyte-io/rdlt-connectors`) are deliberately separate: connectors
consume the SDK as pinned dependencies and enter only through
`rdlt-certify`. This document is the ritual that keeps the two honest —
one row per deliberate forward-pull, newest last.

## The rules

1. **Consumption is pinned by revision**, never floating. A connector
   PR is certified against the revision THIS table records, not
   against whatever the lockfile happens to resolve that day.
2. **Forward-pulls are deliberate and one at a time.** Bump the rev,
   run the full connector gate (`make check` there) twice, record the
   row here. A red gate stops the pull; it never rides along with
   unrelated work.
3. **After the crates are published**, rows gain
   the published crate versions beside the revision, and consumption
   respells git→version crate-by-crate per the 023 packaging rule.
   The certify template's provenance assertion admits git sources
   only; the respelling is the trigger for its update (a registry
   source at the row's published version), made in the same change
   that respells the first crate — the job fails closed until then,
   which is the intended order.

## The matrix

| Date       | engine rev (rdlt) | connectors rev | consumed as | gate | notes |
|------------|-------------------|----------------|-------------|------|-------|
| 2026-08-12 | `360beea8`        | seed           | git dep     | 932 tests standalone-green before AND after the cut | the 044 seed |
| 2026-08-19 | `37088dd5`        | `5c8600e`      | git dep     | green | the wire speaks v1; connectors own their vocabulary |
| 2026-08-21 | `f27c2119`        | *pending pull* | git dep     | — | 076-deepening: shared Arrow seats, gate constants, sdk-local ClassifiedError; **pull required** — the SDK's serve scaffold and error flattening moved |

## The certify job

`certify-connector-pr.yml.template` (beside this file) is the CI
workflow the connectors repo installs: on every connector PR it runs
that crate's `test_certify_wire` suite against the engine revision
recorded here (input `engine_rev`, defaulting to this table's newest
row), so certification is always measured against a CHOSEN contract,
never an accident of the lockfile. The job asserts that provenance —
every engine crate in the graph, named by its seat, must resolve to
the canonical repository at that revision, a path or `[patch]`
redirect counting as a mismatch, and the sdk must be present at all —
or the job fails naming the offender, so the record cannot say one
engine while a PR's manifest resolves another. What it binds is
the honest change: the suites and Makefile it runs are the PR's own
files, so a PR written to pass them does, and the backstop for that is
maintainer diff review, the same trust decision SECURITY.md records for
installing a connector at all.
