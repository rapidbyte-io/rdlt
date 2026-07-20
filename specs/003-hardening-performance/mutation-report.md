# Mutation report (feature 003, data-model §2)

**Threshold**: ≥85% of viable mutants killed; zero undispositioned survivors
(SC-002). Run via `TARGET=mutants make test`; config `.cargo/mutants.toml`
(workspace-tested, nextest).

## Run log

| date | commit | mutants | caught | missed | unviable | timeout | kill rate |
|---|---|---|---|---|---|---|---|
| 2026-07-20 | 852049f (pre-tape snapshot) | 470 | 241 | 127 | 94 | 8 | 64.1% |
| 2026-07-20 | (current, in flight) | — | — | — | — | — | — |

## Survivor dispositions

Filled from the current run; every survivor gets exactly one of:
`new-test <name>` | `dead-code-removed <commit>` | `waived: <reason>`.

*(pending the in-flight run — the pre-tape snapshot's survivor list is stale
against the rewritten shredder and is not dispositioned line-by-line.)*
