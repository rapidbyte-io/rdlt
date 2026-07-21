# Quickstart: Connector Verification

## Audit a parameter

Open `specs/011-connector-verification/matrix.md`, find the row, run its
cells:

```bash
cargo nextest run -p rdlt-postgres -E 'test(<cell name>)'
```

Rows marked `sweep` need `--features failpoints`; `heavy` rows need
`RDLT_HEAVY=1` (and a container runtime + release CLI).

## Measure coverage

```bash
make coverage        # cargo-llvm-cov over nextest, rdlt-postgres
```

Prints total + per-file line coverage. The recorded number, command,
and exclusion classifications live in `benches/RESULTS.md`; the floor
is 80% (contract PM5).

## The rules

`contracts/parameter-matrix.md` (PM1–PM8): behavior-proving cells,
observed defaults, citations over rewrites, classified exclusions,
mismatches always resolved.
