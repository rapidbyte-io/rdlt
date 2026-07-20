# Evidence: Postgres-Source Benchmark Cells (T028)

**Date**: 2026-07-20 | **Identity**: blashyrkh (the 003/004 matrix
machine), AMD Ryzen AI MAX+ 395, kernel 7.0.12-201.fc44, rustc 1.96.0
ac68faa20, podman 5.8.3 (host engine via distrobox-host-exec), release
build, quiet machine. Baseline: **dlt 1.29.0** (`sql_database` source,
existing matrix pin) in postgres:16 containers with `--network=host`.

## Datasets (benches/baseline/seed_pg.sql — deterministic, identity from seed output)

| Dataset | Rows | Content md5 (`md5(string_agg(md5(t::text)…))`) |
|---|---|---|
| pg_wide (12 typed cols) | 1,000,000 | `e840f51738a6b4b15f9f085ea85e3df8` |
| pg_jsonb (nested docs) | 200,000 | `33c1c5186af078733e77b845fce458c2` |

## Protocol

Baseline measured FIRST, same session, same server/datasets. dlt:
in-process self-timing (unchanged house method), backend parameter —
**pyarrow = gated baseline** (dlt's fastest documented pure-dlt config),
sqlalchemy (its default) + connectorx (its Rust reader) = scoreboard.
rdlt: release CLI, `/usr/bin/time -v` (wall + peak RSS). 5 runs per
cell, medians recorded; destination schemas dropped between runs.

## Raw runs (seconds)

| Cell | Runs | Median |
|---|---|---|
| dlt pyarrow pg_wide→DuckDB | 10.00, 10.14, 10.19, 10.26, 10.33 | **10.19** |
| dlt pyarrow pg_wide→Postgres | 16.90, 17.02, 17.02, 17.05, 17.16 | **17.02** |
| dlt sqlalchemy pg_wide→DuckDB | 57.09–57.28 | 57.14 |
| dlt sqlalchemy pg_wide→Postgres | 106.78–107.20 | 107.11 |
| dlt connectorx pg_wide→DuckDB | 2.93, 2.94, 2.94, 2.94, 2.95 | 2.94 |
| dlt pyarrow pg_jsonb→DuckDB | 4.47, 4.49, 4.50, 4.51, 4.54 | 4.505 |
| rdlt pg_wide→DuckDB | 1.16, 1.26, 1.31, 1.40, 1.51 (RSS 434–477 MB, median 444) | **1.31** |
| rdlt pg_jsonb→DuckDB | 0.24, 0.24, 0.24, 0.24, 0.25 (RSS ~127 MB) | 0.24 |
| rdlt pg_wide→Postgres | 1.91, 1.92, 1.92, 1.97, 2.37 (RSS ~138 MB) | **1.92** |

## Cells and bars (measurement-first, 004 protocol)

| Cell | Pair | Multiple | Bar derivation |
|---|---|---|---|
| **pg→DuckDB (gated)** | 10.19 s vs 1.31 s | **7.8×** | worst-case-run multiple 10.00/1.51 = 6.6×; bar **≥ 6×** sits under it with ~10% headroom — cannot flap on observed spread |
| **pg→Postgres (gated)** | 17.02 s vs 1.92 s | **8.9×** | worst case 16.90/2.37 = 7.1×; bar **≥ 6×** (~15% under worst case) |
| vs dlt default (scoreboard) | 57.14 / 107.11 s | 43.6× / 55.8× | context: what a default dlt setup experiences |
| vs connectorx (scoreboard) | 2.94 s vs 1.31 s | **2.2×** | context: rdlt's owned COPY-binary decoder vs dlt delegating to another Rust reader — not a gate, but the R1 thesis confirmed |
| pg_jsonb→DuckDB (scoreboard) | 4.505 s vs 0.24 s | 18.8× | Json escape-hatch path end-to-end |

Memory context: rdlt peaks at 444 MB (DuckDB cell; the destination's
buffer pool dominates) and 138 MB (Postgres cell) vs dlt pyarrow's
615+ MB; the source path alone is bounded far lower (see the
memory-ceiling test: 39 MB peak on a 6.9 GB table with a parquet
destination).

Notes for honest reading: dlt numbers are in-process self-timed inside a
container (excludes interpreter boot — generous to the baseline, as in
every other matrix row); rdlt numbers are whole-process wall time.
connectorx was measured on the DuckDB cell only (its dlt integration
ignores chunking on the pg destination path — see the committed dlt
review). Gated bars are set from this session's measured WORST-case run
pair, not the medians — the 004 flap rule applied at birth.
