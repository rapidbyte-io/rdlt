# rdlt

rdlt is an embeddable engine that moves data from sources to destinations with exactly-once
delivery, persistent schema management and throughput that scales with cores. It is the core of
the Rapidbyte data platform.

**Status:** pre-release. The foundation (tooling, deterministic simulation harness, runtime
primitives), the connector contract (`rdlt-connector`, with in-process certification and
`sqlgen`, the planner SQL destinations share) and the exactly-once engine core (`rdlt-engine`:
`append`, `replace` and `merge`, with schema evolution, name maps and nested data, checked by the
simulation oracle in `rdlt-sim`) are in place. The reference connectors in
`rdlt-connector-reference` are an in-memory source and destination, a data generator, a SQLite
destination and a source and destination of JSON lines and Arrow IPC files; the engine's
integration suite runs against every destination. JSON pushes are coalesced and shredded in
parallel on the compute pool, and every batch is lowered there through a plan made once per table
schema; `normalize` and child tables follow.

## Development

The toolchain is pinned in `rust-toolchain.toml`; every other tool is pinned in `mise.toml`.

```sh
mise install        # install the pinned tools
just --list         # see every recipe
just ci             # what the pull-request gate runs: lint, test, coverage, simulation
just ready          # before pushing: `just ci` plus mutation testing of your change
just sim 42         # replay simulation seed 42
```

Read [CONTRIBUTING.md](CONTRIBUTING.md) before opening a pull request. Architecture decisions live
in [docs/adr](docs/adr).

## License

Apache-2.0. See [LICENSE](LICENSE).
