"""Loads PokeAPI's pokemon and types into SQLite, each run resuming where the last ended."""

from __future__ import annotations

import sys
from pathlib import Path

import rdlt
from pokeapi import PokeApi


def main(connectors: Path, data: Path, base_url: str | None = None) -> rdlt.Outcome:
    """Loads both streams into `pokeapi.db` in `data`, with the SQLite connector in `connectors`.

    The source reads from `base_url`, or from its own root, PokeAPI's, where none is given.
    """
    # Sandboxed on Linux, where the connector may write in `data` alone; macOS has no sandbox.
    host = rdlt.Host(connectors=connectors, writable=data, trust_binaries=sys.platform == "darwin")
    source = rdlt.local(PokeApi, config={} if base_url is None else {"base_url": base_url})
    sqlite = rdlt.Connector(
        "io.rapidbyte.sqlite",
        config={"path": str(data / "pokeapi.db")},
        access=rdlt.Access(write=data),
    )
    # Each stream read on from where the last run ended, and merged by its key, so a page read
    # again replaces its rows rather than adding to them.
    streams = [
        rdlt.stream(name, read="incremental", write=rdlt.merge("id"))
        for name in ("pokemon", "types")
    ]
    pipeline = rdlt.pipeline("pokeapi", source=source, destination=sqlite, streams=streams)
    return pipeline.build(host).run()


if __name__ == "__main__":
    outcome = main(Path(sys.argv[1]).resolve(), Path(sys.argv[2]).resolve(), *sys.argv[3:])
    print(f"{outcome.report.status.value}: committed {outcome.report.rows} rows")
