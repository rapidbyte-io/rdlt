"""What the tests of streams read in phases and as changes share."""

from __future__ import annotations

from pathlib import Path

import rdlt
from rdlt.connector import Cursor, ReadStream


def host(tmp_path: Path, connectors: Path) -> rdlt.Host:
    """A host of the reference connectors in `connectors`, writing in `tmp_path`."""
    return rdlt.Host(connectors=connectors, writable=[tmp_path], trust_binaries=True)


def into_sqlite(tmp_path: Path) -> rdlt.Connector:
    """The reference SQLite connector, writing `out.db` in `tmp_path`."""
    return rdlt.Connector(
        "io.rapidbyte.sqlite",
        config={"path": str(tmp_path / "out.db")},
        access=rdlt.Access(write=[tmp_path]),
    )


class Position(Cursor):
    """Where a read resumes: the next row's position."""

    next: int = 0


def cdc(name: str, source: ReadStream, tmp_path: Path) -> rdlt.PipelineBuilder:
    """The pipeline `name`, reading `source`'s stream `rows` as changes, merged by `id`, once."""
    merged = rdlt.stream("rows", read="cdc", write=rdlt.merge("id"))
    return rdlt.pipeline(
        name,
        source=source,
        destination=into_sqlite(tmp_path),
        streams=[merged],
        retry=rdlt.Retry(max_attempts=1),
    )
