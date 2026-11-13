"""rdlt's Python quick start: a Python function's rows into SQLite, exactly once."""

from __future__ import annotations

import sys
from collections.abc import Iterator
from pathlib import Path

import rdlt
from rdlt.connector import Cursor, checkpoint, stream


class Since(Cursor):
    """Where `users` resumes: the last page committed."""

    after: int = -1


@stream(primary_key="id", cursor=Since)
def users(since: Since) -> Iterator[object]:
    """Three pages of ten users, each followed by the checkpoint after it."""
    for page in range(since.after + 1, 3):
        yield [{"id": page * 10 + n, "name": f"user {page * 10 + n}"} for n in range(10)]
        yield checkpoint(Since(after=page))


def main(connectors: Path, data: Path) -> rdlt.Outcome:
    """Loads `users` into `users.db` in `data`, with the SQLite connector in `connectors`."""
    # Sandboxed on Linux, where the connector may write in `data` alone; macOS has no sandbox.
    host = rdlt.Host(connectors=connectors, writable=data, trust_binaries=sys.platform == "darwin")
    sqlite = rdlt.Connector(
        "io.rapidbyte.sqlite",
        config={"path": str(data / "users.db")},
        access=rdlt.Access(write=data),
    )
    merged = rdlt.stream("users", write=rdlt.merge("id"))
    pipeline = rdlt.pipeline("users", source=users, destination=sqlite, streams=[merged])
    return pipeline.build(host).run()


if __name__ == "__main__":
    outcome = main(Path(sys.argv[1]).resolve(), Path(sys.argv[2]).resolve())
    print(f"{outcome.report.status.value}: committed {outcome.report.rows} rows")
