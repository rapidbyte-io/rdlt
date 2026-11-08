"""Documents the suites build: the generator's rows into SQLite."""

from __future__ import annotations

from pathlib import Path

from rdlt import Json


def generated(name: str, rows: int, database: Path, *, partitions: int = 1) -> dict[str, Json]:
    """`rows` rows of the generator's stream `rows` into the SQLite database `database`."""
    return {
        "version": 1,
        "pipeline": name,
        "source": {
            "connector": "io.rapidbyte.generator",
            "config": {
                "seed": 7,
                "streams": [{"name": "rows", "rows": rows, "partitions": partitions}],
            },
        },
        "destination": {
            "connector": "io.rapidbyte.sqlite",
            "config": {"path": str(database)},
            "access": {"write": [str(database.parent)]},
        },
        "streams": [{"name": "rows"}],
    }
