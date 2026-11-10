"""A run of the installed package, which the release makes of each wheel it builds.

`python smoke.py CONNECTORS`: a Python resource's rows, through a Python transform, into the
memory destination, whose binary `CONNECTORS` holds.
"""

from __future__ import annotations

import sys
from collections.abc import Iterator
from pathlib import Path

import pyarrow as pa

import rdlt

ROWS = 10


@rdlt.resource(name="rows")
def rows() -> Iterator[object]:
    """Ten rows, as dicts."""
    yield [{"n": n} for n in range(ROWS)]


@rdlt.transform()
def marked(batch: pa.RecordBatch) -> pa.RecordBatch:
    """`batch` with a column of its own beside its rows."""
    return batch.append_column("marked", pa.array([True] * batch.num_rows, pa.bool_()))


def main(connectors: Path) -> rdlt.Outcome:
    """Loads `rows` into a memory store with the connector `connectors` holds."""
    host = rdlt.Host(connectors=connectors, trust_binaries=True)
    memory = rdlt.Connector("io.rapidbyte.memory", config={"store": "smoke"})
    streams = [rdlt.stream("rows", transform=[marked])]
    return (
        rdlt.pipeline("smoke", source=rows, destination=memory, streams=streams).build(host).run()
    )


if __name__ == "__main__":
    outcome = main(Path(sys.argv[1]))
    if outcome.report.rows != ROWS:
        sys.exit(f"committed {outcome.report.rows} rows, not {ROWS}")
    print(f"rdlt {rdlt.__version__}, {sys.version}: committed {ROWS} rows")  # noqa: T201
