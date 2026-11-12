"""Exits with 0 once a run of Python into Python has ended, as a script that falls off its end does.

A stream function feeds a transform, and a destination class placed in process lands what it
answers; the script then returns, and the interpreter finalizes and exits.
"""

from __future__ import annotations

import sys
from collections.abc import Iterator
from pathlib import Path

# Appended, so the installed `rdlt` still comes first on the path.
sys.path.append(str(Path(__file__).resolve().parents[2]))

import pyarrow as pa

import rdlt
from rdlt.connector import stream
from tests.connectors import memory


@stream(name="rows")
def rows() -> Iterator[object]:
    for n in range(3000):
        yield {"id": n, "amount": n * 1.5}


@rdlt.transform()
def kept(batch: pa.RecordBatch) -> pa.RecordBatch:
    return batch


outcome = (
    rdlt.pipeline(
        "ran",
        source=rows,
        destination=rdlt.local(memory.Memory, config={"store": "ran"}),
        streams=[rdlt.stream("rows", transform=[kept])],
        commit=rdlt.Commit(rows=100),
    )
    .build(rdlt.Host())
    .run()
)
if outcome.report.status is not rdlt.RunStatus.SUCCEEDED:
    sys.exit(3)
