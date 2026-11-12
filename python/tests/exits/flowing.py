"""Exits with 7 while a run's Python transforms flow into a Python destination, as a script ends.

A stream function feeds a transform, and a destination class placed in process lands what it
answers; the script exits once the transform has been called, with the run still going.
"""

from __future__ import annotations

import itertools
import sys
import threading
from collections.abc import Iterator
from pathlib import Path

# Appended, so the installed `rdlt` still comes first on the path.
sys.path.append(str(Path(__file__).resolve().parents[2]))

import pyarrow as pa

import rdlt
from rdlt.connector import stream
from tests.connectors import memory

called = threading.Event()


@stream(name="rows")
def rows() -> Iterator[object]:
    for n in itertools.count():
        yield {"id": n, "amount": n * 1.5}


@rdlt.transform()
def kept(batch: pa.RecordBatch) -> pa.RecordBatch:
    called.set()
    return batch


run = (
    rdlt.pipeline(
        "flowing",
        source=rows,
        destination=rdlt.local(memory.Memory, config={"store": "flowing"}),
        streams=[rdlt.stream("rows", transform=[kept])],
        commit=rdlt.Commit(rows=100),
    )
    .build(rdlt.Host())
    .start()
)
sys.exit(7 if called.wait(30) else 3)
