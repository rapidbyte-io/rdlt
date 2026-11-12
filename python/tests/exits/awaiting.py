"""Exits with 7 while an async read waits for room in the engine, as a script ends.

An async stream function yields lists of rows faster than a transform that sleeps takes them;
the script exits once the read's event loop has run while a yield waited, with the run still
going. The read yields rows, not Arrow: pyarrow's C++ beneath Rust frames as the interpreter
exits can abort the process on CPython 3.11 and 3.12, a case ADR 0055 accepts.
"""

from __future__ import annotations

import asyncio
import itertools
import sys
import threading
import time
from collections.abc import AsyncIterator
from pathlib import Path

# Appended, so the installed `rdlt` still comes first on the path.
sys.path.append(str(Path(__file__).resolve().parents[2]))

import pyarrow as pa

import rdlt
from rdlt.connector import stream
from tests.connectors import memory

waited = threading.Event()
watchers: list[asyncio.Task[None]] = []


@stream(name="rows")
async def rows() -> AsyncIterator[object]:
    yielding = False

    async def watching() -> None:
        while True:
            if yielding:
                waited.set()
            await asyncio.sleep(0.001)

    watchers.append(asyncio.create_task(watching()))
    for start in itertools.count(step=100):
        yielding = True
        yield [{"id": n} for n in range(start, start + 100)]
        yielding = False


@rdlt.transform()
def slowed(batch: pa.RecordBatch) -> pa.RecordBatch:
    time.sleep(0.002)
    return batch


run = (
    rdlt.pipeline(
        "awaiting",
        source=rows,
        destination=rdlt.local(memory.Memory, config={"store": "awaiting"}),
        streams=[rdlt.stream("rows", transform=[slowed])],
        commit=rdlt.Commit(rows=100),
    )
    .build(rdlt.Host())
    .start()
)
sys.exit(7 if waited.wait(30) else 3)
