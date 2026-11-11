"""Ten thousand batches through a Python source and transform leave memory flat.

The source checkpoints. One that never does is read as one segment, whose peak glibc's arenas
keep once it ends, so resident memory grows across such runs though nothing is kept (ADR 0055).
"""

from __future__ import annotations

import gc
import os
import subprocess
import tracemalloc
from collections.abc import Iterator
from pathlib import Path
from typing import Any, cast

import pyarrow as pa
import pyarrow.compute as pc

import rdlt

MIB = 1 << 20
BATCHES = 10_000
"""Batches the measured run reads."""
ROWS = 1_000
"""Rows a batch, so a column of the measured run's batches is 80 MB in all."""
CHECKPOINTS = 100
"""Batches between checkpoints, so what a run holds at once does not grow with its length."""


class Upto(rdlt.connector.Cursor):
    """The last batch read."""

    batch: int = -1


def resident() -> int:
    """This process's resident bytes, as `ps` says on Linux and macOS alike."""
    said = subprocess.run(
        ["/bin/ps", "-o", "rss=", "-p", str(os.getpid())],
        capture_output=True,
        check=True,
        text=True,
        timeout=30,
    )
    return int(said.stdout) * 1024


def settled() -> tuple[int, int]:
    """Resident bytes and the bytes pyarrow has allocated, once what is free has been let go."""
    gc.collect()
    pa.default_memory_pool().release_unused()
    return resident(), pa.total_allocated_bytes()


def loaded(batches: int, connectors: Path, name: str) -> None:
    @rdlt.connector.stream(name="numbers", cursor=Upto)
    def numbers(upto: Upto) -> Iterator[object]:
        for n in range(upto.batch + 1, batches):
            yield pa.record_batch({"n": pa.array([n] * ROWS, pa.int64())})
            if n % CHECKPOINTS == CHECKPOINTS - 1:
                yield rdlt.connector.checkpoint(Upto(batch=n))

    @rdlt.transform()
    def plus(batch: pa.RecordBatch) -> pa.RecordBatch:
        n = cast("pa.Array[Any]", batch.column("n"))
        return batch.append_column("m", pc.add(n, pa.scalar(1)))

    host = rdlt.Host(connectors=connectors, trust_binaries=True)
    memory = rdlt.Connector("io.rapidbyte.memory", config={"store": name})
    streams = [rdlt.stream("numbers", transform=[plus])]
    pipeline = rdlt.pipeline(name, source=numbers, destination=memory, streams=streams)
    assert pipeline.build(host).run().report.rows == batches * ROWS


def test_ten_thousand_batches_leave_memory_flat(connectors: Path) -> None:
    # The memory destination runs in a process of its own: what it keeps is not counted here.
    loaded(1_000, connectors, "warm")
    resident_before, arrow_before = settled()
    tracemalloc.start()
    before = tracemalloc.take_snapshot()
    loaded(BATCHES, connectors, "measured")
    resident_after, arrow_after = settled()
    after = tracemalloc.take_snapshot()
    tracemalloc.stop()
    own = (tracemalloc.Filter(inclusive=False, filename_pattern=tracemalloc.__file__),)
    differences = after.filter_traces(own).compare_to(before.filter_traces(own), "filename")
    # Counted in blocks, not bytes: a table of the interpreter's own that grows once is one block,
    # where what a run kept of each batch would be thousands.
    blocks = sum(difference.count_diff for difference in differences)
    assert blocks < BATCHES // 4, f"Python's heap holds {blocks} blocks more"
    assert arrow_after - arrow_before < 1 * MIB, (
        f"pyarrow holds {arrow_after - arrow_before} bytes more"
    )
    assert resident_after - resident_before < 96 * MIB, (
        f"resident memory grew {resident_after - resident_before} bytes"
    )
