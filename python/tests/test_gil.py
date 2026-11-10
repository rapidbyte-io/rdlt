"""A pipeline with no Python in it keeps committing while a Python transform holds the GIL."""

from __future__ import annotations

import queue
import sys
import threading
import time
from datetime import UTC, datetime
from pathlib import Path

import pyarrow as pa

import rdlt
from rdlt import Json

HOLDS = (2.0, 4.0, 8.0, 16.0)
"""Seconds each hold of the GIL lasts, each longer than the last, until one sees two commits."""
WAIT = 60.0
"""Seconds any one wait of the test lasts at most."""


def generated(name: str, rows: int) -> dict[str, Json]:
    """`rows` of the generator's stream `name` into a memory store of the same name."""
    return {
        "version": 1,
        "pipeline": name,
        "source": {
            "connector": "io.rapidbyte.generator",
            "config": {"seed": 1, "streams": [{"name": name, "rows": rows}]},
        },
        "destination": {"connector": "io.rapidbyte.memory", "config": {"store": name}},
        "streams": [{"name": name}],
        "commit": {"rows": 1000},
    }


def commits_of(run: rdlt.Run, landed: queue.SimpleQueue[datetime | None]) -> None:
    """Tells `landed` when each of `run`'s commits landed, then `None` once the run ended."""
    for event in run.events():
        if isinstance(event, rdlt.Event) and event.kind == "committed":
            landed.put(event.at)
    landed.put(None)


def next_commit(landed: queue.SimpleQueue[datetime | None]) -> datetime:
    at = landed.get(timeout=WAIT)
    assert at is not None, "the Rust pipeline ended"
    return at


class Hold:
    """A transform that holds the GIL for `seconds` on its first call with rows."""

    def __init__(self, seconds: float) -> None:
        self.seconds = seconds
        self.window: list[datetime] = []

    def __call__(self, batch: pa.RecordBatch) -> pa.RecordBatch:
        """Answers `batch` as it is, the first with rows after the hold."""
        # The engine's call on no rows learns the output's schema.
        if self.window or batch.num_rows == 0:
            return batch
        interval = sys.getswitchinterval()
        # No thread waiting for the GIL takes it back before the hold ends.
        sys.setswitchinterval(self.seconds + WAIT)
        try:
            self.window.append(datetime.now(UTC))
            deadline = time.monotonic() + self.seconds
            while time.monotonic() < deadline:
                pass
            self.window.append(datetime.now(UTC))
        finally:
            sys.setswitchinterval(interval)
        return batch


def test_a_rust_pipeline_commits_while_a_python_transform_holds_the_gil(connectors: Path) -> None:
    # A host each, so the compute thread the transform holds is not one the Rust pipeline uses.
    rust_host = rdlt.Host(connectors=connectors, trust_binaries=True)
    rust = rdlt.Pipeline.from_document(generated("rust", 10**12)).build(rust_host).start()
    landed: queue.SimpleQueue[datetime | None] = queue.SimpleQueue()
    threading.Thread(target=commits_of, args=(rust, landed), daemon=True).start()
    python = generated("python", 100)
    python["streams"] = [{"name": "python", "transform": [{"code": "holds"}]}]
    within: list[datetime] = []
    try:
        commits = [next_commit(landed)]
        # Each hold is longer than the last, so the test assumes no pace of this machine's: the
        # Rust pipeline commits as fast as it can, and a long enough hold sees two commits.
        for seconds in HOLDS:
            hold = Hold(seconds)
            python_host = rdlt.Host(connectors=connectors, trust_binaries=True)
            rdlt.Pipeline.from_document(python).build(python_host.transform("holds", hold)).run()
            assert len(hold.window) == 2, "the transform held the GIL"
            began, ended = hold.window
            # Commits arrive in order: one after the hold says every commit within it has come.
            while commits[-1] <= ended:
                commits.append(next_commit(landed))
            within = [at for at in commits if began <= at <= ended]
            # A commit under way as the hold began may land in it; a second was made whole in it.
            if len(within) >= 2:
                break
    finally:
        rust.cancel()
    assert len(within) >= 2, f"{len(within)} commits landed in {HOLDS[-1]} s of the GIL held"
