"""Table writers a run calls at once where its destination runs them at once."""

from __future__ import annotations

import dataclasses
import threading
from collections.abc import Generator, Iterator, Mapping
from contextlib import contextmanager
from typing import Any, cast

import pyarrow as pa
import pytest

import rdlt
from rdlt.connector import (
    Capabilities,
    Config,
    InvalidData,
    OpenContext,
    Opened,
    StreamContext,
    WriteStats,
    destination,
    stream,
)
from rdlt.model import Json
from tests.connectors import memory

PEER_WAIT = 5.0
"""Seconds a call waits for another to be in flight beside it: a bound on the wait, never a
measure, so a destination that is called one at a time is seen to be, and none hangs."""


class Overlap:
    """Counts a destination's calls in flight, and the most that were at once."""

    def __init__(self) -> None:
        self._changed = threading.Condition()
        self._inside = 0
        self.most = 0

    @contextmanager
    def call(self) -> Generator[None]:
        """Counts a call while it runs, which first waits for another beside it."""
        with self._changed:
            self._inside += 1
            self.most = max(self.most, self._inside)
            self._changed.notify_all()
            self._changed.wait_for(lambda: self._inside > 1, timeout=PEER_WAIT)
        try:
            yield
        finally:
            with self._changed:
                self._inside -= 1
                self._changed.notify_all()


_OVERLAPS: dict[str, Overlap] = {}


class _Staging(Config):
    store: str
    fails: bool = False


_EVENTS: dict[str, list[object]] = {}

_OPENED: dict[str, int] = {}
"""How many table writers each store's sessions opened."""


class _StagingWriter:
    """The memory destination's table writer, each call counted as it waits for another beside it.

    Where its destination fails, a flush of the lower half's rows raises once the upper half's
    flush has staged.
    """

    def __init__(self, writer: Any, store: str, *, fails: bool) -> None:  # noqa: ANN401 - memory's writer
        self.writer, self.store, self.fails = writer, store, fails
        self.lower = False

    def write(self, segment: int, batch: pa.RecordBatch) -> None:
        """Buffers `batch` as the memory destination does."""
        with _OVERLAPS[self.store].call():
            ids = cast("list[int]", batch.column("id").to_pylist())
            self.lower = self.lower or min(ids) < 100
            self.writer.write(segment, batch)

    def flush(self) -> WriteStats:
        """Stages what is buffered, or, for the lower half where it fails, raises."""
        events = _EVENTS[self.store]
        with _OVERLAPS[self.store].call():
            if self.fails and self.lower:
                _STAGED.setdefault(self.store, threading.Event()).wait(timeout=PEER_WAIT)
                message = "the lower half's rows are refused"
                raise InvalidData(message, code="refused_rows")
            stats = self.writer.flush()
            events.append(("staged", stats.rows))
            _STAGED.setdefault(self.store, threading.Event()).set()
            return stats


_STAGED: dict[str, threading.Event] = {}


class _StagingSession:
    def __init__(self, session: Any, store: str, *, fails: bool) -> None:  # noqa: ANN401 - memory's session
        self.session, self.store, self.fails = session, store, fails

    def apply_schema(self, change: Mapping[str, Json]) -> None:
        """As the memory destination applies it."""
        self.session.apply_schema(change)

    def writer(self, table: Mapping[str, Json]) -> _StagingWriter:
        """The memory destination's writer, counted."""
        _OPENED[self.store] = _OPENED.get(self.store, 0) + 1
        return _StagingWriter(self.session.writer(table), self.store, fails=self.fails)

    def discard_staged(self) -> None:
        """As the memory destination discards."""
        self.session.discard_staged()

    def commit(self, meta: Mapping[str, Json]) -> Mapping[str, Json]:
        """As the memory destination commits."""
        return self.session.commit(meta)

    def close(self) -> None:
        """Says it closed."""
        _EVENTS[self.store].append("close")


@destination(id="io.example.staging", config=_Staging, version="1.0.0")
class Staging:
    """The memory destination, running two writers at once, each call counted."""

    def __init__(self, config: _Staging) -> None:
        self.memory = memory.Memory(memory.MemoryConfig(store=config.store))
        self.store, self.fails = config.store, config.fails
        _OVERLAPS.setdefault(self.store, Overlap())
        _EVENTS.setdefault(self.store, [])

    def capabilities(self) -> Capabilities:
        """An append-only destination's, running two writers at once."""
        return dataclasses.replace(Capabilities.minimal(), max_parallel_writers=2)

    def check(self) -> None:
        """Memory is always there."""

    def open(self, context: OpenContext) -> Opened:
        """A session of the memory destination, counted."""
        opened = self.memory.open(context)
        session = _StagingSession(opened.session, self.store, fails=self.fails)
        return Opened(session, opened.epoch, opened.landed, opened.state)


@stream(primary_key="id", partitions=lambda: ["lower", "upper"])
def halves(ctx: StreamContext) -> Iterator[object]:
    """Ten rows a partition: 0 to 9 in the lower, 100 to 109 in the upper."""
    start = 0 if ctx.partition.id == "lower" else 100
    yield pa.record_batch({"id": list(range(start, start + 10))})


def loaded(store: str, *, fails: bool) -> rdlt.Pipeline:
    """A pipeline reading both halves at once into `Staging`, through two writers, one a half."""
    return rdlt.pipeline(
        store,
        source=halves,
        destination=rdlt.local(Staging, config={"store": store, "fails": fails}),
        resources=rdlt.Resources(writers=2, partitions=2),
        retry=rdlt.Retry(max_attempts=1),
    ).build(rdlt.Host())


def test_an_engine_s_writers_stage_at_once_where_the_destination_runs_them_at_once() -> None:
    loaded("staged-at-once", fails=False).run()
    # The engine wrote through two table writers, one a half.
    assert _OPENED["staged-at-once"] == 2
    assert _OVERLAPS["staged-at-once"].most == 2
    assert memory.published("staged-at-once", "halves") == 20


def test_a_writer_that_fails_beside_another_fails_the_attempt_and_publishes_neither() -> None:
    with pytest.raises(rdlt.DestinationError) as failed:
        loaded("failed-at-once", fails=True).run()
    assert failed.value.report().connector_code == "refused_rows"
    assert _OPENED["failed-at-once"] == 2
    assert _OVERLAPS["failed-at-once"].most == 2
    # The upper half staged, and the session closed after it: none of it was published.
    assert _EVENTS["failed-at-once"] == [("staged", 10), "close"]
    assert memory.published("failed-at-once", "halves") == 0
