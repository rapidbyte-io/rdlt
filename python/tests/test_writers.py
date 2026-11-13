"""Table writers called at once where a destination runs them at once, one at a time otherwise."""

from __future__ import annotations

import dataclasses
import json
import threading
import time
from collections.abc import Callable, Generator, Iterator, Mapping
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
from rdlt.connector._declared import Placement
from rdlt.connector._destination import (
    _Connected,  # pyright: ignore[reportPrivateUsage] - the wrapper whose calls are counted
    declaration,
)
from rdlt.model import Json
from tests.connectors import memory

PEER_WAIT = 5.0
"""Seconds a call waits for another to be in flight beside it: a bound on the wait, never a
measure, so a destination that is called one at a time is seen to be, and none hangs."""

GUARD_WAIT = 1.0
"""Seconds each call waits for another where none is to come: a thread started beside it would
have begun long before."""


class Overlap:
    """Counts a destination's calls in flight, and the most that were at once."""

    def __init__(self, wait: float = PEER_WAIT) -> None:
        self._changed = threading.Condition()
        self._inside = 0
        self._wait = wait
        self.most = 0
        self.entered = threading.Event()
        """Set once a call has begun."""
        self.ended: list[str] = []
        """Each call's name, as it ends."""

    @property
    def inside(self) -> int:
        """The calls in flight now."""
        with self._changed:
            return self._inside

    @contextmanager
    def call(self, name: str = "") -> Generator[None]:
        """Counts a call while it runs, which first waits for another beside it."""
        with self._changed:
            self._inside += 1
            self.most = max(self.most, self._inside)
            self.entered.set()
            self._changed.notify_all()
            self._changed.wait_for(lambda: self._inside > 1, timeout=self._wait)
        try:
            yield
        finally:
            with self._changed:
                self._inside -= 1
                self.ended.append(name)
                self._changed.notify_all()


_OVERLAPS: dict[str, Overlap] = {}


class _Counted(Config):
    name: str
    writers: int = 1


class _CountedWriter:
    def __init__(self, overlap: Overlap) -> None:
        self.overlap = overlap

    def write(self, segment: int, batch: pa.RecordBatch) -> None:
        """Waits for another call beside it."""
        del segment, batch
        with self.overlap.call("write"):
            pass

    def flush(self) -> WriteStats:
        """Stages nothing."""
        with self.overlap.call("flush"):
            return WriteStats(0, 0)


class _CountedSession:
    def __init__(self, overlap: Overlap) -> None:
        self.overlap = overlap

    def apply_schema(self, change: Mapping[str, Json]) -> None:
        """Waits for another call beside it."""
        with self.overlap.call("apply_schema"):
            del change

    def writer(self, table: Mapping[str, Json]) -> _CountedWriter:
        """A writer whose calls are counted with the session's."""
        del table
        return _CountedWriter(self.overlap)

    def discard_staged(self) -> None:
        """Waits for another call beside it."""
        with self.overlap.call("discard_staged"):
            pass

    def commit(self, meta: Mapping[str, Json]) -> Mapping[str, Json]:
        """Publishes nothing, once it has waited for another call beside it."""
        with self.overlap.call("commit"):
            at: dict[str, Json] = {"secs_since_epoch": 0, "nanos_since_epoch": 0}
            return {"key": meta["key"], "committed_at": at, "rows": 0, "bytes": 0}

    def close(self) -> None:
        """Holds nothing, once it has waited for another call beside it."""
        with self.overlap.call("close"):
            pass


@destination(id="io.example.counted", config=_Counted, version="1.0.0")
class Counted:
    """A destination that counts its calls in flight, running the writers its configuration says."""

    def __init__(self, config: _Counted) -> None:
        self.overlap = _OVERLAPS.setdefault(config.name, Overlap())
        self.writers = config.writers

    def capabilities(self) -> Capabilities:
        """An append-only destination's, with as many writers as its configuration says."""
        return dataclasses.replace(Capabilities.minimal(), max_parallel_writers=self.writers)

    def check(self) -> None:
        """Nothing to check."""

    def open(self, context: OpenContext) -> Opened:
        """A session counting its calls."""
        del context
        return Opened(_CountedSession(self.overlap), 1, None, [])


TABLE = json.dumps(
    {"path": ["rows"], "name": "rows", "version": 1, "generation": None, "merge": None}
)
"""A table writers are opened for, as the native side hands it over."""

META = json.dumps({"key": {"epoch": 1, "seq": 1}})
"""A commit, as much of it as `Counted` reads."""


def opened(name: str, writers: int, *, asked: bool = True) -> tuple[Any, Placement[Any]]:
    """A session of `Counted` as the native side opens one, and the placement keeping it.

    Where `asked`, its instance is asked its capabilities first, as the native side asks as it
    connects.
    """
    _OVERLAPS.setdefault(name, Overlap())
    placement = Placement(declaration(Counted))
    connected = _Connected(lambda: placement)
    placement.connections[connected] = Counted(_Counted(name=name, writers=writers))
    if asked:
        connected.capabilities()
    session, _ = connected.open("counted")
    return session, placement


def running(call: Callable[[], object]) -> Callable[[], None]:
    """Makes `call` on a daemon thread of its own, answering what waits for it to end.

    That waits a bounded time, fails where the call has not ended, and raises what it raised.
    """
    raised: list[BaseException] = []

    def run() -> None:
        try:
            call()
        except BaseException as error:  # noqa: BLE001 - raised again where it is waited for
            raised.append(error)

    thread = threading.Thread(target=run, daemon=True)
    thread.start()

    def waited() -> None:
        thread.join(timeout=PEER_WAIT * 6)
        assert not thread.is_alive(), "the call did not end"
        if raised:
            raise raised[0]

    return waited


def at_once(*calls: Callable[[], object]) -> None:
    """Makes each of `calls` on a thread of its own, all at once, and waits for them."""
    for waited in [running(call) for call in calls]:
        waited()


def after(first: Callable[[], object], second: Callable[[], object], overlap: Overlap) -> None:
    """Makes `first` on a thread of its own, then, once a call has begun, `second` on another."""
    began = running(first)
    assert overlap.entered.wait(timeout=PEER_WAIT)
    then = running(second)
    began()
    then()


def test_table_writers_are_called_at_once_where_their_destination_runs_them_at_once() -> None:
    session, _held = opened("two-writers", 2)
    first, second = session.writer(TABLE), session.writer(TABLE)
    batch = pa.record_batch({"id": [1]})
    at_once(lambda: first.write(1, batch), lambda: second.write(2, batch))
    overlap = _OVERLAPS["two-writers"]
    assert (overlap.most, overlap.ended) == (2, ["write", "write"])


def test_a_destination_that_runs_one_writer_is_never_called_twice_at_once() -> None:
    _OVERLAPS["one-writer"] = Overlap(GUARD_WAIT)
    session, _held = opened("one-writer", 1)
    first, second = session.writer(TABLE), session.writer(TABLE)
    batch = pa.record_batch({"id": [1]})
    change = json.dumps({"add_column": {"table": json.loads(TABLE), "field": []}})
    at_once(
        lambda: first.write(1, batch),
        lambda: second.write(2, batch),
        first.flush,
        session.discard_staged,
        lambda: session.apply_schema(change),
    )
    overlap = _OVERLAPS["one-writer"]
    every = ["apply_schema", "discard_staged", "flush", "write", "write"]
    assert (overlap.most, sorted(overlap.ended)) == (1, every)


def test_a_writer_s_own_calls_come_one_at_a_time_where_writers_run_at_once() -> None:
    _OVERLAPS["own-calls"] = Overlap(GUARD_WAIT)
    session, _held = opened("own-calls", 2)
    writer = session.writer(TABLE)
    batch = pa.record_batch({"id": [1]})
    at_once(lambda: writer.write(1, batch), writer.flush)
    overlap = _OVERLAPS["own-calls"]
    assert (overlap.most, sorted(overlap.ended)) == (1, ["flush", "write"])


def test_a_session_opened_before_its_capabilities_were_asked_is_called_once_at_a_time() -> None:
    _OVERLAPS["unasked"] = Overlap(GUARD_WAIT)
    session, _held = opened("unasked", 2, asked=False)
    first, second = session.writer(TABLE), session.writer(TABLE)
    batch = pa.record_batch({"id": [1]})
    at_once(lambda: first.write(1, batch), lambda: second.write(2, batch))
    overlap = _OVERLAPS["unasked"]
    assert (overlap.most, overlap.ended) == (1, ["write", "write"])


@pytest.mark.parametrize("call", ["apply_schema", "discard_staged"])
def test_a_session_call_runs_beside_a_writer_s_call_where_writers_run_at_once(call: str) -> None:
    name = f"beside-{call}"
    session, _held = opened(name, 2)
    writer = session.writer(TABLE)
    change = json.dumps({"add_column": {"table": json.loads(TABLE), "field": []}})
    beside = getattr(session, call)
    batch = pa.record_batch({"id": [1]})
    at_once(
        lambda: writer.write(1, batch),
        lambda: beside(change) if call == "apply_schema" else beside(),
    )
    overlap = _OVERLAPS[name]
    assert (overlap.most, sorted(overlap.ended)) == (2, sorted([call, "write"]))


def settled(
    session: Any,  # noqa: ANN401 - the session wrapper, reached by its methods' names
    call: str,
    overlap: Overlap,
    began: list[int],
) -> Callable[[], None]:
    """The session's `call`, `commit` or `close`, which first says the calls then in flight."""
    settling = getattr(session, call)

    def settle() -> None:
        began.append(overlap.inside)
        if call == "commit":
            settling(META)
        else:
            settling()

    return settle


@pytest.mark.parametrize("call", ["commit", "close"])
def test_a_session_call_that_settles_waits_for_a_writer_s_call_in_flight(call: str) -> None:
    # Nothing waits for the flush, as for one the engine abandoned: the call waits for it all the
    # same, and begins only once it ended.
    name = f"settled-{call}"
    overlap = _OVERLAPS[name] = Overlap(GUARD_WAIT)
    session, _held = opened(name, 2)
    writer = session.writer(TABLE)
    began: list[int] = []
    after(writer.flush, settled(session, call, overlap, began), overlap)
    assert (began, overlap.most, overlap.ended) == ([1], 1, ["flush", call])


@pytest.mark.parametrize("call", ["commit", "close"])
def test_no_writer_s_call_begins_while_a_session_call_that_settles_runs(call: str) -> None:
    name = f"settling-{call}"
    overlap = _OVERLAPS[name] = Overlap(GUARD_WAIT)
    session, _held = opened(name, 2)
    writer = session.writer(TABLE)
    began: list[int] = []

    def flush() -> None:
        began.append(overlap.inside)
        writer.flush()

    after(settled(session, call, overlap, []), flush, overlap)
    assert (began, overlap.most, overlap.ended) == ([1], 1, [call, "flush"])


def test_close_waits_for_a_call_queued_behind_one_the_engine_abandoned() -> None:
    overlap = _OVERLAPS["queued"] = Overlap(GUARD_WAIT)
    session, _held = opened("queued", 2)
    writer = session.writer(TABLE)
    flushing = running(writer.flush)
    assert overlap.entered.wait(timeout=PEER_WAIT)
    writing = running(lambda: writer.write(1, pa.record_batch({"id": [1]})))
    # The write is counted before it waits behind the flush, so close waits for it too.
    calls = session._calls
    deadline = time.monotonic() + PEER_WAIT
    while calls._in_flight < 2 and time.monotonic() < deadline:
        time.sleep(0.01)
    assert calls._in_flight == 2, "the write was not counted as it waited behind the flush"
    closing = running(session.close)
    for waited in (flushing, writing, closing):
        waited()
    assert (overlap.most, overlap.ended) == (1, ["flush", "write", "close"])


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
