"""What an author's method answers: the answer its docs state, or a failure naming the method."""

from __future__ import annotations

import gc
import json
import threading
import warnings
from collections.abc import Callable, Iterator
from typing import Any, cast

import pyarrow as pa
import pytest

import rdlt
from rdlt.connector import (
    Config,
    Cursor,
    Opened,
    Receipt,
    WriteStats,
    destination,
    source,
    stream,
)
from rdlt.connector import _destination as destinations
from rdlt.connector import _placed as placed
from rdlt.connector import _source as sources
from rdlt.connector._declared import Placement
from rdlt.model import Json

TABLE = json.dumps(
    {"path": ["rows"], "name": "rows", "version": 1, "generation": None, "merge": None}
)
"""A table, as the native side hands it over."""

CHANGE = json.dumps({"add_column": {"table": json.loads(TABLE), "field": []}})
"""A schema change, as the native side hands it over."""

META = json.dumps(
    {
        "key": {"epoch": 1, "seq": 1},
        "after": None,
        "segments": [{"first": 1, "last": 1}, {"first": 4, "last": 6}],
        "abandoned": [{"first": 2, "last": 2}],
        "state_delta": [],
        "finish_generations": [],
        "child_tables": [],
        "drop_tables": [],
    }
)
"""A commit of segments 1 and 4 to 6 that abandons segment 2, as the native side hands it over."""

BATCH = pa.record_batch({"id": [1]})


def _answering(kind: str) -> Callable[..., object]:
    """A method or function that answers as `kind` says: async, as a generator, or `False`."""
    if kind == "async":

        async def asynchronous(*arguments: object) -> None:
            del arguments

        return asynchronous
    if kind == "generator":

        def generating(*arguments: object) -> Iterator[None]:
            del arguments
            yield

        return generating

    def false(*arguments: object) -> bool:
        del arguments
        return False

    return false


def _refused(method: str, call: Callable[[], object]) -> None:
    """Asserts `call` fails naming `method`, leaving no coroutine that was never awaited."""
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        with pytest.raises(TypeError, match=method):
            call()
        gc.collect()
    unawaited = [
        str(warning.message) for warning in caught if "never awaited" in str(warning.message)
    ]
    assert not unawaited, unawaited


class _Writer:
    def write(self, segment: int, batch: pa.RecordBatch) -> None:
        """Stages nothing."""
        del segment, batch

    def flush(self) -> WriteStats:
        """Staged nothing."""
        return WriteStats(0, 0)


class _Session:
    made: type = _Writer
    told: list[object]

    def __init__(self) -> None:
        self.told = []

    def apply_schema(self, change: object) -> None:
        """Changes nothing."""
        del change

    def writer(self, table: object) -> object:
        """A writer of `made`."""
        del table
        return self.made()

    def discard_staged(self) -> None:
        """Holds nothing staged."""

    def commit(self, meta: Any) -> dict[str, Json]:  # noqa: ANN401 - read as an author reads it
        """Keeps which of segments 0 to 6 the commit publishes and abandons, and publishes none."""
        self.told.append([segment in meta["segments"] for segment in range(7)])
        self.told.append([segment in meta["abandoned"] for segment in range(7)])
        at: dict[str, Json] = {"secs_since_epoch": 0, "nanos_since_epoch": 0}
        return {"key": meta["key"], "committed_at": at, "rows": 0, "bytes": 0}

    def close(self) -> None:
        """Holds nothing open."""
        self.told.append("closed")


class _Answering:
    opened: type = _Session

    def __init__(self, config: Config) -> None:
        del config

    def check(self) -> None:
        """Nothing to check."""

    def open(self, context: object) -> Opened:
        """A session of `opened`, kept as `session`."""
        del context
        self.session = self.opened()
        return Opened(self.session, 1, None, [])

    def published(self, table: object) -> Iterator[pa.RecordBatch]:
        """What a table published: nothing."""
        del table
        yield from ()


_CONNECTION = ("check", "capabilities", "open", "published")
_SESSION = ("apply_schema", "writer", "discard_staged", "commit", "close")
_WRITER = ("write", "flush")


def _connected(method: str = "", kind: str = "") -> tuple[Any, Placement[Any]]:
    """The native side's connection to a destination whose `method` answers as `kind` says."""
    answering = {method: _answering(kind)} if method else {}
    writer = type("Writer", (_Writer,), answering if method in _WRITER else {})
    session = type(
        "Session", (_Session,), {"made": writer, **(answering if method in _SESSION else {})}
    )
    body = {"opened": session, **(answering if method in _CONNECTION else {})}
    named = f"io.example.{method or 'plain'}.{kind or 'plain'}"
    connector = destination(id=named, config=Config, version="1.0.0")(
        type("Answering", (_Answering,), body)
    )
    placement = Placement(destinations.declaration(connector))
    connected = destinations._Connected(lambda: placement)  # pyright: ignore[reportPrivateUsage]
    placement.connections[connected] = connector(Config())
    return connected, placement


_CALLS: dict[str, Callable[[Any], object]] = {
    "check": lambda connected: connected.check(),
    "capabilities": lambda connected: connected.capabilities(),
    "open": lambda connected: connected.open("p"),
    "published": lambda connected: connected.published(TABLE),
    "apply_schema": lambda connected: connected.open("p")[0].apply_schema(CHANGE),
    "writer": lambda connected: connected.open("p")[0].writer(TABLE),
    "discard_staged": lambda connected: connected.open("p")[0].discard_staged(),
    "commit": lambda connected: connected.open("p")[0].commit(META),
    "close": lambda connected: connected.open("p")[0].close(),
    "write": lambda connected: connected.open("p")[0].writer(TABLE).write(1, BATCH),
    "flush": lambda connected: connected.open("p")[0].writer(TABLE).flush(),
}
"""How the native side calls each method, as the engine does."""


@pytest.mark.parametrize(
    ("method", "kind"),
    [
        (method, kind)
        for method in _CALLS
        for kind in ("async", "generator", "false")
        # A destination's `published` yields what it published.
        if (method, kind) != ("published", "generator")
    ],
)
def test_a_destination_method_answering_what_it_does_not_state_fails_naming_itself(
    method: str, kind: str
) -> None:
    connected, _held = _connected(method, kind)
    _refused(rf"{method}\(\)", lambda: _CALLS[method](connected))


def test_a_commit_is_told_its_segment_sets_as_sets_an_author_asks_with_in() -> None:
    connected, held = _connected()
    connected.open("p")[0].commit(META)
    told = next(iter(held.connections.values())).session.told
    assert told[0] == [False, True, False, False, True, True, True]
    assert told[1] == [False, False, True, False, False, False, False]


def test_a_commit_s_segment_set_refuses_the_text_of_a_segment_id() -> None:
    told: list[object] = []

    class Asking(_Session):
        def commit(self, meta: Any) -> dict[str, Json]:  # noqa: ANN401 - read as an author reads it
            """Asks whether the commit publishes the text of a segment id."""
            told.append("1" in meta["segments"])
            return super().commit(meta)

    connected, _held = _connected()
    instance = next(iter(_held.connections.values()))
    instance.opened = Asking
    with pytest.raises(TypeError):
        connected.open("p")[0].commit(META)
    assert told == []


class _Unclosed:
    """A session that says whether it was closed."""

    closed = False

    def apply_schema(self, change: object) -> None:
        """Changes nothing."""
        del change

    def writer(self, table: object) -> _Writer:
        """A writer that stages nothing."""
        del table
        return _Writer()

    def discard_staged(self) -> None:
        """Holds nothing staged."""

    def commit(self, meta: object) -> Receipt:
        """Never called."""
        raise NotImplementedError(meta)

    def close(self) -> None:
        """Says it was closed."""
        self.closed = True


@pytest.mark.parametrize(
    ("landed", "state"),
    [(None, [object()]), (["no", "receipt"], []), (None, [{"key": "k", "value": object()}])],
)
def test_a_session_whose_open_answered_what_rdlt_cannot_tell_is_closed(
    landed: object, state: list[object]
) -> None:
    session = _Unclosed()
    connected, _held = _connected()
    instance = next(iter(_held.connections.values()))

    def opening(context: object) -> Opened:
        del context
        return Opened(session, 1, cast("Any", landed), cast("Any", state))

    instance.open = opening
    with pytest.raises(TypeError):
        connected.open("p")
    assert session.closed


class _Since(Cursor):
    after: int = 0


@pytest.mark.parametrize("kind", ["async", "generator", "false"])
def test_a_source_s_check_answering_what_it_does_not_state_fails_naming_itself(kind: str) -> None:
    @source(id=f"io.example.check.{kind}", config=Config, version="1.0.0")
    class Checking:
        def __init__(self, config: Config) -> None:
            del config

        def check(self) -> None:
            """Replaced below."""

        @stream()
        def rows(self) -> Iterator[object]:
            """Reads nothing."""
            yield from ()

    Checking.check = _answering(kind)  # pyright: ignore[reportAttributeAccessIssue]
    placement = Placement(sources.declaration(Checking))
    connected = placed._Connected(lambda: placement)  # pyright: ignore[reportPrivateUsage]
    placement.connections[connected] = Checking(Config())
    _refused(r"check\(\)", connected.check)


@pytest.mark.parametrize("method", ["streams", "discover"])
def test_an_async_streams_or_discover_fails_as_it_is_asked(method: str) -> None:
    def check(self: object) -> None:
        del self

    def streams(self: object) -> list[object]:
        del self
        return []

    def made(self: object, config: Config) -> None:
        del self, config

    body: dict[str, object] = {
        "__init__": made,
        "check": check,
        "streams": streams,
        method: _answering("async"),
    }
    declared: Any = source(id=f"io.example.async.{method}", config=Config, version="1.0.0")(
        cast("Any", type("Async", (), body))
    )
    placement = Placement(sources.declaration(declared))
    connected = placed._Connected(lambda: placement)  # pyright: ignore[reportPrivateUsage]
    placement.connections[connected] = declared(Config())
    asked = (lambda: connected.streams) if method == "streams" else connected.discover
    _refused(rf"{method}\(\)", asked)


def _native(**given: object) -> Any:  # noqa: ANN401 - the native side's stream
    """The stream `stream(**given)` makes of a read of nothing, as the native side holds it."""

    def rows(ctx: object) -> Iterator[object]:
        del ctx
        yield from ()

    made = stream(name="rows", **given)(rows)  # pyright: ignore[reportArgumentType]
    return placed._native_stream(made, lambda: made, None)  # pyright: ignore[reportPrivateUsage]


@pytest.mark.parametrize("kind", ["async", "generator", "false"])
def test_a_stream_s_committed_answering_what_it_does_not_state_fails_naming_itself(
    kind: str,
) -> None:
    told = _native(cursor=_Since, committed=_answering(kind))
    _refused("committed=", lambda: told.committed([("whole", None)]))


@pytest.mark.parametrize(
    ("named", "kind"),
    [
        ("partitions", "async"),
        ("partitions", "false"),
        ("plan", "async"),
        ("plan", "generator"),
        ("plan", "false"),
        ("acknowledged", "async"),
        ("acknowledged", "generator"),
        ("acknowledged", "false"),
    ],
)
def test_a_stream_s_plan_or_position_answering_what_it_does_not_state_fails_naming_itself(
    named: str, kind: str
) -> None:
    held = _native(cursor=_Since, **{named: _answering(kind)})
    if named == "acknowledged":
        _refused("acknowledged=", lambda: held.acknowledged("whole"))
    else:
        _refused(f"{named}=", lambda: held.plan(0, [], None, []))


@pytest.mark.parametrize("kind", ["async", "false"])
def test_a_read_answering_neither_an_iterable_nor_an_async_one_fails_naming_its_stream(
    kind: str,
) -> None:
    made = stream(name="rows")(_answering(kind))
    held = placed._native_stream(made, lambda: made, None)  # pyright: ignore[reportPrivateUsage]
    _refused("rows", lambda: held.read("whole", None, cast("Any", None)))


class _AsyncWriter(_Writer):
    async def write(self, segment: int, batch: pa.RecordBatch) -> None:  # pyright: ignore[reportIncompatibleMethodOverride]
        """Would stage `batch`, were it awaited."""
        del segment, batch


class _AsyncWrites(_Session):
    made = _AsyncWriter


@destination(id="io.example.async-writes", config=Config, version="1.0.0")
class AsyncWrites(_Answering):
    """A destination whose table writers' `write` is async."""

    opened = _AsyncWrites


def test_a_load_into_a_destination_whose_write_is_async_fails_and_commits_nothing() -> None:
    committed: list[object] = []
    lock = threading.Lock()

    def told(cursors: object) -> None:
        with lock:
            committed.append(cursors)

    @stream(cursor=_Since, committed=told)
    def users(since: _Since) -> Iterator[object]:
        for n in range(since.after + 1, 4):
            yield {"id": n}
            yield rdlt.connector.checkpoint(_Since(after=n))

    built = rdlt.pipeline(
        "async-writes",
        source=users,
        destination=rdlt.local(AsyncWrites, config={}),
        commit=rdlt.Commit(rows=1),
        retry=rdlt.Retry(max_attempts=1),
    )
    with pytest.raises(rdlt.Error) as failed:
        built.build(rdlt.Host()).run()
    report = failed.value.report()
    assert "write()" in " ".join([report.message, *report.causes]), report
    assert committed == []
