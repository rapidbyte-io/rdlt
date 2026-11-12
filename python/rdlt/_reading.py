"""The threads Python code runs on: nothing but Python lies beneath a user's function.

A read's thread and a call's thread are Python's own, started by `_thread`, which returns at
once: an interpreter that exits ends them in Python's frames, and what starts them makes no wait
of its own, so none lands on an exiting interpreter with Rust beneath it.
"""

from __future__ import annotations

import _thread
import asyncio
import sys
import threading
from collections.abc import AsyncIterable, Callable, Iterable
from typing import TYPE_CHECKING, Any, Protocol, cast

if TYPE_CHECKING:
    import pyarrow as pa


def _ended() -> None:
    """Lets `threading` forget this thread, which is ending, as CPython 3.13 and later do.

    Before 3.13, `threading` keeps for ever the object `current_thread()` makes for a thread
    `_thread` started, unless a later thread has its ident:
    https://github.com/python/cpython/issues/114423. Its registry and lock are looked up on each
    call, since a forked child's `threading` makes new ones.
    """
    if sys.version_info < (3, 13):
        registry = vars(threading)
        with registry["_active_limbo_lock"]:
            registry["_active"].pop(_thread.get_ident(), None)


def _forgotten(run: Callable[..., None], *arguments: object) -> None:
    """Runs `run`, then lets `threading` forget this thread, once what `run` held is let go."""
    try:
        run(*arguments)
    finally:
        _ended()


# An async read waits for room in the engine with its event loop free: for this many turns of
# the loop, since room usually comes at once, then with this many seconds between asks.
_TURNS_BEFORE_PAUSING = 100
_PAUSE = 0.001


class Out(Protocol):
    """The native side of a partition's read."""

    def push(self, item: object) -> bool:
        """Pushes what the read yielded; false once the read has ended or the item failed it."""
        ...

    def has_room(self) -> bool:
        """Whether a push would be answered without waiting for the engine."""
        ...

    def finished(self) -> None:
        """Says the read has yielded everything."""
        ...

    def failed(self, error: BaseException) -> None:
        """Says the read ended with `error`."""
        ...


class _Stream(Protocol):
    @property
    def spec(self) -> str:
        """The stream's description, as JSON."""
        ...

    def partitions(
        self,
        phase: int,
        positions: list[tuple[str, bool, str | None]],
        generation: int | None,
        completed: list[int],
    ) -> Iterable[tuple[str, bool]]:
        """Each partition to read, and whether it never ends, given the stream's state."""
        ...

    def committed(self, cursors: list[tuple[str, str | None]]) -> None:
        """Tells the stream its cursors are committed."""
        ...

    def acknowledged(self, partition: str) -> str | None:
        """Where the stream stands for `partition` outside the engine, as JSON."""
        ...

    def read(self, partition: str, cursor: str | None, context: object) -> object:
        """Opens the partition's read: an iterator or an async iterable of what it yields."""
        ...


class _Source(Protocol):
    @property
    def streams(self) -> Iterable[_Stream]:
        """The streams it reads."""
        ...

    def check(self) -> None:
        """Verifies it can read."""
        ...


class _Answer(Protocol):
    """Where a call's thread says how it ended."""

    def returned(self, value: object) -> None:
        """Says the call returned `value`."""
        ...

    def raised(self, error: BaseException) -> None:
        """Says the call raised `error`."""
        ...


class _Calls(Protocol):
    """What one thread of Rust hands its daemon thread to call."""

    def next(self) -> tuple[Callable[..., object], tuple[object, ...], _Answer] | None:
        """The next call to make, waiting for it; `None` once its thread of Rust has ended."""
        ...


def start(  # noqa: PLR0917
    name: str,
    stream: _Stream,
    partition: str,
    cursor: str | None,
    context: object,
    out: Out,
) -> None:
    """Reads the partition on a daemon thread of its own named `name`."""
    _thread.start_new_thread(_forgotten, (_read, name, stream, partition, cursor, context, out))


def _read(  # noqa: PLR0917
    name: str,
    stream: _Stream,
    partition: str,
    cursor: str | None,
    context: object,
    out: Out,
) -> None:
    """Reads the partition to its end and says how it ended."""
    threading.current_thread().name = name
    try:
        opened = stream.read(partition, cursor, context)
        if hasattr(opened, "__aiter__"):
            _read_async(cast("AsyncIterable[object]", opened), out)
        else:
            _read_sync(cast("Iterable[object]", opened), out)
    except BaseException as error:  # noqa: BLE001
        out.failed(error)
    else:
        out.finished()


def _read_sync(opened: Iterable[object], out: Out) -> None:
    iterator = iter(opened)
    try:
        for item in iterator:
            if not out.push(item):
                return
    finally:
        close = getattr(iterator, "close", None)
        if close is not None:
            close()


def _read_async(opened: AsyncIterable[object], out: Out) -> None:
    asyncio.run(pushed(opened, out))


async def pushed(opened: AsyncIterable[object], out: Out) -> None:
    """Reads `opened` in one task: a timeout or a context variable may span its yields."""
    iterator = aiter(opened)
    try:
        async for item in iterator:
            await _room(out)
            if not out.push(item):
                return
    finally:
        aclose = getattr(iterator, "aclose", None)
        if aclose is not None:
            await aclose()


async def _room(out: Out) -> None:
    """Waits for the engine to have room, so what else runs on the loop keeps running."""
    turns = 0
    while not out.has_room():
        await asyncio.sleep(0 if turns < _TURNS_BEFORE_PAUSING else _PAUSE)
        turns += 1


def serve(calls: _Calls) -> None:
    """Makes each call `calls` hands over on a daemon thread of its own, until `calls` closes."""
    _thread.start_new_thread(_forgotten, (_served, calls))


def _served(calls: _Calls) -> None:
    threading.current_thread().name = "rdlt-call"
    while (call := calls.next()) is not None:
        _made(*call)
        # What a call held, its batch among it, stays no longer than the call.
        del call


def _made(function: Callable[..., object], arguments: tuple[object, ...], answer: _Answer) -> None:
    try:
        value = function(*arguments)
    except BaseException as error:  # noqa: BLE001
        answer.raised(error)
    else:
        answer.returned(value)


def transformed(function: Callable[[pa.RecordBatch], object], batch: object) -> object:
    """`function` over the Rust batch `batch`, its answer exported for Rust to read.

    pyarrow releases the GIL inside both, and a thread whose interpreter exits as it takes the
    GIL back must have only Python beneath it.
    """
    import pyarrow as pa  # noqa: PLC0415

    answered = function(pa.record_batch(cast("Any", batch)))
    export = getattr(answered, "__arrow_c_array__", None)
    if export is None:
        message = (
            f"a transform answered {type(answered).__name__}: it must answer one batch, "
            "anything with __arrow_c_array__"
        )
        raise TypeError(message)
    return list(export())


def connected(
    connect: Callable[[str, str | None], _Source], config: str, host: str | None
) -> tuple[_Source, list[tuple[str, _Stream]]]:
    """The source `connect` makes for `host`, with each stream and what it says of itself."""
    source = connect(config, host)
    return source, [(stream.spec, stream) for stream in source.streams]


def method(target: object, name: str, arguments: tuple[object, ...]) -> object:
    """`target`'s method `name`, called with `arguments`."""
    return getattr(target, name)(*arguments)


def checked(source: _Source) -> None:
    """Checks that `source` can read."""
    source.check()


def partitions(
    stream: _Stream,
    phase: int,
    positions: list[tuple[str, bool, str | None]],
    generation: int | None,
    completed: list[int],
) -> list[tuple[str, bool]]:
    """The partitions of `stream`, each with whether it never ends, given its committed state.

    The state is its phase, each partition's position, its id, whether it is done and its cursor
    as JSON, the full read in progress and the full reads recently completed.
    """
    return list(stream.partitions(phase, positions, generation, completed))


def committed(stream: _Stream, cursors: list[tuple[str, str | None]]) -> None:
    """Tells `stream` that `cursors`, each a partition's and as JSON, are committed."""
    stream.committed(cursors)


def acknowledged(stream: _Stream, partition: str) -> str | None:
    """Where `stream` stands for `partition` outside the engine, as JSON; `None` where nowhere."""
    return stream.acknowledged(partition)
