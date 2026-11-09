"""The threads Python code runs on: nothing but Python lies beneath a user's function.

A read's thread and a call's thread are Python's own, started by `_thread`, which returns at
once: an interpreter that exits ends them in Python's frames, and what starts them makes no wait
of its own, so none lands on an exiting interpreter with Rust beneath it.
"""

from __future__ import annotations

import _thread
import asyncio
import threading
from collections.abc import AsyncIterable, Callable, Iterable
from typing import Protocol, cast


class _Out(Protocol):
    """The native side of a partition's read."""

    def push(self, item: object) -> bool:
        """Pushes what the read yielded; false once the read has ended or the item failed it."""
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

    def partitions(self) -> Iterable[str]:
        """The partitions to read."""
        ...

    def read(self, partition: str | None, cursor: str | None, context: object) -> object:
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


def start(  # noqa: PLR0917
    name: str,
    stream: _Stream,
    partition: str | None,
    cursor: str | None,
    context: object,
    out: _Out,
) -> None:
    """Reads the partition on a daemon thread of its own named `name`."""
    _thread.start_new_thread(_read, (name, stream, partition, cursor, context, out))


def _read(  # noqa: PLR0917
    name: str,
    stream: _Stream,
    partition: str | None,
    cursor: str | None,
    context: object,
    out: _Out,
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


def _read_sync(opened: Iterable[object], out: _Out) -> None:
    iterator = iter(opened)
    try:
        for item in iterator:
            if not out.push(item):
                return
    finally:
        close = getattr(iterator, "close", None)
        if close is not None:
            close()


def _read_async(opened: AsyncIterable[object], out: _Out) -> None:
    iterator = aiter(opened)
    loop = asyncio.new_event_loop()
    try:
        while True:
            try:
                item = loop.run_until_complete(anext(iterator))
            except StopAsyncIteration:
                return
            if not out.push(item):
                return
    finally:
        try:
            aclose = getattr(iterator, "aclose", None)
            if aclose is not None:
                loop.run_until_complete(aclose())
        finally:
            loop.close()


def call(function: Callable[..., object], arguments: tuple[object, ...], answer: _Answer) -> None:
    """Calls `function(*arguments)` on a daemon thread of its own and gives `answer` its end."""
    _thread.start_new_thread(_answered, (function, arguments, answer))


def _answered(
    function: Callable[..., object], arguments: tuple[object, ...], answer: _Answer
) -> None:
    threading.current_thread().name = "rdlt-call"
    try:
        value = function(*arguments)
    except BaseException as error:  # noqa: BLE001
        answer.raised(error)
    else:
        answer.returned(value)


def connected(
    connect: Callable[[str], _Source], config: str
) -> tuple[_Source, list[tuple[str, _Stream]]]:
    """The source `connect` makes, with each of its streams and what it says of itself."""
    source = connect(config)
    return source, [(stream.spec, stream) for stream in source.streams]


def checked(source: _Source) -> None:
    """Checks that `source` can read."""
    source.check()


def partitions(stream: _Stream) -> list[str]:
    """The partitions of `stream`."""
    return list(stream.partitions())
