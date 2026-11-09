"""A partition's thread: a read's generator runs with nothing but Python beneath it.

The thread is Python's own, started by `_thread`, which returns at once: an interpreter that exits
ends it in Python's frames, and what starts it holds the GIL throughout, so no wait of its own
lands on an exiting interpreter with Rust beneath it.
"""

from __future__ import annotations

import _thread
import asyncio
import threading
from collections.abc import AsyncIterable, Iterable
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
    def read(self, partition: str | None, cursor: str | None, context: object) -> object:
        """Opens the partition's read: an iterator or an async iterable of what it yields."""
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
