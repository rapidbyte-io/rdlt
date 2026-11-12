"""The threads Python code runs on: nothing but Python lies beneath a user's function.

Every call from Rust, a partition's read among them, is made on one of the process's daemon
threads, which `_thread` starts and which run `serve` for as long as the process lives: an
interpreter that exits ends them in Python's frames, or leaves them waiting, and none ends on its
own, since a thread that ends runs its libraries' teardown, which races the process's exit.
"""

from __future__ import annotations

import _thread
import asyncio
import contextlib
import threading
import traceback
from collections import deque
from collections.abc import AsyncIterable, Callable, Iterable
from contextvars import ContextVar
from typing import TYPE_CHECKING, Any, Protocol, cast

if TYPE_CHECKING:
    import pyarrow as pa


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


class _Context(Protocol):
    """A read's native context, as its router sends through it."""

    def notify(self, notice: object) -> None:
        """Gives the engine `notice`, waiting until the engine takes it."""
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
    """The calls a daemon thread is handed."""

    def next(self) -> tuple[Callable[..., object], tuple[object, ...], _Answer]:
        """Hands over the last call's answer, then waits for the next call to make."""
        ...

    def abandon(self) -> None:
        """Hands over the last call's answer, where one waits, without waiting for another."""
        ...


_IDLE = "rdlt-call"
"""The name of a daemon thread that waits for a call, or makes one other than a read."""


def read(  # noqa: PLR0917
    name: str,
    stream: _Stream,
    partition: str,
    cursor: str | None,
    context: object,
    out: Out,
) -> None:
    """Reads the partition to its end, naming this daemon thread `name` meanwhile.

    It says how the read ended, through `out`.
    """
    thread = threading.current_thread()
    thread.name = name
    try:
        opened = stream.read(partition, cursor, context)
        if hasattr(opened, "__aiter__"):
            _read_async(cast("AsyncIterable[object]", opened), out, context)
        else:
            _read_sync(cast("Iterable[object]", opened), out)
        # What the read held is let go before the engine is told it ended.
        del opened
    except BaseException as error:  # noqa: BLE001
        out.failed(error)
    else:
        out.finished()
    finally:
        thread.name = _IDLE


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


def _read_async(opened: AsyncIterable[object], out: Out, context: object) -> None:
    asyncio.run(pushed(opened, out, context))


async def pushed(opened: AsyncIterable[object], out: Out, context: object) -> None:
    """Reads `opened` in one task: a timeout or a context variable may span its yields.

    What it yields and what its context `context` tells the engine are sent in the order they
    were said, none of them waiting for the engine with the event loop held.
    """
    router = Router(out, context)
    token = _ROUTER.set(router)
    iterator = aiter(opened)
    try:
        async for item in iterator:
            if not await router.yielded(item):
                return
        await router.sent()
    finally:
        aclose = getattr(iterator, "aclose", None)
        if aclose is not None:
            await aclose()
        router.stop()
        _ROUTER.reset(token)


_ROUTER: ContextVar[Router | None] = ContextVar("rdlt_router", default=None)


def router(context: object) -> Router | None:
    """The router of the async read whose context is `context`, asked from its event loop's thread.

    None otherwise, as for a sync read, whose context gives the engine what it says itself.
    """
    found = _ROUTER.get()
    if found is None or found.context is not context or found.thread != threading.get_ident():
        return None
    return found


class Router:
    """Sends what an async read yields and what its context tells the engine, in the order said.

    Each is sent at once where the engine has room and nothing said before it waits. Otherwise it
    waits its turn, and a task sends it once the engine has room, so the loop keeps running. A
    yield returns once its item is sent, as a sync read's waits for the engine; a notice returns
    at once. A Rust read awaits each notice, so it holds one at a time; the router holds, after the
    last yield waiting, at most one of each kind, a replan being said once and a lag being its
    newest, which is what the engine makes of a Rust read's.
    """

    def __init__(self, out: Out, context: object) -> None:
        self.context = context
        self.thread = threading.get_ident()
        self._out = out
        # What waits its turn: an item, the kind of notice it is, none for a yield, and the future
        # its yield waits on.
        self._waiting: deque[tuple[object, str | None, asyncio.Future[bool] | None]] = deque()
        self._sending: asyncio.Task[None] | None = None

    async def yielded(self, item: object) -> bool:
        """Sends `item` after what was said before it; false once the read has gone."""
        if not self._waiting and self._out.has_room():
            return self._out.push(item)
        done: asyncio.Future[bool] = asyncio.get_running_loop().create_future()
        self._wait(item, None, done)
        return await done

    def notify(self, notice: object, kind: str) -> None:
        """Gives the engine `notice`, of `kind`, after what was said before it."""
        if not self._waiting and self._out.has_room():
            cast("_Context", self.context).notify(notice)
            return
        # One of its kind said since the last yield waiting is said again by this one.
        for index in range(len(self._waiting) - 1, -1, -1):
            said = self._waiting[index][1]
            if said is None:
                break
            if said == kind:
                del self._waiting[index]
                break
        self._wait(notice, kind, None)

    async def sent(self) -> None:
        """Waits until what was said has been sent."""
        if self._sending is not None:
            await self._sending

    def stop(self) -> None:
        """Stops sending, dropping what waits."""
        self._waiting.clear()
        if self._sending is not None:
            self._sending.cancel()

    def _wait(self, item: object, kind: str | None, done: asyncio.Future[bool] | None) -> None:
        self._waiting.append((item, kind, done))
        if self._sending is None:
            self._sending = asyncio.get_running_loop().create_task(self._send())

    async def _send(self) -> None:
        try:
            while self._waiting:
                await _room(self._out)
                item, kind, done = self._waiting.popleft()
                if kind is not None:
                    cast("_Context", self.context).notify(item)
                elif done is not None and not done.done():
                    done.set_result(self._out.push(item))
        finally:
            self._sending = None


async def _room(out: Out) -> None:
    """Waits for the engine to have room, so what else runs on the loop keeps running."""
    turns = 0
    while not out.has_room():
        await asyncio.sleep(0 if turns < _TURNS_BEFORE_PAUSING else _PAUSE)
        turns += 1


def serve(calls: _Calls) -> None:
    """Makes each call `calls` is handed, on this daemon thread, for as long as the process lives.

    What a call held, its batch among it, is let go before its answer is handed over. A user's
    function's exception, `SystemExit` and `KeyboardInterrupt` among them, is its call's answer;
    what else escapes is a bug of rdlt's own, said here. The thread then answers what it held or
    was handed, with its answer where one was read and as unanswered otherwise, and waits for good,
    off the pool, rather than end.
    """
    try:
        threading.current_thread().name = _IDLE
        while True:
            _made(*calls.next())
    except BaseException:  # noqa: BLE001
        # Guarded by a `try`, which runs no code of its own as `suppress` would.
        try:  # noqa: SIM105
            traceback.print_exc()
        except BaseException:  # noqa: BLE001, S110
            pass
    # Out of the handler, so what the escape's traceback held of a call, its answer among it, is
    # let go, and the call is told it went unanswered.
    try:
        calls.abandon()
    finally:
        _parked()


def _parked() -> None:
    """Waits for good with the GIL released, as a thread never woken."""
    never = _thread.allocate_lock()
    never.acquire()
    never.acquire()


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


def awaited(
    loop: asyncio.AbstractEventLoop,
    future: asyncio.Future[object],
    value: object,
    error: BaseException | str | None,
) -> None:
    """Hands `value`, or `error` where there is one, to `future` on its event loop `loop`.

    An `error` given as text is the report of a failure of rdlt's own, raised as its `rdlt.Error`.
    Nothing is handed to a future already done, as a cancelled one is, or through a loop that has
    closed.
    """
    if isinstance(error, str):
        from rdlt.errors import (  # noqa: PLC0415 - loaded with the package
            _from_report,  # pyright: ignore[reportPrivateUsage]
        )

        error = _from_report(error)

    def settled() -> None:
        if future.done():
            return
        if error is None:
            future.set_result(value)
        else:
            future.set_exception(error)

    with contextlib.suppress(RuntimeError):
        loop.call_soon_threadsafe(settled)
