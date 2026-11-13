"""The threads Python code runs on: nothing but Python lies beneath a user's function.

Every call from Rust, a partition's read among them, is made on one of the process's daemon
threads, which `_thread` starts and which run `serve` for as long as the process lives: an
interpreter that exits ends them in Python's frames, or leaves them waiting, and none ends on its
own, since a thread that ends runs its libraries' teardown, which races the process's exit.

Each call starts in a context of its own, as on a fresh thread. Thread-local state, by contrast, is
the thread's, as in any pool.

An async read runs on an event loop of its own on its daemon thread, which never waits for the
engine: an item's push that finds no room is sent by the runtime, and a notice that finds none
waits for the runtime to see room come, the runtime then making a socket the loop watches
readable, so the loop runs its other tasks meanwhile.
"""

from __future__ import annotations

import _thread
import asyncio
import contextlib
import threading
import traceback
from collections import deque
from collections.abc import AsyncIterable, Callable, Iterable
from contextvars import Context, ContextVar
from typing import TYPE_CHECKING, Any, Protocol, cast

if TYPE_CHECKING:
    import pyarrow as pa


class Out(Protocol):
    """The native side of a partition's read."""

    def push(self, item: object) -> bool:
        """Pushes what a sync read yielded, waiting for the engine.

        False once the read has ended or the item failed it.
        """
        ...

    def send(self, item: object) -> bool | None:
        """Sends what an async read yielded without waiting for the engine.

        False once the read has ended or the item failed it, and none where it waits for room,
        which `watched` then says came; the rest of the item is then sent by `resume`.
        """
        ...

    def notify(self, notice: object) -> bool | None:
        """Gives the engine a notice an async read said, without waiting for it.

        False once the read has ended, and none where there is no room, which `watched` then
        says came; the notice is not taken, to be given again then.
        """
        ...

    def resume(self) -> bool | None:
        """Sends the rest of the item whose push waited for room, as `send` does.

        None while that push still waits.
        """
        ...

    def watched(self) -> int:
        """The descriptor readable once what waited for room was sent, or room came."""
        ...

    def woken(self) -> None:
        """Takes what made the watched descriptor readable."""
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

    def plan(
        self,
        phase: int,
        positions: list[tuple[str, bool, str | None]],
        generation: int | None,
        completed: list[int],
    ) -> tuple[int | None, list[tuple[str, bool]], list[tuple[str, str]]]:
        """The phase, each partition and whether it never ends, and each start, given its state."""
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

    @property
    def discovers(self) -> bool:
        """Whether it answers its catalog itself."""
        ...

    def check(self) -> None:
        """Verifies it can read."""
        ...


class _Discovering(Protocol):
    def discover(self) -> str:
        """Its catalog, each stream's spec, as JSON."""
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
    router = Router(out, context, asyncio.get_running_loop())
    token = _ROUTER.set(router)
    # The router stops watching for room however the iteration ends, its cleanup's raising too.
    try:
        iterator = aiter(opened)
        try:
            async for item in iterator:
                if not await router.yielded(item):
                    return
        finally:
            aclose = getattr(iterator, "aclose", None)
            if aclose is not None:
                await aclose()
        # A read that ended well sends all it said, what its cleanup said among it.
        await router.sent()
    finally:
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

    Each is sent at once where nothing said before it waits; one at a time, so what is said while
    an item is sent goes after the whole of it. What finds no room is sent once the engine has
    room, the loop waiting meanwhile for a socket the native side makes readable. A yield returns
    once its item is sent, as a sync read's waits for the engine; a notice returns at once. A
    Rust read awaits each notice, so it holds one at a time; the router holds, after the last
    yield waiting, at most one of each kind, a replan being said once and a lag being its newest,
    which is what the engine makes of a Rust read's.
    """

    def __init__(self, out: Out, context: object, loop: asyncio.AbstractEventLoop) -> None:
        self.context = context
        self.thread = threading.get_ident()
        self._out = out
        self._loop = loop
        # What waits its turn: an item, the rest of one part-sent, or a notice; the kind of notice
        # it is, none for a yield; and the future its yield waits on.
        self._waiting: deque[tuple[object, str | None, asyncio.Future[bool] | None]] = deque()
        self._sending: asyncio.Task[None] | None = None
        self._woken: asyncio.Future[None] | None = None
        self._watched = out.watched()
        try:
            loop.add_reader(self._watched, self._wake)
        except NotImplementedError as refused:
            message = (
                "an async read waits for room in the engine through a socket its event loop "
                f"watches, and {type(loop).__name__} cannot watch one: it has no add_reader"
            )
            raise RuntimeError(message) from refused

    async def yielded(self, item: object) -> bool:
        """Sends `item` after what was said before it; false once the read has gone."""
        done: asyncio.Future[bool]
        if self._waiting or self._sending is not None:
            done = self._loop.create_future()
            self._wait(item, None, done)
            return await done
        # The item holds the head while it is made into pushes, so what its making says, as a
        # stream's generator may, goes after the whole of it.
        self._waiting.append((_REST, None, None))
        sent = self._out.send(item)
        if sent is not None:
            self._waiting.popleft()
            return sent
        done = self._loop.create_future()
        self._waiting[0] = (_REST, None, done)
        if self._sending is None:
            self._sending = self._loop.create_task(self._send())
        return await done

    def notify(self, notice: object, kind: str) -> None:
        """Gives the engine `notice`, of `kind`, after what was said before it."""
        if not self._waiting and self._sending is None:
            if self._out.notify(notice) is not None:
                return
        else:
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
        """Stops sending, dropping what waits, and stops watching for room."""
        self._waiting.clear()
        if self._sending is not None:
            self._sending.cancel()
        self._loop.remove_reader(self._watched)

    def _wait(self, said: object, kind: str | None, done: asyncio.Future[bool] | None) -> None:
        self._waiting.append((said, kind, done))
        if self._sending is None:
            self._sending = self._loop.create_task(self._send())

    async def _send(self) -> None:
        try:
            while self._waiting:
                said, kind, done = self._waiting[0]
                if done is not None and done.done():
                    # A yield cancelled: no more of its item is made or sent, as a Rust read
                    # dropped sends no more.
                    self._waiting.popleft()
                    continue
                if said is _REST:
                    sent = self._out.resume()
                elif kind is None:
                    sent = self._out.send(said)
                else:
                    sent = self._out.notify(said)
                if sent is None:
                    if kind is None:
                        self._waiting[0] = (_REST, None, done)
                    await self._room()
                    continue
                self._waiting.popleft()
                if done is not None and not done.done():
                    done.set_result(sent)
        finally:
            self._sending = None

    async def _room(self) -> None:
        self._woken = self._loop.create_future()
        await self._woken

    def _wake(self) -> None:
        self._out.woken()
        woken = self._woken
        if woken is not None and not woken.done():
            woken.set_result(None)


_REST = object()
"""What waits in place of an item whose push waited for room: the rest of it, which goes first."""


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
        value = Context().run(function, *arguments)
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
) -> tuple[_Source, list[tuple[str, _Stream]], bool]:
    """The source `connect` makes for `host`, each stream with its spec, and if it discovers."""
    source = connect(config, host)
    return source, [(stream.spec, stream) for stream in source.streams], source.discovers


def discovered(source: _Discovering) -> str:
    """The catalog `source` answers, as JSON."""
    return source.discover()


def method(target: object, name: str, arguments: tuple[object, ...]) -> object:
    """`target`'s method `name`, called with `arguments`."""
    return getattr(target, name)(*arguments)


def checked(source: _Source) -> None:
    """Checks that `source` can read."""
    source.check()


def planned(
    stream: _Stream,
    phase: int,
    positions: list[tuple[str, bool, str | None]],
    generation: int | None,
    completed: list[int],
) -> tuple[int | None, list[tuple[str, bool]], list[tuple[str, str]]]:
    """The plan of `stream`, given its committed state, as the native side reads it.

    The plan is its phase, each partition with whether it never ends, and where each partition of
    a new phase starts, its cursor as JSON. The state is its phase, each partition's position, its
    id, whether it is done and its cursor as JSON, the full read in progress and the full reads
    recently completed.
    """
    return stream.plan(phase, positions, generation, completed)


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

        # A report that cannot be raised answers with why, rather than leave the future waiting.
        try:
            error = _from_report(error)
        except BaseException as unraised:  # noqa: BLE001
            error = unraised

    def settled() -> None:
        if future.done():
            return
        if error is None:
            future.set_result(value)
        else:
            future.set_exception(error)

    with contextlib.suppress(RuntimeError):
        loop.call_soon_threadsafe(settled)
