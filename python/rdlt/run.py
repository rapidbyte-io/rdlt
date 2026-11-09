# pyright: reportPrivateUsage=false
"""A run in progress: its events, its status, its stops and its end."""

from __future__ import annotations

import asyncio
import enum
from collections.abc import AsyncIterator, Iterator
from typing import Literal, cast

from rdlt import _native
from rdlt.errors import _from_report
from rdlt.model import Event, Json, Lagged, Outcome, Status, _parse


class StopMode(enum.StrEnum):
    """How a run stops."""

    AFTER_COMMIT = "after_commit"
    """Stop reading, commit what is sealed, and end cleanly."""
    NOW = "now"
    """End the current attempt at once; its unpublished staging is discarded at the next open."""


def _item(text: str) -> Event | Lagged:
    fields = _parse(text)
    if "lagged" in fields:
        lagged = cast("dict[str, Json]", fields["lagged"])
        return Lagged(cast("int", lagged["dropped"]))
    return Event.from_fields(cast("dict[str, Json]", fields["event"]))


def _ended(answer: tuple[str, str | None]) -> Outcome:
    outcome, error = answer
    if error is not None:
        raise _from_report(error, outcome)
    return Outcome.from_fields(_parse(outcome))


class Events:
    """A subscription to a run's events: each an `Event`, or a `Lagged` for what was missed.

    Reading it keeps its run going: the run is cancelled once every handle to it, this one
    included, is dropped.
    """

    def __init__(self, native: _native.Events, pipeline: str) -> None:
        self._native = native
        self._pipeline = pipeline

    def __repr__(self) -> str:
        return f"Events({self._pipeline!r})"

    def __iter__(self) -> Iterator[Event | Lagged]:
        return self

    def __next__(self) -> Event | Lagged:
        return _item(next(self._native))

    def __aiter__(self) -> AsyncIterator[Event | Lagged]:
        return self

    async def __anext__(self) -> Event | Lagged:
        while (text := self._native.take()) is None:
            if not await self._native.readable():
                raise StopAsyncIteration
        return _item(text)


class Run:
    """A pipeline's run; dropping every reference cancels it, unless a cancelled await left it."""

    def __init__(self, native: _native.Run, pipeline: str) -> None:
        self._native = native
        self._pipeline = pipeline

    def __repr__(self) -> str:
        return f"Run({self._pipeline!r})"

    def events(self) -> Events:
        """A subscription to the run's events.

        The first subscription sees every event from the run's first; a later one sees them from
        now on, or, having fallen too far behind, a `Lagged` saying how many it missed.
        """
        return Events(self._native.events(), self._pipeline)

    def status(self) -> Status:
        """Where the run stands now.

        Raises:
            rdlt.StoppedError: the run was cancelled.
        """
        return Status.from_fields(_parse(self._native.status()))

    def stop(self, mode: StopMode | Literal["after_commit", "now"] = StopMode.AFTER_COMMIT) -> None:
        """Asks the run to stop."""
        self._native.stop(str(mode))

    def wait(self) -> Outcome:
        """Waits for the run's end, the GIL released.

        As the CLI does, a first Ctrl-C asks the run to stop after its next commit and waits on;
        a second cancels it now and raises `KeyboardInterrupt`.

        Raises:
            rdlt.Error: the subclass for the error that ended the run, its `outcome` set;
                `rdlt.StoppedError` where the run was cancelled.
        """
        return _ended(self._native.wait())

    def cancel(self) -> None:
        """Cancels the run now; what it staged and did not commit is discarded at the next open."""
        self._native.cancel()

    async def wait_async(self) -> Outcome:
        """Awaits the run's end.

        Cancelling the awaiting task, as `asyncio.timeout` and `asyncio.wait_for` do, asks the run
        to stop after its next commit and raises `CancelledError` at once. The run finishes in
        the background, this handle stays usable, a later `await run.wait_async()` gives its
        outcome, and `run.cancel()` stops it now.

        Raises:
            rdlt.Error: the subclass for the error that ended the run, its `outcome` set;
                `rdlt.StoppedError` where the run was cancelled.
        """
        try:
            return _ended(await self._native.wait_async())
        except asyncio.CancelledError:
            self._native.detach()
            raise
