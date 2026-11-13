"""A Python read as the native source reads it: its spec, its plan and its generator."""

from __future__ import annotations

import json
from collections.abc import AsyncIterable, Callable, Iterable, Sequence
from typing import Any, cast

from rdlt import _native
from rdlt.connector._context import (
    Partition,
    PartitionPlan,
    PartitionState,
    StreamContext,
    StreamState,
)
from rdlt.connector._cursor import Cursor
from rdlt.connector._spec import StreamSpec, described

Read = Callable[..., Iterable[object] | AsyncIterable[object]]
"""A stream's read: takes its cursor first where it declares one, and `ctx` where it names it."""

Committed = Sequence[tuple[str, Any]]
"""Committed cursors: each partition's id, `whole` for a stream that is not split, and its cursor,
of the stream's `Cursor` type, or `None` for a stream that declares none."""

Acknowledged = Callable[[str], Cursor | None]
"""Where a stream stands for a partition outside the engine: the cursor it was last told is
committed, or `None` where it keeps none."""

Planned = tuple[int | None, list[tuple[str, bool]], list[tuple[str, str]]]
"""A plan as the native side reads it: its phase, each partition's id and whether it never ends,
and each start's partition and cursor as JSON."""


class Stream:
    """One stream, as the native source holds it: `read` and `plan` reach the author's code.

    They reach it weakly, so a function or a class that holds its pipeline can be collected with
    that pipeline; what may still read it holds it in the open.
    """

    def __init__(
        self,
        spec: StreamSpec,
        read: Read,
        cursor: type[Cursor] | None,
        plan: Callable[[StreamState], PartitionPlan] | None,
        *,
        stated: bool,
        committed: Callable[[Committed], None] | None,
        acknowledged: Acknowledged | None,
        cursor_version: int,
        takes_context: bool,
    ) -> None:
        self._name = str(spec.name)
        self._read = read
        self._cursor = cursor
        self._plan = plan
        self._committed = committed
        self._acknowledged = acknowledged
        self._takes_context = takes_context
        self._spec = json.dumps(
            {
                "spec": described(spec),
                "planned": plan is not None,
                "stated": stated,
                "committed": committed is not None,
                "acknowledged": acknowledged is not None,
                "cursor_version": cursor_version,
            }
        )

    @property
    def spec(self) -> str:
        """The stream's description, as JSON."""
        return self._spec

    def plan(
        self,
        phase: int,
        positions: Sequence[tuple[str, bool, str | None]],
        generation: int | None,
        completed: Sequence[int],
    ) -> Planned:
        """The phase and partitions to read, and where partitions of a new phase start.

        `phase`, `positions`, `generation` and `completed` are the stream's committed state, each
        position a partition's id, whether it is done, and its cursor as JSON: none where its
        plan names no state.

        Raises:
            TypeError: a start is no cursor of the stream's type.
        """
        if self._plan is None:
            return (None, [], [])
        stood = {
            partition: self._position(cursor, done=done) for partition, done, cursor in positions
        }
        planned = self._plan(StreamState(phase, stood, generation, tuple(completed)))
        partitions = [
            (each.id, each.unbounded) if isinstance(each, Partition) else (each, False)
            for each in planned.partitions
        ]
        starts = [self._start(partition, cursor) for partition, cursor in planned.starts.items()]
        return (planned.phase, partitions, starts)

    def _start(self, partition: str, cursor: object) -> tuple[str, str]:
        """Where a plan starts `partition`, as JSON."""
        if self._cursor is None or not isinstance(cursor, self._cursor):
            expected = "no cursor" if self._cursor is None else self._cursor.__name__
            message = (
                f"stream {self._name!r}: the plan starts partition {partition!r} at a "
                f"{type(cursor).__name__}: a start is the stream's cursor, {expected}"
            )
            raise TypeError(message)
        return (partition, cursor.model_dump_json())

    def _position(self, cursor: str | None, *, done: bool) -> object:
        """Where a partition stands: done, or `cursor` read as the stream's, none without one."""
        if done:
            return PartitionState.DONE
        return None if self._cursor is None else _parsed(self._cursor, cursor)

    def committed(self, cursors: Sequence[tuple[str, str | None]]) -> None:
        """Tells the author's code that `cursors`, each as JSON, are committed."""
        if self._committed is None:
            return
        of = self._cursor
        parsed = [
            (partition, None if of is None else _parsed(of, text)) for partition, text in cursors
        ]
        self._committed(parsed)

    def acknowledged(self, partition: str) -> str | None:
        """Where the author's code says the stream stands for `partition`, as JSON.

        Raises:
            TypeError: it answered neither the stream's cursor nor `None`.
        """
        if self._acknowledged is None:
            return None
        found = self._acknowledged(partition)
        if found is None:
            return None
        if self._cursor is None or not isinstance(found, self._cursor):
            expected = "no cursor" if self._cursor is None else self._cursor.__name__
            message = (
                f"stream {self._name!r}: acknowledged= answered {type(found).__name__}: it "
                f"answers the stream's cursor, {expected}, or None"
            )
            raise TypeError(message)
        return found.model_dump_json()

    def read(
        self, partition: str, cursor: str | None, context: _native.StreamContext
    ) -> Iterable[object] | AsyncIterable[object]:
        """Opens the read, its cursor parsed and the `ctx` it names built."""
        del partition
        arguments: list[object] = []
        if self._cursor is not None:
            arguments.append(_parsed(self._cursor, cursor))
        keywords: dict[str, object] = {"ctx": StreamContext(context)} if self._takes_context else {}
        return self._read(*arguments, **keywords)


def as_plan(stream: str, answered: object, *, whole: bool) -> PartitionPlan:
    """What a stream's `plan=`, where `whole`, or its `partitions=` answered, as a plan.

    `plan=` answers a whole plan; `partitions=` its partitions, of the phase the state records.

    Raises:
        TypeError: `plan=` answered no `PartitionPlan`, or `partitions=` one string.
    """
    if not whole:
        return PartitionPlan(cast("Sequence[str | Partition]", answered))
    if not isinstance(answered, PartitionPlan):
        message = (
            f"stream {stream!r}: plan= answered a {type(answered).__name__}: it answers a "
            "PartitionPlan"
        )
        raise TypeError(message)
    return answered


def _parsed(of: type[Cursor], cursor: str | None) -> Cursor:
    """`cursor`, JSON, as an `of`; `of`'s default, where a read starts, for none."""
    return of() if cursor is None else of.model_validate_json(cursor)
