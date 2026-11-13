"""A table read as a database's change stream is: a snapshot, then its changes, which never end.

The snapshot holds keys `0..rows`, each inserted at the position it captured. The changes follow
it, one a position, each changing a key no other change touches: an update, an update that leaves
`value` unchanged, or a delete. Where the changes partition stands is kept in a SQLite file the
configuration names, outside any connection, as a replication slot outlives its clients.
"""

from __future__ import annotations

import sqlite3
import time
from collections.abc import Iterator, Sequence
from contextlib import closing
from typing import NamedTuple

import pyarrow as pa

from rdlt import ReadMode
from rdlt.connector import (
    CURSOR_UNISSUED,
    OP_COLUMN,
    SEQ_COLUMN,
    UNCHANGED_COLUMN,
    ChangeOp,
    Config,
    Cursor,
    InvalidData,
    Partition,
    PartitionPlan,
    PartitionState,
    StreamContext,
    StreamState,
    changes,
    checkpoint,
    source,
    stream,
)

SNAPSHOT = 0
"""The phase the snapshot is read in."""

CHANGES = 1
"""The phase the changes are read in."""

CAPTURED = 100
"""The position the snapshot captured: its rows carry it, and the changes start after it."""

PAGE = 20
"""Rows a change batch holds."""

ROWS = pa.schema(
    [
        pa.field("id", pa.int64()),
        pa.field("value", pa.string()),
        pa.field("n", pa.int64()),
        pa.field("changed_at", pa.timestamp("us", tz="UTC")),
    ]
)
"""The table's columns; `changed_at` is when a row's change happened, its position in
microseconds."""


class Row(NamedTuple):
    """A change row: its op, position and key, its `value` and `n`, and whether it keeps `value`."""

    op: ChangeOp
    position: int
    key: int
    value: str | None
    n: int | None
    unchanged: bool


class ChangelogConfig(Config):
    """The keys the snapshot holds, the changes after it, and the file that keeps the slot.

    `changes` is at most `rows`, so no key changes twice.
    """

    rows: int
    changes: int
    slot: str


class Position(Cursor):
    """Where a read resumes: the next key of the snapshot, or the next change's position."""

    next: int = 0


def change(position: int, rows: int) -> Row:
    """The change at `position`, past the snapshot's: the `i`th changes key `i - 1` of `rows`."""
    ordinal = position - CAPTURED
    key = (ordinal - 1) % rows
    match ordinal % 3:
        case 0:
            return Row(ChangeOp.DELETE, position, key, None, None, unchanged=False)
        case 1:
            return Row(ChangeOp.UPDATE, position, key, f"value {ordinal}", ordinal, unchanged=False)
        case _:
            return Row(ChangeOp.UPDATE, position, key, None, ordinal, unchanged=True)


def expected(rows: int, count: int) -> list[tuple[int, str, int]]:
    """The table `rows` keys and `count` changes leave, as `(id, value, n)` by key."""
    table = {key: (f"value 0-{key}", 0) for key in range(rows)}
    for position in range(CAPTURED + 1, CAPTURED + count + 1):
        row = change(position, rows)
        if row.op is ChangeOp.DELETE:
            del table[row.key]
        else:
            value = table[row.key][0] if row.unchanged else str(row.value)
            table[row.key] = (value, int(row.n or 0))
    return [(key, value, n) for key, (value, n) in sorted(table.items())]


def batch(rows: Sequence[Row]) -> pa.RecordBatch:
    """`rows` as a change batch: the table's columns, then its op, position and unchanged flags.

    A row that leaves `value` unchanged flags it, field 1, in its bitmap.
    """
    return pa.record_batch(
        {
            "id": pa.array([row.key for row in rows], pa.int64()),
            "value": pa.array([row.value for row in rows], pa.string()),
            "n": pa.array([row.n for row in rows], pa.int64()),
            "changed_at": pa.array([row.position for row in rows], pa.timestamp("us", tz="UTC")),
            OP_COLUMN: pa.array([int(row.op) for row in rows], pa.int8()),
            SEQ_COLUMN: pa.array([row.position.to_bytes(16, "big") for row in rows], pa.binary(16)),
            UNCHANGED_COLUMN: pa.array(
                [b"\x02" if row.unchanged else None for row in rows], pa.binary()
            ),
        }
    )


@source(id="io.example.changelog", config=ChangelogConfig, version="1.0.0", acknowledged=True)
class Changelog:
    """Reads its snapshot, a partition that ends, then its changes, one that never ends."""

    def __init__(self, config: ChangelogConfig) -> None:
        self.config = config
        with self._slot() as slot:
            slot.execute(
                "CREATE TABLE IF NOT EXISTS slot (partition TEXT PRIMARY KEY, next INTEGER)"
            )

    def _slot(self) -> closing[sqlite3.Connection]:
        return closing(sqlite3.connect(self.config.slot, timeout=30, isolation_level=None))

    def check(self) -> None:
        """The slot's file opens."""
        with self._slot():
            pass

    def planned(self, state: StreamState) -> PartitionPlan:
        """The snapshot until it is read to its end, then the changes, from after its position."""
        if state.phase == CHANGES:
            return PartitionPlan([Partition("changes", unbounded=True)])
        snapshot = state.partitions.get("snapshot")
        ended = snapshot is PartitionState.DONE or (
            isinstance(snapshot, Position) and snapshot.next == self.config.rows
        )
        if not ended:
            return PartitionPlan(["snapshot"])
        return PartitionPlan(
            [Partition("changes", unbounded=True)],
            phase=CHANGES,
            starts={"changes": Position(next=CAPTURED + 1)},
        )

    @stream(
        primary_key="id",
        cursor=Position,
        read_modes=[ReadMode.CDC],
        schema=ROWS,
        change_time="changed_at",
        plan="planned",
        committed="settled",
        acknowledged="standing",
    )
    def rows(self, start: Position, ctx: StreamContext) -> Iterator[object]:
        """Reads the snapshot's keys, or the changes, from `start`."""
        if ctx.partition.id == "snapshot":
            yield from self._snapshot(start, ctx)
        else:
            yield from self._changes(start, ctx)

    def _snapshot(self, start: Position, ctx: StreamContext) -> Iterator[object]:
        if start.next > self.config.rows:
            message = f"the snapshot has no key {start.next}"
            raise InvalidData(message, code=CURSOR_UNISSUED)
        for first in range(start.next, self.config.rows, PAGE):
            last = min(first + PAGE, self.config.rows)
            inserted = [
                Row(ChangeOp.INSERT, CAPTURED, key, f"value 0-{key}", 0, unchanged=False)
                for key in range(first, last)
            ]
            yield changes(batch(inserted))
            # Every other page too, so a read no barrier reaches still has points to resume from.
            if ctx.checkpoint_due() or last == self.config.rows or first // PAGE % 2 == 1:
                yield checkpoint(Position(next=last))

    def _changes(self, start: Position, ctx: StreamContext) -> Iterator[object]:
        end = CAPTURED + self.config.changes + 1
        if not CAPTURED < start.next <= end:
            message = f"the changes have no position {start.next}"
            raise InvalidData(message, code=CURSOR_UNISSUED)
        for first in range(start.next, end, PAGE):
            last = min(first + PAGE, end)
            yield changes(
                batch([change(position, self.config.rows) for position in range(first, last)])
            )
            yield checkpoint(Position(next=last))
        # Caught up, a following read waits for changes that never come until it is asked to stop.
        while ctx.follows and not ctx.stopped():
            time.sleep(0.01)

    def settled(self, cursors: Sequence[tuple[str, Position]]) -> None:
        """Keeps the changes partition's position in the slot, never moving it back."""
        with self._slot() as slot:
            for partition, cursor in cursors:
                if partition != "changes":
                    continue
                slot.execute(
                    "INSERT INTO slot VALUES (?, ?) ON CONFLICT (partition) DO UPDATE "
                    "SET next = max(next, excluded.next)",
                    (partition, cursor.next),
                )

    def standing(self, partition: str) -> Position | None:
        """Where `partition` stands in the slot: the changes' last position settled, or none."""
        with self._slot() as slot:
            found = slot.execute(
                "SELECT next FROM slot WHERE partition = ?", (partition,)
            ).fetchone()
        return None if found is None else Position(next=found[0])
