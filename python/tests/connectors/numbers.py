"""A source of one stream with a cursor that declares no partitions, as most connectors are."""

from __future__ import annotations

from collections.abc import Iterator
from typing import Literal

from rdlt.connector import Config, Cursor, checkpoint, source, stream


class NumbersConfig(Config):
    """How many rows the stream holds, and where a read of it stops."""

    rows: int
    end: Literal["rows", "never"]
    """`rows`: on rows after a checkpoint halfway; `never`: having never checkpointed."""


class Since(Cursor):
    """The last id a read checkpointed."""

    after: int = -1


@source(id="io.example.numbers", config=NumbersConfig, version="1.0.0")
class Numbers:
    """Reads the ids after its cursor, in the partition of a stream that declares none."""

    def __init__(self, config: NumbersConfig) -> None:
        self.config = config

    def check(self) -> None:
        """Reads need nothing checked first."""

    @stream(primary_key="id", cursor=Since)
    def numbers(self, since: Since) -> Iterator[object]:
        """The ids after `since`, checkpointed halfway where the read stops on rows."""
        after = list(range(since.after + 1, self.config.rows))
        halfway = (len(after) + 1) // 2 - 1
        for position, n in enumerate(after):
            yield {"id": n}
            if self.config.end == "rows" and position == halfway:
                yield checkpoint(Since(after=n))
