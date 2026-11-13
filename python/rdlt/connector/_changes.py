"""Change batches: Arrow data whose rows are changes, each a `ChangeOp`, as Rust's are."""

from __future__ import annotations

import enum

from rdlt import _native

OP_COLUMN = _native.OP_COLUMN
"""The column holding each row's `ChangeOp`, an `int8`."""

SEQ_COLUMN = _native.SEQ_COLUMN
"""The column holding each row's position in its source, 16 bytes, big-endian, compared bytewise:
`pa.binary(16)`, as `position.to_bytes(16, "big")` makes one."""

UNCHANGED_COLUMN = _native.UNCHANGED_COLUMN
"""The optional column flagging the columns an update left unchanged: a `pa.binary()` bitmap a
row, whose bit `i % 8` of byte `i // 8` flags the field at ordinal `i`, null for none."""


class ChangeOp(enum.IntEnum):
    """What a change row does, as the Rust SDK's `ChangeOp`: each member is its code.

    So `int(op)` is Rust's `code()`, and `ChangeOp(code)` its `from_code`.
    """

    INSERT = 0
    """A new row."""
    UPDATE = 1
    """A new version of an existing row."""
    DELETE = 2
    """A removed row; only key columns need values."""
    TRUNCATE = 3
    """Every row of the table the source truncated before this position; no column needs a
    value."""


def changes(batch: object) -> _native.Changes:
    """A change batch to yield, as a Rust read's `Emitter::changes` pushes one.

    `batch` is anything Arrow a read yields, a batch or a stream of them, holding `OP_COLUMN` and
    `SEQ_COLUMN`, and `UNCHANGED_COLUMN` where an update leaves columns unchanged. The engine
    reads it only from a stream read as `cdc`, and fails a read otherwise, coded `push_unexpected`,
    as it fails rows from a stream read as `cdc`. It checks the columns where it reads them: a
    batch without them, or of other types, fails the read, coded `change_batch`. An empty batch is
    skipped, as Rust's is.

    Raises:
        TypeError: `batch` is no Arrow data.
    """
    return _native.Changes(batch)
