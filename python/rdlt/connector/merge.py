"""Merging a merge table's rows by key, for a destination that keeps them as Arrow batches.

Each function is the Rust SDK's of `rdlt_connector::merge`, run in Rust, the GIL released while it
merges. A schema is anything with `__arrow_c_schema__`, such as `to_arrow(schema)` or a
`pyarrow.Schema`, and each batch of a sequence anything with `__arrow_c_array__`, or
`__arrow_c_stream__` for several, as a pyarrow `Table` or a polars `DataFrame`: no one Arrow library
is required. What a function answers any Arrow library reads without a copy, such as
`pyarrow.record_batch(batch)`. A merge table's rows are given back as batches of the columns they
hold, and `read_back` gives a reader each batch with every column.

A refusal raises `InvalidData` with Rust's code, such as `merge_key_invalid`, `sequence_missing` or
`value_unholdable`, and a failure of the kit itself `ConnectorError`, each saying the function and
Rust's error. What is no Arrow raises `TypeError`, and a key rdlt cannot read `ValueError`.
"""

from __future__ import annotations

import json
from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass
from typing import TypeVar, cast

from rdlt import _native
from rdlt.connector._failures import ConnectorError, InvalidData
from rdlt.connector._tables import ChangeColumns, MergeKey, RootKey
from rdlt.model import Json

__all__ = [
    "Merged",
    "admitted",
    "folded",
    "holds",
    "merge_children_sparse",
    "merge_sparse",
    "read_back",
    "refuse_history_generation",
    "tombstone_schema",
    "written_schema",
]

T = TypeVar("T")


@dataclass(frozen=True, slots=True)
class Merged:
    """A merge table's rows once merged, as the Rust SDK's `Merged`.

    `rows` are batches of the columns they hold; `tombstones` those of a change stream's table,
    the rows it removed outright, which a later merge takes as `buried`, and none for any other.
    """

    rows: list[_native.Batch]
    tombstones: list[_native.Batch]


def merge_sparse(
    schema: object,
    published: Iterable[object],
    buried: Iterable[object],
    incoming: Iterable[object],
    key: MergeKey,
) -> Merged:
    """The rows of a table that published `published` once `incoming` merges in by `key`.

    For a history table each row versions its key in sequence order; for a change stream's
    table each row applies in sequence order as its op says, past `buried`, its tombstones;
    otherwise an incoming row replaces the published row of its key, and among incoming rows of
    one key the greatest sequence wins. `schema` is the table's.
    """
    rows, tombstones = _refused(
        _native.merge_sparse,
        _schema(schema, "schema"),
        _batches(published, "published"),
        _batches(buried, "buried"),
        _batches(incoming, "incoming"),
        json.dumps(key),
    )
    return Merged(rows, tombstones)


def merge_children_sparse(  # noqa: PLR0917 - Rust's six arguments, in Rust's order
    schema: object,
    published: Iterable[object],
    incoming: Iterable[object],
    key: MergeKey,
    root: RootKey,
    roots: Iterable[object],
) -> list[_native.Batch]:
    """A child table's rows once the root rows `roots` a commit publishes replace their children.

    Published rows of those roots go, and of `incoming`, the rows of each root's winning row, by
    root id and sequence, are added.
    """
    return _refused(
        _native.merge_children_sparse,
        _schema(schema, "schema"),
        _batches(published, "published"),
        _batches(incoming, "incoming"),
        json.dumps(key),
        json.dumps(root),
        _batches(roots, "roots"),
    )


def read_back(schema: object, rows: Iterable[object]) -> list[_native.Batch]:
    """`rows` as a reader is given them: each batch with every column of `schema`, at its type.

    A column a batch does not hold is nulls, which cost a reader a reference whatever the rows.
    """
    return _refused(_native.read_back, _schema(schema, "schema"), _batches(rows, "rows"))


def written_schema(stored: object, changes: ChangeColumns) -> _native.Schema:
    """The schema a change stream's written batches have.

    It is `stored`, every column nullable since a truncate names no key, then the columns
    `changes` names.
    """
    return _native.written_schema(_schema(stored, "stored"), json.dumps(changes))


def admitted(batch: object, stored: object | None, key: MergeKey) -> None:
    """Refuses `batch`, written for a table merged by `key`, unless its rows can be merged.

    Every row has a sequence and, for a change stream's, an op, a deletion time where a history
    table's deletes are soft, and unchanged flags of columns other than its key and sequence that
    `stored`, the table's columns, holds where they are known.
    """
    export = getattr(batch, "__arrow_c_array__", None)
    if export is None:
        message = f"batch is a batch, anything with __arrow_c_array__, not a {type(batch).__name__}"
        raise TypeError(message)
    exported = tuple(cast("Callable[[], Iterable[object]]", export)())
    stored_schema = None if stored is None else _schema(stored, "stored")
    _refused(_native.admitted, exported, stored_schema, json.dumps(key))


def holds(batches: Iterable[object], schema: object) -> None:
    """Refuses `batches` unless each column `schema` names converts to its type, every value kept.

    What a table holds must fit a type before a column takes it.
    """
    _refused(_native.holds, _batches(batches, "batches"), _schema(schema, "schema"))


def tombstone_schema(schema: object, key: MergeKey) -> _native.Schema:
    """The schema of the tombstones of a table that has `schema`, merged by `key`.

    It is the key's columns, nullable, then the sequence as the bytes it compares by.
    """
    return _refused(_native.tombstone_schema, _schema(schema, "schema"), json.dumps(key))


def folded(
    schema: object, batches: Iterable[object], shapes: int, cells: int
) -> list[_native.Batch]:
    """`batches` as at most `shapes` batches, for a destination that pays for each.

    The largest stay as they are and the others are joined, smallest first, in groups that each
    hold at most `cells` cells without a value; more batches are given back where that asks for
    them.

    Raises:
        ValueError: `shapes` is less than 1.
    """
    return _refused(
        _native.folded, _schema(schema, "schema"), _batches(batches, "batches"), shapes, cells
    )


def refuse_history_generation(table: Mapping[str, Json]) -> None:
    """Refuses a table writer of `table` where it is a replace generation of a history table.

    A history table merges into its table alone.
    """
    _refused(_native.refuse_history_generation, json.dumps(table))


def _refused(function: Callable[..., T], *arguments: object) -> T:
    """What `function` answers of `arguments`, a refusal raised as `rdlt.connector`'s failure."""
    try:
        return function(*arguments)
    except _native.MergeError as failed:
        kind, code, message = cast("tuple[str, str | None, str]", failed.args)
        raised = InvalidData if kind == "data" else ConnectorError
        raise raised(message, code=code) from None


def _schema(schema: object, name: str) -> object:
    """`schema`'s capsule, which Python asks it for, so none of its methods runs beneath Rust."""
    export = getattr(schema, "__arrow_c_schema__", None)
    if export is None:
        message = (
            f"{name} is an Arrow schema, anything with __arrow_c_schema__, "
            f"not a {type(schema).__name__}"
        )
        raise TypeError(message)
    return cast("Callable[[], object]", export)()


def _batches(batches: Iterable[object], name: str) -> list[object]:
    """Each of `batches` as the capsules Python asks it for: a batch's pair, or a stream's one."""
    if hasattr(batches, "__arrow_c_array__") or hasattr(batches, "__arrow_c_stream__"):
        message = f"{name} is a sequence of batches, not one: put it in a list"
        raise TypeError(message)
    exported: list[object] = []
    for batch in batches:
        array = getattr(batch, "__arrow_c_array__", None)
        stream = getattr(batch, "__arrow_c_stream__", None)
        if array is not None:
            exported.append(tuple(cast("Callable[[], Iterable[object]]", array)()))
        elif stream is not None:
            exported.append(cast("Callable[[], object]", stream)())
        else:
            message = (
                f"{name} holds batches, anything with __arrow_c_array__, or __arrow_c_stream__ "
                f"for several, not a {type(batch).__name__}"
            )
            raise TypeError(message)
    return exported
