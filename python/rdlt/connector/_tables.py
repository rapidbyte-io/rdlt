"""What a destination is told of a merge table's key, and a table's schema as Arrow."""

from __future__ import annotations

import json
from collections.abc import Mapping
from typing import Literal, TypedDict

from rdlt import _native
from rdlt.model import Json


class RootKey(TypedDict):
    """How a child table follows its root table, as the Rust SDK's `RootKey` says it.

    `table` is the root table's name, `id` its column holding each row's id, which the child's
    first key column holds, and `seq` its sequence column. A commit that publishes rows of the
    root replaces their children with those of each root's winning row.
    """

    table: str
    id: str
    seq: str


class ChangeColumns(TypedDict):
    """How a change stream's rows apply, as the Rust SDK's `ChangeColumns` says it.

    `op` names the column of each row's op code, an `int8`; `unchanged` the column flagging an
    update's unchanged columns, where rows may flag some; `deletion` is `"Hard"`, rows removed,
    or `{"Soft": {"at": column}}`, rows kept and marked deleted in that column.
    """

    op: str
    unchanged: str | None
    deletion: Literal["Hard"] | Mapping[Literal["Soft"], Mapping[Literal["at"], str]]


class HistoryColumns(TypedDict):
    """The columns of a history table's versions, as the Rust SDK's `HistoryColumns` says them."""

    valid_from: str
    valid_to: str
    is_current: str
    row_hash: str


class MergeKey(TypedDict):
    """How a merge table matches rows, as the Rust SDK's `MergeKey` says it: `table["merge"]`.

    `columns` are the key's columns, `seq` the column ordering rows within a commit, and `root`,
    `changes` and `history` say a child table's root, a change stream's columns and a history
    table's columns, where the table is one.
    """

    columns: list[str]
    seq: str
    root: RootKey | None
    changes: ChangeColumns | None
    history: HistoryColumns | None


def to_arrow(schema: Mapping[str, Json]) -> _native.Schema:
    """The Arrow schema of `schema`, a table schema as a `create` change carries it.

    It is the Rust SDK's `TableSchema::to_arrow`: each column at the Arrow type the engine writes
    it as, a JSON or UUID column named so in its field's metadata. The merge kit takes it, and any
    Arrow library reads it without a copy, such as `pyarrow.schema(to_arrow(schema))`.

    Raises:
        ValueError: `schema` is no table schema rdlt can read.
    """
    return _native.to_arrow(json.dumps(schema))
