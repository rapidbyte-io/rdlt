"""What a stream says of itself, as the Rust SDK's `StreamSpec` says it: its name and its reads."""

from __future__ import annotations

import enum
import json
from collections.abc import Sequence
from dataclasses import KW_ONLY, dataclass, field
from typing import Literal, cast

from rdlt import _native
from rdlt.model import Json, ReadMode, StreamName

ReadWord = Literal["full", "incremental", "cdc"]
"""A read mode, by its name in a document."""

_CheckpointingWord = Literal["natural", "on_demand"]
"""When a stream checkpoints, by its name."""


class Checkpointing(enum.Enum):
    """When a stream's partitions checkpoint, as the Rust SDK's `Checkpointing` says it."""

    NATURAL = "natural"
    """Only where the read chooses: the engine never asks."""
    ON_DEMAND = "on_demand"
    """Also at the next safe point after the engine asks, which `ctx.checkpoint_due()` says."""


@dataclass(frozen=True, slots=True)
class StreamSpec:
    """One stream a source offers, and how it can be read, as the Rust SDK's `StreamSpec`.

    Its defaults are `StreamSpec::new`'s: read in full, checkpointing where the read chooses,
    replayable, with no schema, key or change time. `name` is a name, or a `rdlt.StreamName`
    within a namespace, such as a database schema. `schema` is any Arrow schema, anything with
    `__arrow_c_schema__`, which the engine takes as its `TableSchema`, as `with_schema` declares
    it: a type with no logical equivalent, such as an interval or a union, a decimal with a
    negative scale, two fields of one name or nesting beyond 64 levels is refused as the spec is
    made. `primary_key` is a column's name or a sequence of them, held as a tuple, `read_modes`
    `ReadMode`s or their names, and `checkpointing` a `Checkpointing` or its name. `change_time`
    names the top-level column holding each change's source commit time. `@stream` says a
    stream's spec from its declaration, its `ReadStream.spec`, and a source's `discover` answers
    one for each stream of its catalog.

    Raises:
        TypeError: `schema` is no Arrow schema, or `read_modes` is one name.
        ValueError: `schema` is one Rust cannot take, or a read mode or `checkpointing` is none of
            its enum's.
    """

    name: str | StreamName
    _: KW_ONLY
    schema: object = None
    primary_key: str | Sequence[str] | None = None
    read_modes: Sequence[ReadMode | ReadWord] = (ReadMode.FULL,)
    checkpointing: Checkpointing | _CheckpointingWord = Checkpointing.NATURAL
    replayable: bool = True
    change_time: str | None = None
    _table: Json = field(default=None, init=False, repr=False, compare=False)
    """The schema as the engine's `TableSchema`, in its JSON, converted once."""

    def __post_init__(self) -> None:
        shown = str(self.name)
        if isinstance(self.read_modes, str):
            message = f"stream {shown!r}: read_modes= is a sequence of read modes, not one"
            raise TypeError(message)
        object.__setattr__(self, "read_modes", tuple(ReadMode(mode) for mode in self.read_modes))
        if isinstance(self.primary_key, str):
            object.__setattr__(self, "primary_key", (self.primary_key,))
        elif self.primary_key is not None:
            object.__setattr__(self, "primary_key", tuple(self.primary_key))
        object.__setattr__(self, "checkpointing", Checkpointing(self.checkpointing))
        object.__setattr__(self, "_table", _table_schema(self.schema, shown))


def described(spec: StreamSpec) -> dict[str, Json]:
    """`spec` in the words the native side reads it in."""
    named = spec.name if isinstance(spec.name, StreamName) else StreamName(spec.name)
    return {
        "name": named.name,
        "namespace": named.namespace,
        "schema": spec._table,  # pyright: ignore[reportPrivateUsage] - the spec's own conversion
        "primary_key": None if spec.primary_key is None else list(spec.primary_key),
        # Normalised as the spec was made, as its annotations, wide for what it takes, cannot say.
        "read_modes": [cast("ReadMode", mode).value for mode in spec.read_modes],
        "checkpointing": cast("Checkpointing", spec.checkpointing).value,
        "replayable": spec.replayable,
        "change_time": spec.change_time,
    }


def _table_schema(schema: object, shown: str) -> Json:
    """`schema`, an Arrow schema, as the engine's `TableSchema` in its JSON; none for none.

    Python asks for the schema's capsule, so no method of the schema's runs beneath Rust.

    Raises:
        TypeError: `schema` is no Arrow schema, or exports none.
        ValueError: `schema` is one Rust cannot take.
    """
    if schema is None:
        return None
    export = getattr(schema, "__arrow_c_schema__", None)
    if export is None:
        message = (
            f"stream {shown!r}: schema= takes an Arrow schema, anything with __arrow_c_schema__ "
            f"such as a pyarrow.Schema, and a {type(schema).__name__} has none"
        )
        raise TypeError(message)
    try:
        converted = _native.table_schema(export())
    except ValueError as refused:
        message = f"stream {shown!r}: {refused}"
        raise ValueError(message) from refused
    except TypeError as refused:
        message = f"stream {shown!r}: its schema's __arrow_c_schema__ answered no schema: {refused}"
        raise TypeError(message) from refused
    return json.loads(converted)
