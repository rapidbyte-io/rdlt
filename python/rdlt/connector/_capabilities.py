"""What a destination can store and how it commits, as the Rust SDK's `Capabilities` says it."""

from __future__ import annotations

import json
from collections.abc import Mapping
from dataclasses import asdict, dataclass, field
from typing import Literal, cast

from rdlt import _native
from rdlt.model import Json


@dataclass(frozen=True, slots=True, kw_only=True)
class WriteModes:
    """The write modes a destination supports."""

    append: bool = False
    replace: bool = False
    merge: bool = False
    history: bool = False


@dataclass(frozen=True, slots=True, kw_only=True)
class DeleteModes:
    """How a destination can apply a change stream's deletes."""

    hard: bool = False
    soft: bool = False


@dataclass(frozen=True, slots=True, kw_only=True)
class NestedSupport:
    """Which nested values a destination stores natively."""

    structs: bool = False
    lists: bool = False
    json: bool = False


@dataclass(frozen=True, slots=True, kw_only=True)
class SchemaChanges:
    """Which schema changes a destination applies in place."""

    add_column: bool = False
    widenings: frozenset[tuple[str, str]] = field(default_factory=frozenset[tuple[str, str]])
    """Each widening it applies, from one type kind to another, such as `("int32", "int64")`."""


@dataclass(frozen=True, slots=True, kw_only=True)
class IdentifierRules:
    """The identifier rules the engine's naming follows for a destination."""

    case: Literal["preserve", "lower", "upper"]
    max_len: int
    """The longest identifier, in bytes."""
    chars: Literal["any", "ascii_word"]
    reserved: frozenset[str] = frozenset()
    reserved_table_prefixes: frozenset[str] = frozenset()


@dataclass(frozen=True, slots=True, kw_only=True)
class Capabilities:
    """What a destination can store and how it commits, starting from `Capabilities.minimal()`.

    `dataclasses.replace(Capabilities.minimal(), write_modes=WriteModes(append=True, merge=True))`
    declares a destination that merges too.
    """

    write_modes: WriteModes
    delete_modes: DeleteModes
    partial_updates: bool
    merge_changes: bool
    drop_tables: bool
    nested: NestedSupport
    types: frozenset[str]
    """The type kinds it stores, such as `int64` and `utf8`."""
    schema_changes: SchemaChanges
    identifiers: IdentifierRules
    max_parallel_writers: int

    def __post_init__(self) -> None:
        """Refuses capabilities the Rust SDK's `Capabilities` cannot hold, as no writers.

        Raises:
            ValueError: rdlt cannot read them.
        """
        refused = _native.unreadable_capabilities(self._json())
        if refused is not None:
            raise ValueError(refused)

    @classmethod
    def minimal(cls) -> Capabilities:
        """An append-only destination's, as the Rust SDK's `Capabilities::minimal()`.

        It stores scalar types, adds columns, keeps the case of identifiers of up to 63 ASCII word
        characters, and runs one table writer.
        """
        found = cast("dict[str, Json]", json.loads(_native.minimal_capabilities()))
        return cls._from(found)

    @classmethod
    def _from(cls, found: Mapping[str, Json]) -> Capabilities:
        def part(name: str) -> dict[str, Json]:
            return cast("dict[str, Json]", found[name])

        changes = part("schema_changes")
        rules = part("identifiers")
        widenings = cast("list[list[str]]", changes["widenings"])
        return cls(
            write_modes=WriteModes(**cast("dict[str, bool]", part("write_modes"))),
            delete_modes=DeleteModes(**cast("dict[str, bool]", part("delete_modes"))),
            partial_updates=bool(found["partial_updates"]),
            merge_changes=bool(found["merge_changes"]),
            drop_tables=bool(found["drop_tables"]),
            nested=NestedSupport(**cast("dict[str, bool]", part("nested"))),
            types=frozenset(cast("list[str]", found["types"])),
            schema_changes=SchemaChanges(
                add_column=bool(changes["add_column"]),
                widenings=frozenset((first, second) for first, second in widenings),
            ),
            identifiers=IdentifierRules(
                case=cast("Literal['preserve', 'lower', 'upper']", rules["case"]),
                max_len=cast("int", rules["max_len"]),
                chars=cast("Literal['any', 'ascii_word']", rules["chars"]),
                reserved=frozenset(cast("list[str]", rules["reserved"])),
                reserved_table_prefixes=frozenset(
                    cast("list[str]", rules["reserved_table_prefixes"])
                ),
            ),
            max_parallel_writers=cast("int", found["max_parallel_writers"]),
        )

    def _json(self) -> str:
        """The Rust SDK's `Capabilities`, as JSON, each set sorted."""
        document = asdict(self)
        document["types"] = sorted(self.types)
        document["schema_changes"]["widenings"] = sorted(
            list(pair) for pair in self.schema_changes.widenings
        )
        document["identifiers"]["reserved"] = sorted(self.identifiers.reserved)
        document["identifiers"]["reserved_table_prefixes"] = sorted(
            self.identifiers.reserved_table_prefixes
        )
        return json.dumps(document)
