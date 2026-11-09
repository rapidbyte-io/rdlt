"""Configuration values that are never shown."""

from __future__ import annotations

import hmac
from typing import Any, Generic, TypeVar, cast, get_args

from pydantic import GetCoreSchemaHandler, GetJsonSchemaHandler
from pydantic.json_schema import JsonSchemaValue
from pydantic_core import core_schema

T = TypeVar("T")


class Secret(Generic[T]):
    """A value redacted in `repr`, `str`, serialization, logs and errors; `expose()` reads it."""

    __slots__ = ("_value",)

    def __init__(self, value: T) -> None:
        self._value = value

    def expose(self) -> T:
        """The secret value."""
        return self._value

    def __repr__(self) -> str:
        return "Secret('***')"

    __str__ = __repr__

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, Secret):
            return NotImplemented
        mine: object = self._value
        theirs: object = cast("Secret[object]", other)._value
        if isinstance(mine, str) and isinstance(theirs, str):
            return hmac.compare_digest(mine.encode(), theirs.encode())
        return bool(mine == theirs)

    def __hash__(self) -> int:
        return hash(Secret)

    @classmethod
    def __get_pydantic_core_schema__(
        cls,
        source: Any,  # noqa: ANN401 - a typing construct
        handler: GetCoreSchemaHandler,
    ) -> core_schema.CoreSchema:
        """The inner type's validation, the result wrapped, serialized as the redaction."""
        (inner,) = get_args(source) or (str,)
        return core_schema.no_info_after_validator_function(
            cls,
            handler.generate_schema(inner),
            serialization=core_schema.plain_serializer_function_ser_schema(lambda _secret: "***"),
        )

    @classmethod
    def __get_pydantic_json_schema__(
        cls, schema: core_schema.CoreSchema, handler: GetJsonSchemaHandler
    ) -> JsonSchemaValue:
        """The inner type's schema, marked `writeOnly`, as the connector's published schema says."""
        shown = handler(schema)
        shown["writeOnly"] = True
        return shown
