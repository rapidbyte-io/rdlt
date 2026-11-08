# pyright: reportUnusedFunction=false
"""The values rdlt reports, as frozen dataclasses built from the JSON the native layer answers."""

from __future__ import annotations

import json
from collections.abc import Mapping
from dataclasses import dataclass
from typing import TypeAlias, cast

Json: TypeAlias = "bool | int | float | str | list[Json] | dict[str, Json] | None"
"""A JSON value, as `json.loads` returns it."""


def _parse(text: str) -> dict[str, Json]:
    value = cast("Json", json.loads(text))
    if not isinstance(value, dict):
        raise TypeError("the native layer answered JSON that is no object")
    return value


def _text(fields: Mapping[str, Json], key: str) -> str:
    value = fields.get(key)
    if not isinstance(value, str):
        raise TypeError(f"field {key!r} is not text")
    return value


def _maybe_text(fields: Mapping[str, Json], key: str) -> str | None:
    value = fields.get(key)
    if value is not None and not isinstance(value, str):
        raise TypeError(f"field {key!r} is neither text nor null")
    return value


def _flag(fields: Mapping[str, Json], key: str) -> bool:
    value = fields.get(key)
    if not isinstance(value, bool):
        raise TypeError(f"field {key!r} is not a boolean")
    return value


@dataclass(frozen=True, slots=True)
class ErrorReport:
    """What a failure reports, as the facade's `ErrorReport` serializes it."""

    kind: str
    """The failure's kind: `config`, `schema`, `source`, `destination`, `transform`, `fenced`,
    `memory`, `cancelled` or `internal`."""
    origin: str
    """Who failed: `engine`, `source` or `destination`."""
    stream: str | None
    """The stream it belongs to, where it belongs to one."""
    code: str | None
    """The engine's code for it."""
    connector_code: str | None
    """The code a connector's error carried, shown and bounded."""
    message: str
    """A one-line description naming its subject."""
    causes: tuple[str, ...]
    """Each cause, outermost first: a Python traceback among them, for an exception in Python."""
    retryable: bool
    """Whether a new attempt could have succeeded."""

    @classmethod
    def from_fields(cls, fields: Mapping[str, Json]) -> ErrorReport:
        """The report `fields`, the facade's JSON, holds."""
        causes = fields.get("causes")
        if not isinstance(causes, list) or not all(isinstance(cause, str) for cause in causes):
            raise TypeError("field 'causes' is not a list of text")
        return cls(
            kind=_text(fields, "kind"),
            origin=_text(fields, "origin"),
            stream=_maybe_text(fields, "stream"),
            code=_maybe_text(fields, "code"),
            connector_code=_maybe_text(fields, "connector_code"),
            message=_text(fields, "message"),
            causes=tuple(cast("list[str]", causes)),
            retryable=_flag(fields, "retryable"),
        )
