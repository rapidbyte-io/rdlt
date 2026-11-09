"""A connector's configuration and its errors: each names the field and fault, never a value."""

from __future__ import annotations

import json
from typing import TypeVar, cast

import pydantic

from rdlt.connector._failures import InvalidConfig
from rdlt.model import Json


class Config(pydantic.BaseModel):
    """A connector's configuration; its JSON Schema is the connector's published one."""

    model_config = pydantic.ConfigDict(frozen=True, extra="forbid")


C = TypeVar("C", bound=Config)

_FAULTS = {
    "missing": "it lacks a required field",
    "extra_forbidden": "the connector knows no such field",
    "too_short": "it holds the wrong number of items",
    "too_long": "it holds the wrong number of items",
    "literal_error": "its value names no choice the connector knows",
    "enum": "its value names no choice the connector knows",
}

_HOLDS: list[tuple[type, str]] = [
    (bool, "a boolean"),
    (int, "a number"),
    (float, "a number"),
    (str, "text"),
    (list, "a list"),
    (dict, "an object"),
]


def _fault(kind: str) -> str:
    if kind in _FAULTS:
        return _FAULTS[kind]
    if kind.endswith(("_type", "_parsing")):
        return "its value is of the wrong type"
    return "its value is not valid"


def _held(config: Json, location: tuple[int | str, ...]) -> str | None:
    value = config
    for part in location:
        if isinstance(value, dict) and isinstance(part, str) and part in value:  # noqa: SIM114 - each arm narrows its own types
            value = value[part]
        elif isinstance(value, list) and isinstance(part, int) and 0 <= part < len(value):
            value = value[part]
        else:
            return None
    if value is None:
        return "null"
    return next((said for kind, said in _HOLDS if isinstance(value, kind)), None)


def validated(config: type[C], text: str) -> C:
    """`text`, a configuration's JSON, as `config`.

    Raises:
        InvalidConfig: coded `config_invalid`, naming the first field refused, its fault and what
            kind of value it holds; or, where a validator raised what pydantic does not report,
            naming that exception's type alone.
    """
    document = cast("Json", json.loads(text))
    try:
        return config.model_validate(document)
    except pydantic.ValidationError as invalid:
        # Pydantic's error carries the input; only the field and fault are kept.
        first = invalid.errors(include_input=False, include_url=False, include_context=False)[0]
        location = tuple(first["loc"])
        fault = _fault(first["type"])
        if first["type"] == "missing":
            parent = ".".join(str(part) for part in location[:-1]) or "."
            message = f"config field {parent}: {fault}, `{location[-1]}`"
            raise InvalidConfig(message, code="config_invalid") from None
        path = ".".join(str(part) for part in location) or "."
        held = _held(document, location)
        suffix = f" (it holds {held})" if held else ""
        message = f"config field {path}: {fault}{suffix}"
        raise InvalidConfig(message, code="config_invalid") from None
    except Exception as raised:  # noqa: BLE001 - a validator's own exception refuses it too
        # Its message may quote the value it refused: only its type is kept.
        message = f"config: its validation raised {type(raised).__name__}"
        raise InvalidConfig(message, code="config_invalid") from None
