"""A connector's configuration: validated by pydantic, its errors naming paths and never values."""

from __future__ import annotations

import json

import pydantic
import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

from rdlt.connector import Config, InvalidConfig, Secret
from rdlt.connector._config import validated


class Endpoint(Config):
    """One entry of a list a configuration holds."""

    host: str
    port: int


class TicketsConfig(Config):
    """A connector's configuration, with a secret and nested values."""

    base_url: str
    token: Secret[str]
    endpoints: list[Endpoint] = pydantic.Field(default_factory=list[Endpoint])


def test_a_secret_is_redacted_where_it_is_shown_and_read_with_expose() -> None:
    config = validated(TicketsConfig, json.dumps({"base_url": "https://x", "token": "hunter2"}))
    assert config.token.expose() == "hunter2"
    for shown in (repr(config), str(config), repr(config.token), config.model_dump_json()):
        assert "hunter2" not in shown


def test_a_secret_s_schema_is_its_inner_type_s_and_write_only() -> None:
    schema = TicketsConfig.model_json_schema()["properties"]["token"]
    assert schema["type"] == "string"
    assert schema["writeOnly"] is True


@pytest.mark.parametrize(
    ("config", "expected"),
    [
        ({"token": "t"}, "config field .: it lacks a required field, `base_url`"),
        (
            {"base_url": 7, "token": "t"},
            "config field base_url: its value is of the wrong type (it holds a number)",
        ),
        (
            {"base_url": "u", "token": "t", "extra": 1},
            "config field extra: the connector knows no such field (it holds a number)",
        ),
        (
            {"base_url": "u", "token": "t", "endpoints": [{"host": "h", "port": "x"}]},
            "config field endpoints.0.port: its value is of the wrong type (it holds text)",
        ),
    ],
)
def test_an_invalid_configuration_names_its_path_and_fault(
    config: dict[str, object], expected: str
) -> None:
    with pytest.raises(InvalidConfig) as raised:
        validated(TicketsConfig, json.dumps(config))
    assert str(raised.value) == expected


@settings(deadline=None)
@given(
    marker=st.text(alphabet="abcdefghijklmnopqrstuvwxyz0123456789", min_size=12, max_size=20),
    field=st.sampled_from(["base_url", "token", "port", "unknown"]),
)
def test_no_configuration_error_quotes_a_value(marker: str, field: str) -> None:
    config: dict[str, object] = {"base_url": ["not", "text", marker], "token": {"nested": marker}}
    config[field] = {"value": marker} if field != "port" else marker
    with pytest.raises(InvalidConfig) as raised:
        validated(TicketsConfig, json.dumps(config))
    assert marker not in str(raised.value)


class Checked(Config):
    """A configuration whose validator raises what pydantic does not wrap."""

    port: int

    @pydantic.field_validator("port")
    @classmethod
    def positive(cls, port: int) -> int:
        """Refuses a negative port, quoting it."""
        if port < 0:
            message = f"port {port} is negative"
            raise TypeError(message)
        return port


def test_a_validator_s_own_exception_is_a_refusal_that_quotes_nothing() -> None:
    with pytest.raises(InvalidConfig) as raised:
        validated(Checked, json.dumps({"port": -48151623}))
    assert raised.value.code == "config_invalid"
    assert "48151623" not in str(raised.value)
    assert raised.value.__cause__ is None
    assert raised.value.__suppress_context__
