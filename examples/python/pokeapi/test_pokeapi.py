"""The PokeAPI source against a local server that answers in the API's shape.

It is certified, its failures are told apart, and it runs as the example's program runs it.
"""

from __future__ import annotations

import email.utils
import importlib.util
import json
import runpy
import sqlite3
import stat
import subprocess
import sys
from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import cast

import pytest

import rdlt
from local_api import Api
from pokeapi import PokeApi, PokeApiConfig, Position
from rdlt.connector import (
    ConnectorError,
    InvalidConfig,
    InvalidData,
    RateLimited,
    StreamContext,
    Transient,
    Unauthorized,
)
from rdlt.testing import certify_source
from tests.sandboxes import NO_SANDBOX, without_sandbox

EXAMPLE = Path(__file__).resolve().parent
PROGRAM = EXAMPLE / "main.py"

KILLED_TYPES = 40_000
"""Types the kill clause's load reads: about a megabyte of rows, a load that outlasts the commits
K-SOURCE's kills follow."""

Serve = Callable[..., Api]


class _Unstopped:
    """A read's context that never asks the read to stop, for a stream a test reads itself."""

    def stopped(self) -> bool:
        return False


UNSTOPPED = cast("StreamContext", _Unstopped())


def launcher(directory: Path) -> Path:
    """A launcher of the example's connector in `directory`, as an installed connector's is."""
    path = directory / "rdlt-connector-pokeapi"
    path.write_text(
        f"#!{sys.executable} -I\nimport runpy\n"
        f"runpy.run_path({str(EXAMPLE / 'pokeapi.py')!r}, run_name='__main__')\n"
    )
    path.chmod(stat.S_IRWXU)
    return path


def test_the_source_passes_every_clause_that_applies_to_it(serve: Serve, tmp_path: Path) -> None:
    api = serve(60, KILLED_TYPES)
    config = {"base_url": api.url, "page_size": 100, "limit": None}
    certification = certify_source(PokeApi, config=config, launcher=launcher(tmp_path))
    results = certification.report().results
    by_outcome = {
        outcome: [result.clause for result in results if result.outcome == outcome]
        for outcome in ("passed", "inapplicable")
    }
    assert by_outcome == {
        "passed": [
            *("P-HANDSHAKE", "P-ORDER", "P-ROLE", "P-LIMITS", "P-HEARTBEAT", "P-MALFORMED"),
            *("P-CREDIT", "S-CHECK", "S-DISCOVER", "S-PLAN", "S-RESUME", "S-STOP", "S-BARRIER"),
            "K-SOURCE",
        ],
        # Its one partition never ends, and it keeps no position outside the engine.
        "inapplicable": ["S-PARTITION", "S-ACK"],
    }
    assert len(results) == sum(map(len, by_outcome.values()))


@pytest.mark.parametrize(
    ("status", "retry_after", "kind", "wait"),
    [
        (429, 7, RateLimited, 7.0),
        (429, None, RateLimited, 1.0),
        (500, None, Transient, None),
        (503, None, Transient, None),
        (401, None, Unauthorized, None),
        (403, None, Unauthorized, None),
        (404, None, InvalidConfig, None),
    ],
)
def test_each_status_the_api_fails_with_is_an_error_of_its_kind(
    serve: Serve,
    status: int,
    retry_after: int | None,
    kind: type[ConnectorError],
    wait: float | None,
) -> None:
    api = serve(1)
    api.fail("/api/v2/pokemon", status, retry_after)
    source = PokeApi(PokeApiConfig(base_url=api.url))
    with pytest.raises(kind) as raised:
        source.check()
    assert getattr(raised.value, "retry_after", None) == wait


def test_a_429_that_names_a_date_asks_for_a_wait_until_then(serve: Serve) -> None:
    api = serve(1)
    date = datetime.now(UTC) + timedelta(seconds=8)
    api.fail("/api/v2/pokemon", 429, retry_after=email.utils.format_datetime(date, usegmt=True))
    with pytest.raises(RateLimited) as raised:
        PokeApi(PokeApiConfig(base_url=api.url)).check()
    # An HTTP date holds whole seconds, and a moment passed since the server wrote it.
    assert raised.value.retry_after is not None
    assert 6 <= raised.value.retry_after <= 8


def test_a_response_cut_short_is_transient(serve: Serve) -> None:
    api = serve(1)
    api.cut("/api/v2/pokemon")
    with pytest.raises(Transient):
        PokeApi(PokeApiConfig(base_url=api.url)).check()


def test_a_connection_the_api_refuses_is_transient(serve: Serve) -> None:
    api = serve(1)
    api.close()
    with pytest.raises(Transient):
        PokeApi(PokeApiConfig(base_url=api.url)).check()


@pytest.mark.parametrize(
    "body",
    [
        "not JSON",
        "[1, 2]",
        '{"next": null, "results": null}',
        '{"next": null, "results": [{"name": "pokemon-1", "url": 1}]}',
    ],
)
def test_a_body_of_another_shape_is_invalid_data(serve: Serve, body: str) -> None:
    api = serve(1)
    api.give("/api/v2/pokemon", body)
    with pytest.raises(InvalidData):
        PokeApi(PokeApiConfig(base_url=api.url)).check()


def detail(field: str, value: str) -> str:
    """A pokemon whose `field` is `value`, and whose other fields are as the API sends them."""
    numbers = {"id": "1", "height": "5", "weight": "10"} | {field: value}
    shape = '"name": "pokemon-1", "types": [], "abilities": [], "stats": []'
    return (
        "{" + ", ".join(f'"{name}": {number}' for name, number in numbers.items()) + f", {shape}}}"
    )


def entry(url: str) -> str:
    """A page of one type, whose URL is `url`."""
    return f'{{"next": null, "results": [{{"name": "type-5", "url": "{url}"}}]}}'


@pytest.mark.parametrize(
    ("stream", "path", "body"),
    [
        ("pokemon", "/api/v2/pokemon/1", detail("height", '"5"')),
        ("pokemon", "/api/v2/pokemon/1", detail("height", "-1")),
        ("pokemon", "/api/v2/pokemon/1", detail("weight", "7.0")),
        ("pokemon", "/api/v2/pokemon/1", detail("id", str(2**64))),
        ("types", "/api/v2/type", entry("/type/+5/")),
        ("types", "/api/v2/type", entry("/type/5a/")),
        ("types", "/api/v2/type", entry(f"/type/{2**64}/")),
    ],
)
def test_a_body_a_read_cannot_take_is_invalid_data(
    serve: Serve, stream: str, path: str, body: str
) -> None:
    # Each body is JSON: only the value under test is one a read cannot take.
    json.loads(body)
    api = serve(1)
    api.give(path, body)
    source = PokeApi(PokeApiConfig(base_url=api.url))
    with pytest.raises(InvalidData):
        list(getattr(source, stream)(Position(), UNSTOPPED))


def test_a_page_without_next_is_the_last(serve: Serve) -> None:
    api = serve(1)
    api.give("/api/v2/type", '{"results": [{"name": "type-1", "url": "/type/1/"}]}')
    source = PokeApi(PokeApiConfig(base_url=api.url))
    pages = list(source.pages("type", Position(), UNSTOPPED))
    assert [len(entries) for entries, _ in pages] == [1]


@pytest.mark.parametrize("base_url", ["ftp://pokeapi.co/api/v2", "pokeapi"])
def test_a_root_the_source_cannot_read_from_is_refused(base_url: str) -> None:
    with pytest.raises(InvalidConfig):
        PokeApi(PokeApiConfig(base_url=base_url))


def shown(text: str) -> list[str]:
    """What each run of the program in `text`'s first console block prints, in order.

    Each runs it as `python examples/python/pokeapi/main.py`, with the connectors' directory and the
    database's, which the test names its own.
    """
    block = text.split("```console\n", 1)[1].split("```", 1)[0]
    program = str(PROGRAM.relative_to(EXAMPLE.parents[2]))
    runs: list[str] = []
    for line in block.splitlines(keepends=True):
        if line.startswith("$ "):
            command = line.split()[1:]
            if command[0].endswith("python"):
                assert command[1:2] == [program], command
                assert len(command[2:]) == 2, command
                runs.append("")
        elif runs:
            runs[-1] += line
    return runs


def printed(
    connectors: Path,
    data: Path,
    api: Api,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> str:
    """What the program prints, run as its README runs it, loading from `api` into `data`."""
    monkeypatch.setattr(sys, "argv", [str(PROGRAM), str(connectors), str(data), api.url])
    try:
        runpy.run_path(str(PROGRAM), run_name="__main__")
    except rdlt.ConfigError as error:
        if error.code not in NO_SANDBOX:
            raise
        without_sandbox(str(error))
    return capsys.readouterr().out


def count(data: Path, query: str) -> int:
    """The count `query` answers in the database the program loads in `data`."""
    with sqlite3.connect(data / "pokeapi.db") as database:
        found: int = database.execute(query).fetchone()[0]
    return found


def test_a_second_run_resumes_and_commits_nothing_as_the_readme_shows(
    serve: Serve,
    connectors: Path,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    api = serve(200)
    runs = [printed(connectors, tmp_path, api, monkeypatch, capsys) for _ in range(2)]
    assert runs == shown((EXAMPLE / "README.md").read_text(encoding="utf-8"))
    assert count(tmp_path, 'SELECT count(DISTINCT "id") FROM "pokemon"') == 151
    # A pokemon's nested lists land as JSON in its row: here, its two abilities.
    abilities = 'SELECT count(*) FROM "pokemon" WHERE json_array_length("abilities") = 2'
    assert count(tmp_path, abilities) == 151
    # Both streams read at once, and the API is asked one thing at a time.
    assert api.most_at_once == 1


def test_a_failure_the_engine_retries_loses_no_row(
    serve: Serve, connectors: Path, tmp_path: Path
) -> None:
    api = serve(200)
    api.fail("/api/v2/pokemon/3", 500)
    api.cut("/api/v2/pokemon/5")
    api.fail("/api/v2/type", 429, retry_after=1)
    spec = importlib.util.spec_from_file_location("pokeapi_main", PROGRAM)
    assert spec is not None
    assert spec.loader is not None
    program = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(program)
    try:
        outcome: rdlt.Outcome = program.main(connectors, tmp_path, api.url)
    except rdlt.ConfigError as error:
        if error.code not in NO_SANDBOX:
            raise
        without_sandbox(str(error))
    assert (outcome.error, outcome.report.rows, api.unserved()) == (None, 151 + 21, [])
    assert outcome.report.attempted > 1
    distinct = count(tmp_path, 'SELECT count(DISTINCT "id") FROM "pokemon"')
    assert (count(tmp_path, 'SELECT count(*) FROM "pokemon"'), distinct) == (151, 151)


def test_a_run_that_fails_exits_1(serve: Serve, connectors: Path, tmp_path: Path) -> None:
    api = serve(200)
    api.fail("/api/v2/pokemon/1", 404)
    arguments = [str(PROGRAM), str(connectors), str(tmp_path), api.url]
    ended = subprocess.run([sys.executable, *arguments], capture_output=True, check=False)
    assert ended.returncode == 1, ended.stderr
