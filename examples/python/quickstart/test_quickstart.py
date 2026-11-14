"""The quick start prints what its READMEs show, and the repository's README shows it whole."""

from __future__ import annotations

import runpy
import shlex
import sqlite3
import subprocess
import sys
from pathlib import Path

import pytest

import rdlt
from tests.sandboxes import NO_SANDBOX, without_sandbox

EXAMPLE = Path(__file__).resolve().with_name("main.py")
REPOSITORY = EXAMPLE.parents[3]
CODE = f"```python\n{EXAMPLE.read_text(encoding='utf-8')}```\n"
"""The quick start as the repository's README shows it."""
CONNECTORS = "${XDG_DATA_HOME:-$HOME/.local/share}/rdlt/connectors"
"""The connectors' directory each command names, where the README's install puts them; the test
runs the program with its own."""


def shown(text: str) -> list[str]:
    """What each run of the quick start in `text`'s first console block prints, in order.

    Each runs it as `python examples/python/quickstart/main.py`, with the connectors' directory and
    the database's, which the test names its own.
    """
    block = text.split("```console\n", 1)[1].split("```", 1)[0]
    program = str(EXAMPLE.relative_to(REPOSITORY))
    runs: list[str] = []
    for line in block.splitlines(keepends=True):
        if line.startswith("$ "):
            command = shlex.split(line)[1:]
            if command[0].endswith("python"):
                assert command[1:2] == [program], command
                assert command[2:3] == [CONNECTORS], command
                assert len(command[2:]) == 2, command
                runs.append("")
        elif runs:
            runs[-1] += line
    return runs


def printed(
    connectors: Path,
    data: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> str:
    """What the quick start prints, run as its README runs it, loading into `data`."""
    monkeypatch.setattr(sys, "argv", [str(EXAMPLE), str(connectors), str(data)])
    try:
        runpy.run_path(str(EXAMPLE), run_name="__main__")
    except rdlt.ConfigError as error:
        if error.code not in NO_SANDBOX:
            raise
        without_sandbox(str(error))
    return capsys.readouterr().out


def test_the_quick_start_loads_its_rows_once_as_its_readmes_show(
    tmp_path: Path,
    connectors: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    runs = [printed(connectors, tmp_path, monkeypatch, capsys) for _ in range(2)]
    assert runs == shown((EXAMPLE.parent / "README.md").read_text(encoding="utf-8"))
    readme = (REPOSITORY / "README.md").read_text(encoding="utf-8")
    assert runs == shown(readme[readme.index(CODE) :])
    with sqlite3.connect(tmp_path / "users.db") as database:
        assert database.execute('SELECT count(DISTINCT id) FROM "users"').fetchone() == (30,)


def test_the_readme_s_quick_start_is_the_example() -> None:
    assert CODE in (REPOSITORY / "README.md").read_text(encoding="utf-8")


def test_a_quick_start_that_fails_exits_1(connectors: Path, tmp_path: Path) -> None:
    # The host refuses a directory for the database that is not there.
    arguments = [str(EXAMPLE), str(connectors), str(tmp_path / "missing")]
    ended = subprocess.run([sys.executable, *arguments], capture_output=True, check=False)
    assert ended.returncode == 1, ended.stderr
