"""The transforms example prints what its README shows, and its own code is certified."""

from __future__ import annotations

import importlib.util
import os
import runpy
import subprocess
import sys
from pathlib import Path
from types import ModuleType

import pyarrow as pa
import pytest

import rdlt
from rdlt.testing import certify_transform
from tests.sandboxes import NO_SANDBOX, without_sandbox

EXAMPLE = Path(__file__).resolve().parent
PROGRAM = EXAMPLE / "main.py"
KEY = "ORDERS_KEY"
"""The mask's key, which the program reads from the environment."""


def program() -> ModuleType:
    """The example's program, imported under a name of its own."""
    spec = importlib.util.spec_from_file_location("transforms_main", PROGRAM)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def shown(text: str) -> tuple[dict[str, str], list[str]]:
    """The variables `text`'s first console block exports, and what each run of the program prints.

    Each runs it as `python examples/python/transforms/main.py`, with the connectors' directory and
    the database's, which the test names its own.
    """
    block = text.split("```console\n", 1)[1].split("```", 1)[0]
    path = str(PROGRAM.relative_to(EXAMPLE.parents[2]))
    exported: dict[str, str] = {}
    runs: list[str] = []
    for line in block.splitlines(keepends=True):
        if line.startswith("$ "):
            command = line.split()[1:]
            if command[0] == "export":
                name, value = command[1].split("=", 1)
                exported[name] = value
            elif command[0].endswith("python"):
                assert command[1:2] == [path], command
                assert len(command[2:]) == 2, command
                runs.append("")
        elif runs:
            runs[-1] += line
    return exported, runs


def test_a_load_and_a_refused_one_print_what_the_readme_shows(
    connectors: Path,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    exported, runs = shown((EXAMPLE / "README.md").read_text(encoding="utf-8"))
    assert list(exported) == [KEY]
    assert len(runs) == 1
    for name, value in exported.items():
        monkeypatch.setenv(name, value)
    monkeypatch.setattr(sys, "argv", [str(PROGRAM), str(connectors), str(tmp_path)])
    try:
        runpy.run_path(str(PROGRAM), run_name="__main__")
    except rdlt.ConfigError as error:
        if error.code not in NO_SANDBOX:
            raise
        without_sandbox(str(error))
    assert capsys.readouterr().out == runs[0]


def test_a_mask_with_no_key_is_refused_as_the_pipeline_is_built(
    connectors: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.delenv(KEY, raising=False)
    with pytest.raises(rdlt.ConfigError) as raised:
        program().main(connectors, tmp_path)
    if raised.value.code in NO_SANDBOX:
        without_sandbox(str(raised.value))
    # A reference that does not resolve is told by its code, its value never shown.
    assert raised.value.code == "secret_unresolved", raised.value


def test_a_load_that_fails_exits_1(connectors: Path, tmp_path: Path) -> None:
    # With no key for the mask, the first load's pipeline is not built.
    arguments = [str(PROGRAM), str(connectors), str(tmp_path)]
    environment = {name: value for name, value in os.environ.items() if name != KEY}
    ended = subprocess.run(
        [sys.executable, *arguments], capture_output=True, check=False, env=environment
    )
    assert ended.returncode == 1, ended.stderr


def test_the_transform_and_the_filter_pass_every_clause_that_applies() -> None:
    loaded = program()
    # The orders as the transforms meet them, after the rename.
    renamed = [
        {("quantity" if name == "qty" else name): value for name, value in order.items()}
        for order in loaded.ORDERS
    ]
    orders = pa.RecordBatch.from_pylist(renamed)
    for transform in (loaded.priced, loaded.real):
        results = certify_transform(transform, inputs=[orders]).report().results
        by_outcome = {
            outcome: [result.clause for result in results if result.outcome == outcome]
            for outcome in ("passed", "inapplicable")
        }
        assert by_outcome == {
            "passed": ["T-REPLAY", "T-ROWWISE", "T-SCHEMA", "T-ROWS"],
            # The orders name no key, and neither declares a bound, a lineage or what it reads.
            "inapplicable": ["T-KEYS", "T-BOUND", "T-LINEAGE", "T-READS"],
        }, transform.name
        assert len(results) == sum(map(len, by_outcome.values()))
