"""The quick start runs as the README shows it, and the README shows it whole."""

from __future__ import annotations

import importlib.util
import sqlite3
from pathlib import Path

import rdlt
from tests.sandboxes import NO_SANDBOX, without_sandbox

REPOSITORY = Path(__file__).resolve().parents[2]
EXAMPLE = REPOSITORY / "python" / "examples" / "quickstart.py"


def test_the_quick_start_loads_its_rows_once(tmp_path: Path, connectors: Path) -> None:
    spec = importlib.util.spec_from_file_location("quickstart", EXAMPLE)
    assert spec is not None
    assert spec.loader is not None
    quickstart = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(quickstart)
    try:
        first = quickstart.main(connectors, tmp_path)
    except rdlt.ConfigError as error:
        if error.code not in NO_SANDBOX:
            raise
        without_sandbox(str(error))
    second = quickstart.main(connectors, tmp_path)
    assert (first.report.rows, second.report.rows) == (30, 0)
    with sqlite3.connect(tmp_path / "users.db") as database:
        assert database.execute('SELECT count(DISTINCT id) FROM "users"').fetchone() == (30,)


def test_the_readme_s_quick_start_is_the_example() -> None:
    readme = (REPOSITORY / "README.md").read_text(encoding="utf-8")
    assert f"```python\n{EXAMPLE.read_text(encoding='utf-8')}```\n" in readme
