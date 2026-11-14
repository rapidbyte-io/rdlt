"""The warehouse destination in process, served and certified, and the example's program."""

from __future__ import annotations

import runpy
import stat
import subprocess
import sys
from contextlib import closing
from pathlib import Path
from typing import cast

import pytest

import rdlt
import warehouse
from rdlt.connector import Fenced, OpenContext
from rdlt.model import Json
from rdlt.testing import certify_destination
from tests.sandboxes import NO_SANDBOX, without_sandbox
from warehouse import Warehouse, WarehouseConfig
from warehouse_store import Store, connected

EXAMPLE = Path(__file__).resolve().parent
PROGRAM = EXAMPLE / "main.py"


def launcher(directory: Path) -> Path:
    """A launcher of the warehouse in `directory`, as an installed connector's is."""
    path = directory / "rdlt-connector-warehouse"
    path.write_text(
        f"#!{sys.executable} -I\nimport runpy\nimport sys\nsys.path.append({str(EXAMPLE)!r})\n"
        f"runpy.run_path({str(EXAMPLE / 'warehouse.py')!r}, run_name='__main__')\n"
    )
    path.chmod(stat.S_IRWXU)
    return path


def test_the_warehouse_passes_every_destination_clause_its_kill_clause_killed(
    tmp_path: Path, tmp_path_factory: pytest.TempPathFactory
) -> None:
    certification = certify_destination(
        Warehouse,
        config={"path": str(tmp_path / "certified.db")},
        launcher=launcher(tmp_path_factory.mktemp("launchers")),
    )
    results = certification.report().results
    unpassed = {result.clause: result.reason for result in results if result.outcome != "passed"}
    by_outcome = {
        outcome: [result.clause for result in results if result.outcome == outcome]
        for outcome in ("passed", "inapplicable")
    }
    assert by_outcome == {
        "passed": [
            *("P-HANDSHAKE", "P-ORDER", "P-ROLE", "P-LIMITS", "P-HEARTBEAT", "P-MALFORMED"),
            *("D-CHECK", "D-EPOCH", "D-STAGING", "D-COMMIT", "D-LANDED", "D-RETRY", "D-STATE"),
            *("D-DISCARD", "D-REPLACE", "D-SCHEMA", "D-MERGE", "D-DELETE", "D-PARTIAL"),
            *("D-TRUNCATE", "D-HIST", "D-CHILDREN", "D-ENCODING", "D-TABLES", "D-NAMES"),
            *("D-WRITERS", "D-OWNED", "D-DROP", "D-FENCE", "K-DESTINATION"),
        ],
        # A destination grants credit and spends none.
        "inapplicable": ["P-CREDIT"],
    }, unpassed
    assert len(results) == sum(map(len, by_outcome.values()))
    # A clause that does not apply says why.
    assert all(result.reason for result in results if result.outcome == "inapplicable"), unpassed
    killed = certification.result("K-DESTINATION")
    assert killed.note is not None
    assert killed.note.startswith("killed: "), killed


def generated(rows: int) -> rdlt.Connector:
    return rdlt.Connector(
        "io.rapidbyte.generator", config={"seed": 5, "streams": [{"name": "rows", "rows": rows}]}
    )


def into(path: Path, *, served: bool) -> rdlt.Connector | rdlt.Local:
    """The warehouse at `path`, spawned by the host from its launcher or placed in process."""
    config = {"path": str(path)}
    if not served:
        return rdlt.local(Warehouse, config=config)
    return rdlt.Connector(
        "io.example.warehouse", config=config, access=rdlt.Access(write=[path.parent])
    )


def host(tmp_path: Path, connectors: Path, launchers: Path) -> rdlt.Host:
    return rdlt.Host(connectors=[connectors, launchers], writable=[tmp_path], trust_binaries=True)


@pytest.fixture
def launchers(tmp_path_factory: pytest.TempPathFactory) -> Path:
    """A directory of its own holding the warehouse's launcher, beside the writable one."""
    return launcher(tmp_path_factory.mktemp("launchers")).parent


def test_a_served_warehouse_reads_back_a_replaced_table_as_its_last_load_alone(
    tmp_path: Path, connectors: Path, launchers: Path
) -> None:
    path = tmp_path / "replaced.db"
    for rows in (40, 15):
        rdlt.pipeline(
            "replaced",
            source=generated(rows),
            destination=into(path, served=True),
            streams=[rdlt.stream("rows", write=rdlt.replace())],
            commit=rdlt.Commit(rows=10),
        ).build(host(tmp_path, connectors, launchers)).run()
    table = warehouse.published(str(path), "rows")
    assert sorted(cast("list[int]", table["id"].to_pylist())) == list(range(15))


@pytest.mark.parametrize("served", [False, True], ids=["in-process", "served"])
def test_a_merge_load_keeps_one_row_per_key_however_often_it_runs(
    tmp_path: Path, connectors: Path, launchers: Path, *, served: bool
) -> None:
    path = tmp_path / "merged.db"
    for rows in (30, 50):
        rdlt.pipeline(
            "merged",
            source=generated(rows),
            destination=into(path, served=served),
            streams=[rdlt.stream("rows", write=rdlt.merge("id"))],
            commit=rdlt.Commit(rows=7),
        ).build(host(tmp_path, connectors, launchers)).run()
    table = warehouse.published(str(path), "rows")
    assert sorted(cast("list[int]", table["id"].to_pylist())) == list(range(50))


KEYED: dict[str, Json] = {
    "path": ["rows"],
    "name": "rows",
    "version": 1,
    "generation": None,
    "merge": {
        "columns": ["id"],
        "seq": "_rdlt_seq",
        "root": None,
        "changes": None,
        "history": None,
    },
}
"""A merge table's writer, as a session is told it."""


def test_a_session_a_newer_one_fenced_claims_no_table_its_pipeline_owns(tmp_path: Path) -> None:
    path = str(tmp_path / "fenced.db")
    warehouse = Warehouse(WarehouseConfig(path=path))
    stale = warehouse.open(OpenContext("fenced")).session
    stale.writer(KEYED)
    latest = warehouse.open(OpenContext("fenced")).session
    latest.writer(KEYED)
    appended = {**KEYED, "merge": None}
    with pytest.raises(Fenced):
        stale.writer(appended)
    with pytest.raises(Fenced):
        stale.apply_schema({"add_column": {"table": appended, "field": []}})
    # The newer session's key stands: its rows are still admitted and merged by it.
    with closing(connected(path)) as database:
        held = Store(database).table("rows")
    assert held is not None
    assert held.merge == KEYED["merge"]


def shown(text: str) -> list[str]:
    """What each run of the program in `text`'s first console block prints, in order.

    Each runs it as `python examples/python/destination/main.py`, with the connectors' directory
    and the warehouse's, which the test names its own.
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
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> str:
    """What the program prints, run as its README runs it, loading into `data`."""
    monkeypatch.setattr(sys, "argv", [str(PROGRAM), str(connectors), str(data)])
    try:
        runpy.run_path(str(PROGRAM), run_name="__main__")
    except rdlt.ConfigError as error:
        if error.code not in NO_SANDBOX:
            raise
        without_sandbox(str(error))
    return capsys.readouterr().out


def test_a_second_run_merges_over_the_first_as_the_readme_shows(
    connectors: Path,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    runs = [printed(connectors, tmp_path, monkeypatch, capsys) for _ in range(2)]
    assert runs == shown((EXAMPLE / "README.md").read_text(encoding="utf-8"))
    table = warehouse.published(str(tmp_path / "warehouse.db"), "rows")
    assert sorted(cast("list[int]", table["id"].to_pylist())) == list(range(1000))


def test_a_run_that_fails_exits_1(connectors: Path, tmp_path: Path) -> None:
    # The warehouse cannot make its file in a directory that is not there.
    arguments = [str(PROGRAM), str(connectors), str(tmp_path / "missing")]
    ended = subprocess.run([sys.executable, *arguments], capture_output=True, check=False)
    assert ended.returncode == 1, ended.stderr
