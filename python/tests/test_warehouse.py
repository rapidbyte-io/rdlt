"""A Python destination that writes every way the engine has: in process, served, and certified."""

from __future__ import annotations

from contextlib import closing
from pathlib import Path
from typing import cast

import pytest

import rdlt
from rdlt.connector import Fenced, OpenContext
from rdlt.model import Json
from rdlt.testing import certify_destination
from tests.clauses import DESTINATION_PASSED, outcomes
from tests.connectors import warehouse
from tests.connectors.warehouse import Warehouse, WarehouseConfig
from tests.connectors.warehouse_store import Store, connected
from tests.launchers import warehouse_launcher


def test_the_warehouse_passes_every_destination_clause_its_kill_clause_killed(
    tmp_path: Path, tmp_path_factory: pytest.TempPathFactory
) -> None:
    certification = certify_destination(
        Warehouse,
        config={"path": str(tmp_path / "certified.db")},
        launcher=warehouse_launcher(tmp_path_factory.mktemp("launchers")),
    )
    unpassed = {
        result.clause: result.reason
        for result in certification.report().results
        if result.outcome != "passed"
    }
    expected = {"passed": DESTINATION_PASSED, "inapplicable": ["P-CREDIT"]}
    assert outcomes(certification) == expected, unpassed
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
    return warehouse_launcher(tmp_path_factory.mktemp("launchers")).parent


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
