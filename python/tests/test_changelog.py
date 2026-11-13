"""A source read in phases, as a database's change stream is: in process, served and certified."""

from __future__ import annotations

import sqlite3
from contextlib import closing
from pathlib import Path
from typing import cast

import pytest

import rdlt
from rdlt.testing import certify_source
from tests.clauses import SOURCE_PASSED, outcomes
from tests.connectors.changelog import Changelog, expected
from tests.launchers import changelog_launcher

CERTIFIED = 4_000
"""Keys the certified snapshot holds, and changes after it: 400 batches of 20 rows. A load long
enough that the commits K-SOURCE's kills follow land while it is read, and bounded by what
S-RESUME's clause holds, 64 MiB and 1,048,576 rows, which more small Arrow batches exceed."""


def into_sqlite(tmp_path: Path) -> rdlt.Connector:
    return rdlt.Connector(
        "io.rapidbyte.sqlite",
        config={"path": str(tmp_path / "out.db")},
        access=rdlt.Access(write=[tmp_path]),
    )


def table(tmp_path: Path) -> list[tuple[object, ...]]:
    with closing(sqlite3.connect(tmp_path / "out.db")) as database:
        return database.execute('SELECT id, value, n FROM "rows" ORDER BY id').fetchall()


def changelog(tmp_path: Path, source: rdlt.Connector | rdlt.Local) -> rdlt.PipelineBuilder:
    merged = rdlt.stream("rows", read="cdc", write=rdlt.merge("id"))
    return rdlt.pipeline(
        "changelog", source=source, destination=into_sqlite(tmp_path), streams=[merged]
    )


def config(tmp_path: Path, rows: int = 30, changes: int = 20) -> dict[str, rdlt.Json]:
    return {"rows": rows, "changes": changes, "slot": str(tmp_path / "changelog.slot")}


def test_a_source_read_in_phases_loads_its_snapshot_then_its_changes_once(
    tmp_path: Path, connectors: Path
) -> None:
    host = rdlt.Host(connectors=connectors, writable=[tmp_path], trust_binaries=True)
    built = changelog(tmp_path, rdlt.local(Changelog, config=config(tmp_path)))
    built.build(host).run()
    assert table(tmp_path) == expected(30, 20)
    # The next run reads on from where its changes stood, and finds none.
    assert built.build(host).run().report.rows == 0
    assert table(tmp_path) == expected(30, 20)


def test_a_source_read_in_phases_says_how_it_is_read_and_when_each_change_happened(
    tmp_path: Path, connectors: Path
) -> None:
    host = rdlt.Host(connectors=connectors, writable=[tmp_path], trust_binaries=True)
    built = changelog(tmp_path, rdlt.local(Changelog, config=config(tmp_path)))
    (found,) = built.build(host).discover().streams
    assert found.read_modes == (rdlt.ReadMode.CDC,)
    assert found.raw["change_time"] == ["changed_at"]
    schema = cast("dict[str, list[list[dict[str, rdlt.Json]]]]", found.raw["schema"])
    assert [field[0]["name"] for field in schema["fields"]] == ["id", "value", "n", "changed_at"]


def test_a_source_read_in_phases_passes_every_source_clause(tmp_path: Path) -> None:
    certification = certify_source(
        Changelog,
        config=config(tmp_path, rows=CERTIFIED, changes=CERTIFIED),
        launcher=changelog_launcher(tmp_path / "launchers"),
    )
    assert outcomes(certification) == {"passed": [*SOURCE_PASSED[:-1], "S-ACK", "K-SOURCE"]}


def test_a_host_spawns_a_served_source_read_in_phases_and_loads_its_changes(
    tmp_path: Path, connectors: Path, tmp_path_factory: pytest.TempPathFactory
) -> None:
    launchers = changelog_launcher(tmp_path_factory.mktemp("launchers")).parent
    host = rdlt.Host(connectors=[connectors, launchers], writable=[tmp_path], trust_binaries=True)
    served = rdlt.Connector(
        "io.example.changelog", config=config(tmp_path), access=rdlt.Access(write=[tmp_path])
    )
    changelog(tmp_path, served).build(host).run()
    assert table(tmp_path) == expected(30, 20)
