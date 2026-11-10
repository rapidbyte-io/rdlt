"""The connector `rdlt connector new --lang python` scaffolds, certified as written and installed.

Installed as a directory connector, it is placed by a host, in the sandbox on Linux, runs a
pipeline, and is certified with its kill clause spawning the installed launcher.
"""

from __future__ import annotations

import hashlib
import importlib
import os
import shutil
import sqlite3
import subprocess
import sys
from pathlib import Path
from typing import cast

import pytest

import rdlt
from rdlt.testing import certify_source

REPOSITORY = Path(__file__).resolve().parents[2]
TEMPLATE = REPOSITORY / "crates" / "rdlt-cli" / "templates" / "python"
"""The scaffold as written, which `rdlt connector new` renames."""
RDLT = REPOSITORY / "target" / "debug" / "rdlt"


def certified(project: Path) -> subprocess.CompletedProcess[str]:
    """The run of `project`'s certify test, as its author runs it, writing nothing beside it."""
    env = {**os.environ, "PYTHONDONTWRITEBYTECODE": "1"}
    command = [sys.executable, "-m", "pytest", "-q", "-p", "no:cacheprovider", str(project)]
    return subprocess.run(  # noqa: PLW1510 - its status is asserted
        command, env=env, cwd=project, capture_output=True, text=True, timeout=600
    )


@pytest.mark.timeout(660)
def test_the_scaffold_s_certify_test_passes_as_written() -> None:
    finished = certified(TEMPLATE)
    assert finished.returncode == 0, finished.stdout + finished.stderr


def uv() -> str:
    """Where uv is, which builds wheels and exports the lock here."""
    found = shutil.which("uv")
    if found is None:
        pytest.fail("uv is not on the path")
    return found


def requirements(tmp_path: Path) -> Path:
    """The package's locked dependencies with their hashes, and the wheel of this tree's."""
    exported = subprocess.run(
        [
            uv(),
            "export",
            "--frozen",
            "--no-default-groups",
            "--no-emit-project",
            "--format",
            "requirements-txt",
        ],
        check=True,
        capture_output=True,
        text=True,
        cwd=REPOSITORY,
        timeout=60,
    ).stdout
    (wheel,) = (REPOSITORY / "target" / "wheels").glob("*/rdlt-*.whl")
    digest = hashlib.sha256(wheel.read_bytes()).hexdigest()
    path = tmp_path / "requirements.txt"
    path.write_text(f"{exported}\nrdlt @ {wheel.as_uri()} --hash=sha256:{digest}\n")
    return path


@pytest.mark.network
@pytest.mark.timeout(1800)
def test_a_scaffolded_connector_installs_runs_and_certifies(
    tmp_path: Path, connectors: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    rdlt_command = [RDLT, "--output", "json"]
    subprocess.run(
        [*rdlt_command, "connector", "new", "tickets", "--lang", "python", "--into", tmp_path],
        check=True,
        timeout=60,
    )
    project = tmp_path / "rdlt-connector-tickets"
    wheels = tmp_path / "wheels"
    subprocess.run(
        [uv(), "build", "--wheel", "--out-dir", wheels, project], check=True, timeout=600
    )
    (wheel,) = wheels.glob("rdlt_connector_tickets-*.whl")
    python = Path("/usr/bin/python3") if sys.platform == "linux" else Path(sys.executable)
    subprocess.run(
        [
            *rdlt_command,
            "--connectors",
            connectors,
            "connector",
            "install",
            wheel,
            "--python",
            python,
            "--requirements",
            requirements(tmp_path),
        ],
        check=True,
        timeout=900,
    )
    tree = connectors / "rdlt-connector-tickets"
    launcher = tree / "bin" / "rdlt-connector"
    assert launcher.is_file()
    sandboxed = sys.platform == "linux" and os.environ.get("RDLT_REQUIRE_SANDBOX") == "1"
    host = rdlt.Host(connectors=connectors, writable=[tmp_path], trust_binaries=not sandboxed)
    built = rdlt.pipeline(
        "installed",
        source=rdlt.Connector("io.example.tickets", config={"rows": 100}),
        destination=rdlt.Connector(
            "io.rapidbyte.sqlite",
            config={"path": str(tmp_path / "out.db")},
            access=rdlt.Access(write=[tmp_path]),
        ),
        streams=[rdlt.stream("numbers")],
    )
    assert built.build(host).run().report.rows == 100
    with sqlite3.connect(tmp_path / "out.db") as database:
        assert database.execute('SELECT count(*) FROM "numbers"').fetchone() == (100,)
    finished = certified(project)
    assert finished.returncode == 0, finished.stdout + finished.stderr
    monkeypatch.setattr(sys, "path", [str(project / "src"), *sys.path])
    tickets = cast("type", importlib.import_module("rdlt_connector_tickets").Tickets)
    # As the scaffold's own certification, enough rows that the kill comes before the read ends.
    killed = certify_source(tickets, config={"rows": 50_000}, launcher=launcher)
    assert killed.result("K-SOURCE").outcome == "passed"
