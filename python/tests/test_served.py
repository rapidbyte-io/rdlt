"""A Python connector served by the Rust server, spawned by a host as any connector binary is."""

from __future__ import annotations

import signal
import socket
import sqlite3
import subprocess
import sys
from pathlib import Path
from typing import cast

import pytest

import rdlt
from rdlt.connector import (
    OpenContext,
    Opened,
    acknowledging_source_factory,
    destination,
    readable_destination_factory,
    serve,
)
from tests.connectors import pysqlite
from tests.connectors.both import BothDestination, BothSource
from tests.connectors.tickets import Tickets
from tests.launchers import TESTS, both_launcher, launcher, pysqlite_launcher, tickets_launcher


@pytest.fixture
def launchers(tmp_path_factory: pytest.TempPathFactory) -> Path:
    """A directory of its own holding the tickets connector's launcher.

    It lies beside the test's writable directory: a host places no connector found within a
    writable root.
    """
    return tickets_launcher(tmp_path_factory.mktemp("launchers")).parent


def into_sqlite(tmp_path: Path) -> rdlt.Connector:
    return rdlt.Connector(
        "io.rapidbyte.sqlite",
        config={"path": str(tmp_path / "out.db")},
        access=rdlt.Access(write=[tmp_path]),
    )


def test_a_host_spawns_and_loads_a_served_python_connector(
    tmp_path: Path, connectors: Path, launchers: Path
) -> None:
    host = rdlt.Host(connectors=[connectors, launchers], writable=[tmp_path], trust_binaries=True)
    built = rdlt.pipeline(
        "served",
        source=rdlt.Connector("io.example.tickets", config={"pages": 4, "token": "t0ken"}),
        destination=into_sqlite(tmp_path),
        streams=[rdlt.stream("tickets", read="incremental")],
    )
    assert built.build(host).run().report.rows == 40
    with sqlite3.connect(tmp_path / "out.db") as database:
        assert database.execute('SELECT count(*) FROM "tickets"').fetchone() == (40,)


def test_a_host_spawns_and_loads_into_a_served_python_destination(
    tmp_path: Path, connectors: Path, tmp_path_factory: pytest.TempPathFactory
) -> None:
    served = pysqlite_launcher(tmp_path_factory.mktemp("launchers")).parent
    host = rdlt.Host(connectors=[connectors, served], writable=[tmp_path], trust_binaries=True)
    path = tmp_path / "served.db"
    built = rdlt.pipeline(
        "served-pysqlite",
        source=rdlt.Connector(
            "io.rapidbyte.generator",
            config={"seed": 3, "streams": [{"name": "rows", "rows": 40}]},
        ),
        destination=rdlt.Connector(
            "io.example.pysqlite", config={"path": str(path)}, access=rdlt.Access(write=[tmp_path])
        ),
        streams=[rdlt.stream("rows")],
        commit=rdlt.Commit(rows=10),
    )
    built.build(host).run()
    assert pysqlite.published(str(path), "rows") == 40


def test_one_launcher_serves_a_source_and_a_destination_a_host_spawns(
    tmp_path: Path, connectors: Path, tmp_path_factory: pytest.TempPathFactory
) -> None:
    served = both_launcher(tmp_path_factory.mktemp("launchers")).parent
    host = rdlt.Host(connectors=[connectors, served], writable=[tmp_path], trust_binaries=True)
    path = tmp_path / "both.db"
    built = rdlt.pipeline(
        "both",
        source=rdlt.Connector("io.example.both", config={"pages": 4, "token": "t0ken"}),
        destination=rdlt.Connector(
            "io.example.both", config={"path": str(path)}, access=rdlt.Access(write=[tmp_path])
        ),
        streams=[rdlt.stream("tickets", read="incremental")],
    )
    assert built.build(host).run().report.rows == 40
    assert pysqlite.published(str(path), "tickets") == 40


@pytest.mark.parametrize(
    "connectors",
    [
        (),
        (Tickets, BothSource),
        (pysqlite.PySqlite, BothDestination),
        (Tickets, object),
        (acknowledging_source_factory(Tickets), BothSource),
        ([Tickets],),
    ],
    ids=[
        "nothing",
        "two sources",
        "two destinations",
        "no connector",
        "two sources probed",
        "a list",
    ],
)
def test_serve_takes_one_class_of_each_role_at_most(connectors: tuple[object, ...]) -> None:
    before = signal.getsignal(signal.SIGINT)
    with pytest.raises(TypeError, match="serve"):
        serve(*cast("tuple[type, ...]", connectors), args=["--no-such-flag"])
    assert signal.getsignal(signal.SIGINT) == before


@destination(id="io.example.unpublished", config=pysqlite.SqliteConfig)
class Unpublished:
    """A destination with no `published(table)`, which reads nothing back."""

    def __init__(self, config: pysqlite.SqliteConfig) -> None:
        del config

    def check(self) -> None:
        """Nothing to check."""

    def open(self, context: OpenContext) -> Opened:
        """Never reached: it is only declared."""
        raise NotImplementedError(context.pipeline)


@pytest.mark.parametrize("named", ["PROBED", "PROBED_PAIR"])
def test_the_installed_launcher_refuses_a_probed_factory_alone_or_in_its_pair(
    named: str, tmp_path: Path
) -> None:
    refused = launcher(tmp_path, "both", named)
    ran = subprocess.run([refused], capture_output=True, text=True, timeout=60, check=False)
    assert ran.returncode != 0
    assert "acknowledging_source_factory(BothSource)" in ran.stderr, ran.stderr
    assert "a launcher's own code" in ran.stderr, ran.stderr


def test_a_probe_is_asked_of_a_class_that_can_answer_it() -> None:
    with pytest.raises(TypeError, match="no source"):
        acknowledging_source_factory(pysqlite.PySqlite)
    with pytest.raises(TypeError, match="no destination"):
        readable_destination_factory(Tickets)
    with pytest.raises(TypeError, match="published"):
        readable_destination_factory(Unpublished)
    assert serve(acknowledging_source_factory(Tickets), args=["--no-such-flag"]) == 1


@pytest.mark.parametrize(
    "failure",
    [
        ("fail_with_token", "runtime_error"),
        ("fail_transient", "transient"),
        ("fail_deadline", "tests.connectors.tickets.deadline_exceeded"),
    ],
)
@pytest.mark.parametrize("placement", ["in_process", "served"])
def test_a_failure_s_code_is_the_same_in_process_and_served(
    placement: str, failure: tuple[str, str], tmp_path: Path, connectors: Path, launchers: Path
) -> None:
    flag, code = failure
    config: dict[str, rdlt.Json] = {"pages": 3, "token": "t0ken", flag: True}
    directories = [connectors]
    source: rdlt.Connector | rdlt.Local
    if placement == "served":
        directories.append(launchers)
        source = rdlt.Connector("io.example.tickets", config=config)
    else:
        source = rdlt.local(Tickets, config=config)
    built = rdlt.pipeline(
        "failing",
        source=source,
        destination=into_sqlite(tmp_path),
        streams=[rdlt.stream("tickets", read="incremental")],
        retry=rdlt.Retry(max_attempts=1),
    )
    host = rdlt.Host(connectors=directories, writable=[tmp_path], trust_binaries=True)
    with pytest.raises(rdlt.SourceError) as raised:
        built.build(host).run()
    assert raised.value.report().connector_code == code


def test_a_served_connector_ignores_ctrl_c_and_ends_cleanly() -> None:
    host_end, connector_end = socket.socketpair()
    script = (
        f"import sys\nsys.path.append({str(TESTS)!r})\n"
        "from rdlt.connector import serve\nfrom tests.connectors.tickets import Tickets\n"
        f"raise SystemExit(serve(Tickets, args=['--rdlt-fd', '{connector_end.fileno()}']))\n"
    )
    with host_end:
        served = subprocess.Popen(
            [sys.executable, "-I", "-c", script],
            stdin=subprocess.PIPE,
            stderr=subprocess.PIPE,
            pass_fds=(connector_end.fileno(),),
        )
        connector_end.close()
        host_end.settimeout(60)
        # Its first bytes, the protocol's settings, say it is serving.
        assert host_end.recv(1), "the connector serves its socket"
        served.send_signal(signal.SIGINT)
    # Its host closing the connection and its standard input ends it.
    _, stderr = served.communicate(input=b"", timeout=60)
    assert served.returncode == 0, stderr.decode()
    assert b"Traceback" not in stderr, stderr.decode()


def test_arguments_it_cannot_read_are_said_and_fail_the_launcher(
    capsys: pytest.CaptureFixture[str],
) -> None:
    before = signal.getsignal(signal.SIGINT)
    assert serve(Tickets, args=["--no-such-flag"]) == 1
    assert "--no-such-flag" in capsys.readouterr().err
    assert signal.getsignal(signal.SIGINT) == before
