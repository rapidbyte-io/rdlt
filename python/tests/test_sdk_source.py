"""Connectors written with the SDK, placed in process."""

from __future__ import annotations

import gc
import importlib.metadata
import os
import sqlite3
import weakref
from collections.abc import Iterator
from pathlib import Path
from typing import cast

import pytest

import rdlt
from rdlt.connector import Config, Cursor, source, stream
from rdlt.connector import _source as declared
from tests.connectors.tickets import Tickets


def into_sqlite(tmp_path: Path) -> rdlt.Connector:
    return rdlt.Connector(
        "io.rapidbyte.sqlite",
        config={"path": str(tmp_path / "out.db")},
        access=rdlt.Access(write=[tmp_path]),
    )


def count(tmp_path: Path) -> int:
    with sqlite3.connect(tmp_path / "out.db") as database:
        return int(database.execute('SELECT count(*) FROM "tickets"').fetchone()[0])


def test_a_connector_runs_in_process_with_its_configuration(
    tmp_path: Path, connectors: Path
) -> None:
    host = rdlt.Host(connectors=connectors, writable=[tmp_path], trust_binaries=True)
    local = rdlt.local(Tickets, config={"pages": 5, "token": "t0ken"})
    rdlt.pipeline("tickets", source=local, destination=into_sqlite(tmp_path)).build(host).run()
    assert count(tmp_path) == 50


def test_a_document_names_a_connector_the_host_registered(tmp_path: Path, connectors: Path) -> None:
    host = rdlt.Host(connectors=connectors, writable=[tmp_path], trust_binaries=True).source(
        Tickets
    )
    document: dict[str, rdlt.Json] = {
        "version": 1,
        "pipeline": "named",
        "source": {"connector": "io.example.tickets", "config": {"pages": 3, "token": "t0ken"}},
        "destination": {
            "connector": "io.rapidbyte.sqlite",
            "config": {"path": str(tmp_path / "out.db")},
            "access": {"write": [str(tmp_path)]},
        },
        "streams": [{"name": "tickets"}],
    }
    rdlt.Pipeline.from_document(document).build(host).run()
    assert count(tmp_path) == 30


def test_a_refused_configuration_is_a_config_error_naming_no_value(
    tmp_path: Path, connectors: Path
) -> None:
    host = rdlt.Host(connectors=connectors, writable=[tmp_path], trust_binaries=True)
    local = rdlt.local(Tickets, config={"pages": "hunter2-pages", "token": "t0ken"})
    with pytest.raises(rdlt.ConfigError) as raised:
        rdlt.pipeline("refused", source=local, destination=into_sqlite(tmp_path)).build(host)
    assert "hunter2-pages" not in str(raised.value)
    assert "pages" in str(raised.value)


_PLANTED = "s3cr3t-t0ken-value"


def test_a_secret_in_a_source_exception_is_scrubbed(
    tmp_path: Path, connectors: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("RDLT_TEST_TOKEN", _PLANTED)
    host = rdlt.Host(
        connectors=connectors,
        writable=[tmp_path],
        allow_env=["RDLT_TEST_TOKEN"],
        trust_binaries=True,
    )
    local = rdlt.local(
        Tickets,
        config={"pages": 3, "token": "${env:RDLT_TEST_TOKEN}", "fail_with_token": True},
    )
    built = rdlt.pipeline(
        "scrubbed",
        source=local,
        destination=into_sqlite(tmp_path),
        retry=rdlt.Retry(max_attempts=1),
    )
    with pytest.raises(rdlt.SourceError) as raised:
        built.build(host).run()
    report = raised.value.report()
    for shown in (str(raised.value), report.message, *report.causes):
        assert _PLANTED not in shown
    assert os.environ["RDLT_TEST_TOKEN"] == _PLANTED


def test_a_class_without_check_is_refused_where_it_is_declared() -> None:
    with pytest.raises(TypeError, match="check"):

        @source(id="io.example.unchecked", config=Config)
        class Unchecked:
            def __init__(self, config: Config) -> None:
                del config


class _Since(Cursor):
    after: int = -1


class _PagesConfig(Config):
    pages: int


def test_a_stream_can_name_the_method_that_answers_its_partitions(
    tmp_path: Path, connectors: Path
) -> None:
    @source(id="io.example.parted", config=_PagesConfig, version="1.0.0")
    class Parted:
        def __init__(self, config: _PagesConfig) -> None:
            self.pages = config.pages

        def check(self) -> None:
            """Reads need nothing checked first."""

        def regions(self) -> list[str]:
            return ["eu", "us"]

        @stream(cursor=_Since, partitions="regions")
        def rows(self, since: _Since, ctx: rdlt.StreamContext) -> Iterator[object]:
            for page in range(since.after + 1, self.pages):
                yield [{"id": f"{ctx.partition}-{page}"}]

    host = rdlt.Host(connectors=connectors, writable=[tmp_path], trust_binaries=True)
    placed = rdlt.local(Parted, config={"pages": 2})
    rdlt.pipeline("parted", source=placed, destination=into_sqlite(tmp_path)).build(host).run()
    with sqlite3.connect(tmp_path / "out.db") as database:
        ids = sorted(row[0] for row in database.execute('SELECT "id" FROM "rows"'))
    assert ids == ["eu-0", "eu-1", "us-0", "us-1"]


def test_partitions_without_a_ctx_are_refused_where_the_stream_is_declared() -> None:
    with pytest.raises(TypeError, match="partitions"):

        @stream(partitions=lambda _self: ["a"])
        def rows(self: object) -> Iterator[object]:
            del self
            yield {"id": 1}


class _Holder:
    """What a connector's closure reaches, which holds the pipeline it serves."""

    pipeline: rdlt.Pipeline | None = None


def _held_tickets(holder: _Holder) -> type:
    """A connector class whose method reaches `holder`."""

    class HeldConfig(Config):
        pages: int

    @source(id="io.example.held", config=HeldConfig, version="1.0.0")
    class Held:
        def __init__(self, config: HeldConfig) -> None:
            self.pages = config.pages

        def check(self) -> None:
            """Reads need nothing checked first."""

        @stream(primary_key=["id"])
        def rows(self) -> Iterator[object]:
            held = holder.pipeline is not None
            yield {"id": self.pages, "held": held}

    return Held


def test_a_connector_holding_its_own_pipeline_is_collected(
    tmp_path: Path, connectors: Path
) -> None:
    holder = _Holder()
    held = _held_tickets(holder)
    host = rdlt.Host(connectors=connectors, writable=[tmp_path], trust_binaries=True)
    holder.pipeline = rdlt.pipeline(
        "cycle",
        source=rdlt.local(held, config={"pages": 1}),
        destination=into_sqlite(tmp_path),
    ).build(host)
    collected = weakref.ref(holder)
    del holder, held, host
    gc.collect()
    assert collected() is None


def test_a_connector_a_host_registers_holding_its_own_pipeline_is_collected(
    tmp_path: Path, connectors: Path
) -> None:
    holder = _Holder()
    held = _held_tickets(holder)
    host = rdlt.Host(connectors=connectors, writable=[tmp_path], trust_binaries=True).source(held)
    document: dict[str, rdlt.Json] = {
        "version": 1,
        "pipeline": "cycle",
        "source": {"connector": "io.example.held", "config": {"pages": 1}},
        "destination": {
            "connector": "io.rapidbyte.sqlite",
            "config": {"path": str(tmp_path / "out.db")},
            "access": {"write": [str(tmp_path)]},
        },
        "streams": [{"name": "rows"}],
    }
    holder.pipeline = rdlt.Pipeline.from_document(document).build(host)
    collected = weakref.ref(holder)
    del holder, held, host
    gc.collect()
    assert collected() is None


def test_a_built_pipeline_and_its_run_keep_its_connector(tmp_path: Path, connectors: Path) -> None:
    @source(id="io.example.kept", config=_PagesConfig, version="1.0.0")
    class Kept:
        def __init__(self, config: _PagesConfig) -> None:
            self.pages = config.pages

        def check(self) -> None:
            """Reads need nothing checked first."""

        @stream(primary_key=["id"])
        def rows(self) -> Iterator[object]:
            yield {"id": self.pages}

    host = rdlt.Host(connectors=connectors, writable=[tmp_path], trust_binaries=True)
    built = rdlt.pipeline(
        "kept", source=rdlt.local(Kept, config={"pages": 1}), destination=into_sqlite(tmp_path)
    ).build(host)
    del host
    kept = weakref.ref(Kept)
    del Kept
    gc.collect()
    assert kept() is not None, "a built pipeline reads with its connector"
    run = built.start()
    del built
    gc.collect()
    assert kept() is not None, "a run in progress reads with its connector"
    run.wait()
    assert count_rows(tmp_path) == 1


def count_rows(tmp_path: Path) -> int:
    with sqlite3.connect(tmp_path / "out.db") as database:
        return int(database.execute('SELECT count(*) FROM "rows"').fetchone()[0])


_INLINE = "hunter2-written-inline"


def test_a_local_connector_never_shows_its_configuration(tmp_path: Path, connectors: Path) -> None:
    placed = rdlt.local(Tickets, config={"pages": "many", "token": _INLINE})
    for shown in (repr(placed), str(placed), f"{[placed]}"):
        assert _INLINE not in shown
    assert isinstance(hash(placed), int)
    host = rdlt.Host(connectors=connectors, writable=[tmp_path], trust_binaries=True)
    with pytest.raises(rdlt.ConfigError) as raised:
        rdlt.pipeline("shown", source=placed, destination=into_sqlite(tmp_path)).build(host)
    report = raised.value.report()
    for shown in (str(raised.value), repr(raised.value), report.message, *report.causes):
        assert _INLINE not in shown


def _tables(tmp_path: Path) -> set[str]:
    with sqlite3.connect(tmp_path / "out.db") as database:
        listed = database.execute("SELECT name FROM sqlite_master WHERE type = 'table'")
        return {str(row[0]) for row in listed}


def test_streams_narrows_what_a_local_connector_loads(tmp_path: Path, connectors: Path) -> None:
    @source(id="io.example.two", config=_PagesConfig, version="1.0.0")
    class Two:
        def __init__(self, config: _PagesConfig) -> None:
            self.pages = config.pages

        def check(self) -> None:
            """Reads need nothing checked first."""

        @stream(primary_key=["id"])
        def wanted(self) -> Iterator[object]:
            yield {"id": self.pages}

        @stream(primary_key=["id"])
        def unwanted(self) -> Iterator[object]:
            yield {"id": self.pages}

    built = rdlt.pipeline(
        "narrowed",
        source=rdlt.local(Two, config={"pages": 1}),
        destination=into_sqlite(tmp_path),
        streams=[rdlt.stream("wanted")],
    )
    entries = cast("list[dict[str, rdlt.Json]]", built.document()["streams"])
    assert [entry["name"] for entry in entries] == ["wanted"]
    host = rdlt.Host(connectors=connectors, writable=[tmp_path], trust_binaries=True)
    built.build(host).run()
    tables = _tables(tmp_path)
    assert "wanted" in tables
    assert "unwanted" not in tables


def _twin() -> type:
    """A connector class under the id every twin has."""

    @source(id="io.example.twin", config=_PagesConfig, version="1.0.0")
    class Twin:
        def __init__(self, config: _PagesConfig) -> None:
            self.pages = config.pages

        def check(self) -> None:
            """Reads need nothing checked first."""

        @stream(primary_key=["id"])
        def rows(self) -> Iterator[object]:
            yield {"id": self.pages}

    return Twin


def test_two_classes_under_one_id_are_refused(tmp_path: Path, connectors: Path) -> None:
    first, second = _twin(), _twin()
    host = rdlt.Host(connectors=connectors, writable=[tmp_path], trust_binaries=True).source(first)
    host.source(first)
    with pytest.raises(ValueError, match=r"io\.example\.twin"):
        host.source(second)
    pipeline = rdlt.pipeline(
        "twins", source=rdlt.local(second, config={"pages": 1}), destination=into_sqlite(tmp_path)
    )
    with pytest.raises(ValueError, match=r"io\.example\.twin"):
        pipeline.build(host)
    same = rdlt.local(first, config={"pages": 1})
    rdlt.pipeline("twin", source=same, destination=into_sqlite(tmp_path)).build(host).run()


class _Base:
    """Streams a connector class inherits, one calling a method of its own."""

    def __init__(self, config: _PagesConfig) -> None:
        self.pages = config.pages

    def check(self) -> None:
        """Reads need nothing checked first."""

    def page(self, name: str) -> list[dict[str, rdlt.Json]]:
        return [{"id": n, "from": name} for n in range(self.pages)]

    @stream(primary_key=["id"])
    def inherited(self) -> Iterator[object]:
        yield self.page("inherited")


def test_a_subclass_reads_its_base_s_streams_and_a_stream_stays_a_method(
    tmp_path: Path, connectors: Path
) -> None:
    @source(id="io.example.derived", config=_PagesConfig, version="1.0.0")
    class Derived(_Base):
        @stream(primary_key=["id"])
        def own(self) -> Iterator[object]:
            yield from self.inherited()

    host = rdlt.Host(connectors=connectors, writable=[tmp_path], trust_binaries=True)
    placed = rdlt.local(Derived, config={"pages": 3})
    rdlt.pipeline("derived", source=placed, destination=into_sqlite(tmp_path)).build(host).run()
    with sqlite3.connect(tmp_path / "out.db") as database:
        for table in ("inherited", "own"):
            landed = database.execute(f'SELECT count(*) FROM "{table}"').fetchone()
            assert landed == (3,), table


def test_a_subclass_s_plain_method_replaces_a_stream_of_the_same_name() -> None:
    @source(id="io.example.replaced", config=_PagesConfig, version="1.0.0")
    class Replaced(_Base):
        def inherited(self) -> Iterator[object]:
            yield from ()

    assert declared.declaration(Replaced).streams == ()


def test_a_connector_s_version_is_read_once_for_its_package(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    looked: list[int] = []
    distributions = importlib.metadata.packages_distributions

    def counted() -> object:
        looked.append(1)
        return distributions()

    monkeypatch.setattr(importlib.metadata, "packages_distributions", counted)
    declared.package_version.cache_clear()

    @source(id="io.example.unversioned", config=_PagesConfig)
    class Unversioned:
        def __init__(self, config: _PagesConfig) -> None:
            del config

        def check(self) -> None:
            """Reads need nothing checked first."""

    declared.factory(Unversioned)
    declared.factory(Unversioned)
    assert len(looked) == 1
