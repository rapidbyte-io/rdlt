"""A source's streams found at run time, the catalog it discovers, and a declared schema."""

from __future__ import annotations

import gc
import sqlite3
import weakref
from collections.abc import Iterator
from contextlib import closing
from functools import partial
from pathlib import Path
from typing import TYPE_CHECKING

import pyarrow as pa
import pytest

import rdlt
from rdlt.connector import (
    Checkpointing,
    Config,
    Cursor,
    InvalidConfig,
    ReadStream,
    StreamSpec,
    source,
    stream,
)
from rdlt.testing import certify_source
from tests.clauses import KILLED_PAGES, SOURCE_PASSED, SOURCE_UNTOLD, outcomes
from tests.connectors.tables import Tables
from tests.failures import said
from tests.launchers import tables_launcher
from tests.processes import children, ended

if TYPE_CHECKING:
    from rdlt.model import CatalogStream

MAX_CATALOG_STREAMS = 65_536
"""The most streams a catalog holds, the Rust SDK's `MAX_CATALOG_STREAMS`."""

EVERY = [rdlt.stream("*")]
"""Every stream the source finds, which only its connection knows."""


def host(tmp_path: Path, connectors: Path, *more: Path) -> rdlt.Host:
    return rdlt.Host(connectors=[connectors, *more], writable=[tmp_path], trust_binaries=True)


def into_sqlite(tmp_path: Path) -> rdlt.Connector:
    return rdlt.Connector(
        "io.rapidbyte.sqlite",
        config={"path": str(tmp_path / "out.db")},
        access=rdlt.Access(write=[tmp_path]),
    )


def rows_of(tmp_path: Path, table: str) -> list[tuple[object, ...]]:
    with closing(sqlite3.connect(tmp_path / "out.db")) as database:
        columns = [
            row[1]
            for row in database.execute(f'PRAGMA table_info("{table}")')
            if not str(row[1]).startswith("_rdlt")
        ]
        selected = ", ".join(f'"{column}"' for column in sorted(columns))
        return sorted(database.execute(f'SELECT {selected} FROM "{table}"').fetchall(), key=repr)


def made(directory: Path, users: int = 3, notes: int = 2, events: int = 2) -> dict[str, rdlt.Json]:
    """The tables source's configuration over two databases in `directory`.

    The main one has `users`, keyed by an integer, and `notes`, keyed by text; the one attached
    as the schema `archive` has `events`.
    """
    with closing(sqlite3.connect(directory / "in.db")) as database, database:
        database.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)")
        database.execute("CREATE TABLE notes (slug TEXT PRIMARY KEY, body TEXT, weight REAL)")
    with closing(sqlite3.connect(directory / "archive.db")) as database, database:
        database.execute("CREATE TABLE events (seq INTEGER PRIMARY KEY, kind TEXT)")
    config: dict[str, rdlt.Json] = {
        "path": str(directory / "in.db"),
        "attached": {"archive": str(directory / "archive.db")},
    }
    return grown(config, users, notes, events)


def grown(
    config: dict[str, rdlt.Json], users: int, notes: int, events: int
) -> dict[str, rdlt.Json]:
    """`config`'s tables holding `users` users, `notes` notes and `events` events."""
    with closing(sqlite3.connect(str(config["path"]))) as database, database:
        database.executemany(
            "INSERT OR IGNORE INTO users VALUES (?, ?)", [(n, f"user {n}") for n in range(users)]
        )
        database.executemany(
            "INSERT OR IGNORE INTO notes VALUES (?, ?, ?)",
            [(f"note-{n:03}", f"body {n}", n / 2) for n in range(notes)],
        )
    attached = config["attached"]
    assert isinstance(attached, dict)
    with closing(sqlite3.connect(str(attached["archive"]))) as database, database:
        database.executemany(
            "INSERT OR IGNORE INTO events VALUES (?, ?)", [(n, f"kind {n}") for n in range(events)]
        )
    return config


def catalog_of(built: rdlt.Pipeline) -> dict[str, CatalogStream]:
    return {found.name: found for found in built.discover().streams}


def columns(found: CatalogStream) -> list[tuple[str, str, bool]]:
    """Each column of `found`'s declared schema: its name, its type and whether it holds nulls."""
    schema = found.raw["schema"]
    assert isinstance(schema, dict), found.raw
    fields = schema["fields"]
    assert isinstance(fields, list)
    described: list[tuple[str, str, bool]] = []
    for field in fields:
        assert isinstance(field, list)
        node = field[0]
        assert isinstance(node, dict)
        described.append((str(node["name"]), str(node["type"]), bool(node["nullable"])))
    return described


FOUND = ["archive.events", "notes", "users"]
"""The streams the tables source finds, as its catalog shows them."""


def test_a_source_s_streams_are_the_tables_its_connection_finds(
    tmp_path: Path, connectors: Path
) -> None:
    local = rdlt.local(Tables, config=made(tmp_path))
    built = rdlt.pipeline("found", source=local, destination=into_sqlite(tmp_path), streams=EVERY)
    built = built.build(host(tmp_path, connectors))
    catalog = catalog_of(built)
    assert sorted(catalog) == FOUND
    assert catalog["users"].primary_key == (("id",),)
    assert catalog["notes"].primary_key == (("slug",),)
    assert catalog["archive.events"].raw["name"] == {"namespace": "archive", "name": "events"}
    assert built.run().report.rows == 7
    assert rows_of(tmp_path, "users") == [(0, "user 0"), (1, "user 1"), (2, "user 2")]
    assert rows_of(tmp_path, "notes") == [("body 0", "note-000", 0.0), ("body 1", "note-001", 0.5)]
    assert rows_of(tmp_path, "archive.events") == [("kind 0", 0), ("kind 1", 1)]


def test_each_stream_found_declares_its_table_s_schema(tmp_path: Path, connectors: Path) -> None:
    local = rdlt.local(Tables, config=made(tmp_path))
    built = rdlt.pipeline("schemas", source=local, destination=into_sqlite(tmp_path), streams=EVERY)
    catalog = catalog_of(built.build(host(tmp_path, connectors)))
    assert columns(catalog["users"]) == [("id", "int64", False), ("name", "utf8", True)]
    assert columns(catalog["notes"]) == [
        ("slug", "utf8", False),
        ("body", "utf8", True),
        ("weight", "float64", True),
    ]
    assert columns(catalog["archive.events"]) == [("seq", "int64", False), ("kind", "utf8", True)]


def test_each_stream_found_resumes_from_its_own_cursor_of_its_own_type(
    tmp_path: Path, connectors: Path
) -> None:
    config = made(tmp_path)
    local = rdlt.local(Tables, config=config)
    streams = [
        rdlt.stream("users", read="incremental"),
        rdlt.stream("notes", read="incremental"),
        rdlt.stream("archive.events", read="incremental"),
    ]
    built = rdlt.pipeline(
        "resumed", source=local, destination=into_sqlite(tmp_path), streams=streams
    )
    every_host = host(tmp_path, connectors)
    assert built.build(every_host).run().report.rows == 7
    grown(config, users=5, notes=4, events=3)
    assert built.build(every_host).run().report.rows == 5
    assert [row[0] for row in rows_of(tmp_path, "users")] == [0, 1, 2, 3, 4]
    assert [row[1] for row in rows_of(tmp_path, "notes")] == [f"note-00{n}" for n in range(4)]
    assert [row[1] for row in rows_of(tmp_path, "archive.events")] == [0, 1, 2]


def test_a_class_with_streams_and_stream_methods_is_refused_where_it_is_declared() -> None:
    with pytest.raises(TypeError, match=r"streams\(\)"):

        @source(id="io.example.mixed", config=Config, version="1.0.0")
        class Mixed:
            def __init__(self, config: Config) -> None:
                del config

            def check(self) -> None:
                """Nothing to check."""

            def streams(self) -> list[ReadStream]:
                return []

            @stream()
            def rows(self) -> Iterator[object]:
                yield {"id": 1}


def test_a_class_with_streams_whose_base_has_stream_methods_is_refused() -> None:
    class Base:
        @stream()
        def rows(self) -> Iterator[object]:
            yield {"id": 1}

    with pytest.raises(TypeError, match="rows"):

        @source(id="io.example.mixed", config=Config, version="1.0.0")
        class Mixed(Base):
            def __init__(self, config: Config) -> None:
                del config

            def check(self) -> None:
                """Nothing to check."""

            def streams(self) -> list[ReadStream]:
                return []


def test_streams_that_are_no_method_are_refused_where_they_are_declared() -> None:
    with pytest.raises(TypeError, match="streams"):

        @source(id="io.example.listed", config=Config, version="1.0.0")
        class Listed:
            streams: tuple[str, ...] = ("users",)

            def __init__(self, config: Config) -> None:
                del config

            def check(self) -> None:
                """Nothing to check."""


@pytest.mark.parametrize("method", ["streams", "discover"])
def test_an_async_streams_or_discover_is_refused_where_it_is_declared(method: str) -> None:
    async def answering(self: object) -> list[object]:
        del self
        return []

    def streams(self: object) -> list[ReadStream]:
        del self
        return []

    def check(self: object) -> None:
        del self

    body: dict[str, object] = {"check": check, "streams": streams, method: answering}
    with pytest.raises(TypeError, match=f"{method}.*async"):
        source(id="io.example.async", config=Config, version="1.0.0")(type("Async", (), body))


class _Names(Config):
    names: tuple[str, ...] = ()
    count: int = 0
    refused: bool = False


@source(id="io.example.named", config=_Names, version="1.0.0")
class _Named:
    """A source whose streams are those its configuration names, or `count` of them."""

    def __init__(self, config: _Names) -> None:
        self.config = config

    def check(self) -> None:
        """Nothing to check."""

    def streams(self) -> list[ReadStream]:
        if self.config.refused:
            raise InvalidConfig("the database has no schema named sales", code="schema_missing")
        names = self.config.names or [f"s{n}" for n in range(self.config.count)]
        return [stream(name)(partial(self.rows, name)) for name in names]

    def rows(self, name: str) -> Iterator[object]:
        yield {"stream": name}


class _Discovered(Config):
    names: list[str]


@source(id="io.example.discovering", config=_Discovered, version="1.0.0")
class _Discovering:
    """A source that discovers its own catalog: its streams' specs, each keyed by `stream`."""

    def __init__(self, config: _Discovered) -> None:
        self.names = config.names

    def check(self) -> None:
        """Nothing to check."""

    def streams(self) -> list[ReadStream]:
        return [stream(name)(partial(self.rows, name)) for name in dict.fromkeys(self.names)]

    def discover(self) -> list[StreamSpec]:
        return [StreamSpec(name, primary_key=("stream",)) for name in self.names]

    def rows(self, name: str) -> Iterator[object]:
        yield {"stream": name}


def test_streams_that_raise_fail_the_source_s_connect_as_their_kind(
    tmp_path: Path, connectors: Path
) -> None:
    local = rdlt.local(_Named, config={"refused": True})
    built = rdlt.pipeline(
        "refused", source=local, destination=into_sqlite(tmp_path), streams=[rdlt.stream("a")]
    )
    with pytest.raises(rdlt.ConfigError) as raised:
        built.build(host(tmp_path, connectors)).run()
    assert raised.value.report().connector_code == "schema_missing"


@pytest.mark.parametrize("answered", ["streams", "discover"])
def test_an_answer_that_names_one_stream_twice_is_refused(
    answered: str, tmp_path: Path, connectors: Path
) -> None:
    names: list[rdlt.Json] = ["a", "b", "a"]
    local = (
        rdlt.local(_Named, config={"names": names})
        if answered == "streams"
        else rdlt.local(_Discovering, config={"names": names})
    )
    built = rdlt.pipeline(
        "twice", source=local, destination=into_sqlite(tmp_path), streams=[rdlt.stream("a")]
    )
    with pytest.raises(rdlt.SourceError) as raised:
        built.build(host(tmp_path, connectors)).run()
    assert not raised.value.retryable
    assert "stream a appears more than once in the catalog" in said(raised.value)


def test_streams_beyond_what_a_catalog_holds_are_refused(tmp_path: Path, connectors: Path) -> None:
    local = rdlt.local(_Named, config={"count": MAX_CATALOG_STREAMS + 1})
    built = rdlt.pipeline(
        "many", source=local, destination=into_sqlite(tmp_path), streams=[rdlt.stream("s0")]
    )
    with pytest.raises(rdlt.SourceError) as raised:
        built.build(host(tmp_path, connectors)).run()
    assert not raised.value.retryable
    assert "beyond the limit of 65536" in said(raised.value)


def test_a_source_that_discovers_answers_its_own_catalog(tmp_path: Path, connectors: Path) -> None:
    local = rdlt.local(_Discovering, config={"names": ["a", "b"]})
    built = rdlt.pipeline(
        "discovered", source=local, destination=into_sqlite(tmp_path), streams=EVERY
    )
    catalog = catalog_of(built.build(host(tmp_path, connectors)))
    assert sorted(catalog) == ["a", "b"]
    assert [found.primary_key for found in catalog.values()] == [(("stream",),), (("stream",),)]


class _Holder:
    """What an instance of `_HeldFound` keeps: the pipeline that placed it, once it is built."""

    pipeline: rdlt.Pipeline | None = None


_HOLDERS: dict[str, _Holder] = {}


class _HeldConfig(Config):
    holder: str


@source(id="io.example.held-found", config=_HeldConfig, version="1.0.0")
class _HeldFound:
    """A source whose instance keeps the holder its configuration names.

    Its streams, found as it connects, hold the instance through its bound method.
    """

    def __init__(self, config: _HeldConfig) -> None:
        self.holder = _HOLDERS.get(config.holder)

    def check(self) -> None:
        """Reads need nothing checked first."""

    def streams(self) -> list[ReadStream]:
        return [stream("rows", primary_key="id")(self.rows)]

    def rows(self) -> Iterator[object]:
        yield {"id": 1}


def test_a_source_whose_found_streams_reach_its_pipeline_is_collected_with_its_processes(
    tmp_path: Path, connectors: Path
) -> None:
    gc.collect()
    before = children()
    holder = _HOLDERS.setdefault("found", _Holder())
    every_host = host(tmp_path, connectors)
    holder.pipeline = rdlt.pipeline(
        "found-cycle",
        source=rdlt.local(_HeldFound, config={"holder": "found"}),
        destination=into_sqlite(tmp_path),
        streams=EVERY,
    ).build(every_host)
    holder.pipeline.run()
    spawned = children() - before
    del _HOLDERS["found"]
    collected = weakref.ref(holder)
    del holder, every_host
    gc.collect()
    assert (collected() is None, ended(spawned)) == (True, True)


class _Since(Cursor):
    after: int = -1


def test_a_stream_s_spec_is_what_its_declaration_says() -> None:
    @stream(primary_key="id", cursor=_Since)
    def users(since: _Since) -> Iterator[object]:
        del since
        yield {"id": 1}

    assert users.spec == StreamSpec(
        "users",
        primary_key=("id",),
        read_modes=(rdlt.ReadMode.FULL, rdlt.ReadMode.INCREMENTAL),
    )


def test_a_spec_s_key_of_one_column_is_that_column() -> None:
    assert StreamSpec("users", primary_key="id").primary_key == ("id",)
    assert StreamSpec("users", primary_key=["id", "at"]).primary_key == ("id", "at")


def test_a_spec_s_read_modes_are_modes_or_their_names_and_never_one_name() -> None:
    spec = StreamSpec("users", read_modes=["full", rdlt.ReadMode.CDC])
    assert spec.read_modes == (rdlt.ReadMode.FULL, rdlt.ReadMode.CDC)
    with pytest.raises(TypeError, match="read_modes= is a sequence of read modes, not one"):
        StreamSpec("users", read_modes="cdc")  # pyright: ignore[reportArgumentType]
    with pytest.raises(TypeError, match="read_modes= is a sequence of read modes, not one"):

        @stream(cursor=_Since, read_modes="cdc")  # pyright: ignore[reportArgumentType]
        def users(since: _Since) -> Iterator[object]:
            del since
            yield {"id": 1}


@pytest.mark.parametrize("given", ["on_demand", Checkpointing.ON_DEMAND])
def test_a_spec_says_when_it_checkpoints_by_a_member_or_its_name(given: object) -> None:
    spec = StreamSpec("users", checkpointing=given)  # pyright: ignore[reportArgumentType]
    assert spec.checkpointing is Checkpointing.ON_DEMAND
    with pytest.raises(ValueError, match="sometimes"):
        StreamSpec("users", checkpointing="sometimes")  # pyright: ignore[reportArgumentType]


@source(id="io.example.by-name", config=Config, version="1.0.0")
class _ByName:
    """A source whose found stream names the method it is told its commits by."""

    def __init__(self, config: Config) -> None:
        del config

    def check(self) -> None:
        """Nothing to check."""

    def streams(self) -> list[ReadStream]:
        return [stream("rows", cursor=_Since, committed="settle")(self.rows)]

    def settle(self, cursors: object) -> None:
        del cursors

    def rows(self, since: _Since) -> Iterator[object]:
        del since
        yield {"id": 1}


def test_a_found_stream_that_names_a_method_is_refused_as_its_source_connects(
    tmp_path: Path, connectors: Path
) -> None:
    built = rdlt.pipeline(
        "by-name", source=rdlt.local(_ByName), destination=into_sqlite(tmp_path), streams=EVERY
    )
    with pytest.raises(rdlt.SourceError) as raised:
        built.build(host(tmp_path, connectors)).run()
    assert raised.value.report().connector_code == "type_error"
    assert "committed= names a method" in said(raised.value)


def test_a_function_s_stream_that_names_a_method_is_refused_as_its_pipeline_is_made(
    tmp_path: Path,
) -> None:
    @stream(cursor=_Since, committed="settle")
    def users(since: _Since) -> Iterator[object]:
        del since
        yield {"id": 1}

    with pytest.raises(TypeError, match="committed= names a method"):
        rdlt.pipeline("by-name", source=users, destination=into_sqlite(tmp_path))


def test_a_stream_is_named_within_its_namespace(tmp_path: Path, connectors: Path) -> None:
    @stream(rdlt.StreamName("orders", namespace="sales"), primary_key="id")
    def orders() -> Iterator[object]:
        yield [{"id": 1}, {"id": 2}]

    built = rdlt.pipeline("namespaced", source=orders, destination=into_sqlite(tmp_path))
    built = built.build(host(tmp_path, connectors))
    (found,) = built.discover().streams
    assert found.name == "sales.orders"
    assert found.raw["name"] == {"namespace": "sales", "name": "orders"}
    assert built.run().report.rows == 2


_ROWS = pa.schema([pa.field("id", pa.int64(), nullable=False), pa.field("name", pa.string())])


def _nested(levels: int) -> pa.DataType:
    """An integer beneath `levels` structs."""
    nested: pa.DataType = pa.int64()
    for _ in range(levels):
        nested = pa.struct([pa.field("inner", nested)])
    return nested


class _Exported:
    """A schema known only by `__arrow_c_schema__`, as a library other than pyarrow exports one."""

    def __init__(self, schema: pa.Schema) -> None:
        self._schema = schema

    def __arrow_c_schema__(self) -> object:
        return self._schema.__arrow_c_schema__()


@pytest.mark.parametrize("declared", [_ROWS, _Exported(_ROWS)], ids=["pyarrow", "exported"])
def test_a_declared_schema_is_the_stream_s_in_its_catalog(
    declared: object, tmp_path: Path, connectors: Path
) -> None:
    @stream(primary_key="id", schema=declared)
    def users() -> Iterator[object]:
        yield pa.record_batch({"id": [1, 2], "name": ["a", "b"]}, schema=_ROWS)

    built = rdlt.pipeline("declared", source=users, destination=into_sqlite(tmp_path))
    built = built.build(host(tmp_path, connectors))
    (found,) = built.discover().streams
    assert columns(found) == [("id", "int64", False), ("name", "utf8", True)]
    assert built.run().report.rows == 2


@pytest.mark.parametrize(
    "refused",
    [
        pa.schema([pa.field("span", pa.month_day_nano_interval())]),
        pa.schema([pa.field("either", pa.dense_union([pa.field("n", pa.int64())]))]),
        pa.schema([pa.field("amount", pa.decimal128(5, -2))]),
        pa.schema([pa.field("id", pa.int64()), pa.field("id", pa.string())]),
        # A top-level column is the first level, so 64 structs beneath it nest 65 deep.
        pa.schema([pa.field("deep", _nested(64))]),
    ],
    ids=["interval", "union", "negative-scale", "repeated-name", "too-deep"],
)
def test_a_schema_rust_cannot_take_is_refused_where_it_is_declared(refused: pa.Schema) -> None:
    with pytest.raises(ValueError, match="'measured'"):

        @stream(schema=refused)
        def measured() -> Iterator[object]:
            yield {"n": 1}


def test_a_spec_a_source_discovers_is_refused_where_it_is_made_with_a_schema_rust_cannot_take() -> (
    None
):
    with pytest.raises(ValueError, match="'measured'"):
        StreamSpec("measured", schema=pa.schema([pa.field("span", pa.month_day_nano_interval())]))


class _Uncapsuled:
    """A schema whose export answers no capsule."""

    def __arrow_c_schema__(self) -> object:
        return "no capsule"


def test_a_schema_whose_export_is_no_capsule_is_refused_naming_its_stream() -> None:
    with pytest.raises(TypeError, match="'measured'"):

        @stream(schema=_Uncapsuled())
        def measured() -> Iterator[object]:
            yield {"id": 1}


def test_a_schema_that_is_no_arrow_schema_is_refused_where_it_is_declared() -> None:
    with pytest.raises(TypeError, match="__arrow_c_schema__"):

        @stream(schema={"id": "int64"})
        def measured() -> Iterator[object]:
            yield {"id": 1}


def test_a_source_whose_streams_its_connection_found_passes_every_source_clause(
    tmp_path: Path,
) -> None:
    config = made(tmp_path, users=KILLED_PAGES * 10, notes=40, events=40)
    certification = certify_source(
        Tables, config=config, launcher=tables_launcher(tmp_path / "launchers")
    )
    assert outcomes(certification) == {"passed": SOURCE_PASSED, "inapplicable": SOURCE_UNTOLD}


def test_a_host_spawns_a_served_source_whose_streams_its_connection_found(
    tmp_path: Path, connectors: Path, tmp_path_factory: pytest.TempPathFactory
) -> None:
    launchers = tables_launcher(tmp_path_factory.mktemp("launchers")).parent
    served = rdlt.Connector(
        "io.example.tables", config=made(tmp_path), access=rdlt.Access(write=[tmp_path])
    )
    built = rdlt.pipeline(
        "served-tables", source=served, destination=into_sqlite(tmp_path), streams=EVERY
    )
    built = built.build(host(tmp_path, connectors, launchers))
    catalog = catalog_of(built)
    assert sorted(catalog) == FOUND
    assert columns(catalog["users"]) == [("id", "int64", False), ("name", "utf8", True)]
    assert built.run().report.rows == 7
    assert [row[0] for row in rows_of(tmp_path, "users")] == [0, 1, 2]
    assert rows_of(tmp_path, "archive.events") == [("kind 0", 0), ("kind 1", 1)]
