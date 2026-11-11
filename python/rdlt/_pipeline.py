# pyright: reportPrivateUsage=false
"""Pipelines: described by a document or a builder, built against a host, then checked and run."""

from __future__ import annotations

import json
import os
from collections.abc import Mapping, Sequence
from dataclasses import KW_ONLY, dataclass, field
from datetime import timedelta
from pathlib import Path
from typing import Literal, TypeAlias, cast

from rdlt import _native
from rdlt._transform import Transform, named_once
from rdlt.connector import _destination, _source
from rdlt.connector._declared import Registered, registered_once
from rdlt.connector._source import ReadStream
from rdlt.host import Host, StrPath, _names, _paths
from rdlt.model import Catalog, Json, Outcome, ReadMode, _parse
from rdlt.run import Run, _ended

_ReadWord: TypeAlias = Literal["full", "incremental", "cdc"]
_WriteWord: TypeAlias = Literal["append", "replace", "merge", "history"]
_DeleteWord: TypeAlias = Literal["hard", "soft", "ignore"]
_TruncateWord: TypeAlias = Literal["apply", "ignore"]
_RetentionWord: TypeAlias = Literal["fail", "reset"]
_PolicyWord: TypeAlias = Literal["evolve", "freeze", "discard_row", "discard_value"]
_UnsupportedWord: TypeAlias = Literal["variant_column", "refuse"]
_NestedWord: TypeAlias = Literal["native", "json", "normalize"]
_UntilWord: TypeAlias = Literal["exhausted", "forever"]


def _kept(fields: Mapping[str, Json | None]) -> dict[str, Json]:
    """`fields` without the entries nothing was said of."""
    return {key: value for key, value in fields.items() if value is not None}


@dataclass(frozen=True, slots=True)
class WriteMode:
    """How a stream's rows land: `append()`, `replace()`, `merge(*key)` or `history(*key)`."""

    _document: Json = field(hash=False)

    def __repr__(self) -> str:
        return f"WriteMode({self._document!r})"


def append() -> WriteMode:
    """Every row is added."""
    return WriteMode("append")


def replace() -> WriteMode:
    """Each run's rows replace the table's, at the run's commit."""
    return WriteMode("replace")


def merge(*key: str) -> WriteMode:
    """A row replaces the row with the same `key`."""
    if not key:
        raise ValueError("a merge needs at least one key column")
    return WriteMode({"merge": {"key": list(key)}})


def history(*key: str) -> WriteMode:
    """Every version of each `key` is kept."""
    if not key:
        raise ValueError("a history needs at least one key column")
    return WriteMode({"history": {"key": list(key)}})


@dataclass(frozen=True, slots=True, kw_only=True)
class Access:
    """What a pipeline lets its connector reach, within the host's roots."""

    read: StrPath | Sequence[StrPath] = ()
    """Paths it may read."""
    write: StrPath | Sequence[StrPath] = ()
    """Paths it may read and write."""
    network: bool = False
    """Whether it shares the host's network, where the host allows it."""

    def __post_init__(self) -> None:
        object.__setattr__(self, "read", _paths(self.read))
        object.__setattr__(self, "write", _paths(self.write))

    def _document(self) -> dict[str, Json]:
        document: dict[str, Json] = {}
        read = _paths(self.read)
        write = _paths(self.write)
        if read:
            document["read"] = [os.fspath(path) for path in read]
        if write:
            document["write"] = [os.fspath(path) for path in write]
        if self.network:
            document["network"] = True
        return document


@dataclass(frozen=True, slots=True)
class Connector:
    """A connector a pipeline names by its id; the host decides where it runs.

    `rdlt.Connector("io.rapidbyte.sqlite", config={"path": "/data/out.db"})`.
    """

    reference: str
    """The connector's id, such as `io.rapidbyte.sqlite`."""
    _: KW_ONLY
    config: Mapping[str, Json] | None = field(default=None, repr=False, hash=False)
    """Its configuration, which may hold a credential written inline and is never shown."""
    access: Access | None = None
    """What its placement lets it reach."""
    version: str | None = None
    """The versions accepted, a semver requirement such as `^1.2`."""
    digest: str | None = None
    """The SHA-256 of the binary that must run, in hex."""
    endpoint: str | None = field(default=None, repr=False)
    """Where it listens, `grpcs://host:port`, for a remote connector; never shown, as it may hold
    a credential."""

    def _document(self) -> dict[str, Json]:
        pinned = {"version": self.version, "digest": self.digest, "endpoint": self.endpoint}
        reference: dict[str, Json] = {"id": self.reference}
        reference.update(_kept(pinned))
        named: Json = self.reference if len(reference) == 1 else reference
        document: dict[str, Json] = {"connector": named}
        if self.config is not None:
            document["config"] = dict(self.config)
        if self.access is not None:
            document["access"] = self.access._document()
        return document


@dataclass(frozen=True, slots=True)
class Local:
    """A connector class placed in process, trusted as the program's own code."""

    connector: type
    """The class, which `@source` or `@destination` made."""
    config: Mapping[str, Json] | None = field(default=None, repr=False, hash=False)
    """Its configuration, which may hold a credential written inline and is never shown."""


def local(connector: type, *, config: Mapping[str, Json] | None = None) -> Local:
    """`connector`, a class `@source` or `@destination` made, placed in process with `config`."""
    return Local(connector, config)


@dataclass(frozen=True, slots=True, kw_only=True)
class SchemaRules:
    """What a schema does with a value that would change its table, or a change it cannot apply."""

    policy: _PolicyWord | None = None
    """What happens to a value that would change its table, or that its column cannot hold."""
    on_unsupported: _UnsupportedWord | None = None
    """What happens to a change the destination cannot apply in place."""

    def _document(self) -> dict[str, Json]:
        return _kept({"policy": self.policy, "on_unsupported": self.on_unsupported})


@dataclass(frozen=True, slots=True, kw_only=True)
class Column:
    """A column's own settings, over its stream's and the defaults."""

    policy: _PolicyWord | None = None
    """What happens to a value that would change its table, or that it cannot hold."""
    on_unsupported: _UnsupportedWord | None = None
    """What happens to a change of it the destination cannot apply in place."""
    nested: _NestedWord | Mapping[str, Json] | None = field(default=None, hash=False)
    """How its nested values are stored: a word, or `{"normalize": {"max_depth": n}}`."""
    hint: str | Mapping[str, Json] | None = field(default=None, hash=False)
    """Its fixed type: a word such as `int64`, or `{"decimal": {"precision": p, "scale": s}}`."""

    def _document(self) -> dict[str, Json]:
        return _kept(
            {
                "policy": self.policy,
                "on_unsupported": self.on_unsupported,
                "nested": None if self.nested is None else _value(self.nested),
                "hint": None if self.hint is None else _value(self.hint),
            }
        )


def _value(value: str | Mapping[str, Json]) -> Json:
    return value if isinstance(value, str) else dict(value)


def _step(step: Mapping[str, Json] | Transform) -> dict[str, Json]:
    return {"code": step.name} if isinstance(step, Transform) else dict(step)


@dataclass(frozen=True, slots=True, kw_only=True)
class Stream:
    """A stream a pipeline selects, and how it reads and writes it."""

    name: str
    """The stream's name, as the source's catalog shows it, or a pattern of `*` and `?`."""
    read: ReadMode | _ReadWord | None = None
    """How it is read; the defaults' where it says nothing."""
    write: WriteMode | _WriteWord | None = None
    """How its rows land; the defaults' where it says nothing."""
    deletes: _DeleteWord | None = None
    """What a change stream's deletes do to its table; hard where it says nothing."""
    on_truncate: _TruncateWord | None = None
    """What a change stream's truncates do to its table; applied where it says nothing."""
    on_retention_loss: _RetentionWord | None = None
    """What it does where its source dropped where a read would resume; fails where it says
    nothing."""
    schema: SchemaRules | None = None
    """What its schema does, where it differs from the defaults'."""
    nested: _NestedWord | Mapping[str, Json] | None = field(default=None, hash=False)
    """How its nested values are stored: a word, or `{"normalize": {"max_depth": n}}`."""
    columns: Mapping[str, Column] = field(default_factory=dict[str, Column], hash=False)
    """Each top-level column's own settings, by name."""
    include: str | Sequence[str] | None = None
    """The only top-level columns it loads, beside its keys and change columns."""
    exclude: str | Sequence[str] | None = None
    """Top-level columns it leaves out."""
    transform: Sequence[Mapping[str, Json] | Transform] = field(default=(), repr=False, hash=False)
    """What reshapes its rows, in order: an `@rdlt.transform` or `@rdlt.filter`, or a step as a
    document writes it: `{"sql": "..."}`, `{"rename": {...}}`, `{"constant": {...}}`,
    `{"mask": {...}}` or `{"code": "name"}`. Never shown: a mask's key is only ever a reference,
    but a step is the author's text."""

    def __post_init__(self) -> None:
        if self.include is not None:
            object.__setattr__(self, "include", _names(self.include))
        if self.exclude is not None:
            object.__setattr__(self, "exclude", _names(self.exclude))

    def _document(self) -> dict[str, Json]:
        write = self.write._document if isinstance(self.write, WriteMode) else self.write
        columns: dict[str, Json] = {
            name: column._document() for name, column in self.columns.items()
        }
        return _kept(
            {
                "name": self.name,
                "read": None if self.read is None else str(self.read),
                "write": write,
                "deletes": self.deletes,
                "on_truncate": self.on_truncate,
                "on_retention_loss": self.on_retention_loss,
                "schema": None if self.schema is None else self.schema._document(),
                "nested": None if self.nested is None else _value(self.nested),
                "columns": columns or None,
                "include": None if self.include is None else list(_names(self.include)),
                "exclude": None if self.exclude is None else list(_names(self.exclude)),
                "transform": [_step(step) for step in self.transform] or None,
            }
        )


def stream(
    name: str,
    /,
    *,
    read: ReadMode | _ReadWord | None = None,
    write: WriteMode | _WriteWord | None = None,
    deletes: _DeleteWord | None = None,
    on_truncate: _TruncateWord | None = None,
    on_retention_loss: _RetentionWord | None = None,
    schema: SchemaRules | None = None,
    nested: _NestedWord | Mapping[str, Json] | None = None,
    columns: Mapping[str, Column] | None = None,
    include: str | Sequence[str] | None = None,
    exclude: str | Sequence[str] | None = None,
    transform: Sequence[Mapping[str, Json] | Transform] = (),
) -> Stream:
    """The stream `name`, read as `read` and written as `write`."""
    return Stream(
        name=name,
        read=read,
        write=write,
        deletes=deletes,
        on_truncate=on_truncate,
        on_retention_loss=on_retention_loss,
        schema=schema,
        nested=nested,
        columns={} if columns is None else columns,
        include=include,
        exclude=exclude,
        transform=transform,
    )


@dataclass(frozen=True, slots=True)
class StreamName:
    """A stream by its identity: for one neither the pipeline nor the catalog holds now."""

    name: str
    """The stream's name, within its namespace."""
    namespace: str | None = None
    """Its namespace, such as a database schema; `None` for a stream without one."""


def _duration(span: timedelta) -> str:
    microseconds = span // timedelta(microseconds=1)
    if microseconds % 1_000_000 == 0:
        return f"{microseconds // 1_000_000}s"
    if microseconds % 1_000 == 0:
        return f"{microseconds // 1_000}ms"
    return f"{microseconds}us"


def _maybe_duration(span: timedelta | None) -> Json:
    return None if span is None else _duration(span)


@dataclass(frozen=True, slots=True, kw_only=True)
class Commit:
    """When a run commits: whichever threshold it reaches first."""

    every: timedelta | None = None
    """How long after the last commit the next is due."""
    rows: int | None = None
    """Rows after which a commit is due."""
    size: int | str | None = None
    """Bytes after which a commit is due: a whole number, or digits and a unit such as `64MiB`."""

    def _document(self) -> dict[str, Json]:
        return _kept({"every": _maybe_duration(self.every), "rows": self.rows, "bytes": self.size})


@dataclass(frozen=True, slots=True, kw_only=True)
class Resources:
    """What a run may hold: its memory budget, its writers and the partitions it reads at once."""

    memory: int | str | None = None
    """The memory budget: bytes, or digits and a unit such as `512MiB`."""
    writers: int | None = None
    """Concurrent destination writers."""
    partitions: int | None = None
    """Partitions read at once."""

    def _document(self) -> dict[str, Json]:
        return _kept(
            {"memory": self.memory, "writers": self.writers, "partitions": self.partitions}
        )


@dataclass(frozen=True, slots=True, kw_only=True)
class Retry:
    """How a run retries a failed attempt; the engine's default for each it does not say."""

    max_attempts: int | None = None
    """Attempts, the first included, and tries of a commit."""
    initial: timedelta | None = None
    """The longest delay after the first failure."""
    max_delay: timedelta | None = None
    """The longest delay after any failure."""

    def _document(self) -> dict[str, Json]:
        return _kept(
            {
                "max_attempts": self.max_attempts,
                "initial": _maybe_duration(self.initial),
                "max_delay": _maybe_duration(self.max_delay),
            }
        )


@dataclass(frozen=True, slots=True, kw_only=True)
class Defaults:
    """What every stream takes where it says nothing."""

    read: ReadMode | _ReadWord | None = None
    """How streams are read."""
    write: WriteMode | _WriteWord | None = None
    """How streams' rows reach their tables."""
    schema: SchemaRules | None = None
    """What schemas do."""
    nested: _NestedWord | Mapping[str, Json] | None = field(default=None, hash=False)
    """How nested values are stored: a word, or `{"normalize": {"max_depth": n}}`."""

    def _document(self) -> dict[str, Json]:
        write = self.write._document if isinstance(self.write, WriteMode) else self.write
        return _kept(
            {
                "read": None if self.read is None else str(self.read),
                "write": write,
                "schema": None if self.schema is None else self.schema._document(),
                "nested": None if self.nested is None else _value(self.nested),
            }
        )


def _functions(source: object) -> list[ReadStream]:
    """`source`'s stream functions.

    Raises:
        TypeError: something in it is no stream: a plain function, a class or a name.
    """
    given: list[object] = (
        list(cast("Sequence[object]", source))
        if isinstance(source, Sequence) and not isinstance(source, str)
        else [source]
    )
    functions: list[ReadStream] = []
    for each in given:
        if not isinstance(each, ReadStream):
            named = getattr(each, "__name__", type(each).__name__)
            message = (
                f"source={named} is no stream: @rdlt.connector.stream makes a function one, and "
                "a connector is rdlt.Connector(...) or rdlt.local(...)"
            )
            raise TypeError(message)
        functions.append(each)
    return functions


def _entry(declared: ReadStream) -> dict[str, Json]:
    """A stream's entry as its declaration says: incremental where it has a cursor."""
    entry: dict[str, Json] = {"name": declared.name}
    if declared.cursor is not None:
        entry["read"] = "incremental"
    return entry


def _local_reference(answers: str, local: Local) -> dict[str, Json]:
    """The document's reference to a local connector that answers to `answers`."""
    document: dict[str, Json] = {"connector": answers}
    if local.config is not None:
        document["config"] = dict(local.config)
    return document


def _local_document(local: Local) -> tuple[dict[str, Json], dict[str, dict[str, Json]]]:
    """A local connector's source document, and an entry for each of its streams."""
    declared = _source.declaration(local.connector)
    document = _local_reference(declared.id, local)
    return document, {method.name: _entry(method) for method in declared.streams}


def pipeline(
    name: str,
    /,
    *,
    source: Connector | Local | ReadStream | Sequence[ReadStream],
    destination: Connector | Local,
    streams: Sequence[Stream] = (),
    defaults: Defaults | None = None,
    commit: Commit | None = None,
    resources: Resources | None = None,
    retry: Retry | None = None,
    until: _UntilWord | timedelta | None = None,
) -> PipelineBuilder:
    """The pipeline `name`, from `source` to `destination`; the same document a file would hold.

    `source` is a connector, `rdlt.local(SomeSource, config=...)` for a class `@source` made, or
    one or more functions `@rdlt.connector.stream` made streams, read in process as the streams
    of one source, `io.rapidbyte.functions`. A function's stream is always selected; a local
    connector's streams are each selected where `streams` names none of them, and otherwise only
    those it names. A stream named exactly keeps what its declaration says, `read: incremental`
    for one with a cursor, under what its entry says, `write` among it; one a pattern selects
    reads as the defaults say. `destination` is a connector, or
    `rdlt.local(SomeDestination, config=...)` for a class `@destination` made.

    Raises:
        TypeError: `source` holds something that is no stream, such as a plain function.
    """
    factories: list[_native.SourceFactory] = []
    destinations: list[_native.DestinationFactory] = []
    held: list[object] = []
    connectors: Registered = ()
    entries: dict[str, dict[str, Json]] = {}
    source_document: dict[str, Json]
    if isinstance(source, Connector):
        source_document = source._document()
    elif isinstance(source, Local):
        source_document, entries = _local_document(source)
        if streams:
            named_streams = {given.name for given in streams}
            entries = {name: entry for name, entry in entries.items() if name in named_streams}
        made, placement = _source.placed(source.connector)
        factories.append(made)
        held.append(placement)
        answers = _source.declaration(source.connector).id
        connectors = registered_once(connectors, "source", answers, source.connector)
    else:
        functions = _functions(source)
        source_document = {"connector": _source.FUNCTIONS}
        entries = {made.name: _entry(made) for made in functions}
        factories.append(_source.functions_placed(functions))
        held.extend(functions)
    named: set[str] = set()
    repeated: list[dict[str, Json]] = []
    transforms: tuple[tuple[str, Transform], ...] = ()
    for given in streams:
        if given.name in named:
            repeated.append(given._document())
        else:
            named.add(given.name)
            entries[given.name] = {**entries.get(given.name, {}), **given._document()}
        for step in given.transform:
            if isinstance(step, Transform):
                transforms = named_once(transforms, step.name, step)
    if isinstance(destination, Local):
        answers = _destination.declaration(destination.connector).id
        destination_document = _local_reference(answers, destination)
        made_destination, destination_placement = _destination.placed(destination.connector)
        destinations.append(made_destination)
        held.append(destination_placement)
        connectors = registered_once(connectors, "destination", answers, destination.connector)
    else:
        destination_document = destination._document()
    document: dict[str, Json] = {
        "version": 1,
        "pipeline": name,
        "source": source_document,
        "destination": destination_document,
        "streams": [*entries.values(), *repeated],
    }
    document.update(
        _kept(
            {
                "defaults": None if defaults is None else defaults._document(),
                "commit": None if commit is None else commit._document(),
                "resources": None if resources is None else resources._document(),
                "retry": None if retry is None else retry._document(),
                "until": _duration(until) if isinstance(until, timedelta) else until,
            }
        )
    )
    return PipelineBuilder(
        document=document,
        sources=factories,
        destinations=destinations,
        transforms=transforms,
        held=held,
        connectors=connectors,
    )


class PipelineBuilder:
    """A pipeline described and not yet built: `build(host)` checks it against the host."""

    def __init__(
        self,
        *,
        document: Mapping[str, Json] | None = None,
        path: Path | None = None,
        sources: Sequence[_native.SourceFactory] = (),
        destinations: Sequence[_native.DestinationFactory] = (),
        transforms: Sequence[tuple[str, Transform]] = (),
        held: Sequence[object] = (),
        connectors: Registered = (),
    ) -> None:
        self._document = None if document is None else dict(document)
        self._path = path
        self._sources = tuple(sources)
        self._destinations = tuple(destinations)
        self._transforms = tuple(transforms)
        self._held = tuple(held)
        self._connectors = tuple(connectors)

    def __repr__(self) -> str:
        if self._document is None:
            return f"PipelineBuilder(path={self._path!r})"
        return f"PipelineBuilder({self._document.get('pipeline')!r})"

    def document(self) -> dict[str, Json]:
        """The document the builder holds; a builder from a path reads it when built."""
        if self._document is None:
            raise TypeError("a pipeline read from a path prints its document once built")
        return dict(self._document)

    def build(self, host: Host) -> Pipeline:
        """Validates, places and handshakes every connector; after it no configuration error occurs.

        Raises:
            rdlt.Error: the subclass for the first mistake found, which never quotes a value.
            ValueError: a transform's name, or a connector class's id, the host gives another.
        """
        if self._sources or self._destinations or self._transforms or self._held:
            registered = host._transforms
            for name, made in self._transforms:
                registered = named_once(registered, name, made)
            connectors = host._connectors
            for role, answers, connector in self._connectors:
                connectors = registered_once(connectors, role, answers, connector)
            host = host._replaced(
                _sources=(*host._sources, *self._sources),
                _destinations=(*host._destinations, *self._destinations),
                _transforms=registered,
                _held=(*host._held, *self._held),
                _connectors=connectors,
            )
        native = host._native()
        if self._path is not None:
            return Pipeline(_native.Pipeline.build_path(self._path, native), host)
        text = json.dumps(self._document, ensure_ascii=False)
        return Pipeline(_native.Pipeline.build(text, native), host)


class Pipeline:
    """A built pipeline."""

    def __init__(self, native: _native.Pipeline, host: Host) -> None:
        self._native = native
        # What the native pipeline reaches only weakly, held in the open.
        self._host = host
        self._name = cast("str", _parse(native.document())["pipeline"])

    def __repr__(self) -> str:
        return f"Pipeline({self._name!r})"

    @staticmethod
    def from_path(path: StrPath) -> PipelineBuilder:
        """The pipeline the YAML or JSON document at `path` describes."""
        return PipelineBuilder(path=Path(path))

    @staticmethod
    def from_document(document: Mapping[str, Json]) -> PipelineBuilder:
        """The pipeline `document`, a document as a mapping, describes."""
        return PipelineBuilder(document=document)

    def document(self) -> dict[str, Json]:
        """The built pipeline as its document, every secret still a reference."""
        return _parse(self._native.document())

    def check(self) -> None:
        """Validates, places and handshakes again; no data moves.

        Raises:
            rdlt.Error: the subclass for what failed.
        """
        self._native.check()

    def discover(self) -> Catalog:
        """The source's catalog."""
        return Catalog.from_json(self._native.discover())

    def reset(
        self, streams: str | StreamName | Sequence[str | StreamName], *, tables: bool = False
    ) -> dict[str, Json]:
        """Resets `streams`' committed state, and with `tables` drops their tables too.

        Each stream is named as the source's catalog shows it, or by its identity, a `StreamName`.
        A single name is one stream. A reset is a write: it fences every run of the pipeline in
        progress. Answers what it reset: `streams`, `dropped` and `epoch`.

        Raises:
            rdlt.Error: the subclass for what refused it.
        """
        selected = [streams] if isinstance(streams, (str, StreamName)) else list(streams)
        return _parse(self._native.reset(selected, tables=tables))

    def start(self) -> Run:
        """Starts a run; it is cancelled once every reference to it and to its events is dropped."""
        return Run(self._native.start(), self._name, self._host)

    def run(self) -> Outcome:
        """Runs to the end, the GIL released.

        As the CLI does, a first Ctrl-C asks the run to stop after its next commit and answers
        its outcome; a second cancels the run now and raises `KeyboardInterrupt`.

        Raises:
            rdlt.Error: the subclass for the error that ended the run, its `outcome` set.
        """
        return _ended(self._native.run())

    async def run_async(self) -> Outcome:
        """Runs to the end in asyncio.

        A cancellation of the awaiting task, as `asyncio.timeout` and `asyncio.wait_for` deliver,
        asks the run to stop after its next commit, raises `CancelledError` at once, and leaves
        the run to finish in the background.

        Raises:
            rdlt.Error: the subclass for the error that ended the run, its `outcome` set.
        """
        return await self.start().wait_async()
