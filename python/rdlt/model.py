# pyright: reportUnusedFunction=false
"""The values rdlt reports, as frozen dataclasses built from the JSON the native layer answers."""

from __future__ import annotations

import enum
import json
from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from typing import TypeAlias, cast

Json: TypeAlias = "bool | int | float | str | list[Json] | dict[str, Json] | None"
"""A JSON value, as `json.loads` returns it."""


def _parse(text: str) -> dict[str, Json]:
    value = cast("Json", json.loads(text))
    if not isinstance(value, dict):
        raise TypeError("the native layer answered JSON that is no object")
    return value


def _text(fields: Mapping[str, Json], key: str) -> str:
    value = fields.get(key)
    if not isinstance(value, str):
        raise TypeError(f"field {key!r} is not text")
    return value


def _maybe_text(fields: Mapping[str, Json], key: str) -> str | None:
    value = fields.get(key)
    if value is not None and not isinstance(value, str):
        raise TypeError(f"field {key!r} is neither text nor null")
    return value


def _count(fields: Mapping[str, Json], key: str) -> int:
    value = fields.get(key)
    if isinstance(value, bool) or not isinstance(value, int):
        raise TypeError(f"field {key!r} is not a whole number")
    return value


def _flag(fields: Mapping[str, Json], key: str) -> bool:
    value = fields.get(key)
    if not isinstance(value, bool):
        raise TypeError(f"field {key!r} is not a boolean")
    return value


def _object(fields: Mapping[str, Json], key: str) -> dict[str, Json]:
    value = fields.get(key)
    if not isinstance(value, dict):
        raise TypeError(f"field {key!r} is not an object")
    return value


def _instant(value: Json) -> datetime:
    if not isinstance(value, dict):
        raise TypeError("an instant is not seconds since the epoch")
    seconds = _count(value, "secs_since_epoch")
    nanos = _count(value, "nanos_since_epoch")
    return datetime.fromtimestamp(seconds, UTC) + timedelta(microseconds=nanos // 1000)


def _span(value: Json) -> timedelta:
    if not isinstance(value, dict):
        raise TypeError("a duration is not seconds and nanoseconds")
    return timedelta(seconds=_count(value, "secs"), microseconds=_count(value, "nanos") // 1000)


def _variant(value: Json) -> tuple[str, dict[str, Json]]:
    """A variant the facade writes, as its name and its fields, the name alone having none."""
    if isinstance(value, str):
        return value, {}
    if isinstance(value, dict) and len(value) == 1:
        ((name, inner),) = value.items()
        return name, inner if isinstance(inner, dict) else {}
    raise TypeError("neither a name nor one variant")


@dataclass(frozen=True, slots=True)
class ErrorReport:
    """What a failure reports, as the facade's `ErrorReport` serializes it."""

    kind: str
    """The failure's kind: `config`, `schema`, `source`, `destination`, `transform`, `fenced`,
    `memory`, `cancelled` or `internal`."""
    origin: str
    """Who failed: `engine`, `source` or `destination`."""
    stream: str | None
    """The stream it belongs to, where it belongs to one."""
    code: str | None
    """The engine's code for it."""
    connector_code: str | None
    """The code a connector's error carried, shown and bounded."""
    message: str
    """A one-line description naming its subject."""
    causes: tuple[str, ...]
    """Each cause, outermost first: a Python traceback among them, for an exception in Python."""
    retryable: bool
    """Whether a new attempt could have succeeded."""

    @classmethod
    def from_fields(cls, fields: Mapping[str, Json]) -> ErrorReport:
        """The report `fields`, the facade's JSON, holds."""
        causes = fields.get("causes")
        if not isinstance(causes, list) or not all(isinstance(cause, str) for cause in causes):
            raise TypeError("field 'causes' is not a list of text")
        return cls(
            kind=_text(fields, "kind"),
            origin=_text(fields, "origin"),
            stream=_maybe_text(fields, "stream"),
            code=_maybe_text(fields, "code"),
            connector_code=_maybe_text(fields, "connector_code"),
            message=_text(fields, "message"),
            causes=tuple(cast("list[str]", causes)),
            retryable=_flag(fields, "retryable"),
        )


@dataclass(frozen=True, slots=True)
class StreamName:
    """A stream by its identity, its name within its namespace, as the Rust SDK's `StreamName`.

    A source names a stream of a database schema so, and a reset names one neither the pipeline
    nor the catalog holds now. It shows as `namespace.name`, or its name where it has none.
    """

    name: str
    """The stream's name, within its namespace."""
    namespace: str | None = None
    """Its namespace, such as a database schema; `None` for a stream without one."""

    def __str__(self) -> str:
        """The stream as a catalog shows it, `namespace.name`."""
        return self.name if self.namespace is None else f"{self.namespace}.{self.name}"


class ReadMode(enum.StrEnum):
    """How a stream is read."""

    FULL = "full"
    """Every record, every run."""
    INCREMENTAL = "incremental"
    """Records past the cursor last committed."""
    CDC = "cdc"
    """A snapshot, then changes."""


@dataclass(frozen=True, slots=True)
class CatalogStream:
    """One stream a source offers."""

    name: str
    """The stream's name, `namespace.name` where it has a namespace."""
    primary_key: tuple[tuple[str, ...], ...] | None
    """Each key column's path from the table's root, where the source declares a key."""
    read_modes: tuple[ReadMode, ...]
    """How it can be read."""
    raw: Mapping[str, Json] = field(repr=False, hash=False)
    """The whole spec, as the facade serializes it."""

    @classmethod
    def from_fields(cls, fields: Mapping[str, Json]) -> CatalogStream:
        """The stream `fields`, one spec of the facade's catalog, describes."""
        identity = _object(fields, "name")
        namespace = _maybe_text(identity, "namespace")
        name = _text(identity, "name")
        shown = name if namespace is None else f"{namespace}.{name}"
        keys = cast("list[list[str]] | None", fields.get("primary_key"))
        primary_key = None if keys is None else tuple(tuple(key) for key in keys)
        modes = cast("list[str]", fields.get("read_modes") or [])
        return cls(shown, primary_key, tuple(ReadMode(mode) for mode in modes), fields)


@dataclass(frozen=True, slots=True)
class Catalog:
    """The streams a source offers."""

    streams: tuple[CatalogStream, ...]

    @classmethod
    def from_json(cls, text: str) -> Catalog:
        """The catalog `text`, the facade's JSON, holds."""
        specs = cast("list[dict[str, Json]]", json.loads(text))
        return cls(tuple(CatalogStream.from_fields(spec) for spec in specs))


class RunStatus(enum.Enum):
    """How a run ended, or that it has not."""

    RUNNING = "running"
    SUCCEEDED = "succeeded"
    STOPPED = "stopped"
    FAILED = "failed"
    CANCELLED = "cancelled"


@dataclass(frozen=True, slots=True)
class StreamReport:
    """What one stream committed."""

    rows: int
    committed_bytes: int
    commits: int
    raw: Mapping[str, Json] = field(repr=False, hash=False)
    """The whole stream report, as the facade serializes it."""

    @classmethod
    def from_fields(cls, fields: Mapping[str, Json]) -> StreamReport:
        """The stream's report `fields` hold."""
        return cls(
            _count(fields, "rows"), _count(fields, "bytes"), _count(fields, "commits"), fields
        )


@dataclass(frozen=True, slots=True)
class Report:
    """What a run committed, attempt by attempt."""

    pipeline: str
    status: RunStatus
    attempted: int
    elapsed: timedelta
    rows: int
    committed_bytes: int
    commits: int
    peak_memory: int
    streams: Mapping[str, StreamReport] = field(hash=False)
    raw: Mapping[str, Json] = field(repr=False, hash=False)
    """The whole report, as the facade serializes it."""

    @classmethod
    def from_fields(cls, fields: Mapping[str, Json]) -> Report:
        """The report `fields`, the facade's JSON, hold."""
        streams = _object(fields, "streams")
        return cls(
            pipeline=_text(fields, "pipeline"),
            status=RunStatus(_text(fields, "status")),
            attempted=_count(fields, "attempted"),
            elapsed=_span(fields.get("elapsed")),
            rows=_count(fields, "rows"),
            committed_bytes=_count(fields, "bytes"),
            commits=_count(fields, "commits"),
            peak_memory=_count(fields, "peak_memory"),
            streams={
                name: StreamReport.from_fields(cast("dict[str, Json]", report))
                for name, report in streams.items()
            },
            raw=fields,
        )


@dataclass(frozen=True, slots=True)
class Outcome:
    """How a run ended: what it committed, and the error that ended it, if one did."""

    report: Report
    error: ErrorReport | None

    @classmethod
    def from_fields(cls, fields: Mapping[str, Json]) -> Outcome:
        """The outcome `fields` hold."""
        error = fields.get("error")
        return cls(
            Report.from_fields(_object(fields, "report")),
            None if error is None else ErrorReport.from_fields(cast("dict[str, Json]", error)),
        )


@dataclass(frozen=True, slots=True)
class Status:
    """Where a run stands now."""

    phase: str
    """What the run is in the middle of: `opening`, `reading`, `committing`, `stopping`, …"""
    since: datetime
    streams: Mapping[str, Json] = field(hash=False)
    report: Report = field(hash=False)
    """The run so far, the attempt in progress listed as ending now."""
    raw: Mapping[str, Json] = field(repr=False, hash=False)
    """The whole status, as the facade serializes it."""

    @classmethod
    def from_fields(cls, fields: Mapping[str, Json]) -> Status:
        """The status `fields` hold."""
        phase, _ = _variant(fields.get("phase"))
        return cls(
            phase=phase,
            since=_instant(fields.get("since")),
            streams=_object(fields, "streams"),
            report=Report.from_fields(_object(fields, "report")),
            raw=fields,
        )


@dataclass(frozen=True, slots=True)
class Event:
    """Something a run did; `seq` is gap-free within a run."""

    seq: int
    at: datetime
    pipeline: str
    attempt: int
    kind: str
    """What happened: `attempt_started`, `committed`, `retrying`, `run_ended`, …"""
    detail: Mapping[str, Json] = field(hash=False)
    """The kind's own fields."""

    @classmethod
    def from_fields(cls, fields: Mapping[str, Json]) -> Event:
        """The event `fields` hold."""
        kind, detail = _variant(fields.get("kind"))
        return cls(
            seq=_count(fields, "seq"),
            at=_instant(fields.get("at")),
            pipeline=_text(fields, "pipeline"),
            attempt=_count(fields, "attempt"),
            kind=kind,
            detail=detail,
        )


@dataclass(frozen=True, slots=True)
class Lagged:
    """A subscriber fell behind: `dropped` events were lost to it, and it carries on."""

    dropped: int
