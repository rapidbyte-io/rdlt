"""A source connector for PokeAPI: its `pokemon` and `types` streams, read a page at a time."""

from __future__ import annotations

import contextlib
import email.utils
import http.client
import socket
import ssl
import threading
import urllib.error
import urllib.parse
import urllib.request
from collections.abc import Callable, Iterator
from datetime import UTC, datetime
from http import HTTPStatus
from typing import Annotated, TypeVar

import pydantic

from rdlt.connector import (
    Config,
    ConnectorError,
    Cursor,
    InvalidConfig,
    InvalidData,
    RateLimited,
    StreamContext,
    Transient,
    Unauthorized,
    checkpoint,
    serve,
    source,
    stream,
)

USER_AGENT = "rdlt-example-pokeapi"
"""What the connector tells the API it is."""

TIMEOUT = 30.0
"""The longest a request may take, its whole answer read, in seconds."""

TLS = ssl.create_default_context()
"""The TLS context of every request: the system's certificates, checked, and HTTP/1.1 offered, as
urllib's own context offers it."""
TLS.set_alpn_protocols(["http/1.1"])

DEFAULT_RETRY_AFTER = 1.0
"""The wait a 429 asks for where it says none, in seconds."""

U64_MAX = 2**64 - 1
"""The largest count or id the source reads, as the Rust source's `u64` holds it."""

Count = Annotated[int, pydantic.Field(ge=0, le=U64_MAX)]
"""A count or id, as the Rust source's `u64`: none negative, none past `U64_MAX`."""


class PokeApiConfig(Config):
    """The configuration the connector accepts; its JSON Schema is published."""

    base_url: str = pydantic.Field(
        default="https://pokeapi.co/api/v2", description="The API's root."
    )
    page_size: int = pydantic.Field(
        default=50, ge=1, le=100, description="How many entries each list request asks for."
    )
    limit: int | None = pydantic.Field(
        default=151,
        ge=0,
        description=(
            "The most rows each stream reads in all its runs together, or null for every one: "
            "a stream that has read them reads nothing more."
        ),
    )


class Position(Cursor):
    """Where a stream resumes: the list offset its next page starts at.

    An offset keeps its place only in a list that grows at its end alone.
    """

    offset: int = 0


class Answer(pydantic.BaseModel):
    """What the API answers, read strictly, as the Rust source's structs read it.

    No text or float is taken for an integer.
    """

    model_config = pydantic.ConfigDict(strict=True)


class Entry(Answer):
    """One entry of a list: a name and the URL of its detail."""

    name: str
    url: str

    def id(self) -> int:
        """The id the entry's URL ends in, a run of digits, as `/type/12/` does."""
        last = self.url.rstrip("/").rsplit("/", 1)[-1]
        if not (last.isascii() and last.isdigit()) or int(last) > U64_MAX:
            raise InvalidData(f"entry {self.name} has no id in its url")
        return int(last)


class Page(Answer):
    """One page of a list, as `/pokemon?offset=&limit=` answers it."""

    next: str | None = None
    """The next page's URL, or none on the last page."""
    results: list[Entry]


class Detail(Answer):
    """A pokemon as it lands: its own fields, and its types, abilities and stats as nested JSON."""

    id: Count
    name: str
    height: Count
    weight: Count
    base_experience: Count | None = None
    types: pydantic.JsonValue
    abilities: pydantic.JsonValue
    stats: pydantic.JsonValue


Answered = TypeVar("Answered", bound=Answer)


@source(id="io.example.pokeapi", config=PokeApiConfig)
class PokeApi:
    """The connector: the API's root, and how much a read asks of it."""

    def __init__(self, config: PokeApiConfig) -> None:
        parts = urllib.parse.urlsplit(config.base_url)
        if parts.scheme not in ("http", "https") or not parts.netloc:
            raise InvalidConfig("base_url is not an HTTP URL")
        self.base_url = config.base_url.rstrip("/")
        self.page_size = config.page_size
        self.limit = config.limit
        # Held across each request, so the API is asked one thing at a time by every stream.
        self.turn = threading.Lock()

    def check(self) -> None:
        """Asks for the first page of one pokemon."""
        self.get("pokemon?offset=0&limit=1", Page)

    @stream(primary_key="id", cursor=Position)
    def pokemon(self, after: Position, ctx: StreamContext) -> Iterator[object]:
        """Each entry of `/pokemon`, read in full from `/pokemon/{id}`."""
        yield from self.read("pokemon", after, ctx, lambda entries: self.details(entries, ctx))

    @stream(primary_key="id", cursor=Position)
    def types(self, after: Position, ctx: StreamContext) -> Iterator[object]:
        """Each entry of `/type`, its id read from its URL."""
        yield from self.read(
            "type", after, ctx, lambda entries: [{"id": e.id(), "name": e.name} for e in entries]
        )

    def read(
        self,
        resource: str,
        after: Position,
        ctx: StreamContext,
        rows: Callable[[list[Entry]], list[object] | None],
    ) -> Iterator[object]:
        """The rows `rows` makes of each page of `resource`'s list from `after` on, and checkpoints.

        Each page is followed by its checkpoint. The read ends where `rows` makes none, as where
        the engine asked the read to stop.
        """
        for entries, position in self.pages(resource, after, ctx):
            made = rows(entries)
            if made is None:
                return
            yield made
            yield checkpoint(position)

    def details(self, entries: list[Entry], ctx: StreamContext) -> list[object] | None:
        """Each entry's pokemon, read in full, or none where the engine asked the read to stop."""
        details: list[object] = []
        for entry in entries:
            # A request in flight blocks its thread, within its timeout: a stop is seen between
            # requests.
            if ctx.stopped():
                return None
            details.append(self.get(f"pokemon/{entry.id()}", Detail).model_dump())
        return details

    def pages(
        self, resource: str, after: Position, ctx: StreamContext
    ) -> Iterator[tuple[list[Entry], Position]]:
        """The pages of `resource`'s list from `after` on, up to the limit, each with its end.

        They end early where the engine asks the read to stop.
        """
        offset = after.offset
        while (self.limit is None or offset < self.limit) and not ctx.stopped():
            asked = (
                self.page_size if self.limit is None else min(self.page_size, self.limit - offset)
            )
            page = self.get(f"{resource}?offset={offset}&limit={asked}", Page)
            if not page.results:
                return
            offset += len(page.results)
            yield page.results, Position(offset=offset)
            if page.next is None:
                return

    def get(self, path: str, shape: type[Answered]) -> Answered:
        """The JSON at `path`, beneath the API's root, read as `shape`."""
        request = urllib.request.Request(  # noqa: S310 - its scheme was checked as configured
            f"{self.base_url}/{path}", headers={"User-Agent": USER_AGENT}
        )
        watch = Watch()
        opener = urllib.request.build_opener(Watched(watch), WatchedTls(watch))
        with self.turn:
            watch.timer.start()
            try:
                with opener.open(request, timeout=TIMEOUT) as response:
                    body: bytes = response.read()
            except urllib.error.HTTPError as error:
                raise failure(error.code, error.headers.get("Retry-After"), path) from error
            except (OSError, http.client.HTTPException) as error:
                raise watch.failure(path, error) from error
            finally:
                watch.timer.cancel()
        if watch.expired:
            raise watch.failure(path, None)
        try:
            return shape.model_validate_json(body)
        except pydantic.ValidationError as error:
            raise InvalidData(f"{path} answered another shape") from error


class Watch:
    """Ends a request once `TIMEOUT` has passed since it was sent, its answer read or not.

    It shuts the socket of the request's connection down, which ends a read blocked on it however
    the answer trickles in: its status line, its headers or its body. It keeps the socket the
    connection connected, which urllib drops from the connection before its body is read, while
    the response still reads through it.
    """

    def __init__(self) -> None:
        self.connection: http.client.HTTPConnection | None = None
        self.sock: socket.socket | None = None
        self.expired = False
        self.timer = threading.Timer(TIMEOUT, self.expire)

    def expire(self) -> None:
        """Marks the request's time up and ends its connection."""
        self.expired = True
        self.end()

    def connected(self, sock: socket.socket) -> None:
        """Keeps `sock`, the socket a connection of the request connected.

        A redirect's connection replaces it; a time that ran out while it connected ends it at once.
        """
        self.sock = sock
        if self.expired:
            self.end()

    def end(self) -> None:
        """Shuts the request's socket down, ending the read waiting on it.

        That is the socket a connection connected, or the one a connection holds before its TLS
        handshake, such as a proxy's tunnel's.
        """
        connecting = None if self.connection is None else self.connection.sock
        for sock in (self.sock, connecting):
            if sock is not None:
                # A socket the response closed since has nothing left to end. The descriptor's own
                # shutdown, since an `SSLSocket`'s drops its TLS state under a read still using it.
                with contextlib.suppress(OSError):
                    socket.socket.shutdown(sock, socket.SHUT_RDWR)

    def failure(self, path: str, error: Exception | None) -> Transient:
        """What a request for `path` that failed with `error`, if any, failed as."""
        if self.expired:
            return Transient(f"GET {path} took longer than {TIMEOUT:g} s")
        return Transient(f"GET {path} failed: {error!r}")

    def plain(
        self,
        host: str,
        /,
        *,
        port: int | None = None,
        timeout: float = TIMEOUT,
        source_address: tuple[str, int] | None = None,
        blocksize: int = 8192,
    ) -> http.client.HTTPConnection:
        """A connection to `host`, which the watch ends."""
        connection = Plain(host, port, timeout, source_address, blocksize)
        connection.watch = self
        self.connection = connection
        return connection

    def tls(
        self,
        host: str,
        /,
        *,
        port: int | None = None,
        timeout: float = TIMEOUT,
        source_address: tuple[str, int] | None = None,
        blocksize: int = 8192,
        context: ssl.SSLContext | None = None,
    ) -> http.client.HTTPConnection:
        """A TLS connection to `host`, which the watch ends."""
        connection = Tls(
            host,
            port,
            timeout=timeout,
            source_address=source_address,
            context=context,
            blocksize=blocksize,
        )
        connection.watch = self
        self.connection = connection
        return connection


class Plain(http.client.HTTPConnection):
    """An HTTP connection its watch ends."""

    watch: Watch

    def connect(self) -> None:
        """Connects, the socket its watch's."""
        super().connect()
        self.watch.connected(self.sock)


class Tls(http.client.HTTPSConnection):
    """An HTTPS connection its watch ends."""

    watch: Watch

    def connect(self) -> None:
        """Connects, the socket its watch's."""
        super().connect()
        self.watch.connected(self.sock)


class Watched(urllib.request.HTTPHandler):
    """Opens each HTTP connection for `watch`, redirects' too."""

    def __init__(self, watch: Watch) -> None:
        super().__init__()
        self.watch = watch

    def http_open(self, req: urllib.request.Request) -> http.client.HTTPResponse:
        """Opens `req`'s connection for the watch."""
        return self.do_open(self.watch.plain, req)


class WatchedTls(urllib.request.HTTPSHandler):
    """Opens each HTTPS connection for `watch`, redirects' too, its certificate checked."""

    def __init__(self, watch: Watch) -> None:
        super().__init__(context=TLS)
        self.watch = watch

    def https_open(self, req: urllib.request.Request) -> http.client.HTTPResponse:
        """Opens `req`'s connection for the watch."""
        return self.do_open(self.watch.tls, req, context=TLS)


def failure(status: int, retry_after: str | None, path: str) -> ConnectorError:
    """What a request for `path` the API answered with `status` failed with."""
    message = f"GET {path} answered {status}"
    if status == HTTPStatus.TOO_MANY_REQUESTS:
        wait = None if retry_after is None else asked(retry_after)
        return RateLimited(message, retry_after=DEFAULT_RETRY_AFTER if wait is None else wait)
    if status in (HTTPStatus.UNAUTHORIZED, HTTPStatus.FORBIDDEN):
        return Unauthorized(message)
    if status >= HTTPStatus.INTERNAL_SERVER_ERROR:
        return Transient(message)
    return InvalidConfig(message)


def asked(value: str) -> float | None:
    """The wait a `Retry-After` of `value` asks for, in seconds; none where it names none.

    It is a count of seconds, or an HTTP date, of which one already past asks for no wait.
    """
    if value.isascii() and value.isdigit():
        return float(value)
    try:
        date = email.utils.parsedate_to_datetime(value)
    except (TypeError, ValueError):
        return None
    if date.tzinfo is None:
        date = date.replace(tzinfo=UTC)
    return max(0.0, (date - datetime.now(UTC)).total_seconds())


if __name__ == "__main__":
    raise SystemExit(serve(PokeApi))
