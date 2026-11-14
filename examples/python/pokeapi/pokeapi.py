"""A source connector for PokeAPI: its `pokemon` and `types` streams, read a page at a time."""

from __future__ import annotations

import email.utils
import http.client
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
"""The longest each wait on a request's socket may take, in seconds."""

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
    """Where a stream resumes: the list offset its next page starts at."""

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
        before = after
        for entries, position in self.pages(resource, after, ctx):
            made = rows(entries)
            if made is None:
                return
            # A read that names `ctx` checkpoints on demand: a barrier the engine raised is
            # answered before the page, where the last one ended.
            if ctx.checkpoint_due():
                yield checkpoint(before)
            yield made
            yield checkpoint(position)
            before = position

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
        with self.turn:
            try:
                with urllib.request.urlopen(request, timeout=TIMEOUT) as response:  # noqa: S310
                    body: bytes = response.read()
            except urllib.error.HTTPError as error:
                raise failure(error.code, error.headers.get("Retry-After"), path) from error
            except (OSError, http.client.HTTPException) as error:
                raise Transient(f"GET {path} failed: {error!r}") from error
        try:
            return shape.model_validate_json(body)
        except pydantic.ValidationError as error:
            raise InvalidData(f"{path} answered another shape") from error


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
