"""A small connector the SDK's tests run in process, served and spawned."""

from __future__ import annotations

from collections.abc import Iterator

from rdlt.connector import (
    Config,
    Cursor,
    Secret,
    StreamContext,
    Transient,
    checkpoint,
    source,
    stream,
)


class TicketsConfig(Config):
    """Its configuration: how many pages, the token, and how its second page fails, if it does."""

    pages: int
    token: Secret[str]
    fail_with_token: bool = False
    fail_transient: bool = False
    fail_deadline: bool = False


class DeadlineExceeded(Transient):
    """The API took longer than its deadline: a failure named as one of the host's codes is."""


class Since(Cursor):
    """Where the stream resumes: the last page read."""

    after: int = -1


@source(id="io.example.tickets", config=TicketsConfig, version="1.0.0")
class Tickets:
    """Reads pages of ten tickets, checkpointing every other page and when it is due.

    Its stream declares a partition that ends, so certification checks how it is planned.
    """

    def __init__(self, config: TicketsConfig) -> None:
        self.config = config

    def check(self) -> None:
        """Refuses an empty token."""
        if not self.config.token.expose():
            raise PermissionError("the token is empty")

    def planned(self) -> list[str]:
        """The stream's partitions: one, which ends once its last page is read."""
        return ["pages"]

    @stream(primary_key=["id"], cursor=Since, partitions="planned")
    def tickets(self, since: Since, ctx: StreamContext) -> Iterator[object]:
        """Reads the pages after `since`."""
        for page in range(since.after + 1, self.config.pages):
            if self.config.fail_with_token and page == 1:
                raise RuntimeError(f"the API refused token {self.config.token.expose()}")
            if self.config.fail_transient and page == 1:
                raise Transient("the API is restarting")
            if self.config.fail_deadline and page == 1:
                raise DeadlineExceeded("the API took too long")
            yield [{"id": page * 10 + n, "page": page} for n in range(10)]
            if ctx.checkpoint_due() or page == self.config.pages - 1 or page % 2 == 1:
                yield checkpoint(Since(after=page))
