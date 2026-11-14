"""A local server that answers as PokeAPI does, with pages it generates at the size a test asks."""

from __future__ import annotations

import json
import threading
import time
from dataclasses import dataclass
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import cast
from urllib.parse import SplitResult, parse_qs, urlsplit

TYPES = 21
"""The types a server lists unless told otherwise, as many as the API lists."""

HELD = 0.001
"""How long the server holds each request before it answers, in seconds, so requests a client sent
at once would overlap."""

FAILED = json.dumps({"detail": "failed as the test asked"})
"""The body a failure the test asked for answers with."""

Json = dict[str, object]


@dataclass(frozen=True)
class Response:
    """What a request gets: a status, a `Retry-After` where it asks for a wait, and a body."""

    status: int
    retry_after: int | str | None
    body: str
    cut: bool = False
    """Whether the response claims more than its body, and its connection closes."""


class Api:
    """A server of `pokemon` pokemon and `types` types, answering on a thread of its own."""

    def __init__(self, pokemon: int, types: int) -> None:
        self.counts = {"pokemon": pokemon, "type": types}
        self._answers: dict[str, list[Response]] = {}
        self._lock = threading.Lock()
        self._in_flight = 0
        self.most_at_once = 0
        """The most requests the server held at once."""
        self._server = _Server(self)
        host, port = self._server.server_address[:2]
        self.url = f"http://{host!s}:{port}/api/v2"
        """Its root, as a connector's `base_url`."""
        threading.Thread(target=self._server.serve_forever, name="api", daemon=True).start()

    def fail(self, path: str, status: int, retry_after: int | str | None = None) -> None:
        """Answers the next request for `path`, such as `/api/v2/pokemon/3`, with `status`.

        `retry_after` is the `Retry-After` it sends, seconds or an HTTP date, if any.
        """
        with self._lock:
            self._answers.setdefault(path, []).append(Response(status, retry_after, FAILED))

    def give(self, path: str, body: str) -> None:
        """Answers the next request for `path` with a 200 whose body is `body`."""
        with self._lock:
            self._answers.setdefault(path, []).append(Response(200, None, body))

    def cut(self, path: str) -> None:
        """Answers the next request for `path` with a 200 whose connection closes in its body."""
        body = '{"next": null, "results": [{"name": '
        with self._lock:
            self._answers.setdefault(path, []).append(Response(200, None, body, cut=True))

    def unserved(self) -> list[str]:
        """The paths of the answers the server has not given."""
        with self._lock:
            return [path for path, answers in self._answers.items() for _ in answers]

    def answer(self, target: str, agent: str | None) -> Response:
        """What a request for `target` from `agent` gets."""
        with self._lock:
            self._in_flight += 1
            self.most_at_once = max(self.most_at_once, self._in_flight)
        time.sleep(HELD)
        try:
            return self._response(urlsplit(target), agent)
        finally:
            # The request is no longer held once its answer is on its way, which the client reads
            # before it sends another.
            with self._lock:
                self._in_flight -= 1

    def _response(self, parts: SplitResult, agent: str | None) -> Response:
        with self._lock:
            answers = self._answers.get(parts.path)
            given = answers.pop(0) if answers else None
        if agent != "rdlt-example-pokeapi":
            return Response(400, None, json.dumps({"detail": "no User-Agent names the example"}))
        if given is not None:
            return given
        found = self._page(parts.path, parse_qs(parts.query))
        if found is None:
            return Response(404, None, json.dumps({"detail": "Not found."}))
        return Response(200, None, json.dumps(found))

    def close(self) -> None:
        """Stops answering."""
        self._server.shutdown()
        self._server.server_close()

    def _page(self, path: str, query: dict[str, list[str]]) -> Json | None:
        resource, _, rest = path.removeprefix("/api/v2/").strip("/").partition("/")
        if resource not in self.counts:
            return None
        if not rest:
            offset, limit = int(query.get("offset", ["0"])[0]), int(query.get("limit", ["20"])[0])
            return self._list(resource, offset, limit)
        if resource == "pokemon" and rest.isdigit() and 1 <= int(rest) <= self.counts[resource]:
            return self._pokemon(int(rest))
        return None

    def _list(self, resource: str, offset: int, limit: int) -> Json:
        count = self.counts[resource]
        end = min(offset + limit, count)
        link = f"{self.url}/{resource}?offset={{}}&limit={limit}"
        return {
            "count": count,
            "next": link.format(end) if end < count else None,
            "previous": link.format(max(offset - limit, 0)) if offset > 0 else None,
            "results": [self._named(resource, n) for n in range(offset + 1, end + 1)],
        }

    def _pokemon(self, n: int) -> Json:
        stats = ("hp", "attack", "defense")
        return {
            "id": n,
            "name": f"pokemon-{n}",
            # The API knows no base experience of some pokemon.
            "base_experience": None if n % 7 == 0 else 40 + n % 200,
            "height": 1 + n % 20,
            "weight": 10 + n % 900,
            "order": n,
            "is_default": True,
            "types": [{"slot": 1, "type": self._named("type", 1 + n % TYPES)}],
            "abilities": [
                {"ability": self._named("ability", 1 + n % 30), "is_hidden": False, "slot": 1},
                {"ability": self._named("ability", 31 + n % 30), "is_hidden": True, "slot": 3},
            ],
            "stats": [
                {"base_stat": 30 + (n + i) % 100, "effort": 0, "stat": self._named("stat", i + 1)}
                for i, _ in enumerate(stats)
            ],
            "moves": [{"move": self._named("move", 1 + n % 50), "version_group_details": []}],
            "sprites": {"front_default": None},
        }

    def _named(self, resource: str, n: int) -> Json:
        return {"name": f"{resource}-{n}", "url": f"{self.url}/{resource}/{n}/"}


class _Server(ThreadingHTTPServer):
    daemon_threads = True

    def __init__(self, api: Api) -> None:
        super().__init__(("127.0.0.1", 0), _Handler)
        self.api = api


class _Handler(BaseHTTPRequestHandler):
    def do_GET(self) -> None:
        api = cast("_Server", self.server).api
        answered = api.answer(self.path, self.headers.get("User-Agent"))
        payload = answered.body.encode()
        self.send_response(answered.status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(payload) + (64 if answered.cut else 0)))
        if answered.retry_after is not None:
            self.send_header("Retry-After", str(answered.retry_after))
        self.end_headers()
        self.wfile.write(payload)

    def log_message(self, format: str, *args: object) -> None:  # noqa: A002 - http.server's name
        """Logs nothing: a test's own output says what it needs."""
