"""The PokeAPI example's fixtures: local servers of the API."""

from __future__ import annotations

import sys
from collections.abc import Callable, Iterator
from pathlib import Path

import pytest

# Appended, so the tests import the example's modules as its program does.
sys.path.append(str(Path(__file__).resolve().parent))

from local_api import TYPES, Api


@pytest.fixture
def serve() -> Iterator[Callable[..., Api]]:
    """Starts servers of the API, each stopped as the test ends."""
    started: list[Api] = []

    def start(pokemon: int, types: int = TYPES) -> Api:
        api = Api(pokemon, types)
        started.append(api)
        return api

    yield start
    for api in started:
        api.close()
