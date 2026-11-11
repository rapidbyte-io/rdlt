"""The fence row a destination keeps of a pipeline, and how it answers a commit by it."""

from __future__ import annotations

import enum
from collections.abc import Mapping
from dataclasses import dataclass, field

from rdlt.connector._failures import COMMIT_STALE, Fenced
from rdlt.model import Json


class Answer(enum.Enum):
    """How a destination answers a commit by its fence row, as the Rust SDK's `Answer` does."""

    LANDED = "landed"
    """The commit landed already: the fence row's receipt is the answer, and nothing publishes."""
    FENCED = "fenced"
    """A newer session fenced the commit's: nothing publishes."""
    STALE = "stale"
    """The commit expects another commit landed than the one that did: nothing publishes."""
    APPLY = "apply"
    """The commit applies."""


@dataclass(frozen=True, slots=True)
class Fence:
    """A pipeline's fence row: its newest session's epoch, and the last landed commit's receipt.

    `landed` is none before the first commit lands. A commit reads the row under its
    transaction's write lock, decides with `decide`, and where the commit applies writes its own
    receipt into the row in the same transaction.
    """

    epoch: int
    landed: Mapping[str, Json] | None = field(default=None, hash=False)

    def decide(self, meta: Mapping[str, Json]) -> Mapping[str, Json] | None:
        """What to do with the commit `meta` before publishing anything, as `answer` decides.

        Answers the receipt of a commit that landed already, which is the commit's answer and
        publishes nothing, or `None` for a commit to apply.

        Raises:
            Fenced: a newer session fenced the commit's, or, coded `commit_stale`, the commit
                expects another commit landed than the one that did.
        """
        key = meta["key"]
        if not isinstance(key, Mapping):
            raise TypeError("a commit's key is a mapping")
        after = meta.get("after")
        decided = answer(self, key, after if isinstance(after, Mapping) else None)
        if decided is Answer.FENCED:
            message = f"the pipeline is at epoch {self.epoch}, past the commit's {key['epoch']}"
            raise Fenced(message)
        if decided is Answer.STALE:
            landed = None if self.landed is None else self.landed["key"]
            message = f"{_shown(key)} expects {_shown(after)} landed, and {_shown(landed)} did"
            raise Fenced(message, code=COMMIT_STALE)
        return self.landed if decided is Answer.LANDED else None


def _shown(key: Json) -> str:
    """The commit `key` as a message names it, as the Rust SDK does: none is no commit."""
    if not isinstance(key, Mapping):
        return "no commit"
    return f"commit {key['seq']} of epoch {key['epoch']}"


def answer(fence: Fence, key: Mapping[str, Json], after: Mapping[str, Json] | None) -> Answer:
    """How a destination whose fence row is `fence` answers the commit `key` expecting `after`.

    The landed key gets its receipt, whichever session sends it; then a commit of another epoch
    than the row's is fenced; then one expecting another key landed is stale; the rest apply.
    """
    landed = None if fence.landed is None else fence.landed["key"]
    if landed is not None and landed == key:
        return Answer.LANDED
    if fence.epoch != key["epoch"]:
        return Answer.FENCED
    if landed != after:
        return Answer.STALE
    return Answer.APPLY
