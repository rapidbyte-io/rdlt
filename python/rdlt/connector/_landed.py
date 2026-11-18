"""The fence row a destination keeps of a pipeline, and how it answers a commit by it."""

from __future__ import annotations

import enum
import json
import operator
from dataclasses import dataclass, field
from typing import Any, cast

from rdlt import _native
from rdlt.connector._commit import CommitKey, CommitMeta, Receipt
from rdlt.connector._failures import COMMIT_STALE, Fenced


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

    Raises:
        ValueError: `epoch` is no epoch: an integer a `u64` holds, as numpy's may be, and no bool.
    """

    epoch: int
    landed: Receipt | None = field(default=None, hash=False)

    def __post_init__(self) -> None:
        try:
            epoch = -1 if isinstance(self.epoch, bool) else operator.index(cast("Any", self.epoch))
        except TypeError:
            epoch = -1
        if not 0 <= epoch < 2**64:
            message = f"epoch={self.epoch!r} is no epoch: an integer from 0 to 2**64 - 1"
            raise ValueError(message)
        object.__setattr__(self, "epoch", epoch)

    def decide(self, meta: CommitMeta) -> Receipt | None:
        """What to do with the commit `meta` before publishing anything, as `answer` decides.

        Answers the receipt of a commit that landed already, which is the commit's answer and
        publishes nothing, or `None` for a commit to apply.

        Raises:
            Fenced: a newer session fenced the commit's, or, coded `commit_stale`, the commit
                expects another commit landed than the one that did.
            ValueError: the commit's keys or the landed receipt are none rdlt can read.
        """
        key, after = meta["key"], meta["after"]
        decided = answer(self, key, after)
        if decided is Answer.FENCED:
            message = f"the pipeline is at epoch {self.epoch}, past the commit's {key['epoch']}"
            raise Fenced(message)
        if decided is Answer.STALE:
            landed = None if self.landed is None else self.landed["key"]
            message = f"{_shown(key)} expects {_shown(after)} landed, and {_shown(landed)} did"
            raise Fenced(message, code=COMMIT_STALE)
        return self.landed if decided is Answer.LANDED else None


def _shown(key: CommitKey | None) -> str:
    """The commit `key` as a message names it, as the Rust SDK does: none is no commit."""
    if key is None:
        return "no commit"
    return f"commit {key['seq']} of epoch {key['epoch']}"


def answer(fence: Fence, key: CommitKey, after: CommitKey | None) -> Answer:
    """How a destination whose fence row is `fence` answers the commit `key` expecting `after`.

    The landed key gets its receipt, whichever session sends it; then a commit of another epoch
    than the row's is fenced; then one expecting another key landed is stale; the rest apply. It
    is the Rust SDK's `answer` itself.

    Raises:
        ValueError: a key or the landed receipt is none rdlt can read.
    """
    landed = None if fence.landed is None else json.dumps(fence.landed)
    told = None if after is None else json.dumps(after)
    return Answer(_native.answer(fence.epoch, landed, json.dumps(key), told))
