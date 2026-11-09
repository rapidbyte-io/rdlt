"""Where a stream resumes: a pydantic model, kept as JSON in the pipeline's committed state."""

from __future__ import annotations

import pydantic


class Cursor(pydantic.BaseModel):
    """A stream's resume position; its defaults are the stream's start."""

    model_config = pydantic.ConfigDict(frozen=True, extra="forbid")
