"""Write a connector in Python: its configuration, its reads or writes, the SDK's choreography."""

from __future__ import annotations

from rdlt.connector._capabilities import (
    Capabilities,
    DeleteModes,
    IdentifierRules,
    NestedSupport,
    SchemaChanges,
    WriteModes,
)
from rdlt.connector._config import Config
from rdlt.connector._context import (
    Exclusion,
    Headroom,
    Limits,
    Partition,
    PartitionPlan,
    PartitionState,
    StreamContext,
    StreamState,
    checkpoint,
)
from rdlt.connector._cursor import Cursor
from rdlt.connector._declared import ConnectContext
from rdlt.connector._destination import OpenContext, Opened, WriteStats, destination
from rdlt.connector._failures import (
    COLUMN_MISSING,
    COMMIT_STALE,
    CURSOR_UNISSUED,
    RETENTION_LOST,
    SCHEMA_CONFLICT,
    TABLE_OWNED,
    ConnectorError,
    Fenced,
    InvalidConfig,
    InvalidData,
    RateLimited,
    Transient,
    Unauthorized,
    Unsupported,
)
from rdlt.connector._landed import Answer, Fence, answer
from rdlt.connector._secret import Secret
from rdlt.connector._serve import acknowledging_source_factory, readable_destination_factory, serve
from rdlt.connector._source import ReadStream, source, stream
from rdlt.connector._spec import Checkpointing, StreamSpec

__all__ = [
    "COLUMN_MISSING",
    "COMMIT_STALE",
    "CURSOR_UNISSUED",
    "RETENTION_LOST",
    "SCHEMA_CONFLICT",
    "TABLE_OWNED",
    "Answer",
    "Capabilities",
    "Checkpointing",
    "Config",
    "ConnectContext",
    "ConnectorError",
    "Cursor",
    "DeleteModes",
    "Exclusion",
    "Fence",
    "Fenced",
    "Headroom",
    "IdentifierRules",
    "InvalidConfig",
    "InvalidData",
    "Limits",
    "NestedSupport",
    "OpenContext",
    "Opened",
    "Partition",
    "PartitionPlan",
    "PartitionState",
    "RateLimited",
    "ReadStream",
    "SchemaChanges",
    "Secret",
    "StreamContext",
    "StreamSpec",
    "StreamState",
    "Transient",
    "Unauthorized",
    "Unsupported",
    "WriteModes",
    "WriteStats",
    "acknowledging_source_factory",
    "answer",
    "checkpoint",
    "destination",
    "readable_destination_factory",
    "serve",
    "source",
    "stream",
]
