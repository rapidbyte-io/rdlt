"""Write a connector in Python: its configuration, its reads or writes, the SDK's choreography."""

from __future__ import annotations

from rdlt.connector import merge
from rdlt.connector._capabilities import (
    Capabilities,
    DeleteModes,
    IdentifierRules,
    NestedSupport,
    SchemaChanges,
    WriteModes,
)
from rdlt.connector._changes import (
    OP_COLUMN,
    SEQ_COLUMN,
    UNCHANGED_COLUMN,
    ChangeOp,
    changes,
)
from rdlt.connector._commit import CommitKey, CommitMeta, Receipt, SegmentRange, SegmentSet
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
from rdlt.connector._destination import DestinationConnector, OpenContext, Opened, destination
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
from rdlt.connector._session import Session, TableWriter, WriteStats
from rdlt.connector._source import ReadStream, SourceConnector, source, stream
from rdlt.connector._spec import Checkpointing, StreamSpec
from rdlt.connector._tables import ChangeColumns, HistoryColumns, MergeKey, RootKey, to_arrow

__all__ = [
    "COLUMN_MISSING",
    "COMMIT_STALE",
    "CURSOR_UNISSUED",
    "OP_COLUMN",
    "RETENTION_LOST",
    "SCHEMA_CONFLICT",
    "SEQ_COLUMN",
    "TABLE_OWNED",
    "UNCHANGED_COLUMN",
    "Answer",
    "Capabilities",
    "ChangeColumns",
    "ChangeOp",
    "Checkpointing",
    "CommitKey",
    "CommitMeta",
    "Config",
    "ConnectContext",
    "ConnectorError",
    "Cursor",
    "DeleteModes",
    "DestinationConnector",
    "Exclusion",
    "Fence",
    "Fenced",
    "Headroom",
    "HistoryColumns",
    "IdentifierRules",
    "InvalidConfig",
    "InvalidData",
    "Limits",
    "MergeKey",
    "NestedSupport",
    "OpenContext",
    "Opened",
    "Partition",
    "PartitionPlan",
    "PartitionState",
    "RateLimited",
    "ReadStream",
    "Receipt",
    "RootKey",
    "SchemaChanges",
    "Secret",
    "SegmentRange",
    "SegmentSet",
    "Session",
    "SourceConnector",
    "StreamContext",
    "StreamSpec",
    "StreamState",
    "TableWriter",
    "Transient",
    "Unauthorized",
    "Unsupported",
    "WriteModes",
    "WriteStats",
    "acknowledging_source_factory",
    "answer",
    "changes",
    "checkpoint",
    "destination",
    "merge",
    "readable_destination_factory",
    "serve",
    "source",
    "stream",
    "to_arrow",
]
