#!/usr/bin/env python3
"""io.rapidbyte.pyjsonl — the Python proof connector: a deliberately
small jsonl SOURCE speaking the rdlt connector protocol v0 over a Unix
domain socket, with zero Rust anywhere in it.

The proof claim this file exists for: the SAME standalone certifier
that certifies the first-party Rust connectors certifies this one, with
the SAME clause vocabulary — the protocol is the contract, not any sdk.
Everything here is implemented from the wire contract alone: the
handshake line grammar (one stdout line, then stdout falls silent), the
proto file's message shapes, and the error-frame rule (the message
carries bare cause text; classification travels only as the enum).

Shape:
  - config: {"dir": "<path>"} — one field, unknown top-level fields are
    refused at the handshake with a typed FATAL error frame;
  - streams: one per *.jsonl file in the configured directory, named by
    the file's stem;
  - Read: one raw_json frame per line, then one checkpoint frame
    carrying a byte-offset cursor at file end; a read with a
    since-cursor resumes from that byte offset;
  - cancellation: observed between frames — a client that drops the
    read stream stops the loop at the next frame boundary.

Dependencies: the Python standard library, grpcio, and the vendored
generated stubs (which need the protobuf runtime). Nothing else.
"""

import argparse
import glob
import json
import os
import sys
import tempfile
import threading
from concurrent import futures

import grpc

import rdlt_connector_v0_pb2 as pb
import rdlt_connector_v0_pb2_grpc as pb_grpc

CONNECTOR_ID = "io.rapidbyte.pyjsonl"
CONNECTOR_VERSION = "0.1.0"

# The RPC protocol version this connector implements — the handshake
# line advertises it as both ends of the accepted range, and the
# Handshake RPC refuses anything else.
PROTOCOL_VERSION = 0

# The handshake LINE format's own version — independent of the RPC
# protocol version above (the line format could reach 2 while the RPC
# protocol stays at 0).
LINE_FORMAT_VERSION = 1

# The persisted cursor document's format gate. A resume carrying any
# other value is refused typed rather than misread: the offset's
# meaning belongs to the version that wrote it.
CURSOR_FORMAT_VERSION = 1

# The protocol's frame ceiling (64 MiB), mirrored on this server so a
# legally large frame is never refused by a stale 4 MiB default.
MAX_FRAME_BYTES = 64 * 1024 * 1024
# `raw_json` is nested inside a protobuf ReadFrame, whose tag and
# length prefix must fit the gRPC send ceiling too. Keep deliberate
# headroom rather than relying on the current varint width.
MAX_RAW_JSON_BYTES = MAX_FRAME_BYTES - 16

# The frozen pre-handshake refusal: every RPC but Handshake and the
# config-free Spec answers this FAILED_PRECONDITION status until a
# handshake has completed — a protocol-state violation, not a connector
# outcome, so it is a raw status rather than an error frame.
HANDSHAKE_NOT_COMPLETED = "handshake has not completed"

CONFIG_SCHEMA = {
    "type": "object",
    "additionalProperties": False,
    "required": ["dir"],
    "properties": {
        "dir": {
            "type": "string",
            "description": "directory whose *.jsonl files become streams",
        }
    },
}


def spec_document() -> bytes:
    """The ConnectorSpec JSON served by Spec and carried on HandshakeOk.

    One producer for both so the identity the handshake reports can
    never skew from the spec document — the certifier fails any
    connector whose two answers disagree.
    """
    return json.dumps(
        {
            "name": CONNECTOR_ID,
            "version": CONNECTOR_VERSION,
            "config_schema": CONFIG_SCHEMA,
        }
    ).encode()


def fatal(message: str) -> pb.ErrorFrame:
    """A FATAL error frame carrying bare cause text.

    The classification travels ONLY as the enum — the receiving client
    renders it exactly once on reconstruction, so baking a rendering
    like "fatal source error: " into the message would double-frame it.
    """
    return pb.ErrorFrame(classification=pb.FATAL, message=message)


class Session:
    """The one per-process session: unpopulated until a handshake
    succeeds, then it carries the validated stream directory.

    v0 is one-handshake-per-process: a second Handshake on a populated
    session is refused, and every non-exempt RPC before the first one
    aborts with the frozen precondition status.
    """

    def __init__(self):
        self._lock = threading.Lock()
        self._dir = None

    def populate(self, directory: str) -> bool:
        """Claim the session; False when a handshake already did."""
        with self._lock:
            if self._dir is not None:
                return False
            self._dir = directory
            return True

    def directory(self):
        with self._lock:
            return self._dir

    def require(self, context) -> str:
        """The populated directory, or the pre-handshake refusal."""
        directory = self.directory()
        if directory is None:
            context.abort(grpc.StatusCode.FAILED_PRECONDITION, HANDSHAKE_NOT_COMPLETED)
        return directory


def validate_config(config_json: bytes):
    """Validate the handshake's config document.

    Returns (directory, None) on success, (None, cause) on refusal. A
    cause never echoes the document's bytes or values — a config may
    carry credentials, so refusals name fields and rules, not content.
    """
    try:
        config = json.loads(config_json)
    except ValueError:
        return None, "the config document is not valid JSON"
    if not isinstance(config, dict):
        return None, "the config document must be a JSON object"
    unknown = sorted(key for key in config if key != "dir")
    if unknown:
        return None, (
            f"unknown config field `{unknown[0]}` — this connector's config "
            "carries exactly one field, `dir`"
        )
    if "dir" not in config:
        return None, "the config document is missing the required `dir` field"
    directory = config["dir"]
    if not isinstance(directory, str):
        return None, "the config field `dir` must be a string"
    if not os.path.isdir(directory):
        return None, "the configured `dir` does not exist or is not a directory"
    return directory, None


def stream_names(directory: str):
    """The declared streams: one per *.jsonl file, named by stem, in
    sorted order so discovery is deterministic across reads.

    Symlinks are rejected outright (isfile alone FOLLOWS them): a link
    planted inside the configured directory could otherwise pull any
    readable file on the host into a stream.
    """

    def plain_file(name):
        path = os.path.join(directory, name)
        return os.path.isfile(path) and not os.path.islink(path)

    return sorted(
        name[: -len(".jsonl")]
        for name in os.listdir(directory)
        if name.endswith(".jsonl") and plain_file(name)
    )


class Connector(pb_grpc.ConnectorServicer):
    """The role-generic service: Handshake, Check, and the config-free
    Spec (the one RPC that legally answers before any handshake)."""

    def __init__(self, session: Session):
        self._session = session

    def Handshake(self, request, context):
        def refuse(message):
            return pb.HandshakeReply(error=fatal(message))

        if request.expected_role != "source":
            return refuse(
                f"this connector serves role `source`, not `{request.expected_role}`"
            )
        if request.protocol_version != PROTOCOL_VERSION:
            return refuse(
                f"protocol version {request.protocol_version} is outside this "
                f"connector's supported range {PROTOCOL_VERSION}..={PROTOCOL_VERSION}"
            )
        directory, cause = validate_config(request.config_json)
        if cause is not None:
            return refuse(cause)
        if not self._session.populate(directory):
            return refuse("a handshake has already completed on this connector process")
        return pb.HandshakeReply(
            ok=pb.HandshakeOk(
                connector_id=CONNECTOR_ID,
                connector_version=CONNECTOR_VERSION,
                spec_json=spec_document(),
                # Empty for sources — DestinationCapabilities is the
                # destination role's document.
                capabilities_json=b"",
                # The v0 posture: one cursor format exists, so there is
                # nothing to negotiate and the map ships empty.
                state_format_versions={},
            )
        )

    def Check(self, request, context):
        self._session.require(context)
        return pb.CheckReply(ok=pb.Empty())

    def Spec(self, request, context):
        # Deliberately no session check: Spec is static identity,
        # answered from constants alone, before any config exists.
        return pb.SpecReply(spec_json=spec_document())


class SourceService(pb_grpc.SourceServiceServicer):
    """The source half: discover-then-stream over the configured
    directory's *.jsonl files."""

    def __init__(self, session: Session):
        self._session = session

    def Streams(self, request, context):
        directory = self._session.require(context)
        specs = [
            json.dumps(
                {
                    "name": name,
                    "primary_key": None,
                    "cursor_field": None,
                    "type_hints": {},
                    "structured": False,
                }
            ).encode()
            for name in stream_names(directory)
        ]
        return pb.StreamsReply(ok=pb.StreamList(stream_spec_json=specs))

    def Read(self, request, context):
        directory = self._session.require(context)

        # Every refusal below is a connector outcome, so it rides the
        # stream as its first and only frame — a terminal typed error,
        # never a bare status and never a clean end of stream.
        name = None
        try:
            spec = json.loads(request.stream_spec_json)
            if isinstance(spec, dict):
                name = spec.get("name")
        except ValueError:
            pass
        if not isinstance(name, str):
            yield pb.ReadFrame(
                error=fatal(
                    "the read request's stream spec does not decode as a "
                    "stream declaration carrying a `name`"
                )
            )
            return
        if name not in stream_names(directory):
            yield pb.ReadFrame(
                error=fatal(
                    f"no stream named `{name}` — streams are the *.jsonl "
                    "files of the configured directory"
                )
            )
            return

        offset = 0
        if request.HasField("since_cursor_json"):
            offset, cause = decode_cursor(request.since_cursor_json)
            if cause is not None:
                yield pb.ReadFrame(error=fatal(cause))
                return

        path = os.path.join(directory, name + ".jsonl")
        # Open and reject symlinks atomically. The earlier directory
        # discovery is only a snapshot; O_NOFOLLOW closes the swap
        # window between that check and this read.
        descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW)
        with os.fdopen(descriptor, "rb") as stream:
            size = os.fstat(stream.fileno()).st_size
            if offset > size:
                yield pb.ReadFrame(
                    error=fatal(
                        f"the cursor's offset {offset} lies beyond the "
                        f"stream's {size} bytes — the cursor does not belong "
                        "to this stream's current content"
                    )
                )
                return
            stream.seek(offset)
            while True:
                # Cancellation is observed between frames: a client that
                # dropped the stream stops this loop at the next boundary
                # instead of pumping frames nobody reads.
                if not context.is_active():
                    return
                # One line, capped one byte past the frame ceiling: a
                # row longer than a frame can never be sent anyway, and
                # without the cap a single huge line is materialized
                # whole before grpc's send ceiling aborts the RPC. A
                # capped readline that isn't at EOF and doesn't end in
                # a newline has read exactly cap+1 bytes, so the check
                # below can never silently split one row into two.
                line = stream.readline(MAX_RAW_JSON_BYTES + 1)
                if not line:
                    break
                if len(line) > MAX_RAW_JSON_BYTES:
                    yield pb.ReadFrame(
                        error=fatal(
                            f"stream `{name}` carries a line longer than "
                            f"{MAX_RAW_JSON_BYTES} bytes — one row plus its "
                            "protocol framing must fit one "
                            "protocol frame"
                        )
                    )
                    return
                row = line.strip()
                if row:
                    yield pb.ReadFrame(raw_json=bytes(row))
            # One checkpoint per file end: the cursor covers every row
            # above, so a resume from it re-reads nothing and a full
            # re-read equals covered-rows + resumed-rows exactly. The
            # offset is the position actually consumed (tell() on a
            # binary-mode reader is exact after iteration), not the
            # fstat-at-open size — the two differ when the file grows
            # mid-read, and the consumed position is the one the rows
            # above actually cover.
            cursor = {
                "format_version": CURSOR_FORMAT_VERSION,
                "offset": stream.tell(),
            }
            yield pb.ReadFrame(checkpoint_cursor_json=json.dumps(cursor).encode())


def decode_cursor(cursor_json: bytes):
    """Decode a since-cursor. Returns (offset, None) or (None, cause).

    The format gate comes first: an offset written by a different
    format version means something this version must not guess at.
    """
    try:
        cursor = json.loads(cursor_json)
    except ValueError:
        return None, "the since-cursor does not decode as JSON"
    if not isinstance(cursor, dict):
        return None, "the since-cursor is not a cursor document (a JSON object)"
    version = cursor.get("format_version")
    if version != CURSOR_FORMAT_VERSION:
        return None, (
            f"unsupported cursor format_version {version!r} — this connector "
            f"reads format_version {CURSOR_FORMAT_VERSION}"
        )
    offset = cursor.get("offset")
    if not isinstance(offset, int) or isinstance(offset, bool) or offset < 0:
        return None, "the cursor's `offset` must be a non-negative integer"
    return offset, None


def main():
    parser = argparse.ArgumentParser(
        prog="rdlt-connector-pyjsonl",
        description="rdlt Python proof connector — a jsonl source protocol server",
    )
    # Source-only: a `--role=destination` spawn fails argparse's choices
    # gate (stderr usage + exit 2, no handshake line) — exactly how a
    # single-role connector tells a probing provider to try elsewhere.
    parser.add_argument("--role", required=True, choices=["source"])
    parser.add_argument(
        "--version", action="version", version=f"%(prog)s {CONNECTOR_VERSION}"
    )
    parser.parse_args()

    session = Session()
    server = grpc.server(
        futures.ThreadPoolExecutor(max_workers=4),
        options=(
            ("grpc.max_receive_message_length", MAX_FRAME_BYTES),
            ("grpc.max_send_message_length", MAX_FRAME_BYTES),
        ),
    )
    pb_grpc.add_ConnectorServicer_to_server(Connector(session), server)
    pb_grpc.add_SourceServiceServicer_to_server(SourceService(session), server)

    # Reclaim dead predecessors' socket dirs BEFORE minting our own.
    # rmdir-only is the WHOLE liveness check: the Rust side (probes,
    # guards) unlinks the socket FILE on every cleanup path, so a dead
    # sibling's dir is EMPTY and rmdir succeeds exactly then; a LIVE
    # sibling's dir still holds its socket, so rmdir fails harmlessly;
    # rmdir never follows symlinks; and other users' dirs fail on
    # permissions. ONE race is accepted, not absent: between a
    # sibling's own mkdtemp and its bind, its fresh dir is briefly
    # empty and this rmdir can reclaim it — that sibling's bind then
    # fails and it exits loudly (the bind's return is checked below)
    # rather than serving from a vanished directory. Failures here are
    # ignored wholesale — reclaim is best-effort hygiene, never a gate.
    for stale in glob.glob(os.path.join(tempfile.gettempdir(), "rdlt-pyjsonl-*")):
        try:
            os.rmdir(stale)
        except OSError:
            pass

    # A fresh PRIVATE directory per process (mkdtemp: 0700, name
    # unpredictable) keeps the socket path collision-free and
    # attacker-free — a predictable name in the shared world-writable
    # temp dir could be pre-created (bind fails, DoS) or planted as a
    # dangling symlink, which bind(2) would follow to an
    # attacker-chosen location, and a by-path chmod would race the same
    # way. Inside the private dir both the bind and the chmod-to-0600
    # below are safe.
    socket_dir = tempfile.mkdtemp(prefix="rdlt-pyjsonl-")
    socket_path = os.path.join(socket_dir, "connector.sock")
    # add_insecure_port answers 0 on a failed bind (it does not raise);
    # unchecked, the failure would only surface incidentally when the
    # chmod below finds no socket. Exit loudly instead, on stderr —
    # stdout is the machine channel and carries only the handshake line.
    if server.add_insecure_port(f"unix:{socket_path}") == 0:
        print(f"could not bind the connector socket at {socket_path}", file=sys.stderr)
        sys.exit(1)
    os.chmod(socket_path, 0o600)
    server.start()

    # The one stdout line, then stdout falls structurally silent:
    # fd 1 is redirected to /dev/null so even a stray C-level write can
    # never speak on the machine channel again. Logs belong on stderr.
    print(
        f"rdlt-connector|{LINE_FORMAT_VERSION}|{PROTOCOL_VERSION}|"
        f"{PROTOCOL_VERSION}|{socket_path}",
        flush=True,
    )
    devnull = os.open(os.devnull, os.O_WRONLY)
    os.dup2(devnull, 1)
    os.close(devnull)

    # Serve until the parent kills the process — the provider owns this
    # child's lifetime; there is no orderly-shutdown RPC in v0.
    server.wait_for_termination()


if __name__ == "__main__":
    main()
