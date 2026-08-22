#!/usr/bin/env python3
"""io.rapidbyte.pyjsonl — the Python proof connector: a deliberately
small jsonl SOURCE speaking the rdlt connector protocol v1 over a Unix
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
import stat
import sys
import tempfile
import threading
from concurrent import futures

import grpc

import rdlt_connector_v1_pb2 as pb
import rdlt_connector_v1_pb2_grpc as pb_grpc

CONNECTOR_ID = "io.rapidbyte.pyjsonl"
CONNECTOR_VERSION = "0.1.0"

# The RPC protocol version this connector implements — the handshake
# line advertises it as both ends of the accepted range, and the
# Handshake RPC refuses anything else.
PROTOCOL_VERSION = 1

# The handshake LINE format's own version — independent of the RPC
# protocol version above (the line format could reach 2 while the RPC
# protocol stays at its own number).
LINE_FORMAT_VERSION = 1

# The persisted cursor document's format gate. A resume carrying any
# other value is refused typed rather than misread: the offset's
# meaning belongs to the version that wrote it.
CURSOR_FORMAT_VERSION = 1

# The protocol's frame ceiling (64 MiB), mirrored on this server so a
# legally large frame is never refused by a stale 4 MiB default.
MAX_FRAME_BYTES = 64 * 1024 * 1024
# The protocol's document and cursor ceilings: a config or a stream
# spec is held to 8 MiB and a cursor to 4 MiB BEFORE it is parsed —
# the frame ceiling bounds the wire, not what parsing a compact
# document materializes.
MAX_DOCUMENT_BYTES = 8 * 1024 * 1024
MAX_CURSOR_BYTES = 4 * 1024 * 1024
# The protocol's stream ceiling: a source declares at most this many
# streams, and discovery stops — refusing, not truncating — at the
# first file past it rather than listing a directory of any size.
MAX_DECLARED_STREAMS = 1024
# Calls in flight at once, and of those, reads: the executor bounds
# what RUNS; this bounds what is ADMITTED, so pending decoded requests
# cannot pile up behind four busy workers without limit.
MAX_CONCURRENT_RPCS = 64
MAX_CONCURRENT_READS = 16
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

    The wire is one-handshake-per-process: a second Handshake on a populated
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
    if len(config_json) > MAX_DOCUMENT_BYTES:
        return None, (
            f"the config document is {len(config_json)} bytes, over the "
            f"{MAX_DOCUMENT_BYTES}-byte document ceiling"
        )
    try:
        config = json.loads(config_json)
    except ValueError:
        return None, "the config document is not valid JSON"
    if not isinstance(config, dict):
        return None, "the config document must be a JSON object"
    # The unknown field is COUNTED, never named: a key is document
    # content like any value, and a credential-bearing key handed to
    # the wrong place would otherwise ride the refusal into a log.
    unknown = sum(1 for key in config if key != "dir")
    if unknown:
        return None, (
            f"{unknown} unknown config field(s) — this connector's config "
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
    readable file on the host into a stream. The directory is walked
    incrementally and the walk REFUSES at the first eligible file past
    the stream ceiling — a directory of any size costs at most the
    ceiling's worth of retained names. Returns (names, None) or
    (None, cause).
    """
    names = []
    with os.scandir(directory) as entries:
        for entry in entries:
            if not entry.name.endswith(".jsonl"):
                continue
            if entry.is_symlink() or not entry.is_file(follow_symlinks=False):
                continue
            if len(names) >= MAX_DECLARED_STREAMS:
                return None, (
                    f"the configured directory holds more than {MAX_DECLARED_STREAMS} "
                    "*.jsonl files — over the stream ceiling a source may declare"
                )
            names.append(entry.name[: -len(".jsonl")])
    names.sort()
    return names, None


def is_stream(directory: str, name: str) -> bool:
    """Whether `name` is one declared stream: a plain *.jsonl file of
    the directory, judged on that one entry rather than by listing the
    directory again per read."""
    if not name or "/" in name or name in (".", ".."):
        return False
    path = os.path.join(directory, name + ".jsonl")
    try:
        return os.path.isfile(path) and not os.path.islink(path)
    except OSError:
        return False


class Admission:
    """Process-wide call admission: at most MAX_CONCURRENT_RPCS calls
    in flight, and of those at most MAX_CONCURRENT_READS reads. A call
    past either ceiling is refused RESOURCE_EXHAUSTED before it parses
    or touches the filesystem — the executor's workers bound what runs,
    this bounds what waits behind them."""

    def __init__(self):
        self._calls = threading.BoundedSemaphore(MAX_CONCURRENT_RPCS)
        self._reads = threading.BoundedSemaphore(MAX_CONCURRENT_READS)

    def call(self, context):
        return self._admit(self._calls, context, "calls")

    def read(self, context):
        return self._admit(self._reads, context, "reads")

    @staticmethod
    def _admit(semaphore, context, what):
        if not semaphore.acquire(blocking=False):
            context.abort(
                grpc.StatusCode.RESOURCE_EXHAUSTED,
                f"too many concurrent {what} into this connector — the ceiling is reached",
            )
        return _Permit(semaphore)


class _Permit:
    def __init__(self, semaphore):
        self._semaphore = semaphore

    def __enter__(self):
        return self

    def __exit__(self, *_):
        self._semaphore.release()
        return False


class Connector(pb_grpc.ConnectorServicer):
    """The role-generic service: Handshake, Check, and the config-free
    Spec (the one RPC that legally answers before any handshake)."""

    def __init__(self, session: Session, admission: "Admission"):
        self._session = session
        self._admission = admission

    def Handshake(self, request, context):
        with self._admission.call(context):
            return self._handshake(request)

    def _handshake(self, request):
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
                # Declares no state-format ceiling: the document ships
                # empty (the proto field's empty-means-empty-map
                # convention). A source declares nothing the client
                # enforces; the ceiling is the destination role's seat.
                state_format_versions_json=b"",
            )
        )

    def Check(self, request, context):
        with self._admission.call(context):
            self._session.require(context)
            return pb.CheckReply(ok=pb.Empty())

    def Spec(self, request, context):
        # Deliberately no session check: Spec is static identity,
        # answered from constants alone, before any config exists.
        return pb.SpecReply(spec_json=spec_document())


class SourceService(pb_grpc.SourceServiceServicer):
    """The source half: discover-then-stream over the configured
    directory's *.jsonl files."""

    def __init__(self, session: Session, admission: "Admission"):
        self._session = session
        self._admission = admission

    def Streams(self, request, context):
        with self._admission.call(context):
            directory = self._session.require(context)
            names, cause = stream_names(directory)
            if cause is not None:
                return pb.StreamsReply(error=fatal(cause))
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
                for name in names
            ]
            # The framing rule (the proto field's contract): one JSON
            # document per line, joined by single newlines, no trailing
            # newline, empty = zero streams.
            return pb.StreamsReply(ok=pb.StreamList(stream_specs_jsonl=b"\n".join(specs)))

    def Read(self, request, context):
        with self._admission.call(context), self._admission.read(context):
            yield from self._read(request, context)

    def _read(self, request, context):
        directory = self._session.require(context)

        # Every refusal below is a connector outcome, so it rides the
        # stream as its first and only frame — a terminal typed error,
        # never a bare status and never a clean end of stream. The
        # document and cursor ceilings are judged on the raw bytes
        # before anything is parsed.
        if len(request.stream_spec_json) > MAX_DOCUMENT_BYTES:
            yield pb.ReadFrame(
                error=fatal(
                    f"the read request's stream spec is {len(request.stream_spec_json)} "
                    f"bytes, over the {MAX_DOCUMENT_BYTES}-byte document ceiling"
                )
            )
            return
        name = None
        try:
            spec = json.loads(request.stream_spec_json)
            if isinstance(spec, dict):
                name = spec.get("name")
        except ValueError:
            pass
        if not isinstance(name, str) or len(name.encode()) > 1024:
            yield pb.ReadFrame(
                error=fatal(
                    "the read request's stream spec does not decode as a "
                    "stream declaration carrying a `name` within the identifier ceiling"
                )
            )
            return
        if not is_stream(directory, name):
            yield pb.ReadFrame(
                error=fatal(
                    "no such stream — streams are the *.jsonl files of the "
                    "configured directory"
                )
            )
            return

        offset = 0
        if request.HasField("since_cursor_json"):
            if len(request.since_cursor_json) > MAX_CURSOR_BYTES:
                yield pb.ReadFrame(
                    error=fatal(
                        f"the since-cursor is {len(request.since_cursor_json)} bytes, "
                        f"over the {MAX_CURSOR_BYTES}-byte cursor ceiling"
                    )
                )
                return
            offset, cause = decode_cursor(request.since_cursor_json)
            if cause is not None:
                yield pb.ReadFrame(error=fatal(cause))
                return

        path = os.path.join(directory, name + ".jsonl")
        # Open and reject symlinks atomically, and never block: the
        # earlier discovery is only a snapshot, O_NOFOLLOW closes the
        # swap window for a link, O_NONBLOCK plus the fstat below close
        # it for a FIFO (a plain open of one would park this worker
        # until a writer appeared, which for a planted one is never).
        descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
        with os.fdopen(descriptor, "rb") as stream:
            status = os.fstat(stream.fileno())
            if not stat.S_ISREG(status.st_mode):
                yield pb.ReadFrame(
                    error=fatal("the stream's file is not a regular file")
                )
                return
            size = status.st_size
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
        # The version is reported by TYPE when it is not a number: a
        # cursor's values are a document's content, and rendering a
        # hostile container here would amplify it into the refusal.
        found = version if isinstance(version, (int, float)) else type(version).__name__
        return None, (
            f"unsupported cursor format_version {found} — this connector "
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
        # The library's own admission ceiling sits beside this file's:
        # past it a call is refused before any handler runs at all.
        maximum_concurrent_rpcs=MAX_CONCURRENT_RPCS,
        options=(
            ("grpc.max_receive_message_length", MAX_FRAME_BYTES),
            ("grpc.max_send_message_length", MAX_FRAME_BYTES),
        ),
    )
    admission = Admission()
    pb_grpc.add_ConnectorServicer_to_server(Connector(session, admission), server)
    pb_grpc.add_SourceServiceServicer_to_server(SourceService(session, admission), server)

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
    # child's lifetime; the wire has no orderly-shutdown RPC.
    server.wait_for_termination()


if __name__ == "__main__":
    main()
