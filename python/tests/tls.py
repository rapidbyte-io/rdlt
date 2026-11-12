"""Mutual TLS for the tests of a listening connector, made with the `openssl` command.

A CA made for one test, the certificates it issues, and a test connector served with `--listen`.
"""

from __future__ import annotations

import contextlib
import select
import signal
import subprocess
import sys
import threading
from collections.abc import Generator
from dataclasses import dataclass
from pathlib import Path

import rdlt
from tests.launchers import TESTS

_EXTENSIONS = """\
[req]
distinguished_name = named
[named]
[ca]
basicConstraints = critical, CA:TRUE
keyUsage = critical, keyCertSign, cRLSign
subjectKeyIdentifier = hash
[server]
basicConstraints = CA:FALSE
keyUsage = critical, digitalSignature, keyEncipherment
extendedKeyUsage = serverAuth
subjectAltName = DNS:localhost
[client]
basicConstraints = CA:FALSE
keyUsage = critical, digitalSignature, keyEncipherment
extendedKeyUsage = clientAuth
subjectAltName = DNS:host
"""
"""The extensions of the CA's certificate and of those it issues, which rustls requires."""

HOST = "host"
"""The name the host's certificate carries, which a listening connector allows."""


@dataclass(frozen=True, slots=True)
class Pki:
    """A CA's certificate, a connector's certificate and key it issued, and a host's identity."""

    ca: Path
    server: tuple[Path, Path]
    """The connector's certificate and key, for `localhost`."""
    host: rdlt.Tls
    """The host's certificate and key, for `HOST`, and the CA, as `rdlt.Tls` holds them."""


def _openssl(*arguments: str | Path) -> None:
    subprocess.run(["openssl", *map(str, arguments)], check=True, capture_output=True)  # noqa: S607 - the system's openssl, on the path


def _issued(directory: Path, name: str, extensions: Path, purpose: str, serial: int) -> Path:
    """The certificate the CA in `directory` issues for `purpose`, with its key, as `name`."""
    key, request, cert = (directory / f"{name}.{kind}" for kind in ("key", "csr", "pem"))
    subject = f"/CN={name}"
    new = ["req", "-new", "-nodes", "-newkey", "rsa:2048", "-subj", subject, "-config", extensions]
    _openssl(*new, "-keyout", key, "-out", request)
    signing = ["x509", "-req", "-in", request, "-CA", directory / "ca.pem"]
    signing += ["-CAkey", directory / "ca.key", "-set_serial", str(serial), "-days", "2"]
    _openssl(*signing, "-extfile", extensions, "-extensions", purpose, "-out", cert)
    # A key is its owner's alone: the protocol's TLS refuses one others can read.
    key.chmod(0o600)
    return cert


def pki(directory: Path) -> Pki:
    """A CA made in `directory`, and the certificates it issued a connector and a host."""
    extensions = directory / "extensions.cnf"
    extensions.write_text(_EXTENSIONS)
    made = ["req", "-x509", "-new", "-nodes", "-newkey", "rsa:2048", "-days", "2"]
    made += ["-subj", "/CN=rdlt test ca", "-config", extensions, "-extensions", "ca"]
    _openssl(*made, "-keyout", directory / "ca.key", "-out", directory / "ca.pem")
    server = _issued(directory, "server", extensions, "server", 2)
    host = _issued(directory, HOST, extensions, "client", 3)
    return Pki(
        ca=directory / "ca.pem",
        server=(server, directory / "server.key"),
        host=rdlt.Tls(cert=host, key=directory / f"{HOST}.key", ca=directory / "ca.pem"),
    )


@contextlib.contextmanager
def listening(found: Pki, module: str, served: str) -> Generator[str, None, None]:
    """Where what `served` names of `tests.connectors.<module>` listens while the block runs.

    `served` is what the launcher hands `rdlt.connector.serve`, such as `Tickets` or
    `acknowledging_source_factory(Inbox)`, every name in it imported from the module. It is served
    with `--listen` over mutual TLS with `found`'s certificates, and stops as a listening
    connector does at Ctrl-C once the block ends.
    """
    cert, key = found.server
    args = ["--listen", "127.0.0.1:0", "--tls-cert", str(cert), "--tls-key", str(key)]
    args += ["--tls-client-ca", str(found.ca), "--tls-allow-host", HOST]
    script = (
        f"import sys\nsys.path.append({str(TESTS)!r})\n"
        "from rdlt.connector import *\n"
        f"from tests.connectors.{module} import *\n"
        f"raise SystemExit(serve({served}, args={args!r}))\n"
    )
    process = subprocess.Popen(
        [sys.executable, "-I", "-c", script],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    # What it says on its standard error, read as it is written so the pipe never fills.
    said: list[str] = []
    assert process.stderr is not None
    reader = threading.Thread(target=lambda: said.extend(process.stderr or ()), daemon=True)
    reader.start()
    try:
        assert process.stdout is not None
        ready, _, _ = select.select([process.stdout], [], [], 60)
        line = process.stdout.readline().strip() if ready else ""
        address = line.removeprefix("listening on ")
        if address == line:
            # It said nothing of where it listens: let it end, and its standard error be read to
            # the end, before saying why.
            with contextlib.suppress(subprocess.TimeoutExpired):
                process.wait(timeout=10)
            reader.join(timeout=10)
        assert address != line, (
            f"the connector did not say where it listens within 60 s: {line!r}, "
            f"its standard error {''.join(said)!r}"
        )
        yield f"grpcs://localhost:{address.rsplit(':', 1)[1]}"
    finally:
        process.send_signal(signal.SIGINT)
        try:
            process.wait(timeout=30)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait(timeout=30)
        reader.join(timeout=30)
