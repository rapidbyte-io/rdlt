# rdlt-connector-pyjsonl

The Python proof connector: a deliberately small jsonl SOURCE speaking
the rdlt connector protocol v0, certified by the SAME `rdlt-certify`
binary — the same clause vocabulary — that certifies the first-party
Rust connectors. Zero Rust anywhere in it: the protocol is the
contract, not any sdk.

- id `io.rapidbyte.pyjsonl`, version `0.1.0`
- config `{"dir": "<path>"}` — one field; unknown top-level fields are
  refused at the handshake with a typed FATAL error frame
- streams: one per `*.jsonl` file in `dir`, named by file stem
- `Read`: one `raw_json` frame per line, then one checkpoint frame
  carrying `{"format_version": 1, "offset": <bytes>}` at file end; a
  read with a since-cursor resumes from that byte offset, and any
  other `format_version` is refused typed

## Files

| file | role |
|---|---|
| `rdlt_connector_pyjsonl.py` | the whole implementation (stdlib + grpcio) |
| `rdlt-connector-pyjsonl` | launcher, so PATH discovery (`rdlt-connector-` + the id's last `.`-segment) resolves to something spawnable |
| `rdlt_connector_v0_pb2*.py` | vendored generated stubs, pinned-generator header on line 1; `tools/check-python-stubs.sh` regenerates and diffs |
| `requirements.txt` | exact runtime pins (`grpcio`, `protobuf`); the `grpcio-tools` regen pin rides as a comment |
| `fixtures/` | the committed certification fixture: `config.json` + two small jsonl streams |

## Certifying it

From the repo root, with the certifier built
(`cargo build -p rdlt-certify --features bin --bin rdlt-certify`) and a
venv holding the pinned requirements first on PATH:

```console
$ python3 -m venv target/py-certify-venv
$ target/py-certify-venv/bin/pip install -r connectors/python/rdlt-connector-pyjsonl/requirements.txt
$ PATH="$PWD/target/py-certify-venv/bin:$PATH" target/debug/rdlt-certify \
      --role source \
      --config connectors/python/rdlt-connector-pyjsonl/fixtures/config.json \
      connectors/python/rdlt-connector-pyjsonl/rdlt-connector-pyjsonl
```

Every source clause (S1/S2/S4, P1–P7) must render PASS. The test gate
runs exactly this line (venv cached under `target/py-certify-venv`,
rebuilt when `requirements.txt` changes), skipping only when `python3`
is absent.
