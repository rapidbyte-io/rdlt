# Where a pipeline keeps its write-ahead logs

A pipeline whose source cannot read again what it acknowledged keeps a write-ahead log (ADR 0029):
the rows its source was told were committed live there until their commit lands. `rdlt-log-store`
chooses where, from configuration: a local directory, or an S3 bucket, so a log outlives its
machine. Every run of a pipeline at a destination keeps its logs in one store; a run whose store
is another is refused (`wal_store_other`). The design is ADR 0051's.

## A local directory

```json
{ "local": { "base": "/var/lib/rdlt/logs" } }
```

The base is made where it is missing, and it and everything beneath it must be the engine's user's
alone (ADR 0045).

## An S3 bucket

```json
{ "s3": {
    "bucket": "rdlt-logs",
    "prefix": "production/logs",
    "region": "eu-west-1",
    "access_key_id": "${secret:s3_key_id}",
    "secret_access_key": "${file:/run/secrets/s3_secret_key}"
} }
```

| Field | Meaning |
|---|---|
| `bucket` | The bucket: 3 to 63 of `a-z`, `0-9`, `.` and `-`, beginning and ending with a letter or digit. |
| `prefix` | What every key begins with: segments of `A-Za-z0-9._-` joined by `/`, at most 512 bytes. Give each environment its own. |
| `region` | The bucket's region; `us-east-1` for most other stores. |
| `endpoint` | The store's address, where it is not AWS's: `https://host[:port]`. Plain `http://` is taken only to a loopback IP address, as `http://127.0.0.1:9000`, never to a name. |
| `path_style` | `true` to name the bucket in the request's path, as most stores other than AWS's ask. |
| `access_key_id`, `secret_access_key`, `session_token` | Each one secret reference, `${env:NAME}`, `${file:/absolute/path}` or `${secret:name}`, resolved by the resolver the operator gives the host, and again every five minutes. A key written in the configuration itself is refused. |
| `part_bytes` | Bytes of a part of a chunk uploaded in parts, from 5 MiB to 5 GiB; 8 MiB by default. |

The store is reached over TLS 1.2 or 1.3, checked against the system's trusted roots: a store
whose certificate a private authority signs needs that authority among them. No redirect is
followed and no proxy used, and no credential is taken from the environment, a profile or the
instance's metadata.

### What the bucket must allow

The credentials need, beneath the prefix: `s3:PutObject`, `s3:GetObject`, `s3:DeleteObject`,
`s3:AbortMultipartUpload`, and `s3:ListBucket` on the bucket. Without `s3:ListBucket`, S3 answers
a missing object as forbidden, and the probe refuses the store (`wal_storage_denied`).

Give the bucket a lifecycle rule that ends incomplete multipart uploads after a day: an upload a
crash interrupts leaves parts that no listing shows, and that are billed, until it is ended.

Whoever may write beneath the prefix holds the log's authority, as whoever may write a local base
does: grant it to the engine's credentials alone.

### The probe

Opening the store probes it before any log is kept there: an object created, then created again,
which must be refused; listed, which must show it; an upload in parts made and read back; both
deleted, then found missing. A store that fails is refused, `wal_storage_unsupported`, and no log
runs on it unfenced.

| Store | Observed |
|---|---|
| AWS S3 | Conditional create (`If-None-Match: *`) since 2024, and listings are strongly consistent. |
| MinIO | Passes, in the tests (a build of `RELEASE.2026-08-04`). |
| RustFS | Passes, in the tests. It refuses a range ending past a signed 64-bit length, which the log never asks for. |
| Versity S3 gateway | Passes the probe (`versity/versitygw`, posix backend). |
| A store without conditional create | Refused by the probe. |

GCS and Azure Blob are not supported.

## Cost and latency

| Operation | Requests to the store |
|---|---|
| A commit | A HEAD of the log's mark, a PUT of its chunk, a HEAD of the mark again; for a chunk past a part, a multipart upload's beginning, its parts and its completion as well. |
| Each commit's horizon | A LIST of the pipeline's open logs. |
| A chunk no longer needed | A LIST of its log and a DELETE, two for one uploaded in parts. |
| Opening a log, removing it | A LIST and a PUT; a DELETE, a LIST and a DELETE an object. |
| A replay | A LIST of each log and a GET for each 64 KiB a frame is read in. |

A commit's chunk is durable once its PUT is acknowledged, tens of milliseconds on S3 for a small
chunk, longer as it grows; its source hears of the commit after that. A request is tried up to five
times, each attempt within 30 seconds and a second more a MiB it moves, with a random wait growing
to 5 seconds between attempts; one that never succeeds fails the attempt retryably
(`wal_storage_unavailable`).

A staged chunk is held in memory up to a part, 8 MiB by default, beside the engine's memory
budget, one a running load.
