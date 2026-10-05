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
| `path_style` | `true` to name the bucket in the request's path, as most stores other than AWS's ask. Otherwise a custom endpoint's host is the bucket's, `https://<bucket>.<host>`, and an endpoint at an IP address is refused. |
| `access_key_id`, `secret_access_key`, `session_token` | Each one secret reference, `${env:NAME}`, `${file:/absolute/path}` or `${secret:name}`, resolved by the resolver the operator gives the host, and again every five minutes; a refresh that fails keeps the keys held and tries again at the next request. A key written in the configuration itself is refused. |
| `part_bytes` | Bytes of a part of a chunk uploaded in parts, from 5 MiB to 5 GiB; 8 MiB by default. Each running load holds a part in memory, reserved from the engine's memory budget: a part larger than half the budget's share for logs is refused (`wal_staging_exceeds_budget`). |

The store is reached over TLS 1.2 or 1.3, checked against the system's trusted roots: a store
whose certificate a private authority signs needs that authority among them. No redirect is
followed and no proxy used, and no credential is taken from the environment, a profile or the
instance's metadata. A request no retry can mend stops the run at once, `wal_storage_refused`:
a certificate no trusted root signs, or a status saying the request itself is wrong, as a 400
other than a timeout or an expired token.

### What the bucket must allow

The credentials need, beneath the prefix: `s3:PutObject`, `s3:GetObject`, `s3:DeleteObject`,
`s3:AbortMultipartUpload`, and `s3:ListBucket` on the bucket. Without `s3:ListBucket`, S3 answers
a missing object as forbidden, and the probe refuses the store (`wal_storage_denied`).

Give the bucket a lifecycle rule that ends incomplete multipart uploads after a day: an upload a
running process gives up it aborts itself, but one a crash interrupts leaves parts that no
listing shows, and that are billed, until the rule ends it.

Whoever may write beneath the prefix holds the log's authority, as whoever may write a local base
does: grant it to the engine's credentials alone.

### The probe

Opening the store probes it before any log is kept there:

- three rounds of four creates of a fresh name racing, of which exactly one must be taken;
- a marker created, its metadata read back, and listed, which must show it;
- an upload in parts made and read back;
- everything it made deleted, then each found missing.

A store that fails is refused, `wal_storage_unsupported`, and no log runs on it unfenced. The
probe is a sample: a store must take only one of creates racing every time, and keep the
metadata a create gives its object, which the probe checks while it looks and cannot prove.

| Store | Observed |
|---|---|
| AWS S3 | Conditional create (`If-None-Match: *`) since 2024, and listings are strongly consistent. |
| MinIO | Passes, in the tests (a build of `RELEASE.2026-08-04`). |
| RustFS | Passes, in the tests. It refuses a range ending past a signed 64-bit length, which the log never asks for. |
| A store without conditional create | Refused by the probe. |

GCS and Azure Blob are not supported.

## Cost and latency

| Operation | Requests to the store |
|---|---|
| A commit | A HEAD of the log's mark, a PUT of its chunk, a HEAD of the mark again; for a chunk past a part, a multipart upload's beginning, its parts and its completion as well. |
| Each commit's horizon | A LIST of the pipeline's open logs. |
| A chunk no longer needed | A DELETE, two for one uploaded in parts, and a GET of its head where the process never read it. |
| Opening a log, removing it | A LIST and a PUT; a DELETE, a LIST and a DELETE an object. |
| A replay | A LIST of each log, and a GET for each 64 KiB a frame is read in. |

A log's objects are its live chunks alone, so what it costs never grows with how many it held. A
log's directory listing more than 65,536 objects is unreadable (`wal_unreadable`). Each chunk's
end names the live chunks before it, two bytes at least each, so a log keeps at most about the
square root of its `log_bytes` in chunks, 65,536 at the default 4 GiB, and a part's body beside
each chunk longer than a part: a `log_bytes` far past 4 GiB can let a log of many small chunks
pass the listing's limit.

A commit's chunk is durable once its PUT is acknowledged, tens of milliseconds on S3 for a small
chunk, longer as it grows; its source hears of the commit after that. A request is tried up to five
times, each attempt within 30 seconds and a second more a MiB it moves, with a random wait growing
to 5 seconds between attempts; one that never succeeds fails the attempt retryably
(`wal_storage_unavailable`).

A staged chunk is held in memory up to a part, 8 MiB by default, reserved from the engine's
memory budget, one a running load, beside 256 KiB through which the load copies frames it keeps
out of old chunks, a piece at a time, however large a frame.

A source that sends more than its log holds between commits loads through it: a batch that finds
the log full has it publish a chunk that lets go of what was committed, waits for a commit where
one can free room, and a commit is due at once; a batch that waits takes the room before any
batch that began to wait after it, so no partition starves while others checkpoint. A load whose
partitions' frames sent and not yet committed with a receipt, beside one commit's frames, take at
most three quarters of `log_bytes` loads through it, however late its receipts arrive; the rest
of the log is kept for gathering chunks, for commits and for what ends a chunk. A chunk holds at
most an eighth of `log_bytes`, but where it holds a single batch frame or the frames gathered out
of a single chunk, and beside that the seals and frame of the commit that ends it: batches sent
while a commit's frame follows its seals wait for that commit, and go to the chunks after it. So
a chunk's frames can always be gathered into a later one. A load fails `log_bytes_exceeded` where the frames its partitions have sent and not yet
checkpointed fill its log, as a source that never checkpoints does: give such a load a larger
`log_bytes`, at least four thirds of what its partitions send between checkpoints.

A log never holds more than `log_bytes`, but where a commit is larger than the room kept for one,
an eighth of `log_bytes` or the largest commit before it, and the carry's eighth: it is written
all the same, seals and commit in one chunk, and the log holds more by at most that commit's
frames until its receipt or the next relief frees what it settled. Batches wait or are refused
meanwhile.
