# ADR 0051: A write-ahead log on object storage

Status: accepted, 2026-10-04.

## Context

ADR 0029 made a load keep a write-ahead log where its source cannot read again what it
acknowledged, and ADR 0045 shaped the log's store as an object store is, with a contract every
store is held to by one suite. The only store kept logs on the engine's own disk: a log lost with
its machine lost the rows its source had been told were committed, and a pipeline could not move
to another machine and replay what the first left.

## Decision

### `ObjectStoreWal`: the contract as object operations

- `rdlt-engine`'s `object-store` feature adds `ObjectStoreWal`, a `WalStore` over any store of
  the `object_store` crate that takes objects and uploads in parts (`WalObjects`). It holds no
  HTTP code; a store is given to it. Every operation is an object operation, with no lock, no
  rename and no operation over several objects.
- Beneath a prefix of one or more segments of `[A-Za-z0-9._-]`, at most 512 bytes, none empty,
  `.` or `..` (`wal_prefix_invalid`): `store` names the store; `probe/` holds the probe's markers;
  each pipeline's objects lie beneath `p.<pipeline>/`, its open logs' marks in `open/<load>` and
  each load's chunks in `logs/<load>/`, a chunk's head `<number:08>.wal`, the bodies of chunks
  uploaded in parts `<number:08>.<token:032x>.body`. A name the store never writes is refused,
  never read (`wal_stray`).
- A log is opened by creating its mark where none of the name exists, holding a random token; a
  load whose `logs/<load>/` holds anything is refused, as one whose removal left something.
  Amended 2026-10-07: the mark is created by one attempt. One whose outcome is unknown is never
  made again, since it may land after the log was removed and open it again, with chunks whose
  creates landed late too, naming chunks the removal deleted, which no replay can read. Its
  token is read back: a mark bearing it is the log's, and where none is there the open fails as
  unavailable, retryably, and the run tries again under another load. A mark that lands later
  opens a log no chunk was published in, which the next replay fences and removes.
- A chunk is staged in memory up to a part, 7 MiB by default. A chunk of a part or less is
  published by one create of its head; a longer one is uploaded in parts as it is staged, each
  part as it fills, the last with the publish, and its head is a 38-byte reference naming the
  body's token and length, with a checksum. Either way the one create of the head where its name
  is free is the durability point: a body no head names is never read.
- A publish creates the head, then asks whether the log's mark is still there, deleting the head
  and its body where it is not and answering `NotFound`. A removal deletes the mark first, then
  lists and deletes the log's objects: a publish that asked before the mark went is listed
  after, and one that asked after deletes itself. A publish that finds the name taken deletes
  its body and answers `AlreadyExists`.
- Every create marks its object with a random token in its metadata (`rdlt-token`). A create
  answered as taken is another's, unless an attempt of it failed with no answer known: then the
  object's token is read back, and one bearing its own is its own, made by the attempt whose
  answer was lost; a name found missing is created again within the attempts. Two creates of
  the same bytes are told apart, so two publishes of one chunk never both succeed. On a create,
  a conflict, a failed precondition or no change is the name taken; on any other request a
  conflict is tried again.
- `remove_staged` drops the stagings this process holds; nothing is staged in the store, so none
  of another process's can be deleted, and fencing never relied on it: a fence takes the next
  number by creating it. A staging dropped with its upload begun has the upload aborted by the
  store's next staging or removal of a log; the bucket's rule for unfinished uploads ends those
  of a process that died.
- Leftovers are the loads whose `logs/` directory holds anything and whose mark does not exist,
  listed in that order, so a log opened between the two listings is never taken for one.
- Deleting a chunk deletes its head, then the body its head names, listing nothing: a chunk is
  gone once its head is, and what deleting it costs is its own, whatever the log's age. A log's
  chunks are one listing of its directory, which holds its live chunks alone.
- A listing is read a page at a time, each page within an attempt's deadline, and holds at most
  65,536 objects: a log's directory holding more is unreadable (`wal_unreadable`), as a store
  that keeps what it was told it deleted would make it. Each chunk's end names the live chunks
  before it, two bytes at least each, so n live chunks hold n(n-1) bytes of ends and a log keeps
  at most about the square root of its `log_bytes` in chunks: 65,536 at the default 4 GiB, with
  a body beside each chunk longer than a part. A `log_bytes` far past that can let a log of many
  small chunks pass the limit, as the operator documentation says.
- A read is a ranged GET whose body is read no further than the length asked
  (`InvalidData` beyond it); a range ending past `i64::MAX` asks for everything from its offset,
  which every S3 server takes; one starting at or past the object's end answers empty after a
  look at the object. A chunk's kind, whole or in parts, is remembered once read, for at most
  4,096 chunks.
- The store tells the engine half of what its parts hold, 10,000 parts' worth, as its largest
  chunk (`WalStore::chunk_bytes`), and the engine bounds a load's log by the lower of that and
  `GrowthLimits::log_bytes`. The half left holds the frames the engine's bound does not count,
  the header, seals, commit and end; a staging past the parts' whole is refused
  (`wal_storage_unsupported`).
- What a staging holds in memory, a part, is the store's `WalStore::staging_bytes`, which the
  engine reserves from the memory budget's share for logs as a load's log starts, beside the
  256 KiB a carry copies through. One that leaves the share too little for a commit's frame
  recording a full share of cursors, counted twice over for its encoding, and 4 KiB of frame
  head is refused (`wal_staging_exceeds_budget`; ADR 0045 states the bound), so the default part
  is 7 MiB, which the default budget holds: at the default 256 MiB budget a part past 8,122,368
  bytes is refused.

### Every request bounded

- Each request is tried at most five times by default; each attempt within 30 seconds and a
  second more for each MiB it moves, counting no more than a chunk holds; a random wait up to
  100 ms before the first retry, each up to twice the last, at most 5 seconds. Waits and draws
  come from a `Clock` given to the store (`SystemClock` in production), since the `Env` holds
  the store; the simulation gives a seeded one.
- A failure whose causes hold a `StoreRefusal`, which the store's client gives a request that
  no attempt can mend, is `wal_storage_refused`, before any other class. An answer the contract
  names is reported as it is; a refusal of the credentials is `wal_storage_denied`; a request the
  store does not implement, or a store that does not do what a log needs,
  `wal_storage_unsupported`; all three are final. Anything else is tried again, and a
  request every attempt of which failed or ran past its deadline is `wal_storage_unavailable`,
  retryable. Uploads given up are aborted within the same bounds.

### The probe

Opening a log store probes it:

- three rounds of four creates of a fresh name racing, of which exactly one must be taken in
  each round;
- a marker created, its token read back from its metadata, and listed;
- an upload in parts made and read back;
- every object it made deleted, then each found missing, not refused.

A store that fails is refused (`wal_storage_unsupported`, `wal_storage_denied` or
`wal_storage_refused`), what the probe made deleted. A probe is a sample: it refuses a store that
takes two creates of one name while it looks, keeps no metadata, or lists no fresh object, and
cannot show that a store never does. That a store takes only one of creates racing, every time,
is what its operator states of it.

### `rdlt-log-store`: choosing the store

- `LogStoreConfig` is `{"local": {"base": ...}}` or `{"s3": {...}}`: bucket, prefix, region,
  endpoint, path style, part length, and credentials. It opens a `LocalWal` or, for S3, builds
  `object_store`'s S3 client and opens an `ObjectStoreWal` on it, which probes.
- Each credential, the access key id, the secret key and a session token, is one secret reference
  (`SecretReference::parse`), resolved only by the resolver the operator gives (ADR 0043),
  before anything is asked of the store, and again every five minutes, so a rotated secret is
  taken up. A literal credential is refused. Errors name the field and never its value
  (`rdlt_connector::parse_config`), and no credential is ever shown.
- The client reaches the store over TLS 1.2 or 1.3, checked against the system's trusted roots,
  HTTP/1.1, with no redirect followed and no proxy; plain HTTP only to a loopback IP address,
  never to a name. It tries each request once: the log tries it again on its clock. S3's
  conditional put is on; no credential comes from the environment, a profile or instance
  metadata.
- The client marks as a `StoreRefusal` a failed TLS handshake, and a status that says the
  request itself is wrong: 400, but for a timeout or an expired token, 405, 411, 413, 414, 431,
  501 and 505. Any other status `object_store` does not name is tried again.
- A refresh of the credentials that fails keeps those held, and resolves them again at the next
  ask; only the first resolution, before anything is asked of the store, is final.
- Without path style, a custom endpoint's host is the bucket's (`https://<bucket>.<host>`), as
  S3's virtual hosts are; an endpoint at an IP address names no bucket's host and must use path
  style.
- A bucket name is 3 to 63 of `[a-z0-9.-]`, beginning and ending with a letter or digit, no
  `..`, no address; a region `[a-z0-9-]`; a part from 5 MiB to 5 GiB.

### Tests

- The contract's suite moves to `rdlt_engine::conformance` (the `conformance` feature) and runs
  against the store in memory, in parts, and under failed, slow, hung, raced, answerless and
  late requests and failed deletions, through `rdlt_testkit::objects::Faulty`; loads land every row
  once under each.
- Half of the simulation's seeds that keep logs keep them in an `ObjectStoreWal` over a store in
  memory whose requests fail, stall, hang, race and lose their answers, whose creates and
  deletions may land after their client gave up on them, with one to four attempts a request,
  on a clock whose waits are drawn longer. Half of the worlds that keep logs and checkpoint
  give a log 128 KiB to 1 MiB, less than many of their loads send, so batches wait for commits.
- Tests reach a store served over TLS by a test authority the client trusts, at the bucket's
  host, and one whose certificate names another host, refused.
- Tests in containers run the suite on RustFS and MinIO, each image named by its digest, which
  opening probes first, and kill a pipeline in a process of its own at each step of a commit,
  then run it again: every row lands once and no log is left. Their ports are published on the
  loopback address alone. The default test profile filters
  them out; `just containers` and a Linux CI job run them.

## Rulings

- **Listings must be consistent.** S3, GCS, Azure Blob and MinIO list an object once it is
  written. A store built on listing cannot make a missed open mark safe: two attempts would
  miss each other's logs. The probe refuses a store whose listing misses a fresh object, and a
  log's chunks are listed once, trusted as listed: completing a stale listing cost a mark kept
  for every chunk ever deleted, which no live chunk bounded. Cost if wrong: an eventually
  consistent store that passes the probe could let concurrent attempts both run, or a fence miss
  the newest chunk.
- **A store keeps metadata.** A create's token is its object's metadata, so a lost answer is
  told apart from another's create of the same bytes. The probe refuses a store that keeps none.
  Cost: a store that drops `x-amz-meta-*` headers cannot keep logs.
- **Parts, not one put of a whole chunk.** `object_store` takes a put's whole body in memory, and
  a chunk can be as large as the log, 4 GiB by default, which the memory budget does not hold.
  Bounding the log by a small chunk instead made fast sources fail `log_bytes_exceeded`. Cost: a
  chunk past a part costs an upload's beginning, its parts and its completion beside the head; an
  interrupted upload leaves parts no listing shows until the bucket's rule for unfinished
  uploads ends them.
- **No read cache.** A scan reads a batch frame 64 KiB at a time, one GET each. A cache across
  calls could serve bytes of a chunk another process deleted. Cost: replaying a 256 MiB chunk
  takes about 4,000 GETs, only after a crash.
- **One request more a commit.** A staging asks whether its log is open rather than trusting what
  this process opened. Cost: a HEAD a commit.
- **Statuses are told apart by the client.** `object_store` keeps HTTP statuses private, so the
  S3 client marks those that say the request is wrong, and the log classes the mark wherever it
  lies among an error's causes. A 400 is read for its code, at most 4 KiB of it: S3 answers a
  timeout and an expired token with 400 too, which another attempt or a refresh mends. Cost: a
  store answering a passing fault with another of the listed statuses fails its run at once,
  retryable by the next run alone.
- **S3 alone.** GCS and Azure would need builders, credentials and emulators of their own; the
  engine's store takes them unchanged when they come.
- **MinIO's image is Pigsty's build.** MinIO publishes no image any more; the tests run a build
  of its source, fixed by digest.
- **No CA file.** A private endpoint's authority is trusted through the system's roots.
- **What a store stages is charged to the budget.** A part is held in memory as it fills; it is
  reserved once, from the share for logs, as a load's log starts, and a part that leaves that
  share too little for a commit recording a full share of cursors, counted twice over for its
  encoding, and 4 KiB of frame head is refused. Cost: a load keeps a part reserved while its log
  is open, written to or not, and the default part is 7 MiB rather than 8.

## Consequences

- An embedder chooses a pipeline's log store in configuration; the local store is unchanged.
- A log on S3 costs, each commit, a HEAD of its mark, a PUT of its chunk and a HEAD of the mark
  again; a chunk past the configured part (7 MiB by default) adds a multipart upload; deleting a chunk no longer needed is a
  DELETE, two for one uploaded in parts, and a GET of its head where the process never read it. Opening a log is a LIST and a PUT, removing it a DELETE,
  a LIST and a DELETE an object; every commit lists the open marks once, for its horizon. What a
  log costs is bounded by its live chunks, never by how many it held. A commit waits for its PUT
  to be acknowledged, tens of milliseconds on S3.
- A bucket needs a rule ending unfinished uploads, and credentials that may list the bucket, or
  S3 answers a missing object as forbidden and the probe refuses it.
- `rdlt_connector::parse_config` and `SecretReference::parse` are public.
