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
  each load's chunks in `logs/<load>/`, a chunk's head `<number:08>.wal` and the bodies of chunks
  uploaded in parts `<number:08>.<token:032x>.body`. A name the store never writes is refused,
  never read (`wal_stray`).
- A log is opened by creating its mark where none of the name exists, holding a random token; a
  load whose `logs/<load>/` holds anything is refused, as one whose removal left something.
- A chunk is staged in memory up to a part, 8 MiB by default. A chunk of a part or less is
  published by one create of its head; a longer one is uploaded in parts as it is staged, each
  part as it fills, the last with the publish, and its head is a 38-byte reference naming the
  body's token and length, with a checksum. Either way the one create of the head where its name
  is free is the durability point: a body no head names is never read.
- A publish creates the head, then asks whether the log's mark is still there, deleting the head
  and its body where it is not and answering `NotFound`. A removal deletes the mark first, then
  lists and deletes the log's objects: a publish that asked before the mark went is listed
  after, and one that asked after deletes itself. A publish that finds the name taken deletes
  its body and answers `AlreadyExists`.
- A create answered as taken reads the object back and compares it with what it wrote: one that
  holds the same bytes is its own, made by an attempt whose answer was lost; a name answered taken
  and found missing, as S3 answers two creates racing, is created again within the attempts.
- `remove_staged` drops the stagings this process holds; nothing is staged in the store, so none
  of another process's can be deleted, and fencing never relied on it: a fence takes the next
  number by creating it.
- Leftovers are the loads whose `logs/` directory holds anything and whose mark does not exist,
  listed in that order, so a log opened between the two listings is never taken for one.
- A chunk listing is completed by asking for the number after the highest listed until one is
  missing, so a listing that misses the newest chunk, just published, still finds it.
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

### Every request bounded

- Each request is tried at most five times by default; each attempt within 30 seconds and a
  second more for each MiB it moves, counting no more than a chunk holds; a random wait up to
  100 ms before the first retry, each up to twice the last, at most 5 seconds. Waits and draws
  come from a `Clock` given to the store (`SystemClock` in production), since the `Env` holds
  the store; the simulation gives a seeded one.
- An answer the contract names is reported as it is; a refusal of the credentials is
  `wal_storage_denied`; a request the store does not implement, or a store that does not do what
  a log needs, `wal_storage_unsupported`; both are final. Anything else is tried again, and a
  request every attempt of which failed or ran past its deadline is `wal_storage_unavailable`,
  retryable. Uploads given up are aborted within the same bounds.

### The probe

Opening a log store probes it: a marker created, then created again, which must be refused; the
marker listed, which must show it; an upload in parts made and read back; then both deleted, and
the marker found missing, not refused. A store that fails is refused (`wal_storage_unsupported`,
or `wal_storage_denied`), its markers deleted, so no log runs unfenced on a store that takes a
second create of one name, nor on one whose listing misses a fresh object.

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
- A bucket name is 3 to 63 of `[a-z0-9.-]`, beginning and ending with a letter or digit, no
  `..`, no address; a region `[a-z0-9-]`; a part from 5 MiB to 5 GiB.

### Tests

- The contract's suite moves to `rdlt_engine::conformance` (the `conformance` feature) and runs
  against the store in memory, in parts, and under failed, slow, hung, raced and answerless
  requests and failed deletions, through `rdlt_testkit::objects::Faulty`; loads land every row
  once under each.
- Half of the simulation's seeds that keep logs keep them in an `ObjectStoreWal` over a store in
  memory whose requests fail, stall, hang, race, lose their answers and list a log's chunks
  without the newest, with one to four attempts a request.
- Tests in containers run the suite on RustFS and MinIO, each image named by its digest, which
  opening probes first, and kill a pipeline in a process of its own at each step of a commit,
  then run it again: every row lands once and no log is left. The default test profile filters
  them out; `just containers` and a Linux CI job run them.

## Rulings

- **Listings must be consistent.** S3, GCS, Azure Blob and MinIO list an object once it is
  written. A store built on listing cannot make a missed open mark safe: two attempts would
  miss each other's logs. The probe refuses a store whose listing misses a fresh object; a chunk
  listing that misses the newest chunk is completed. Cost if wrong: an eventually consistent
  store that passes the probe could let concurrent attempts both run.
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
- **Errors are told apart by kind only.** `object_store` keeps HTTP statuses private, so a status
  it does not name, a permanent 400 among them, is tried again before it is reported as
  unavailable.
- **S3 alone.** GCS and Azure would need builders, credentials and emulators of their own; the
  engine's store takes them unchanged when they come.
- **MinIO's image is Pigsty's build.** MinIO publishes no image any more; the tests run a build
  of its source, fixed by digest.
- **No CA file.** A private endpoint's authority is trusted through the system's roots.

## Consequences

- An embedder chooses a pipeline's log store in configuration; the local store is unchanged.
- A log on S3 costs, each commit, a HEAD of its mark, a PUT of its chunk and a HEAD of the mark
  again; a chunk past 8 MiB adds a multipart upload; deleting a chunk no longer needed is a LIST
  and a DELETE. Opening a log is a LIST and a PUT, removing it a DELETE, a LIST and a DELETE an
  object; every commit lists the open marks once, for its horizon. A commit waits for its PUT
  to be acknowledged, tens of milliseconds on S3.
- A bucket needs a rule ending unfinished uploads, and credentials that may list the bucket, or
  S3 answers a missing object as forbidden and the probe refuses it.
- `rdlt_connector::parse_config` and `SecretReference::parse` are public.

ADR 0045 is amended by this one: an object-store backend exists.
