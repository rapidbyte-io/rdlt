# rdlt-certify

The standalone connector certifier: point it at any connector
executable and it certifies the binary against the same conformance
clauses first-party connectors answer to. "Certified" means exactly
one thing here — the connector passes those clauses, out of process,
over the real wire.

```console
$ rdlt-certify --role source path/to/my-connector
$ rdlt-certify --role destination --config config.json io.example.warehouse
$ rdlt-certify --role destination --config config.json --kill-matrix io.example.warehouse
$ rdlt-certify --explain
```

The target is a path to a connector binary, or a connector id resolved
on `PATH` by the runtime's naming convention (the last `.`-segment of
the id, prefixed `rdlt-connector-`). `--config` names a JSON file with
the connector's own config document; without it the document is `{}`.
The config is carried to the connector and never printed — reports name
clauses, not config bytes.

## The report

One verdict line per clause on stdout, each id followed by its fixed
title:

```text
PASS P3 (spec/handshake identity agreement)
FAIL P5 (one Arrow batch per read frame): an arrow read frame carried 2 record batches — …
SKIP K-D4 (SIGKILL between write and publish, then exactly-once on re-run): no table probe supplied — …
```

`--report json` emits the same entries as one JSON document on stdout
(diagnostics stay on stderr). `--explain` prints every clause id,
title, and definition — the same table below — and exits 0.

Exit codes:

- **0** — every clause passed (skips are honest non-verdicts, not
  failures — with one deliberate exception, enforced in the LIBRARY so
  embedders gating on `Report::passed` share it: an unacknowledged
  skipped SOURCE clause (S1/S2/S4) folds as a failure and refuses with
  exit 1 unless `--accept-skips` (the library's `accept_skips`)
  acknowledges it, because a source that never checkpoints looks
  identical to one that merely forgot resume; kill-matrix and
  destination probe skips never refuse);
- **1** — at least one clause failed; the report lists each failure;
- **2** — the run was refused before certification could judge
  anything: the target did not resolve or spawn (the runtime's own
  error spelling on stderr), the `--config` file was unreadable or not
  JSON, or the arguments were invalid.

Every clause is timeout-bounded: a connector that stalls FAILS the
clause — the certifier never hangs.

### Destination read-back: `--probe-cmd`

The destination read-back clauses (D1–D6, D8) and the kill matrix's
convergence judgment count reader-visible rows, which the wire cannot
do — that needs a per-destination table probe. `--probe-cmd '<sh
line>'` supplies one as a shell line: `{{table}}` is substituted (only
table names matching `[A-Za-z0-9_]+` are ever spliced in), the line
runs via `sh -c` with a bounded timeout, and its stdout must be one
number — the reader-visible row count.

```console
$ rdlt-certify --role destination --config config.json \
    --probe-cmd 'psql "$DSN" -Atc "SELECT count(*) FROM {{table}}"' \
    io.example.warehouse
```

**The command line may carry credentials.** It is never echoed: no
report line or failure message repeats it — a probe failure names what
happened (a non-zero exit, unparseable stdout, a timeout) and fails
the clause under evaluation. Without `--probe-cmd` the read-back
clauses render Skip with the reason. The flag is destination-only:
combining it with `--role source` is a usage error, and a template
without the `{{table}}` placeholder is refused at argument time. The
library API (`certify_destination`, `kill_matrix_destination`) takes a
`TableProbe` directly.

**Single-writer stores need a copy-then-count probe.** A store that
admits one writer (duckdb) refuses EVERY other open while the spawned
connector holds it — a read-only open included — so a probe line that
opens the live store fails and aborts the read-back clauses. Probe a
COPY instead: copy the store file plus its WAL sidecar into a scratch
directory and count in the copy, e.g.

```console
$ rdlt-certify --role destination --config config.json \
    --probe-cmd 'cp store.duckdb "$T/s.duckdb"; cp store.duckdb.wal "$T/s.duckdb.wal" 2>/dev/null; \
duckdb -readonly "$T/s.duckdb" -noheader -csv -c "SELECT count(*) FROM \"{{table}}\""' \
    io.rapidbyte.duckdb
```

(with `T` a scratch directory). Every probe lands at a reply boundary
where the connector is idle, so the copy is consistent — the same
discipline this workspace's own duckdb suite uses.

## The clauses

The same vocabulary `--explain` prints, verbatim:

| Id | Title | Definition |
|----|-------|------------|
| S1 | checkpoint resume law | For every checkpoint the source emits, one full read equals the rows covered by that checkpoint followed by a resumed read since it — resuming from any checkpoint loses nothing and repeats nothing. |
| S2 | checkpoint coverage | A stream must checkpoint at least once during a read. A stream that never checkpoints cannot be certified for resume and fails by name — unless it declares no cursor field at all: an honestly-declared snapshot stream is skipped with the reason, never vacuously passed. Certification refuses a skipped source clause unless the acknowledgment is given (certify_source's accept_skips; the CLI's --accept-skips). |
| S4 | prompt cancellation | When the record channel closes mid-read, the source stops promptly and returns Ok — never an error, never a hang. |
| D1 | staging invisibility | Rows written into a load session but not yet committed are invisible to readers of the table. |
| D2 | atomic state-with-data commit | A commit persists the pipeline's state document atomically with the data: reading state back afterward returns exactly the committed cursor. |
| D3 | idempotent commit receipts | Re-committing the same (load_id, commit_seq) returns the prior receipt and re-publishes nothing. |
| D4 | dead-predecessor staging teardown | A new session makes a dead predecessor's staged rows invisible — only the new session's rows ever publish. |
| D5 | idempotent ensure_table | Ensuring a table that already exists succeeds and disturbs nothing — ensure_table can be repeated freely. |
| D6 | no state for fresh pipelines | A pipeline that has never committed reads back no state. |
| D8 | merge upsert by _rdlt_id | Under the merge write mode, rows sharing an _rdlt_id upsert rather than duplicate. Asserted only for destinations that declare the merge capability; otherwise it is skipped with the reason, never vacuously passed. |
| P1 | one handshake line on stdout | The connector's first stdout line is the handshake line advertising its socket and protocol range, and stdout carries EXACTLY that one line — nothing may follow it. Stdout is the machine channel; logs belong on stderr. |
| P2 | typed unknown-config-field refusal | A config document containing an unknown field must be refused at the handshake with a typed refusal carrying its classification — never accepted, and never surfaced as a dial failure or a dead stream. |
| P3 | spec/handshake identity agreement | The identity the handshake reports (connector id and version) must agree with the connector's own spec document — and, when the operator named a connector id, with that id. Any skew between the two fails the clause. |
| P4 | complete pre-handshake Spec reply | The Spec call must answer with a non-empty name, a non-empty version and a JSON-object config schema — with no config supplied at all. |
| P5 | one Arrow batch per read frame | Every arrow_ipc read frame must decode as one Arrow IPC stream carrying exactly one record batch, judged on the wire bytes. Frames that are not arrow are exempt; a source that serves no arrow frames at all passes vacuously. |
| P6 | typed terminal error frame | A read refusal must arrive as a terminal error frame carrying a real classification value and bare cause text — never a clean end of stream, and never a client-side rendering baked into the message. |
| P7 | tolerated state-format version map | The handshake's state_format_versions field must decode as a map from state kind to format version — which protocol decoding itself enforces, so an undecodable map fails the whole handshake. An empty map is the v0 posture; a populated one is tolerated and threaded onward, never negotiated. |
| P8 | one-session-per-process ceiling | While one session is held, a second concurrent OpenSession on the live socket must be refused with the FailedPrecondition status — v0 allows exactly one session per connector process. |
| P9 | abandoned-session reclaim | A session abandoned without Close (its request stream just ends) must be reclaimed: within 10 seconds a fresh session on the same pipeline must open. |
| P10 | Backend-direct order book | The raw session choreography holds frame by frame, driven without any client-side manners in between: every request is answered with its own reply tag, a write to a never-ensured table is refused with a typed error frame, a publish for a load whose receipt already exists is refused or answers that same receipt (never a fresh mint), and no reply may arrive after close is answered. |
| P11 | one Arrow batch per write frame | Every write frame's arrow_ipc payload must be one Arrow IPC stream carrying exactly one record batch — a multi-batch write frame must be refused with a typed error frame, never accepted. Induced with a two-batch frame on the live socket. |
| P12 | write-side error frames carry cause text | A session refusal must arrive as a typed error frame carrying a real classification value and bare cause text — never a client-side rendering baked into the message. Judged at the induced out-of-order write and already-receipted publish refusals. |
| K-S1 | SIGKILL before the first read, then typed error not hang | The connector is SIGKILLed after the handshake, before any read is opened. The next call on the dead wire must fail with a typed error within 10 seconds — a dead connector must fail the wire, never hang it. |
| K-S2 | SIGKILL mid-read, then typed error not hang | The connector is SIGKILLed mid-read, after the first frame. The stream must surface a typed error within 10 seconds; a stream that was already fully in flight ends cleanly and earns an honest Skip naming the fixture. |
| K-S3 | SIGKILL after the first checkpoint, then typed error not hang | The connector is SIGKILLed after the first checkpoint frame — the resume boundary. Same promise as K-S2: a typed error within 10 seconds, never a hang. |
| K-D1 | SIGKILL after open, then exactly-once on re-run | The connector is SIGKILLed right after the session opens. The dead wire must fail with a typed error within 10 seconds, and a fresh process re-driving the same load under a sibling pipeline scope must leave the table holding exactly the fixture rows — nothing lost, nothing doubled. |
| K-D2 | SIGKILL after ensure, then exactly-once on re-run | The connector is SIGKILLed right after the table is ensured. Same two promises as K-D1: a typed error on the dead wire, then exactly-once convergence on the re-run. |
| K-D3 | SIGKILL after an accepted write, then exactly-once on re-run | The connector is SIGKILLed right after a write is accepted, before any commit. Same two promises as K-D1: a typed error on the dead wire, then exactly-once convergence on the re-run. |
| K-D4 | SIGKILL between write and publish, then exactly-once on re-run | The connector is SIGKILLed after the receipt query is answered but before publish is sent — poised at the commit point. Same two promises as K-D1: a typed error on the dead wire, then exactly-once convergence on the re-run. |
| K-D5 | SIGKILL after publish, then exactly-once on re-run | The connector is SIGKILLed right after publish is answered. Same two promises as K-D1: a typed error on the dead wire, then a re-run that converges on exactly the fixture rows — a kill that let a committed row land twice breaks the count as surely as one that lost a row. |
| K-D6 | SIGKILL after close, then receipt-durable no-op re-run | The connector is SIGKILLed after its session closed cleanly. A re-run on the SAME pipeline must find the dead process's receipt still durable, replay instead of re-publishing, and leave the row count unmoved. This covers cleanly-closed receipt durability only — not crash-resume of a still-open session on the same pipeline. |

## The kill matrix (`--kill-matrix`)

The K-clauses SIGKILL a live connector process at every message
boundary and hold the wire to two promises: a typed error within 10
seconds of the kill (a dead connector must fail the wire, never hang
it), and — for the destination arms — exactly-once convergence proven
by re-run: a fresh process drives the same load to completion and the
table must hold exactly the fixture rows.

### The sibling-scope requirement

The K-D1–K-D5 re-runs ride a **sibling pipeline scope**
(`{pipeline}-r`), not the killed pipeline, and convergence is judged
on the **data**: the shared table's exact row count. For a connector
author this is a real requirement — your destination may hold a
durable per-pipeline session claim (a lease, a lock file) that a
SIGKILLed process can never release, and the matrix does not wait such
a claim out. Re-driving the load under a sibling scope must therefore
converge on the data: a different pipeline scope must be able to reach
the same table and publish exactly-once without waiting for the dead
scope's claim to expire. A destination is free — correct, even — to
keep refusing the *killed* pipeline until its claim times out.

### Standing limitation

Same-pipeline crash-resume for lease-holding connectors is
**uncertified**: no clause proves that the killed pipeline itself can
resume before its claim's TTL expires. K-D6 covers cleanly-closed
receipt durability only — the process died *after* its session closed,
so its claim was released and the same pipeline's re-run must find the
receipt and replay as a no-op.

### Operational note

Two matrix runs against one output root within a lease TTL will
collide: the first run's killed processes leave claims standing on the
arm pipelines, and the second run's sessions meet them. Run each
matrix in a fresh output root.
