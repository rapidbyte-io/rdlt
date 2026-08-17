# rdlt-certify

The wire-side connector certifier. It spawns a connector binary, drives
it over the real wire, and certifies it clause by clause: the same
conformance clauses first-party connectors answer to, judged out of
process. "Certified" means exactly one thing here — the report is
all-Pass.

It re-derives no in-process suite: the source (S) and destination (D)
clauses reuse the testkit's conformance suites against a managed
adapter over the wire; the protocol (P) and kill (K) clauses exist only
out of process and are probed here on raw frames and raw sessions. The
report's spellings are the CLI's stdout contract.

## Command line

```console
$ rdlt-certify --role source path/to/my-connector
$ rdlt-certify --role destination --config config.json io.example.warehouse
$ rdlt-certify --role destination --config config.json --kill-matrix io.example.warehouse
$ rdlt-certify --explain
```

```text
rdlt-certify --role source|destination [--config <file>] [--probe-cmd '<sh line>']
             [--kill-matrix] [--accept-skips <stream[,stream]>] [--report text|json]
             <target>
rdlt-certify --explain
```

- `<target>` is a path to a connector binary (anything containing a path
  separator, or naming an existing file), or a connector id resolved on
  `PATH` by the runtime's naming convention (the last `.`-segment of the
  id, prefixed `rdlt-connector-`). A path target learns its identity
  from the connector's own Spec reply; an id target is
  identity-verified against that id at handshake.
- `--config <file>` names a JSON file holding the connector's own config
  document; without it the document is `{}`. The document is carried to
  the connector and never printed — reports name clauses, not config
  bytes.
- `--report text|json` picks the stdout format (default `text`).
- `--explain` prints every clause id, title and definition (the table
  below) and exits 0.

Every clause is timeout-bounded (30 s): a connector that stalls fails
the clause; the certifier never hangs.

### The report

Stdout carries the report and nothing else; diagnostics go to stderr.
In `text` form it is one verdict line per clause, each id followed by
its fixed title:

```text
PASS P3 (spec/handshake identity agreement)
FAIL P5 (one Arrow batch per read frame): an arrow read frame carried 2 record batches — …
SKIP K-D4 (SIGKILL between write and publish, then exactly-once on re-run): no table probe supplied — …
NOT-REACHED D1 (staging invisibility): the suite reported no verdict for this clause and never concluded its checks — …
```

`SKIP` is an honest non-verdict: the clause was not exercised, and the
line says why. `NOT-REACHED` is the fourth spelling: the suite that
declared the clause died before its checks ran. That is neither a pass
(nothing was proven) nor an honest skip (nobody chose not to exercise
it), so it refuses certification like a failure — a report can exit 1
with `NOT-REACHED` lines and no `FAIL` line at all.

`--report json` emits the same entries in the same order as one JSON
document, `{"entries": [{"clause": "P3", "verdict": "Pass"}, …]}`, with
`Fail`, `Skip` and `NotReached` carrying their reason as the payload.

### Exit codes

- `0` — every clause passed. Skips do not refuse, with one deliberate
  exception: a skipped source clause (S1, S2, S4) refuses unless its
  stream is named in `--accept-skips <stream[,stream]>`. A source that
  never checkpoints looks identical to one that merely forgot resume,
  so the acknowledgment is by stream name; a blanket form would fold a
  regressed co-stream green beside a genuine snapshot stream. The rule
  lives in the library (`clause::s::certify` takes the same stream
  list), so an embedder gating on `Report::passed` shares it. Kill
  matrix skips and destination probe skips never refuse.
- `1` — at least one clause failed or was never reached; the report
  names each. Tooling acting on refusals must read both `FAIL` and
  `NOT-REACHED` — grepping `^FAIL` alone misses a run whose suite died
  mid-flight.
- `2` — the run was refused before certification could judge anything:
  the target did not resolve or spawn (the runtime's own error text on
  stderr), the `--config` file was unreadable or not JSON, or the
  arguments were invalid.

### Destination read-back: `--probe-cmd`

The destination read-back clauses (D1–D6, D8) and the kill matrix's
convergence judgment count reader-visible rows, which the wire cannot
do. `--probe-cmd '<sh line>'` supplies a per-destination table probe:
`{{table}}` is substituted (only table names matching `[A-Za-z0-9_]+`
are ever spliced in), the line runs via `sh -c` with a bounded timeout
(20 s, 1 MiB of stdout), and its stdout must be one number — the
reader-visible row count.

```console
$ rdlt-certify --role destination --config config.json \
    --probe-cmd 'psql "$DSN" -Atc "SELECT count(*) FROM {{table}}"' \
    io.example.warehouse
```

The command line may carry credentials, so it is never echoed: no
report line or failure message repeats it — a probe failure names what
happened (a non-zero exit, unparseable stdout, a timeout) and fails the
clause under evaluation. Without `--probe-cmd` the read-back clauses
render `SKIP` with the reason. The flag is destination-only: combining
it with `--role source` is a usage error, and a template without the
`{{table}}` placeholder is refused at argument time. The library API
takes a `TableProbe` directly.

Single-writer stores need a copy-then-count probe. A store that admits
one writer (duckdb) refuses every other open while the spawned
connector holds it — a read-only open included — so a probe line that
opens the live store fails and aborts the read-back clauses. Copy the
store file plus its WAL sidecar into a scratch directory and count in
the copy:

```console
$ rdlt-certify --role destination --config config.json \
    --probe-cmd 'cp store.duckdb "$T/s.duckdb"; cp store.duckdb.wal "$T/s.duckdb.wal" 2>/dev/null; \
duckdb -readonly "$T/s.duckdb" -noheader -csv -c "SELECT count(*) FROM \"{{table}}\""' \
    io.rapidbyte.duckdb
```

(with `T` a scratch directory). Every probe lands at a reply boundary
where the connector is idle, so the copy is consistent.

### The kill matrix: `--kill-matrix`

The K clauses SIGKILL the live connector process at every message
boundary and hold the wire to two promises: a typed error within 10 s
of the kill (a dead connector must fail the wire, never hang it), and,
for the destination arms, exactly-once convergence proven by re-run — a
fresh process drives the same load to completion and the table must
hold exactly the fixture rows.

The K-D1–K-D5 re-runs ride a sibling pipeline scope (`{pipeline}-r`),
not the killed pipeline, and convergence is judged on the data: the
shared table's exact row count. This is a real requirement on the
connector: a destination may hold a durable per-pipeline session claim
(a lease, a lock file) that a SIGKILLed process can never release, and
the matrix does not wait such a claim out. Re-driving the load under a
sibling scope must reach the same table and publish exactly once
without waiting for the dead scope's claim to expire; the destination
is free — correct, even — to keep refusing the killed pipeline until
its claim times out.

Two limits of the matrix, stated so nobody reads more into a pass:

- Same-pipeline crash-resume for lease-holding connectors is
  uncertified: no clause proves the killed pipeline itself can resume
  before its claim's TTL expires. K-D6 covers cleanly-closed receipt
  durability only — the process died after its session closed, so its
  claim was released and the same pipeline's re-run must find the
  receipt and replay as a no-op.
- Two matrix runs against one output root within a lease TTL collide:
  the first run's killed processes leave claims standing on the arm
  pipelines, and the second run's sessions meet them. Run each matrix
  in a fresh output root.

## The clauses

The vocabulary `--explain` prints, verbatim (this table is pinned to
the library's clause table by test, in both directions):

| Id | Title | Definition |
|----|-------|------------|
| S1 | checkpoint resume law | For every checkpoint the source emits, one full read equals the rows covered by that checkpoint followed by a resumed read since it — resuming from any checkpoint loses nothing and repeats nothing. |
| S2 | checkpoint coverage | A stream must checkpoint at least once during a read. A stream that never checkpoints cannot be certified for resume and fails by name — unless it declares no cursor field at all: an honestly-declared snapshot stream is skipped with the reason, never vacuously passed. Certification refuses a skipped source clause unless its stream is acknowledged by name (the source certifier's accept_skips stream list; the CLI's --accept-skips <stream[,stream]>). |
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
| P13 | unserved-role refusal | Spawning the binary with a --role it does not serve must exit with code 2 before writing any stdout byte — the handshake line is written only for a served role, and the role-less schema probe relies on exactly this refusal signal. The evidence is controlled: exit 2 alone also matches a general usage error, so the served role must answer a handshake line from the same bare argv for the refusal to count — a binary refusing the bare argv for both roles fails the clause. A connector serving both roles has no unserved role, so the clause is skipped with the reason naming both. |
| K-S1 | SIGKILL before the first read, then typed error not hang | The connector is SIGKILLed after the handshake, before any read is opened. The next call on the dead wire must fail with a typed error within 10 seconds — a dead connector must fail the wire, never hang it. |
| K-S2 | SIGKILL mid-read, then typed error not hang | The connector is SIGKILLed mid-read, after the first frame. The stream must surface a typed error within 10 seconds; a stream that was already fully in flight ends cleanly and earns an honest Skip naming the fixture. |
| K-S3 | SIGKILL after the first checkpoint, then typed error not hang | The connector is SIGKILLed after the first checkpoint frame — the resume boundary. Same promise as K-S2: a typed error within 10 seconds, never a hang. |
| K-D1 | SIGKILL after open, then exactly-once on re-run | The connector is SIGKILLed right after the session opens. The dead wire must fail with a typed error within 10 seconds, and a fresh process re-driving the same load under a sibling pipeline scope must leave the table holding exactly the fixture rows — nothing lost, nothing doubled. |
| K-D2 | SIGKILL after ensure, then exactly-once on re-run | The connector is SIGKILLed right after the table is ensured. Same two promises as K-D1: a typed error on the dead wire, then exactly-once convergence on the re-run. |
| K-D3 | SIGKILL after an accepted write, then exactly-once on re-run | The connector is SIGKILLed right after a write is accepted, before any commit. Same two promises as K-D1: a typed error on the dead wire, then exactly-once convergence on the re-run. |
| K-D4 | SIGKILL between write and publish, then exactly-once on re-run | The connector is SIGKILLed after the receipt query is answered but before publish is sent — poised at the commit point. Same two promises as K-D1: a typed error on the dead wire, then exactly-once convergence on the re-run. |
| K-D5 | SIGKILL after publish, then exactly-once on re-run | The connector is SIGKILLed right after publish is answered. Same two promises as K-D1: a typed error on the dead wire, then a re-run that converges on exactly the fixture rows — a kill that let a committed row land twice breaks the count as surely as one that lost a row. |
| K-D6 | SIGKILL after close, then receipt-durable no-op re-run | The connector is SIGKILLed after its session closed cleanly. A re-run on the SAME pipeline must find the dead process's receipt still durable, replay instead of re-publishing, and leave the row count unmoved. This covers cleanly-closed receipt durability only — not crash-resume of a still-open session on the same pipeline. |

## Using the library

The CLI is one consumer of the library; a connector crate's own
certification cell is another. Every name is reached by its module
path — nothing is re-exported at the crate root.

```rust
use rdlt_certify::clause::{d, k, p, s};
use rdlt_certify::report;
use rdlt_certify::target::Target;

// A source: certify the built bin by path, then hold the report to an
// exact all-Pass set (P13 is a dual-role connector's one announced skip).
let report = s::certify(&Target::resolve_path(bin, json!({"path": file})), &[]).await;
report::assert_all_pass(
    &report,
    &["S1", "S2", "S4", "P1", "P2", "P3", "P4", "P5", "P6", "P7"],
    &[("P13", p::SOURCE_DUAL_ROLE_SKIP)],
);

// A destination with a read-back probe (any `TableProbe`), plus the
// kill matrix held to its fixed clause order with fixture advice on a Skip.
let report = d::certify(&target, Some(&probe)).await;
report::assert_all_pass(&report, &["D1", "D2", "D3", "D4", "D5", "D6", /* P… */], &[("D8", d::NO_MERGE_SKIP)]);
let arms = k::destination(&target, Some(&probe)).await;
report::assert_in_order(&arms, &k::DESTINATION, Some("seed a larger fixture"));
```

- `target::Target::resolve_path(path, config)` /
  `target::Target::resolve_id(id, config)` — what to certify; the config
  document is carried, never printed (its `Debug` elides it).
- `clause::s::certify(&target, accept_skips)` → `report::Report`;
  `clause::d::certify(&target, probe)` → `report::Report`. Neither
  hangs nor panics on connector misbehavior — every outcome, including
  "the binary is not a connector at all", is a report entry.
- `clause::k::source(&target)` / `clause::k::destination(&target,
  probe)` → `Vec<report::Entry>`, the kill matrix's arms in
  `clause::k::SOURCE` / `clause::k::DESTINATION` order.
- `report::assert_all_pass(&report, expected, allowed_skips)` — every
  clause in `expected` has an entry and is `Pass`; every `(clause,
  reason)` in `allowed_skips` came out `Skip` with exactly that reason
  (never `Pass`); an empty `allowed_skips` is the strict form. Panics
  with the rendered report otherwise.
- `report::assert_in_order(&entries, expected, fixture_advice)` — the
  kill matrices' stronger shape: the clause sequence is exactly
  `expected`, in order, and every entry is `Pass`. A `Skip` panics
  first and separately, naming `fixture_advice` when given — on a live
  matrix a skip means the fixture failed the cell, not the connector.
- The frozen skip reasons for cells to name: `clause::d::NO_PROBE_SKIP`,
  `clause::d::NO_MERGE_SKIP`, `clause::p::SOURCE_DUAL_ROLE_SKIP`,
  `clause::p::DESTINATION_DUAL_ROLE_SKIP`.
- `probe::Shell::new(line)` — the `--probe-cmd` runner as a
  `TableProbe`, for embedders that want the CLI's shell probe.
- `contract::assert_bin_arg_contract(bin, unserved_roles, version)` and
  `contract::assert_spec_identity(bin, role, id, version)` — the pins
  every served connector bin answers to (the argument contract is
  std-only and runs anywhere; the Spec identity needs a servable bin),
  held once here rather than copied into each connector's smoke suite.

## Module map

```
src/
  lib.rs             crate doc + table of contents; zero re-exports
  report.rs          Clause, CLAUSES, clause_title, Verdict, Entry, Report
                     (render_text/render_json/passed), the S/D fold, the
                     clause timeout, assert_all_pass, assert_in_order
  target.rs          Target + the spawn/resolve substrate (binary resolution,
                     handshake-line probing, run entropy)
  wire.rs            the raw-frame substrate below the client adapters:
                     WireProbe (spawn, attach, raw handshake, raw reads),
                     WireSession (raw destination sessions, judged close),
                     the certifier-authored request frames, refusal_shape
  clock.rs           the clause budget with the probe clock stopped
  probe.rs           Shell — the --probe-cmd TableProbe runner
  contract.rs        assert_bin_arg_contract, assert_spec_identity
  clause/
    mod.rs           table of contents: s, d, p, k
    s.rs             certify() for a source; CLAUSES = S1/S2/S4; the
                     skip-acknowledgment fold
    d.rs             certify() for a destination; CLAUSES = D1–D6/D8;
                     NO_PROBE_SKIP, NO_MERGE_SKIP; the settling adapter
    p.rs             every protocol clause P1–P13 in id order: the
                     role-generic probes, the wire clauses, the session
                     clauses; GENERIC/SELF_PROBED/SOURCE_WIRE/DEST_WIRE/
                     SESSION; both dual-role skips
    k.rs             the SIGKILL matrix: source(), destination(),
                     SOURCE/DESTINATION, the boundaries, convergence
  rogue.rs           test-only misbehaving servers proving each clause can fail
  bin/rdlt-certify.rs the CLI: args, --explain, config loading, target
                     resolution, pre-flight, exit codes
```

## Building and testing

The plain suite (`cargo nextest run -p rdlt-certify`) needs no built
connector: the protocol and kill probes are proven against in-process
rogue servers, and the report, README-table and vocabulary pins run
there.

Two features gate the rest, so no plain workspace command depends on
binaries it does not build:

- `spawn-bins` — the certification cells that spawn the real reference
  connector bin. With `RDLT_BUILD_CONNECTOR_BINS=1` the helper builds
  the bin via cargo before spawning; without it a missing bin fails
  loudly.
- `bin` — the `rdlt-certify` CLI (clap plus a tokio runtime entry
  point), behind `required-features`. The CLI suite needs both:
  `cargo nextest run -p rdlt-certify --features spawn-bins,bin`.

Build the CLI with `cargo build -p rdlt-certify --features bin --bin
rdlt-certify`.
