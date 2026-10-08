# rdlt

rdlt is an embeddable engine that moves data from sources to destinations with exactly-once
delivery, persistent schema management and throughput that scales with cores. It is the core of
the Rapidbyte data platform.

**Status:** pre-release. The foundation (tooling, deterministic simulation harness, runtime
primitives), the connector contract (`rdlt-connector`, with in-process certification and
`sqlgen`, the planner SQL destinations share) and the exactly-once engine core (`rdlt-engine`:
`append`, `replace` and `merge`, with schema evolution, name maps and nested data, checked by the
simulation oracle in `rdlt-sim`) are in place. The reference connectors in
`rdlt-connector-reference` are an in-memory source and destination, a data generator, a SQLite
destination and a source and destination of JSON lines and Arrow IPC files; the engine's
integration suite runs against every destination. JSON pushes are coalesced and shredded in
parallel on the compute pool, and every batch is lowered there through a plan made once per table
schema. Streams can `normalize` nested data into child tables whose rows carry their lineage; a merge
replaces each root's child rows in the same commit, and a dropped row takes its children with it.
Differential tests draw every logical type in every Arrow encoding a source may send and check that
each value lowers, and normalizes, exactly; the simulation drives the same types and encodings
through whole runs and reads every stored cell back by its type, under every schema policy and
setting at every level, with hints, declared columns and merge keys that change type or collide,
predicting each refusal. It injects permanent failures and connector panics at every connector
call, shares a destination between two pipelines, perturbs the scheduler by seed, replays each
seed alike, runs on many threads nightly, and measures how much of the engine it reaches. The wire
protocol for connectors that run out of process (`rdlt-wire`) defines its messages, carries Arrow
batches in Arrow Flight's layout with each schema sent once, and refuses frames beyond its limits
or malformed ones with a typed error, and every other message beyond the limit of what its call
carries; the contract's types convert to and from its messages under
`rdlt-connector`'s `wire` feature. A connector serves the protocol with `rdlt-connector`'s `serve`
feature, one handshaken session per connection. The engine loads through it with `rdlt-host`'s
`RemoteSource` and `RemoteDestination`: errors cross whole, reads and writes keep within credit,
heartbeats notice a lost connector, and each call has its deadline. A provider places each
connector by policy (ADR 0043): in process only what its embedder trusts
(`Registry::trusted`), and in a process of its own inside a sandbox (`Local::sandboxed`, with
bubblewrap on Linux) unless its binaries are stated to be trusted
(`Local::trusting_binaries`). `Local` finds a binary only in the directories configured,
executes the file it opened and hashed, gives it its socket on file descriptor 3 and nothing
else of the host's, bounds and shows its output, keeps its last words for its errors, stops it
when done and respawns it when it is lost. A configuration's secrets are references
(`${env:NAME}`, `${file:/path}`, `${secret:name}`), resolved only as the operator lists, for
the connector the host has verified, and scrubbed from whatever it says back. The engine's integration suite runs against
the reference connectors both ways. `rdlt-host`'s `Remote` reaches connectors listening
on the network over mutual TLS 1.3, redialing them when they are lost, and the integration suite
runs against them too. A listening connector accepts only the hosts named to it, by a name in
their certificates, and holds a bounded number of connections at each stage: peers that never
authenticate take nothing a session needs, and a flood from few addresses closes its own
connections before a host's (ADR 0044). The simulation also places its connectors on hosts of their own on a
simulated network (turmoil's, on paused clocks), through `Remote`, and loads through partitions,
held messages, and connectors that crash or stop and start again. `rdlt-certify` certifies a
connector through the protocol, served in this process, spawned from its binary or listening at an
endpoint: the source and destination clauses, and the protocol's own, as a library and as the
`rdlt-certify` binary. A destination that reads back what it published is certified in every
destination clause from a binary built to serve that: a connector's own binary serves no
certification probe. The kill clauses load through an engine while the
connector is killed, a spawned one with its whole process group, which its host owns and stops
before it exits, and any other by cutting its connections, and check that the load converges exactly once; they need the `kill` feature, which
the binary has. A clause passes only when what it requires was seen: one that applies and was not
observed leaves the report incomplete, and the binary exits 2 (`docs/certify/clauses.md`).
Change streams load through the engine: a CDC source reads a snapshot, then its changes, in phases
the engine advances within a run. Its inserts, updates, partial updates and deletes merge by key
under the seq guard, with deletes hard, soft or ignored; a truncate, which names no key, removes or
marks deleted every row sequenced before it. A change log appends every change instead.
The memory budget is never passed: a push reserves what it keeps alive and is lowered a piece at
a time, each piece reserving what its table stores it as before it is lowered, while checkpoints,
the log, what commits record of tables and what reads keep have shares of the budget no push can
use, and what a connector is told it may send fits them;
JSON integers load exactly at any width, as decimals within 76 digits and as JSON text beyond; a
merge key keeps matching its stored rows or refuses to change type, and a table keeps the merge key
and change time it was loaded by; and an unbounded partition, as
a change stream's, resumes from its last checkpoint rather than ending.
Floats arriving at a column of integers every one of which a float holds exactly land beside them
as floats, not as JSON text.
A table belongs to the pipeline that created it; a connector answers who it is before it receives
its configuration, and a binary changed since it was placed is not started again; and what a
connector sends is checked, from the barriers its checkpoints answer to the waits it asks for.
The memory, JSON lines, Arrow IPC and SQLite destinations merge them, SQLite through `sqlgen`,
whose SQL needs no `ON CONFLICT`, row values or schema change inside a commit but a replace
generation's swap; each remembers the rows a hard delete or truncate removed, so a change sent again
never brings one back. The simulation checks every merged table and log against a model of a
seeded change workload, through faults, crashes, racing runs and changes sent again.
The SQLite destination refuses a batch holding a float that is no number, or negative zero, which
SQLite does not keep as they are (`float_unstorable`).
A source that forgets what it acknowledged, as a message queue does, loads exactly once through a
write-ahead log: each load logs its batches and commits to a local directory or an S3 bucket
(`rdlt-log-store`), the source hears once a commit's frame is durable, and the next attempt commits again whatever the destination missed,
leaving to newer loads the partitions they moved since. A change stream's move from its snapshot
to its changes is logged too, so a replication slot that forgets what it acknowledged loads its
snapshot and changes exactly once. The simulation crashes loads mid-write and tears their logs,
with sources and change sources that refuse to serve again what they acknowledged.
A source that keeps its position outside the engine, as a replication slot or a consumer group
does, says where it stands when certification asks, in process and through the protocol, and
`S-ACK` checks that the position moves only to the cursors the engine says are committed, through
a change stream's phases, reads that never end and the other clauses' reads.
A run can follow its source (`until: forever` or for a while): reads of partitions that never end
wait for more data, streams are planned again as the run goes, so partitions added mid-run start,
retired ones stop and tables are polled as they grow, and everything commits every ten seconds.
The reference connectors include an offset-log source, as a message queue keeps, and the
simulation follows streams whose rows arrive as time passes, through crashes and faults.
A source can tell a following run that its partitions changed, so they are planned again at
once, and how far behind its newest data each read is, which the report totals per stream. A read
whose place the source's retention dropped fails the run as `retention_lost`, or, where the stream
says so, reads again from the earliest data kept and is counted. `S-PARTITION` certifies that a
stream's partitions, planned again from where they stood, cover what is left exactly once.
A stream can be reset between runs, to be read again from its beginning, or with its tables
dropped and released to any pipeline, so a merge table can become a change table and a table can
change hands; a load the reset fenced lands nothing of the stream after it, and `D-DROP`
certifies a destination's drops.
A stream can keep every version of each key (SCD2): a change closes its key's version where the
next begins, at the source's change time or when its batch arrived, a change equal to the current
version opens none, deletes close versions or open deleted ones, and replays change nothing;
`D-HIST` certifies a destination's history against versions worked out by hand.
Real crashes test the whole: a pipeline run in a process of its own is crashed at every
durability step through the engine's `failpoints`, and killed with its spawned connectors as it
loads, then run again; every row lands once and no connector outlives its run (`just crashes`).

## Development

The toolchain is pinned in `rust-toolchain.toml`. Every other tool is pinned in `mise.toml` and
locked in `mise.lock`, which holds each tool's download and its checksum; CONTRIBUTING.md says how
to change one.

```sh
mise install --locked # install the tools as `mise.lock` holds them
just --list         # see every recipe
just ci             # what the gate runs (Linux): lint, test, coverage, Miri, counts, simulation
just ready          # before pushing: lint, tests, and mutation testing of your change
just sim 42         # replay simulation seed 42
just stress 20      # the simulation on many threads and the real clock
just sim-coverage   # how much of the engine the simulation alone reaches
just bench passthrough # a recorded figure: a bench on chosen CPUs, with its commit, load and counts
just instructions   # instruction and allocation counts against main, as CI takes them (Linux)
cargo xtask codegen # regenerate the wire protocol's code after changing its .proto files
```

Read [CONTRIBUTING.md](CONTRIBUTING.md) before opening a pull request. Architecture decisions live
in [docs/adr](docs/adr).

## License

Apache-2.0. See [LICENSE](LICENSE).
