# ADR 0019: Simulating the host over an unreliable network

Status: accepted, 2026-09-27.

## Context

ADR 0018 left the network simulation of the host to M4e. Remote placement had been tested only
over the operating system's network, where partitions, held messages and connectors that vanish
mid-conversation are hard to cause and cannot be replayed. The simulation (§20.2) already runs
the whole engine on a paused clock from a seed, with connectors in the engine's process.

## Decision

- **Both ends take their network from the caller.**
  - A listening connector serves any `rdlt_connector::serve::Listener`, through the public
    `serve_listener`. The binary's `--listen` serves a TCP listener with it.
  - `rdlt-host`'s `Remote` reaches endpoints through any `rdlt_host::Network`; `Tcp`, the
    operating system's, is the default.
  - The host does not read the TCP socket directly to see whether the connector accepted its
    certificate: it reads the connector's first bytes through the TLS stream, and hands them to
    the protocol again. The same code runs over any network.
- **A simulated network, from turmoil.** turmoil 0.7 runs each host on a tokio runtime of its own
  with a paused clock, and steps them together, 1 ms at a time, delivering messages between them.
  The engine's `SimEnv` is tokio's clock, and so are the host's heartbeat and deadlines (ADR
  0018), so the whole engine runs on simulated time unchanged.
  - A seed of either oracle places its connectors on the network one time in four (the swarm feature
    `network`). The source and the destination then listen on hosts of their own over mutual TLS,
    with a CA made for each run, and every run places them through `Remote`. A change world's source
    host, and the peer that stalls, serve the simulated change source; a change world meets no
    hostile host. Its keys come from the operating system, so the TLS bytes differ between runs of a
    seed; what the engine reports does not. A served response carries no `Date` header: hyper takes
    it from the wall clock and its text changes every second, so two runs of a seed would encode
    HTTP/2 header blocks of different lengths, cut the connectors' writes differently and have their
    peers read at other times.
  - One read of the wall clock is left, in h2: it remembers a stream it reset for a second of wall
    time, and ten of them at most, and answers a frame that arrives late for a stream it forgot with
    a reset of its own, where it drops one for a stream it remembers. How many it remembers depends
    on how fast the run goes, so two runs of a seed can end a few milliseconds apart in simulated
    time, more often on a loaded machine. The replay check leaves the end time out of a networked
    seed's comparison, and of a seed whose connectors are played in processes, which speak HTTP/2 to
    the host too, and holds the rest to equal. hyper's server has no setting for these limits, and
    tonic's client does not set them.
  - The feature is drawn apart from the others, so a seed's workload is the same over either
    transport, and seeds found before keep their workload.
  - Each seed draws its messages' latency, and the host's heartbeat, patience and connect deadline.
  - With faults, a fault driver disrupts the network while each faulty run of a phase lasts, each
    run that follows a streaming source, and each reset with the runs it races: partitions both ways
    and one way, messages held and released late, and connectors that crash, dropping their listener
    and every connection at once, or stop gracefully, then start again. A crash leaves the calls a
    connector had begun to run to their end: turmoil's own crash of a host, which would end them
    too, needs the simulation to step turmoil itself. Each fault lasts up to twice the host's
    patience, so the host notices some and not others. The network heals before the runs that must
    succeed.
  - With faults, one placement in twenty goes to a peer that stalls, on a host of its own the
    fault driver never disrupts. It serves both connectors through `serve_connection`, behind a
    transport that drops every frame answering a call after the configuration, and passes pings
    and settings. HTTP/2's keep-alive therefore never notices it; only the host's heartbeat can.
    Once the first answer is withheld, a stalled connection the engine keeps open longer than its
    heartbeat's patience plus the time a close takes to arrive is a violation.
  - In one hostile seed in two (`framing`), connectors that break the protocol listen on a host of
    their own, each fault of the test kit's connector on a port of its own. Beside each attempt's
    runs the engine's host dials one through `Remote`, sets up a connection with options of its
    own, and makes the call its fault breaks:
    - the connection's setup: a configuration as another connector, a handshake with no
      dictionary limit, at the next major version, or accepting a feature never offered;
    - a check once the heartbeat has had twice its patience: pings echoed ahead, none answered, a
      heartbeat that never opens or is refused, pings never read;
    - a read: a schema that is no IPC message, a barrier never asked for, a schema epoch sent
      twice, too many frames that carry no event, too many log lines, and a connector that knows
      no barrier in a read's start;
    - a discovery: status details that do not decode, a catalog too large to decode;
    - a session's opening with a state key past its limit, and writes whose credit trickles or
      is of no bytes.

    On a healed network the host must meet each as its own tests do. Where the attempt's network
    is disrupted, a driver of its own also cuts and holds that host's link, and the host may lose
    the connector instead. Either way the call must end within the bound its deadlines and
    heartbeat set, and nothing of it may outlive the phase on either host. Over 10 000 seeds,
    237 met one on a healed network. With the fix that counts a heartbeat with no room in its
    queue as missed undone, 2 of 1 000 seeds fail.
  - The exactly-once oracle is the same over either transport: the destination holds exactly what
    the model says, whatever the network did.
- **What the simulation found**, each fixed with a regression test:
  - A served writer that panicked ended its write as though it were done, so the host reported a
    protocol violation. The panic now fails the write as an internal error, as a read's does.
  - A host that completed its TLS handshake and never began HTTP/2, its network gone, held its
    connection forever: HTTP/2's server waits for the preface with no deadline and pings no host
    before it has the whole preface. It held a session, and a graceful stop never ended. A host
    now has as long to send HTTP/2's preface as it had for its handshake.
- **`rdlt-sim` depends on `rdlt-host` and `rdlt-wire`.** The spec's crate map (§4) lists the
  engine and the connector contract; simulating the host needs the host. `rdlt-sim` stays a leaf,
  and the engine still links no gRPC.
- **A run that breaks the wire protocol is a finding**, faults or not: a connector and a host that
  keep to the protocol never answer out of turn, however the network or the connector fails.
- **The simulation's coverage leaves out what it never runs**: generated wire code, what process
  placement leaves to the operating system (descriptors, process groups and their reaper, the
  registry of groups, a command past its checks, and bubblewrap; the soak and the host's own tests
  run those, ADR 0052), three files the seeds reach in under half their lines (`local/process.rs`,
  `kills.rs` and `local/grants/opened.rs`), a served binary's entry points, certification (the
  certify crate and the host's connections it uses), the local and in-memory log stores (the
  simulation keeps logs in a store of its own), and test and bench code, modules built only for
  tests or benches among it: references and their comparisons, property-test strategies, the log
  stores' conformance suite, generated numbers, counts of serving tasks and an inline
  environment. The data plane
  (`rdlt_wire::plane`) is hand-written, not generated, and every simulated served read and write
  runs it, so it counts. In-process seeds place their connectors through the host's registry, as an
  embedder does, so its resolution of secrets and its scrubbing of what connectors say run in every
  seed. Played seeds place their connectors through `Local`, so its placement and supervision, the
  output drains with their scrubbing and the witness run in them, and so do the host's digest and
  grant checks. The coverage's floor is what 1 000 seeds reach, 81 % of lines and 71 % of branches.
  A hostile host, in one networked seed in two, reaches the code that refuses a host breaking the
  protocol. Beside the runs it:
  - calls before the handshake and the configuration;
  - configures one connection twice at once;
  - gives up on configurations and opens mid-way;
  - opens sessions beyond a connection's share;
  - sends a configuration beyond its limit;
  - holds a connection's window with writes that stop within a message.

  The connectors it targets count what each hostile connection makes them do, and fail the seed
  where serving lets two configurations connect at once or holds more sessions than it may. They
  start again, every other life, under a version named after a secret they were told.

  Every configuration refers to three secrets:
  - one that ends in whitespace;
  - one of several lines, each line of 8 or more non-whitespace characters found alone;
  - one shaped as a version.

  The connectors quote them in their faults. A secret in a run's error or report, in an error
  placing a connector, or in an event or span logged on the seed's thread fails the seed. The
  simulation still reaches little of:
  - certification's probes;
  - the scan's refusals of messages it cannot walk or whose decoded count passes its bound;
  - secrets read from files and the environment;
  - refusals of a log a store garbled.

## Consequences

- turmoil's TCP does not retransmit what a partition dropped, so a connection that lost data in a
  partition never recovers: the host notices it by its heartbeat and redials. Real TCP recovers
  from short partitions, so the simulation is the harsher.
- turmoil steps every host 1 ms at a time: long idle stretches cost steps, where the paused clock
  alone would jump.
- turmoil's sockets take 64 segments before a write waits, so no simulated peer can stop a host
  writing HTTP/2's preface: the bound on a connection's setup is held by the host's own tests,
  over a pipe of one byte. The simulation's engine closes each failed attempt's session before
  the next attempt opens one, and drops each call it gives up on before its next, so a
  connection's session bound is never reached here; the served connector's own tests hold it.
