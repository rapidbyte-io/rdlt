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
  - The host no longer reads the TCP socket directly to see whether the connector accepted its
    certificate: it reads the connector's first bytes through the TLS stream, and hands them to
    the protocol again. The same code runs over any network.
- **A simulated network, from turmoil.** turmoil 0.7 runs each host on a tokio runtime of its own
  with a paused clock, and steps them together, 1 ms at a time, delivering messages between them.
  The engine's `SimEnv` is tokio's clock, and so are the host's heartbeat and deadlines (ADR
  0018), so the whole engine runs on simulated time unchanged.
  - A seed places its connectors on the network one time in four (the swarm feature `network`).
    The source and the destination then listen on hosts of their own over mutual TLS, with a CA
    made for each run, and every run places them through `Remote`. Its keys come from the
    operating system, so the TLS bytes differ between runs of a seed; what the connectors and the
    engine do does not.
  - The feature is drawn apart from the others, so a seed's workload is the same over either
    transport, and seeds found before keep their workload.
  - Each seed draws its messages' latency, and the host's heartbeat, patience and connect deadline.
  - With faults, a fault driver disrupts the network while each faulty run lasts: partitions both
    ways and one way, messages held and released late, and connectors that crash, dropping their
    listener and every connection at once, or stop gracefully, then start again. A crash leaves
    the calls a connector had begun to run to their end: turmoil's own crash of a host, which
    would end them too, needs the simulation to step turmoil itself. Each fault lasts up to twice the host's
    patience, so the host notices some and not others. The network heals before the runs that
    must succeed.
  - The oracle is unchanged: the destination holds exactly what the model says, whatever the
    network did.
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
- **The simulation's coverage leaves out what it never runs**: generated wire code, process
  placement, and a served binary's entry points. Its floor stays 82 % of lines and 73 % of
  branches.

## Consequences

- A seed over the network takes about twice as long as one in process: 10 000 seeds take about
  two and a half minutes on a pull request.
- turmoil's TCP does not retransmit what a partition dropped, so a connection that lost data in a
  partition never recovers: the host notices it by its heartbeat and redials. Real TCP recovers
  from short partitions, so the simulation is the harsher.
- turmoil steps every host 1 ms at a time: long idle stretches cost steps, where the paused clock
  alone would jump.
