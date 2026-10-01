# ADR 0044: A listening connector and its hosts

Status: accepted, 2026-10-01.

## Context

ADR 0018 placed connectors on the network over mutual TLS, and left a listening connector weaker
than the model ADR 0037 now sets:

- It took a handshake permit before it accepted a connection, and had as many permits as a
  process usually has file descriptors. Peers that connected and said nothing held every permit:
  an authenticated host waited for a handshake to time out, and the same peers could fill the
  descriptor table and fail the files of sessions already served. Each refused connection wrote a
  line of its own.
- It accepted every certificate its CA had issued, resumed TLS sessions without checking the
  certificate again, had no revocation, said nothing of which host a session served, and read its
  private key from a file anyone could read. One certificate could hold every session.
- Certification's probes, a destination reading back what it published and a source telling where
  it stands, were compiled into every served connector and switched on by the host's handshake.
  The read-back built a whole table in memory. The generator and memory test connectors were
  ordinary binaries, and the memory destination's stores were one process's, shared by its hosts.
- A host could report any position committed, read or not, and a source moved its position to it.
- An endpoint refused for carrying credentials was repeated whole in the error.

## Decision

- **A connector accepts the hosts named to it.**
  - `--tls-allow-host <name>`, once for each host, is required to listen. A certificate is
    accepted where its CA bundle issued it, it is valid now, and a DNS name or URI among its
    subject alternative names is listed. DNS names compare without regard to ASCII case, URIs
    exactly; the common name is never read. No mode accepts every certificate of the CA.
  - The name accepted is the host's identity: it is written once for each session, it counts the
    host's sessions, and the connector is told it (`ConnectContext::host`).
  - Neither end resumes a TLS session or issues a ticket: every connection is a full handshake.
  - Short-lived certificates are the control for a lost key. `--tls-client-crl <path>` adds
    revocation lists, read at start: the whole chain is checked, and a certificate whose issuer
    has no current list, or a list past its next update, is refused. A list is renewed by
    starting the connector again.
  - A private key is read only from a regular file the user owns that its group and others have
    no access to, on both ends, checked on the handle that is then read. The error names the
    file and its mode.
- **Unauthenticated peers cost an authenticated host nothing.** `ListenLimits` holds the numbers.
  - Every connection is accepted at once; accepting waits for no permit.
  - Connections that have not completed their TLS handshake are at most 64. A further one closes
    one of them, drawn at random, and is never refused itself; each has 5 s.
  - An accepted host holds at most 64 connections. Beyond 256 sessions a connection waits, among
    64 at most and for 10 s at most; beyond those it is closed.
  - At start the connector works out the file descriptors its limits need: the unauthenticated
    and one more, the waiting, four for each session, and 64 of its own. It raises its soft limit
    that far where the hard limit allows, and refuses to listen where it does not;
    `--max-sessions` serves fewer.
  - Refused connections are counted by reason and reported in one line every 10 s in which any
    was refused. No line is written for a peer that has not authenticated.
- **Certification's probes are a feature and a choice.**
  - `rdlt-connector`'s `certify` feature, which `testing` implies, holds `ReadBack`, the readable
    and the acknowledging factories, and the served calls. Without it both calls answer
    unsupported.
  - With it, a binary serves a probe only where its `main` serves
    `readable_destination_factory` or `acknowledging_source_factory`. A connector served by its
    type, or by its plain factory, refuses the probe whatever its host offers, so another crate
    enabling the feature cannot switch it on. The `read_back` attribute flag is gone.
  - A read-back is sent a batch at a time, each once its reader took the one before, and is read
    no further than its host takes.
  - The reference connectors' SQLite and files binaries serve the plain factories. The generator
    and memory binaries are built only with the crate's `test-connectors` feature.
  - The memory destination keeps its stores for each host apart.
- **A host reports committed only what it was sent.** A served source remembers, for each host, a
  keyed hash of each checkpoint its reads sent, of its stream and partition, 2^18 at most. A
  report of any other position is refused as transient, with the code `position_unsent`, and
  nothing of the report is told to the source. The checkpoints are remembered for the process, not
  the connection, so a host that dials again reports what it committed.
- **The pipeline id stays the key of ownership and state.** The host's identity does not scope
  it: state lives in the destination, and must be found from every placement and after a host's
  certificate changes. The hosts named to one connector are one trust domain. Two of them using
  one pipeline id fence each other by epoch, and the later open owns the pipeline's tables.
- **An endpoint carries no secret, and an error repeats none.** `Endpoint::parse` answers an
  `EndpointError` for credentials, a path, a query, a fragment, a port or a host that is none,
  which never repeats the endpoint. A provider reports it as `ProviderError::Endpoint`; every
  other error names an endpoint by its host and port.

Rejected:
- **Closing the oldest unauthenticated connection.** A flood then closes every handshake that
  takes longer than 64 arrivals. Drawn at random, each handshake survives an arrival 63 times in
  64, and a host that is closed dials again.
- **`invalid_message` for a position no read sent.** A connector started again remembers nothing
  it sent, and an engine whose commit was in flight reports checkpoints of the process before.
  That is no fault of the engine's: refused as transient, it reads again and reports what the new
  process sends.
- **Scoping pipelines by the host's certificate name.** A renamed or replaced host would lose its
  pipelines' state, and what a destination stores would depend on how it was reached.
- **Remembering sent checkpoints for a connection.** A dropped connection would refuse the report
  of a commit that landed while the connector kept running.
- **A flag that restores acceptance of every certificate of the CA.** Nothing is published, so
  nothing needs the old behaviour.

## Consequences

- Every listening connector's command line names its hosts. A host whose certificate names it only
  in its common name needs a certificate with a subject alternative name.
- A private key mounted for a group or for others, as a Kubernetes secret is by default, is
  refused: mount it with mode 0400 for the connector's user.
- A connector with revocation lists refuses every host once a list is past its next update,
  until it is started again with a current one.
- Under a flood of unauthenticated connections an honest handshake is sometimes closed, and its
  host dials again. No session already served is affected.
- A process that may open fewer than about 1,200 files serves fewer sessions, by
  `--max-sessions`, or does not listen.
- A connector started again between a checkpoint and its report fails that attempt as
  transient; the next attempt reads from the committed positions.
- A source's position can lag its destination by one commit after such a restart, until the next
  commit is reported.
- Certifying a connector over the wire needs a binary built for it, whose `main` names the
  probing factory. A connector's own binary cannot be certified in the clauses that read back.
- A compromised host named to a connector can open the pipelines of the other hosts named to it.
  Hosts that must not trust each other get a connector each.
- The bytes a host stages before a commit are bounded by the memory-accounting milestone, not
  here.
