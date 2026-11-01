# ADR 0044: A listening connector and its hosts

Status: accepted, 2026-10-01.

## Context

ADR 0018 places connectors on the network over mutual TLS. A listening connector must hold to the
model ADR 0037 sets, where a weaker one would:

- take a handshake permit before it accepts a connection, with as many permits as a process
  usually has file descriptors, so peers that connect and say nothing hold every permit: an
  authenticated host waits for a handshake to time out, the same peers fill the descriptor table
  and fail the files of sessions already served, and each refused connection writes a line of its
  own;
- accept every certificate its CA issued, resume TLS sessions without checking the certificate
  again, have no revocation, say nothing of which host a session serves, and read its private key
  from a file anyone can read, so one certificate holds every session;
- compile certification's probes, a destination reading back what it published and a source
  telling where it stands, into every served connector, switched on by the host's handshake, with
  a read-back that builds a whole table in memory, ship the generator and memory test connectors
  as ordinary binaries, and share the memory destination's stores among its hosts;
- let a host report any position committed, read or not, and move a source's position to it;
- repeat whole in its error an endpoint refused for carrying credentials.

## Decision

- **A connector accepts the hosts named to it.**
  - `--tls-allow-host <name>`, once for each host, is required to listen. A certificate is
    accepted where its CA bundle issued it, it is valid now, and a DNS name or URI among its
    subject alternative names is listed. DNS names compare without regard to ASCII case, URIs
    exactly; the common name is never read. No mode accepts every certificate of the CA.
  - A name that no certificate can carry is refused at start: a wildcard, a final dot, a space,
    an IP address, a name outside ASCII. An international name is listed in its `xn--` form.
  - The name accepted is the host's identity: it is written once for each session, it counts the
    host's sessions, and the connector is told it (`ConnectContext::host`). A certificate that
    carries several listed names is the first of them in sorted order, on every connection.
  - Neither end resumes a TLS session or issues a ticket: every connection is a full handshake,
    and one that does not agree on HTTP/2 is closed.
  - Short-lived certificates are the control for a lost key. `--tls-client-crl <path>` adds
    revocation lists, read at start: the whole chain is checked, and a certificate whose issuer
    has no current list, or a list past its next update, is refused. The summary of refused
    connections counts a revoked certificate, and one no current list covers, apart from every
    other refusal.
  - A private key is read only from a regular file the user owns that its group and others have
    no access to, on both ends. Every file of a TLS configuration is opened without waiting for
    it and examined on the handle that is then read; what is no regular file is refused.
- **Peers that never authenticate hold little, and mostly close their own connections.**
  `ListenLimits` holds the numbers.
  - Every connection is accepted at once; accepting waits for no permit, and after a failed
    accept only accepting pauses.
  - Connections that have not completed their TLS handshake are at most 64, each with 5 s. A
    further one is never refused: it closes the oldest connection of the origin that holds the
    most of them, the newcomer counted, drawn at random among origins that hold as many. An
    origin is an IPv4 address, or an IPv6 /64.
  - What that guarantees: a host's handshake is not closed by a flood while any origin holds
    more than one of the 64 places. A flood from fewer than 64 origins therefore closes only its
    own connections, and takes no descriptor a session needs.
  - What it does not: a flood from 64 or more origins leaves every origin one place, and then
    each arrival closes one of them drawn at random, the host's among them: its handshake of
    T seconds survives R such arrivals a second with probability (63/64)^(R x T). A peer behind
    the host's own address, a NAT for one, counts as the host: its flood closes the host's
    handshake once 64 of its connections arrive within the handshake. A firewall in front of the
    connector is the control for both.
  - Measured on loopback, 200 dials each: a flood of 5,000 connections a second from another
    address closed none of the host's handshakes; from the host's own address, one.
  - Sessions are 256 at most. Each named host holds its share of them, an equal one by default
    and `--max-host-sessions` otherwise; a connector refuses to listen where a host may hold
    none, or where the other hosts, each holding all it may, would leave a host no session.
    Beyond the sessions served a connection waits, among 64 at most and for 10 s at most; beyond
    those it is closed. A session that ends, however it ends, gives its host its place back.
  - A connection holds three destination sessions open at once, as many as its descriptors
    allow beside its socket. A session counts from before its destination opens it until its
    close ends, so Opens and Closes in flight count too. A further session opened closes the
    connection's oldest once the calls in flight on it end: the Open waits for them.
  - At start the connector works out the file descriptors its limits need: the unauthenticated
    and one more, the waiting, four for each session, and 64 of its own. It raises its soft limit
    that far where the hard limit allows, and refuses to listen where it does not;
    `--max-sessions` serves fewer.
  - A write's writer is not counted there. It lives no longer than its call, and a connection
    holds 200 calls at most, HTTP/2's limit of concurrent streams. A destination whose writer
    opens a descriptor of its own beside its session's can take up to 200 more for each
    connection: it keeps its writers' files among its sessions', or is served with fewer
    sessions. A write holds its session only to take its writer, so eviction does not wait for
    a write in flight.
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
  - **A release builds a connector's package alone**: `cargo build --release --package
    rdlt-connector-reference`. A build of the whole workspace turns the feature on for every
    package, through the certifier's dependency on it, and then the plain factory is all that
    keeps a probe out. `cargo xtask shipped`, part of the lint, fails when the package built
    alone turns on a test or certification feature, or builds a binary it does not ship.
  - A read-back is sent a batch at a time, each once its reader took the one before, and is read
    no further than its host takes.
  - The reference connectors' SQLite and files binaries serve the plain factories. The generator
    and memory binaries are built only with the crate's `test-connectors` feature.
  - The memory destination keeps its stores for each host apart.
- **A host reports committed what it was sent or reads from, and the engine reports every
  partition a commit covers.**
  - A served source remembers, for each host, a keyed hash of each checkpoint its reads sent,
    of its stream and partition, 2^18 at most, and apart from them where the host's latest read
    of each partition started. A checkpoint is the source's statement of a position. Where a
    read started is the host's, which by asking to read from there has said everything before
    it is committed, and which the source vouched for by accepting the read. A report of any
    other position is refused as transient, with the code `position_unsent`, and nothing of the
    report is told to the source. The positions are remembered for the process, not the
    connection, so a host that dials again reports what it committed. One type holds the rule,
    `rdlt_connector::Sent`, which a source that binds its reports in process uses too.
  - Where a read started is noted only once the source has accepted the read: when it sends
    data or a checkpoint, or ends the read cleanly. A read the source refuses, or that fails
    before either, leaves nothing to report. It is one start a partition a host, replaced by
    the next read's, so no number of checkpoints forgets it.
  - A source therefore refuses, before it sends anything, a cursor it cannot have issued, as
    one beyond everything it holds, with the code `cursor_unissued`: a start it accepts is a
    position its host may report. A source that starts elsewhere than the cursor it was given,
    from a position it keeps itself, sends a checkpoint of where it starts before anything
    else, and the host is heard for that position and not for the cursor it gave. The
    reference log and change sources refuse a cursor no read of theirs was sent.
  - The engine reports the committed position of every partition a commit covers, moved or
    not. A partition whose read was sent nothing is sealed where it started, and reported
    there: so the source's kept position reaches the committed one though the partition never
    moves again.
  - A report that fails, refused or lost, leaves its cursors owed, in the coordinator's memory,
    and every later commit's report carries them, a commit with nothing to publish included, so
    the commit interval bounds how late they are told. A newer cursor of a partition takes the
    place of the cursor owed, so no report carries a cursor older than one before it. The
    attempt's last commit is the commit made once every partition has ended and no phase follows;
    the commit of a phase's end that another phase follows is not, and its owed cursors go on to
    the next phase's commits. A partition at a cursor still owed at the attempt's last commit,
    or owed by as many failed reports in a row as the run makes attempts, ends the attempt with
    the report's error once every stream's report is made, and the next tells it from where its
    read starts. A source that refuses every report therefore fails the run once no attempt is
    left. Each failed report is counted in the run report's `acks.failed`.
  - A partition that ends done is told its last position with the commit that records it
    done: the last checkpoint among the commit's seals, or with none there the cursor the
    partition stood at, so a report of that cursor that failed in an earlier attempt is still
    made. No later attempt reads the partition, so a cursor of it still owed is given up, and
    counted in `acks.dropped`, at the attempt's last commit, after as many failed reports in a
    row as the run makes attempts, or at once where its source refuses it as `position_unsent`,
    which a source started again does for ever. Nothing owed is kept across attempts, in state
    or in memory: a durable entry would need a commit that publishes nothing to clear it, and
    a source started again would refuse it for ever.
  - A commit that publishes no row and records nothing but partitions where they stood is no
    progress: it does not reset the count of failed attempts. Positions are compared byte for
    byte. A source whose every attempt moves a position still retries without end; a limit on
    such resets is the control plane's to add.
- **The pipeline id stays the key of ownership and state.** The host's identity does not scope
  it: state lives in the destination, and must be found from every placement and after a host's
  certificate changes. The hosts named to one connector are one trust domain. Two of them using
  one pipeline id fence each other by epoch, and the later open owns the pipeline's tables.
- **An endpoint carries no secret, and an error repeats none.** `Endpoint::parse` answers an
  `EndpointError` for credentials, a path, a query, a fragment, a port that is not digits from
  1 to 65535, or a host that is none, which never repeats the endpoint. A provider reports it as
  `ProviderError::Endpoint`; every other error, and a reference's debug form, names an endpoint
  by its host and port.

Rejected:
- **Closing a member drawn at random among all.** A flood from one address then closes a
  host's handshake with every 64th arrival: on a slow path, almost always.
- **Closing the oldest unauthenticated connection of all.** A flood then closes every handshake
  that takes longer than 64 arrivals.
- **`invalid_message` for a position no read sent.** A connector started again remembers nothing
  it sent, and an engine whose commit was in flight reports checkpoints of the process before.
  That is no fault of the engine's, and the refusal must be one it can retry.
- **Reporting to a source only the positions a commit moved.** A report that failed once was
  then never made again while its partition stayed idle, and a source that refused every
  report gave runs that succeeded.
- **Failing the attempt for a report that fails.** A report lost, or refused by a source that
  forgot it, then cost a reopen, the discard of everything staged since the commit, and its
  read again, though the next commit's report carries it.
- **Retrying a report in place.** The next commit is at most an interval away, and a wait on
  the coordinator would only delay it.
- **Hearing a host only for checkpoints.** An idle partition's position is one some earlier
  process sent: a connector started again would refuse it on every retry.
- **Scoping pipelines by the host's certificate name.** A renamed or replaced host would lose its
  pipelines' state, and what a destination stores would depend on how it was reached.
- **Remembering sent checkpoints for a connection.** A dropped connection would refuse the report
  of a commit that landed while the connector kept running.
- **A flag that restores acceptance of every certificate of the CA.** Nothing is published, so
  nothing needs the old behaviour.
- **Reading certificates and revocation lists again without a restart.** It needs a signal or
  a watch in every served binary, and a certificate and its key replaced as one. A listening
  connector stops gracefully and frees its address at once, so a restart is the rotation. The
  library that reads a list does not tell its next update, so the connector cannot warn before
  a list goes stale: the summary says so once it has.
- **Refusing a session opened beyond those a connection holds.** A host that leaves the sessions
  of failed attempts open would stop loading; closing its oldest costs it nothing it still uses.
- **Closing the oldest session under a call in flight.** The call holds the session, so
  forgetting it closes nothing, and a host that starts a call on each session it opens keeps
  more of them than its share.
- **Counting a write's writers in the descriptor estimate.** At 200 calls a connection, the
  estimate would ask every listening connector for tens of thousands of descriptors, for
  writers most destinations give none of their own.

## Consequences

- Every listening connector's command line names its hosts. A host whose certificate names it only
  in its common name needs a certificate with a subject alternative name.
- A private key mounted for a group or for others, as a Kubernetes secret is by default, is
  refused: mount it with mode 0400 for the connector's user.
- A connector with revocation lists refuses every host once a list is past its next update,
  until it is started again with a current one. Its summary line says which.
- A flood of unauthenticated connections from many origins, or from the host's own address, can
  still close a host's handshake, and the host dials again. No session already served is
  affected, by any flood.
- A process that may open fewer than about 1,200 files serves fewer sessions, by
  `--max-sessions`, or does not listen.
- A connector started again between a checkpoint and its report refuses it as transient. A
  partition at a cursor stays owed, and fails the attempt at its last commit where its reads
  have not failed it already; the next attempt reads on from the committed positions, which
  the new process hears it for, and reports them.
- A host that reads from a position it never committed, which the source issued and accepts,
  can then report that position: it is one of the hosts named to the connector, which may
  already read and write through it. It cannot report a position the source refuses to read
  from, so a source that follows the rule is never moved beyond what it holds.
- A source that refuses every report can still see a run succeed where its partition ends
  done: the attempt gives the done partition's cursor up and counts it in `acks.dropped`. No
  row is lost or doubled, and the position that trails is the refusing source's own.
- A source that accepts a cursor it did not issue, and acts on reports, can be moved there by a host
  named to it. The rule is the connector author's to keep; certification does not check it.
- A session used after three newer ones were opened on its connection is gone, and answers
  `no_session`.
- An Open beyond three on a connection waits for the calls in flight on its oldest session, for
  as long as they take; the host's deadline on the Open bounds the wait.
- Certifying a connector over the wire needs a binary built for it, whose `main` names the
  probing factory. A connector's own binary cannot be certified in the clauses that read back.
- A compromised host named to a connector can open the pipelines of the other hosts named to it.
  Hosts that must not trust each other get a connector each.
- The reference log and change sources keep their consumer groups and slots for the process,
  by name, whichever host names them, and never free one: a named host can read and move
  another's. They are examples; a source built on them keeps its stores for each host, as the
  memory destination does.
- The bytes a host's writes stage in a listening connector are bounded by the staged limit, four
  frames' bytes between two flushes (ADR 0039).
