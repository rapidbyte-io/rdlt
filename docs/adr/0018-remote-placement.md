# ADR 0018: Placing connectors on the network, over mutual TLS

Status: accepted, 2026-09-26. Amended by ADR 0044, 2026-10-01: which hosts a connector accepts,
session resumption, private keys, and how many connections a listening connector holds.

## Context

ADR 0017 left two things to M4d:
- remote placement: connectors listening on the network, reached over TCP with mutual TLS (§13.4);
- a network simulation of the host under turmoil.

Each is a plan's worth.
- Remote placement brings TLS, a listening mode for the served binary, redialing, and the placement matrix's third leg.
- The simulation brings a harness that composes turmoil's simulated network with the simulation's own runtime and clock.

TLS is security-sensitive, so the owner decided its policy before this milestone started.

## Decision

- **M4d is split again.**
  - M4d (this record) is remote placement.
  - M4e is the network simulation of the host.
  - M4f is `rdlt-certify` and the `K` kill matrix, which ADR 0017 called M4e.
- **The TLS policy is the owner's** (2026-09-26):
  - rustls on the `ring` provider, TLS 1.3 alone, no OpenSSL.
  - Mutual authentication always: a host presents a certificate, and a listening connector requires one. There is no plaintext mode, not even behind a flag.
  - A host verifies the connector's certificate against its CA bundle and the endpoint's host name. A connector verifies each host's certificate against its own CA bundle, and accepts it only where it names a host the connector was told to accept (ADR 0044).
  - Neither end resumes a session: every connection is a full handshake, so a certificate is verified, and must be valid, each time it is presented. A connector may be given revocation lists.
  - Certificates and keys come from PEM files named by path; secret references come with M7's secrets. A private key is read only from a regular file of the user's alone.
  - Tests make their CA at test time; no key is ever committed.
- **The policy is in one place: `rdlt_wire::tls`**, behind `rdlt-wire`'s `tls` feature.
  - `server_config` and `client_config` build both ends' configurations, with ALPN `h2` alone. `server_config` takes the hosts accepted: their CA bundle, their names and their revocation lists.
  - `TlsError` names the file a failure came from.
- **A served binary listens with `--listen <address>`**, and needs `--tls-cert`, `--tls-key`, `--tls-client-ca` and a `--tls-allow-host` for each host it accepts. `--listen` and `--rdlt-fd` exclude each other.
  - It says where it listens on standard output (`listening on <address>`), so port 0 works.
  - Every connection is accepted at once. Those that have not completed their TLS handshake are 64 at most, each with 5 s; a further one closes one of them, drawn at random (ADR 0044).
  - At most 256 sessions are served at once, 64 of one host; a further connection waits, among 64 at most and for 10 s at most. A peer that never completes its handshake is among the unauthenticated, and holds neither a session nor a place in that queue.
  - It refuses to listen where its process may open fewer file descriptors than those limits need.
  - A failed accept pauses accepting for 100 ms.
  - Each connection is one session of the protocol, and pings its host over HTTP/2 every 5 s. A host that answers no ping for 30 s is gone.
  - The first `SIGTERM` or `SIGINT` stops it gracefully. It closes its listening socket, so a successor can listen at the address at once. It ends every host's heartbeat stream, which would otherwise hold its connection open. It finishes the calls in flight, then ends. A second signal stops it at once.
  - It ignores its standard input: a standalone server's standard input may be `/dev/null`, whose end is no request to stop. A spawned connector's is, as before.
- **`Remote` places connectors whose reference names an endpoint**, and every other one with its fallback: `Remote::new(identity, ca).fallback(Local::new())`.
  - An endpoint is `grpcs://host:port`, a host name or IP address and a port, and nothing else. One that is refused is not repeated in the error (ADR 0044).
  - The host connects over TCP with `TCP_NODELAY`, completes the TLS handshake and waits for the connector to accept it, all within the connect deadline.
  - In TLS 1.3 a connector checks the host's certificate after the host's handshake has completed, and a refusal arrives as an alert that the host's first write would only find as a closed connection. An HTTP/2 server speaks first, so the host waits for the connector's first bytes, or its alert. A refused certificate is then `ProviderError::Tls`, and on a redial an `Auth` error, which no retry mends. A connection lost in the handshake stays transient.
  - An unreachable endpoint is `ProviderError::Unreachable`, a malformed one `ProviderError::Endpoint`. A remote placement has no digest.
- **Supervision covers both placements.**
  - A lost connector is started again for the next call: respawned when this process spawned it, redialed when it listens elsewhere.
  - A connector that ends its heartbeat stream is stopping. The host lets its calls in flight finish, and starts it again for the next call. A connection whose transport closed is started again too.
  - Whatever is started again must serve the spec that was checked at placement: the same id, version and configuration schema. A redial reaches whatever listens at the endpoint now; another connector there is a configuration error, which no retry mends.
  - The host's connections ping over HTTP/2 every heartbeat interval, and give up after the heartbeat's patience (§12.6).
- **The placement matrix has its third leg.** The engine's destination-facing scenarios run against the SQLite and files destinations spawned, and listening on the network over mutual TLS. Each run of the remote leg starts its own listening connector, with a CA made for it.
- **The host's timers stay on tokio's clock.** The simulation's `SimEnv` is tokio's paused clock, and turmoil drives tokio's clock, so M4e's simulation runs the host's heartbeat and deadlines on simulated time.

## Consequences

- A listening connector trusts the hosts named to it, among the certificates its CA issued (ADR 0044).
- Certificates and revocation lists rotate by restarting the listening connector, and by placing the connector again on the host.
- The engine's test suite starts a listening connector per remote run: about 25 s of wall-clock time for the matrix.
