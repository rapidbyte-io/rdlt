# Security

## Trust model

- The engine and the host are the trusted computing base. A connector is untrusted code, unless
  it is compiled into the embedder's binary and registered in process.
- Everything a connector sends is untrusted: data, control messages, state, receipts, errors and
  timing. The engine validates it, bounds it, charges it to a budget and puts a deadline on every
  wait, so that no connector can crash, hang or exhaust the engine, or reach another pipeline.
- Untrusted connectors run outside the engine's process: remotely over mutual TLS, or in a local
  process inside a sandbox. In-process placement is for trusted connectors.
- A connector can still send wrong data, and a destination can lose the data it is given: rdlt
  confines a connector, it does not vouch for it.

ADR 0037 records this model. It is being put in place by a hardening program, and until that is
complete, run only connectors you trust. The mechanisms in place today:
- connectors listening on the network are reached over TLS 1.3 with mutual authentication;
- a connector's process starts with a cleared environment, and receives its configuration only
  after the host has checked its identity and, where one is named, its binary's digest;
- frames from a connector are checked against size limits before they are decoded.

## Build and supply chain

- `unsafe` code is in one audited crate, `rdlt-adopt`. Every other crate, test, example, bench
  and fuzz target forbids it, which the compiler enforces for the code it compiles there; the
  lint rejects the keyword everywhere else it is a token, macro bodies included, which the
  compiler does not check.
- Dependencies are locked, and every lockfile is checked against the RustSec advisory database
  on each change and each night.
- Development and CI tools are locked to a release archive and its checksum; GitHub Actions are
  pinned to commits that are checked against the versions they claim.
- CI runs with a read-only token and no secrets. One job has the token in its environment, and
  it builds nothing.

ADR 0048 records these.

## Certification

`rdlt-certify` treats the connector it certifies as hostile input: every wait has a deadline, and
what a connector sends is charged against a limit before it is held, expanded or rendered, by its
rows, its bytes and, for JSON, its records. A column read back is cast only between kinds a test
shows cannot panic. A clause passes only when the behaviour it names was seen, in a mode the
connector declares. A panic is not caught: it ends the run with a failure, never a pass. The
certifier stops the process group of each connector it spawned before it exits, unless it is killed
outright or aborted; a member that left its group is beyond that. It is no control against a
connector built to pass: a connector can tell a certification from an engine's load, and a
destination's read-back is its own account of what it published (`docs/certify/clauses.md`). What
holds against an untrusted connector is the trust model above: the engine's limits, deadlines and
confinement.

ADR 0050 records this.

## Reporting a vulnerability

Report vulnerabilities privately through GitHub: open the repository's **Security** tab and choose
**Report a vulnerability**. Do not open a public issue.
