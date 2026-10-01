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

## Reporting a vulnerability

Report vulnerabilities privately through GitHub: open the repository's **Security** tab and choose
**Report a vulnerability**. Do not open a public issue.
