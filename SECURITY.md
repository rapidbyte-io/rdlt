# Security policy

## Supported versions

rdlt is pre-release and supports only the current repository version. Security
fixes may change on-disk formats, configuration rules, and connector behavior
in place; legacy formats and compatibility shims are not supported.

## Trust boundaries

- Pipeline and connector configuration documents may be attacker-influenced.
  Their raw size, YAML graph syntax, and materialized cardinality are bounded
  before they can drive allocation.
- Installing or selecting a connector binary is an operator trust decision. A
  spawned connector executes with the operator's OS identity, but receives a
  scrubbed environment and a dedicated process group. Its wire frames,
  identifiers, socket advertisement, and error text are still treated as
  untrusted inputs for host availability, path safety, and terminal safety.
- Connector configuration can contain credentials. It is sent only over the
  private local connector socket and must never be echoed in protocol errors.
- Each pipeline work directory is private to the rdlt process. The WAL uses an
  ownership marker before recursive cleanup and refuses symlinks at file-open
  boundaries. A process that can write that directory has the same OS-level
  authority as rdlt: manifest BLAKE3 trailers detect accidental/torn damage;
  they do not authenticate records against a directory writer.
- Corrupt or undecodable WAL data degrades to source re-extraction when safe.
  A WAL naming a different pipeline is preserved and refused rather than
  cleared.

## Reporting a vulnerability

Please report suspected vulnerabilities privately to the repository owners
before opening a public issue. Include the affected revision, reachable input
boundary, impact, and a minimal reproducer when possible. Do not include live
credentials or production data.
