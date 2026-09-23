# Security

## Trust model

- Connectors are trusted code: the operator chooses which connector binaries or packages run.
  rdlt does not sandbox them. Deployments that need isolation run connectors in separate
  containers and reach them over TCP with mutual TLS.
- Connector output is untrusted data. Every size limit applies to it, decoding failures are typed
  errors rather than crashes, and connector text is sanitized before it reaches a terminal.
- Secrets are redacted in logs, events, reports and error messages.

## Reporting a vulnerability

Report vulnerabilities privately through GitHub: open the repository's **Security** tab and choose
**Report a vulnerability**. Do not open a public issue.
