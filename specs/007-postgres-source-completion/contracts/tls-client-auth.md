# Contract: TLS Client Authentication (mTLS)

Extends `specs/006-postgres-completeness/contracts/tls-policy.md`.
Applies identically to BOTH postgres connectors through the shared
connect path.

## Configuration

```yaml
tls:
  mode: verify_full            # any mode except disable
  root_cert: /etc/rdlt/ca.pem
  client_cert: /etc/rdlt/client.pem   # path or inline PEM
  client_key: /etc/rdlt/client.key    # path or inline PEM; PKCS#8/RSA/SEC1, unencrypted
```

Conn-string equivalents `sslcert=`/`sslkey=` translate to the same
fields (contract connstring-portability.md).

## Rules

| # | Rule |
|---|---|
| C1 | When both `client_cert` and `client_key` resolve, the client presents the certificate chain during the TLS handshake in every mode where TLS is active (prefer/require/verify_ca/verify_full). Server trust evaluation of US is unchanged per mode. |
| C2 | `client_cert` without `client_key` (or vice versa) is a config error at open naming the missing counterpart. Credential + `mode: disable` is a contradiction error. Unreadable/unparseable/encrypted material is a config error naming the input — all BEFORE any connection attempt. |
| C3 | Server rejection of the client credential is the distinguished connect failure `ClientCert`, produced from EITHER layer: TLS-level certificate alerts (certificate_required, bad_certificate, unknown_ca, handshake_failure during client-auth) OR Postgres auth-phase SQLSTATE 28000. It is disjoint from `TrustAnchor`/`Chain`/`Hostname` (which concern our verification of the server). |
| C4 | A key that does not match the certificate fails at config-construction time when detectable, else surfaces as `ClientCert` — never a silent retry loop (the failure is Fatal, not transient). |
| C5 | Verifier composition: client auth combines with every server-verification posture, including the require (accept-any) and verify_ca (chain-only) custom verifiers. No mode changes meaning when a credential is added. |

## Conformance matrix (tls_matrix.rs additions)

| Server posture | Client config | Expected |
|---|---|---|
| cert auth (ssl_ca_file, `hostssl … cert`) | valid cert+key, verify_full | sync succeeds (source AND destination) |
| cert auth | no client credential | connect fails `ClientCert`, message names the missing credential |
| cert auth | wrong-CA client cert | connect fails `ClientCert` |
| cert auth | cert with mismatched key | config error before connect |
| plain hostssl (no client verification) | valid cert+key | sync succeeds (credential offered, unused) |

## Out of scope (documented)

Encrypted private keys; CRL/OCSP revocation; libpq's implicit
`~/.postgresql/postgresql.{crt,key}` defaults (explicit config only).
