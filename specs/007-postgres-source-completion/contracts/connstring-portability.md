# Contract: libpq Connection-String Portability

One front-end at the shared parse gate (`crates/rdlt-postgres/src/tls.rs`
conn path — both connectors). Goal: no syntactically valid libpq
connection string ever fails with a BARE parse error.

## Translation rules

| # | Rule |
|---|---|
| P1 | `sslrootcert=PATH` is extracted (key=value AND URL-query forms) and becomes the TLS trust root, exactly as if `tls.root_cert` had been configured. `sslrootcert=system` selects the platform trust store (the existing native-roots default). |
| P2 | `sslcert=PATH` / `sslkey=PATH` are extracted and become the client credential (contract tls-client-auth.md). The both-or-neither rule applies across SOURCES of the values (one from the string, one from the block is fine). |
| P3 | A conn-string TLS parameter and a TLS-block field that DISAGREE fail typed, naming both sides (`sslrootcert=… vs tls.root_cert=…`) — the same consistency rule sslmode already has. Agreeing duplicates are accepted. |
| P4 | After extraction, the remainder goes to the driver parser. Any parameter IT rejects is re-wrapped into a typed error that NAMES the parameter and, where an rdlt alternative exists, points at it (`sslpassword` → "encrypted keys unsupported"; `gssencmode` → "GSS not supported"). Never a bare "invalid connection string". |
| P5 | libpq's implicit file defaults (`~/.postgresql/root.crt`, `postgresql.crt`, `postgresql.key`) are NOT emulated: absent explicit configuration, behavior is exactly today's. Documented. |
| P6 | Extraction changes NOTHING else about the string: host lists, ports, options, application_name, target_session_attrs etc. pass through byte-identical. |

## application_name default

| # | Rule |
|---|---|
| A1 | When the parsed config carries no `application_name`, both connectors set `rdlt` before connecting — visible in `pg_stat_activity.application_name` for every rdlt session. |
| A2 | A user-supplied `application_name` (conn string) is never overridden. |

## Conformance

Unit corpus (no container): real-world URL shapes — postgres:// and
key=value forms with `sslrootcert`, all three params, `system`,
agreeing and disagreeing block combos, unknown-parameter rejections
each asserting the parameter name appears in the error. Container
cells: `sslrootcert=` URL syncs against the TLS fixture with an empty
tls block (source + destination); `pg_stat_activity` shows `rdlt`
during a sync (SC-006).
