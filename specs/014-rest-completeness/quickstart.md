# Quickstart: REST Source Completeness

## Point it at a real API

```yaml
# rest-source.yaml
base_url: https://api.example.com
auth:
  oauth2_client_credentials:
    token_url: https://auth.example.com/token
    client_id: my-client
    client_secret: "…"
    scopes: [read]
min_request_interval_ms: 100
streams:
  - name: orders
    path: /v2/orders
    records_path: data.items[*]
    pagination: {type: cursor, cursor_path: meta.next_cursor, cursor_param: cursor}
    incremental: {cursor_field: updated_at, start_param: updated_since}
    response_actions:
      - {status: 404, action: end_stream}
  - name: order_lines
    path: /v2/orders/{order_id}/lines
    parent: {stream: orders, placeholders: {order_id: id}, include: [id]}
```

## Verify

```bash
cargo nextest run -p rdlt-connector-rest              # conformance (wiremock mocks)
cargo nextest run -p rdlt-connector-rest --features failpoints -E 'binary(sweep)'  # rest crash points
RDLT_NET=1 cargo nextest run -p rdlt-connector-rest -E 'test(pokeapi)'  # live PokeAPI cell
cargo llvm-cov nextest -p rdlt-connector-rest         # coverage floor ≥80%
TARGET=rest-pg-100k make bench                        # gated bar stays ≥5×
```

## Compose a named connector (US3)

Build from the public pieces only — see the in-crate composed example:
config generator + (optionally) a custom `Paginator` impl + the standard
client; auth/retry/pacing/extraction inherited, never re-implemented.

## The rules

`contracts/rest-source.md` (RS1–RS8): declarative-only configs, loop
guards, S3 retry posture, secret redaction, streaming preserved, public
composition seam, crash discipline, matrix + parity + PokeAPI cell.
