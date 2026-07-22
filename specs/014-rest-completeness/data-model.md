# Data Model: REST Source Completeness

## 1. Source-level config (`RestConfig` — additive evolution)

| Field | Type | Default | Notes |
|---|---|---|---|
| `base_url` | string | required | unchanged |
| `auth` | Auth | `none` | grows: `api_key`, `oauth2_client_credentials` (§2) |
| `headers` | map | `{}` | NEW: source-level default headers |
| `params` | map | `{}` | NEW: source-level default query params |
| `max_concurrency` | u32 | 1 | NEW: parent-child fan-out cap |
| `min_request_interval_ms` | u64 | 0 | NEW: pacing floor between requests |
| `retry_after_cap_secs` | u64 | 300 | NEW: max in-source Retry-After wait |
| `max_pages` | u64 | 10000 | NEW: per-stream loop guard bound |
| `streams` | [RestStream] | required | §3 |

## 2. Auth (frozen spellings + additions)

| Variant | Fields | Secret fields |
|---|---|---|
| `none` \| `bearer` \| `header` \| `basic` | as today (frozen) | token / value / password → `Secret` |
| `api_key` | `name`, `key`, `location: header\|query` (default header) | `key` |
| `oauth2_client_credentials` | `token_url`, `client_id`, `client_secret`, `scopes: []`, `audience?`, `expiry_margin_secs: 60` | `client_secret` (+ fetched token) |

`Secret(String)`: serde-transparent, Debug/Display = `***`; the
schema example generator emits placeholders. Token cache:
single-flight, expiry-margin refresh, one 401 re-fetch then fatal.

## 3. Stream config (`RestStream` — additive)

| Field | Type | Default | Notes |
|---|---|---|---|
| `name`, `path`, `params`, `primary_key`, `type_hints` | as today | | frozen |
| `method` | `get`\|`post` | `get` | NEW |
| `body` | JSON template | absent | NEW; POST only (typed otherwise) |
| `headers` | map | `{}` | NEW: merged over source headers |
| `records_path` | selector string | absent | UPGRADED: JSONPath subset (dot + `[*]` + `[N]`); old dot-paths parse identically |
| `pagination` | Pagination | `none` | grows (§4) |
| `incremental` | block | absent | NEW (§5); old `cursor_field`/`cursor_param` = parsing aliases |
| `response_actions` | [ResponseAction] | `[]` | NEW (§6) |
| `parent` | block | absent | NEW (§7) |

## 4. Pagination (frozen 3 + new 4)

| Variant | Fields | Termination |
|---|---|---|
| `none` | — | one page |
| `page` | `page_param`, `start`, `total_pages_path?`, `total_count_path?` | empty page, or declared total reached |
| `offset` | `offset_param`, `limit_param`, `page_size`, `total_count_path?` | short page, or total reached |
| `cursor` | `cursor_path` (selector), `cursor_param` | cursor absent/null |
| `header_cursor` | `header`, `cursor_param` | header absent |
| `next_url` | `next_url_path` (selector) | field absent/null |
| `link_header` | — (RFC5988 `rel="next"`) | no next link |

All: same-request loop guard + `max_pages` → typed error naming stream
+ state. Library seam: `trait Paginator { fn next(&mut self, response
summary) -> PageDecision }` — config variants compile to it; wrappers
may implement it.

## 5. Incremental block

`{cursor_field, start_param?, end_param?, initial_value?}` — cursor
mechanics unchanged (max-observed, checkpoint-after-rows, resume via
`since`); `start_param` carries the resume value (alias of old
`cursor_param`), `end_param` closes a window when an explicit value is
declared.

## 6. Response actions

`{status?: u16, content_contains?: string, action:
ignore|end_stream|error}` — first match wins; declared-only; content
match bounded to 64KiB. `ignore` = treat as empty page (pagination
still advances/terminates per family); `end_stream` = clean stop;
`error` = explicit fatal (documenting intent).

## 7. Parent block

`{stream, placeholders: {token: parent_field_selector}, include:
[parent_fields]}` — parent must be a declared stream (validated,
ordering enforced); placeholders substitute into path/params/body;
included fields land as `_parent_<name>` (collision-typed). Child
checkpoints at feed end only (recorded caveat, 010-shape).

## 8. Fail points

`rest.request`, `rest.decode`, `rest.checkpoint` — registered in
`FAIL_POINTS`, swept engine-side with armed-fire pins.

## 9. Composed example (US3 proof)

`examples/composed_api.rs` (or module in tests): a mini "named API
connector" built ONLY from public pieces — config generator + custom
`Paginator` impl + the standard client — read through the engine
against wiremock. The PokeAPI live cell (`RDLT_NET=1`) doubles as the
real-world composition proof (list stream + `{name}` detail stream).
