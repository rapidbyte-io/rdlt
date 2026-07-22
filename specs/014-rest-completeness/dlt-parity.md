# dlt parity record — REST source (014)

Reference: dlt 1.29.0 `rest_api` source + `dlt.sources.helpers.rest_client`
(the surface surveyed in research.md R2/R3). Verdict per row: PARITY
(equivalent declarative capability), DEVIATION (deliberately different),
or OUT (not carried this feature, with the revisit trigger).

## Paginators

| dlt | rdlt `pagination.type` | verdict |
|---|---|---|
| `SinglePagePaginator` | `none` | PARITY |
| `PageNumberPaginator` | `page` (+`total_pages_path`/`total_count_path` stops) | PARITY |
| `OffsetPaginator` | `offset` (+`total_count_path` stop) | PARITY |
| `JSONResponseCursorPaginator` | `cursor` (body selector → param; query for GET, body for POST) | PARITY |
| `HeaderCursorPaginator` | `header_cursor` | PARITY |
| `JSONLinkPaginator` | `next_url` (absolute + relative) | PARITY |
| `HeaderLinkPaginator` | `link_header` (RFC5988 `rel="next"`) | PARITY |
| paginator auto-detection | not ported | DEVIATION — silent guessing violates the no-silent-failures principle; a missing paginator against a `next`-looking response stays a plain single page, and misconfigured paginators die on the same-request/`max_pages` guards with typed errors naming the state. |
| custom paginator classes | the public `Paginator` trait | PARITY (the seam; `children.rs::composed_example_runs_through_the_engine` proves composition over public pieces) |

## Auth

| dlt | rdlt `auth` | verdict |
|---|---|---|
| `BearerTokenAuth` | `bearer` | PARITY |
| `APIKeyAuth` (header/query) | `api_key` (`location: header\|query`) | PARITY (dlt's cookie location OUT — no consumer; add with the first cookie-auth API) |
| `HttpBasicAuth` | `basic` | PARITY |
| `OAuth2ClientCredentials` | `oauth2_client_credentials` (+`audience`, `expiry_margin_secs`; single-flight cache, ONE 401 re-fetch then fatal) | PARITY |
| `OAuthJWTAuth` (private-key JWT grant) | — | OUT — needs an RSA signing dep (R9 zero-new-deps). Revisit with the first connector that requires it: Google service accounts, the likely Google-Search-Console prerequisite. |
| custom `AuthConfigBase` classes | arbitrary `header` scheme + the config seam | DEVIATION — no user-supplied auth classes in a declarative document; the composition layer wraps `RestConfig` programmatically instead. |

## Config surface

| dlt | rdlt | verdict |
|---|---|---|
| `client.base_url` / `headers` / `params` | source-level `base_url`/`headers`/`params`, merged UNDER stream values | PARITY |
| `resource.endpoint.{path,method,json,params}` | stream `path`/`method`/`body`/`params` | PARITY |
| `data_selector` (full JSONPath) | `records_path` (dot + `[*]` + `[N]` subset, typed at parse) | DEVIATION — hand-rolled subset (R4, 009 crate-survey rule); unsupported syntax is a typed error NAMING the subset, and the no-selector byte-identical fast path is preserved for the flagship bench. |
| auto data-selection (longest-list heuristic) | not ported | DEVIATION — same no-silent-guessing posture as paginator auto-detection; a wrong shape is a typed no-match error naming path + top-level keys. |
| `incremental` (cursor + start/end params) | `incremental` block `{cursor_field, start_param, end_param+end_value, initial_value}`; pre-014 flat aliases frozen | PARITY |
| `response_actions` (status/content → ignore/raise + callables) | `response_actions` `{status?, content_contains?, action: ignore\|end_stream\|error}`, first-match, declared-only | PARITY for the declarative forms; callable handlers → the composition seam (DEVIATION by design). |
| `resolve`-type params (parent-child) | `parent: {stream, placeholders, include}`, `_parent_<field>` embedding | PARITY (dlt's `include_from_parent` ≙ `include`) |
| transformers / `@dlt.transformer` | — | OUT of the connector — transformation is engine territory (001 architecture), not a REST-source concern. |
| rate limiting (external `requests` hooks) | first-class: `min_request_interval_ms`, `retry_after_cap_secs` bounded in-source waits, `max_concurrency` fan-out limit | PARITY+ (declared, bounded, observable in cells — not delegated to a session object). |

## Discipline dlt does not have

Recorded so the deviation ledger cuts both ways: typed loop guards
(same-request fingerprint + `max_pages`), secrets grep-proof over every
rendering, crash points with armed-fire sweep pins and exactly-once
totals through a real destination, live-API cells gated skip-not-fail,
and the behavior-preservation net (pre-014 cells green unchanged
through the rewrite).
