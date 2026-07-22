# Research: REST Source Completeness

## R1 — Architecture: three-layer split inside one crate

**Decision**: restructure `rdlt-connector-rest` into three public layers
(source-mirroring the postgres crate's module discipline):
`client/` (HTTP execution: auth attachment + OAuth2 lifecycle,
transient/fatal classification, Retry-After waits, pacing),
`read/` (paginator vocabulary + the `Paginator` trait seam, records
extraction, incremental binding, parent-child resolution), and
`config.rs` (the declarative document; grows additively). `RestSource`
composes the three; a wrapper connector composes the same three with
its own config generator (US3). Everything stays in THIS crate — no new
crate: the "rest-core" pattern would repeat sqlcore's role, but unlike
SQL destinations there is exactly ONE consumer today; extract when the
second in-workspace API connector actually exists (the 013 lesson:
extract against two consumers, not zero).

**Alternatives considered**: separate `rdlt-connector-restcore` crate
(rejected: premature — the public module surface gives wrappers the
same composition power; a crate split later is mechanical);
callback-based hooks in config (rejected per spec assumption — configs
stay data).

## R2 — Pagination vocabulary (dlt-surface-grounded) + the seam

**Decision**: the declarative `Pagination` enum grows to: `none`,
`page` (+ `total_pages_path`/`total_count_path` optional stop), `offset`
(+ `total_count_path` optional stop), `cursor` (response-body JSONPath →
request param), `header_cursor` (response header → request param),
`next_url` (response-body JSONPath holding the next URL),
`link_header` (RFC5988 `Link: rel=next`). Termination rules per family:
empty page / short page / absent cursor / absent link / count reached.
Every family runs under one loop guard: a page yielding a request
IDENTICAL to the previous one, or exceeding `max_pages`
(default 10_000), is a typed error naming stream + paginator state
(FR-001). The `Paginator` TRAIT is the library seam: config variants
compile to trait objects; wrappers may implement it for API quirks
(US3-AS3). Serde spellings of the existing three variants are frozen
(FR-011).

**dlt mapping recorded for the parity table**: SinglePagePaginator→none,
PageNumberPaginator→page, OffsetPaginator→offset,
JSONResponseCursorPaginator→cursor, HeaderCursorPaginator→header_cursor,
JSONLinkPaginator→next_url, HeaderLinkPaginator→link_header. dlt's
paginator auto-detection is deliberately NOT ported — silent guessing
against the no-silent-failures principle; a missing paginator with a
`next`-looking response gets a HINT in the typed error instead.

## R3 — Auth: OAuth2 client-credentials, single-flight, redacted

**Decision**: `Auth` grows `ApiKey { name, location: header|query }`
and `Oauth2ClientCredentials { token_url, client_id, client_secret,
scopes, audience?, expiry_margin_secs (default 60) }`. Token lifecycle:
lazy fetch, cached with expiry margin, single-flight refresh (tokio
Mutex around an Option<Token>), ONE re-fetch on 401 then fatal
(spec edge case: post-refresh 401 = wrong credentials, never a loop).
Secrets: a `Secret(String)` newtype with manual Debug (`"***"`),
serde-transparent for config parsing, used for token/password/secret
fields; the grep-proof cell renders Debug + error paths and asserts no
secret substring (SC-002). dlt's OAuthJWTAuth (private-key JWT grant)
is OUT this feature — recorded in the parity table (needs an RSA
signing dep; revisit with the first connector that requires it, e.g.
Google service accounts — noted as the likely GSC prerequisite).

## R4 — Records selection: JSONPath subset, streaming posture kept

**Decision**: selector language = dot paths + `[*]` wildcards + `[N]`
indices (`data.items[*].payload` selects each payload object), parsed
into a typed `Selector` at config validation (typed error on
unsupported syntax, naming the supported subset). No external JSONPath
crate — the subset is ~80 lines over serde_json::Value and the 009
crate-survey rule applies (hand-roll when the dep buys little). The
no-records_path fast path (body streams through untouched) is
PRESERVED — the flagship bench rides it; selector extraction costs one
parse+reserialize exactly as today. No-match stays a typed error naming
the path and the response's top-level keys (US1-AS3).

## R5 — Request shape, response actions, pacing

**Decision**:
- `method: get|post` (default get) + `body:` JSON template per stream;
  pagination params merge into query for GET and into the body's
  declared param names for POST-cursor APIs (the dlt search-endpoint
  pattern).
- `response_actions`: list of `{status?, content_contains?, action:
  ignore|end_stream|error}` — first match wins, declared-only
  (allow-list posture, FR-004); `content_contains` bounded to the first
  64KiB of body.
- Pacing: source-level `max_concurrency` (default 1 — streams already
  read sequentially today; >1 applies to parent-child fan-out) and
  `min_request_interval_ms` (default 0); Retry-After honored up to
  `retry_after_cap_secs` (default 300) IN the source for 429/503 —
  beyond the cap, the existing RateLimited/transient classification
  surfaces to the engine budget unchanged (S3 posture preserved: the
  source never loops on its own beyond the declared waits).

## R6 — Incremental: start/end params over the existing cursor machinery

**Decision**: a stream-level `incremental` block —
`{cursor_field, start_param, end_param?, initial_value?}` — SUPERSEDING
the current flat `cursor_field`/`cursor_param` (which stay as parsing
aliases, frozen spellings). Mechanics unchanged: max-observed cursor,
checkpoint after rows (S2), resume via `since`. `end_param` closes the
window at read start (value = "now" is NOT synthesized — closed windows
take an explicit value or come from the next feature; recorded).

## R7 — Parent-child composition

**Decision**: a child stream declares
`parent: {stream, placeholders: {name: parent_field_path}, include:
[parent_field, ...]}`; `{placeholder}` tokens live in path/params/body.
Execution: the parent stream is read FIRST within the same engine run
(ordering constraint enforced at validation — a child's parent must be
a declared stream); parent records buffer their resolved placeholder
values (bounded: only the referenced fields, not whole records); the
child then issues one paginated sequence per parent value-set. Child
records optionally embed declared parent fields (prefixed
`_parent_<field>` — collision-checked typed). Failures name the
resolved values (spec edge case). Engine cursor semantics: the child
checkpoints only at its own feed end (a mid-parent checkpoint would
resume half-fanned-out — same shape as the 010 scoped-stream caveat,
recorded).

## R8 — Crash points + verification

**Decision**: fail points `rest.request` (before each HTTP send),
`rest.decode` (after body read, before extraction), `rest.checkpoint`
(before cursor checkpoint) — swept via the engine crash_sweep harness
(the rest source joins the existing sweep matrix) with armed-fire pins.
Conformance: wiremock-driven cells per pagination family × auth × action
(the existing mock discipline), engine-driven totals. Coverage: baseline
measured in T001, ≥80% floor, `-p rdlt-connector-rest`. PokeAPI live
cell (FR-013): `RDLT_NET=1`-gated test reading
`/api/v2/pokemon?limit=100` (next_url pagination) + a parent-child
detail stream (`/api/v2/pokemon/{name}`), structural asserts, pacing
100ms — good-citizen. Gated REST→PG bar re-measured before close-out.

## R9 — Dependencies

**Decision**: ZERO new external dependencies. OAuth2
client-credentials is one POST + JSON parse (reqwest + serde — no
oauth2 crate; the 009 survey discipline). Link-header parsing is ~20
lines (RFC5988 subset: `<url>; rel="next"`). JSONPath subset
hand-rolled (R4). Base64 for basic auth already rides reqwest.
