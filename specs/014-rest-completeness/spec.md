# Feature Specification: REST Source Completeness

**Feature Branch**: `014-rest-completeness`

**Created**: 2026-07-22

**Status**: Draft

**Input**: User description: "Make the REST source connector
comprehensive, configurable, flexible, high performance, tested
throughout (+crash points), and designed so other API connectors (e.g.
Google Search Console) can be built as compositions on top of it. Fully
featured; look at ../dlt's rest_api source for the reference surface."

## User Scenarios & Testing *(mandatory)*

### User Story 1 - Any real API, declaratively (Priority: P1)

An operator points the REST source at a real-world API — GitHub, Stripe,
a paginated internal service — and expresses its entire read contract in
the config document: how pages chain (page numbers, offsets, cursors in
the response body or headers, next-URLs in the body or the `Link`
header, total-count driven ranges), how requests authenticate (bearer,
basic, API key in header or query, arbitrary headers, OAuth2
client-credentials with automatic token refresh on expiry), where the
records live in the response (a JSONPath-style selector, not just a
dot-path), request shape (method, per-stream headers and params, JSON
body for POST-style search endpoints), and how non-2xx or
special-content responses are handled (typed error by default; declared
response actions can ignore specific statuses or treat them as
end-of-stream). Every knob is validated up front with typed errors
naming the offending field; every documented default is real.

**Why this priority**: this is the feature — the current surface (3
paginations, 4 auth schemes, dot-path selector, single GET shape) covers
toy APIs; the reference competitor covers ~8 paginators, OAuth2, POST
bodies, and response actions. The gap is what blocks real connectors.

**Independent Test**: a mock API exercising each pagination family, auth
scheme, and response action is read to exactly-correct totals through
the engine; a config with any invalid combination fails typed at parse.

**Acceptance Scenarios**:

1. **Given** each pagination style (page-number, offset-limit,
   response-body cursor, response-header cursor, next-URL in body,
   `Link` header rel=next, total-count range, single page), **When** a
   stream reads a multi-page feed, **Then** every record arrives exactly
   once, termination is correct (empty page / missing cursor / absent
   link / count reached), and a paginator that stops progressing is a
   typed error, never an infinite loop.
2. **Given** each auth scheme incl. OAuth2 client credentials, **When**
   requests are issued, **Then** credentials attach correctly (header or
   query), OAuth2 tokens are fetched lazily, cached, and refreshed on
   expiry/401-retry, and secrets never appear in logs or errors.
3. **Given** a records selector targeting nested arrays (`data.items`,
   wildcard paths), **When** responses arrive, **Then** records are
   extracted correctly, and a selector matching nothing is a typed
   error naming the path and the response's top-level shape.
4. **Given** declared response actions (e.g. 404 → end-of-stream, 403 on
   a specific content match → skip record page), **When** such responses
   arrive, **Then** the declared action applies; undeclared non-2xx
   remain typed errors carrying status + body excerpt.
5. **Given** POST-style endpoints (JSON body templates), **When** the
   stream reads, **Then** the body is sent with pagination applied per
   the declared strategy.

---

### User Story 2 - Incremental, resumable, and rate-limit-correct (Priority: P2)

Streams read incrementally against real API semantics: the cursor field
feeds a declared request parameter (start param; optionally an end param
for closed windows), values checkpoint through the engine's existing
cursor machinery, and resumption re-reads nothing it shouldn't. The
client behaves like a good API citizen under load: typed
transient-vs-fatal classification (5xx/timeouts/connection = transient
for the ENGINE's retry budget; 4xx = fatal unless a response action says
otherwise), `Retry-After` respected on 429/503 within a bounded wait,
and concurrency/pacing knobs (max in-flight requests, minimum request
interval) so a pipeline never hammers an API. Crash discipline as
everywhere: fail points across the read path with armed-fire pins
proving crash/rerun exactly-once through the engine.

**Why this priority**: incremental + politeness is what makes the
connector production-usable rather than demo-usable; crash points are
the house bar.

**Independent Test**: a resumed run against a mock API with a cursor
param re-requests only the tail; a 429 with Retry-After delays and
succeeds; the crash sweep passes with every point firing.

**Acceptance Scenarios**:

1. **Given** `incremental: {cursor_field, start_param}` and a committed
   cursor, **When** the next run starts, **Then** the request carries
   the cursor value in the declared param and totals stay exact under
   the engine's dedup rules; with an `end_param` the window is closed.
2. **Given** a 429/503 with `Retry-After`, **When** within the bounded
   wait, **Then** the client waits and retries; beyond it, a typed
   transient error surfaces to the engine's budget.
3. **Given** pacing knobs, **When** a stream reads many pages, **Then**
   requests respect the declared minimum interval and in-flight cap.
4. **Given** the crash sweep (fail points at request issue, response
   decode, and checkpoint boundaries), **When** armed under
   return/panic/skip-first actions, **Then** every point fires (pinned)
   and crash/rerun converges to exact totals.

---

### User Story 3 - A composition layer for API connectors (Priority: P3)

A developer building a NAMED API connector (the Google Search Console
example) composes it from this crate instead of hand-rolling HTTP: the
building blocks — client (auth + retry + pacing), paginator vocabulary,
records extraction, endpoint description — are usable as a LIBRARY with
a typed builder, so a wrapper connector ships as "a config generator
plus API-specific glue" rather than a fork. Parent-child endpoint
composition works declaratively too: a child stream's path/params can
be resolved from each record of a parent stream (e.g.
`/repos/{owner}/{repo}/issues` fed by a repositories stream), with the
parent fields optionally included in child records.

**Why this priority**: this is the strategic ask — rapidbyte's connector
catalog multiplies through composition; dlt's rest_api plays exactly
this role for its verified sources.

**Independent Test**: an example "API connector" built ONLY from the
crate's public pieces (no raw HTTP) reads a mock nested API
(parent-child) through the engine; the child stream sees resolved
params and includes parent fields.

**Acceptance Scenarios**:

1. **Given** a child endpoint declaring `{placeholder}` params resolved
   from a parent stream's records, **When** the pipeline runs, **Then**
   the child issues one request sequence per parent record, records
   include any declared parent fields, and totals are exact.
2. **Given** the library surface, **When** the example composed
   connector is built, **Then** it uses only public builder/type APIs
   (compile-time proof: the example lives in the crate and builds in
   CI), and auth/retry/pagination behavior is inherited, not
   re-implemented.
3. **Given** a wrapper needing a custom quirk (e.g. an API-specific
   pagination twist), **When** it implements the paginator seam, **Then**
   the rest of the stack (auth, retry, extraction, engine wiring) is
   reused unchanged.

---

### Edge Cases

- **Infinite-loop protection**: any paginator that produces the same
  request twice, or exceeds a configurable max-page bound, is a typed
  error naming the stream and paginator state — never a hung pipeline.
- **Secrets hygiene**: tokens/passwords/client secrets never render in
  Debug, logs, error messages, or the generated config schema examples.
- **Clock-skew on OAuth2**: token expiry is treated with a safety
  margin; a 401 after refresh is fatal (credentials wrong), not a retry
  loop.
- **Large responses**: record extraction streams from the response body
  without buffering the whole page set; memory stays bounded by the
  engine's byte budget as today.
- **Response actions vs typed errors**: actions are ALLOW-lists for
  declared statuses/content only; anything undeclared keeps the strict
  typed-error posture (no silent tolerance creep).
- **Parent-child failure isolation**: a child request failing for ONE
  parent record is a typed error naming the parent's resolved values —
  fail loudly, no partial-silent skips (unless a response action
  declares otherwise).
- **Config compatibility**: existing configs (current pagination/auth
  spellings) keep parsing unchanged — additions are additive; the
  generated JSON schema and round-trip tests extend accordingly.

## Requirements *(mandatory)*

### Functional Requirements

- **FR-001**: Pagination MUST cover: single-page, page-number,
  offset-limit, response-body cursor, response-header cursor, body
  next-URL, `Link`-header next, and total-count driven ranges — each
  with correct termination, a same-request/max-pages loop guard (typed),
  and declarative parameter naming.
- **FR-002**: Auth MUST cover: none, bearer, basic, API key
  (header/query), arbitrary header, and OAuth2 client-credentials
  (token URL, client id/secret, scopes, auto-refresh with expiry
  margin, single-flight token fetch). Secrets MUST be redacted from all
  Debug/log/error output.
- **FR-003**: Endpoints MUST support: GET and POST with JSON body
  templates, per-stream headers/params merged over source-level
  defaults, and a JSONPath-subset records selector (dot paths, array
  wildcards) with typed no-match errors.
- **FR-004**: Response handling MUST be typed-error by default (status +
  bounded body excerpt) with declarative response actions for declared
  statuses/content: `ignore` (empty page), `end_stream`, or `error`
  (explicit). Transient-vs-fatal classification MUST follow the house
  rule (5xx/timeout/connect transient; 4xx fatal unless declared).
- **FR-005**: Incremental MUST bind the committed cursor to a declared
  request parameter (start; optional end for closed windows), riding
  the existing engine cursor machinery — resumption exact, initial
  value supported, cursor field extractable from records as today.
- **FR-006**: The client MUST respect `Retry-After` on 429/503 within a
  bounded wait, and expose pacing knobs: max concurrent requests and
  minimum request interval per source. Defaults MUST be conservative
  and documented.
- **FR-007**: Parent-child composition MUST work declaratively:
  placeholder params in a child endpoint resolved per parent record,
  optional parent-field inclusion in child records, loud typed errors
  naming resolved values on child failures.
- **FR-008**: The crate MUST expose its building blocks as a public
  library surface (client, paginator seam, extraction, endpoint
  builder) sufficient to build a named API connector without raw HTTP —
  proven by a composed example connector in-crate.
- **FR-009**: The read path MUST carry fail points (request, decode,
  checkpoint boundaries) swept with armed-fire pins; crash/rerun MUST
  converge to exact totals through the engine.
- **FR-010**: Performance MUST not regress: the gated REST→PG bar
  (≥5× vs dlt) stays green; response-to-record flow stays streaming
  (bounded memory). New scoreboard cells only if measurement-first
  justified.
- **FR-011**: Existing config documents MUST keep parsing (additive
  evolution); the generated JSON schema, round-trip tests, and README
  reference MUST cover every new field; validation errors MUST name
  fields.
- **FR-012**: Verification to the house standard: traceability
  matrix for the full option surface, ≥80% measured line coverage for
  the crate (baseline first), conformance through the engine against
  mock APIs for every pagination/auth/action row.

### Key Entities

- **Source config**: base URL, source-level auth/headers/params/pacing,
  streams.
- **Stream/endpoint**: path, method, body template, selector,
  pagination, incremental binding, response actions, parent linkage.
- **Paginator**: the termination/progression contract (declarative
  vocabulary + a library seam for custom implementations).
- **Auth provider**: credential attachment + lifecycle (OAuth2 refresh).
- **Response action**: declared (status/content) → behavior mapping.
- **Composed connector example**: the in-crate proof of the library
  surface.

## Success Criteria *(mandatory)*

- **SC-001**: Every pagination family reads a mock multi-page API to
  exact totals with correct termination; loop guards fire typed.
- **SC-002**: Every auth scheme attaches correctly; OAuth2 refresh works
  under expiry and 401; secrets provably absent from output (grep-proof
  cell over Debug/error renderings).
- **SC-003**: Parent-child example connector (public-API-only) reads a
  nested mock API through the engine with exact totals and parent-field
  inclusion.
- **SC-004**: Incremental start/end param binding proven through the
  engine (resume re-requests only the tail); Retry-After honored in a
  live-mock cell; pacing observable.
- **SC-005**: Crash sweep green with every fail point pinned firing;
  conformance suite passes; coverage ≥80% recorded with classified
  exclusions; matrix zero uncited rows.
- **SC-006**: Gated REST→PG benchmark bar stays green; `make check`,
  doc-tests, schema round-trips, semver ("no update required" for
  frozen crates) all green.

## Assumptions

- The reference surface is dlt 1.29.0's `rest_api` source + rest_client
  helpers (paginators/auth/retry audited from the checkout in
  `../dlt`); parity is recorded per-option like 013, with deliberate
  deviations documented individually.
- JSONPath support is the practical subset (dot paths, array
  wildcards, indices) — not a full JSONPath engine; the boundary is
  documented and typed.
- OAuth2 is client-credentials (+ the refresh mechanics); interactive
  flows (authorization-code) are out of scope for a headless engine.
- Custom per-API code-level hooks (dlt's Python callables in
  response_actions/processing_steps) translate to the LIBRARY seam, not
  to config-level callbacks — configs stay declarative data.
- New external dependencies are allowed ONLY if hand-rolling is clearly
  worse (planning decides per 009's crate-survey discipline); reqwest
  stays the HTTP client.

## Out of Scope

- Named API connectors themselves (Google Search Console etc.) — this
  feature builds the layer they'll compose on; the in-crate example
  proves the seam.
- Interactive OAuth flows; API-key rotation services; secret managers.
- GraphQL, SOAP, gRPC sources.
- Response caching layers; webhook/push ingestion.
- dlt features that are engine-level elsewhere (max_table_nesting,
  parallelized resources) or Python-callable-shaped (processing_steps
  as config) — recorded in the parity table, not silently dropped.
