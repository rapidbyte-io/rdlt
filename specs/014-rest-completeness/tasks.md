# Tasks: REST Source Completeness

**Input**: Design documents from `/specs/014-rest-completeness/`

**Prerequisites**: plan.md, research.md (R1–R9), data-model.md,
contracts/rest-source.md (RS1–RS8), quickstart.md

**Tests**: included — the standing discipline: the EXISTING conformance
cells are the rewrite's behavior-preservation net (green at every stage,
never edited beyond mechanical import paths); every new family/scheme/
option lands WITH its wiremock cells; the matrix commits WITH the cells
that close its gaps (011 rule).

**Organization**: tasks grouped by user story; US order is build order.
No big-bang rewrite commit — every task leaves the whole suite green.

## Phase 1: Setup

- [ ] T001 The weld: measure the crate coverage BASELINE
  (`cargo llvm-cov nextest -p rdlt-connector-rest`, recorded — 011 R2
  rule) and pin the existing behavior: run
  `cargo nextest run -p rdlt-connector-rest` + the engine e2e cells
  that ride the rest source; record the passing set in the task notes.
  Then the FAMILY LAYOUT move (owner direction): restructure to
  `crates/rdlt-connector-rest/src/source/{mod.rs,config.rs}` with
  `src/lib.rs` a thin façade (`pub mod source` + root re-exports of
  every currently-public item — the 013 finding-10 lesson); all
  existing tests + the CLI/bench/facade consumers compile UNCHANGED
  (moves only; import paths preserved via re-exports).

## Phase 2: Foundational (blocking all stories)

- [ ] T002 Secret hygiene + client extraction:
  `crates/rdlt-connector-rest/src/source/client/secret.rs`
  (`Secret(String)`: serde-transparent, Debug/Display `***`,
  schemars placeholder) applied to existing auth fields (bearer token,
  basic password, header value — spellings frozen); extract the HTTP
  execution from `read` into
  `crates/rdlt-connector-rest/src/source/client/mod.rs` (`RestClient`:
  build+send+classify, the existing 429/5xx/4xx classification moved
  verbatim, source-level headers/params merge, `min_request_interval_ms`
  pacing + `retry_after_cap_secs` bounded waits per R5); grep-proof
  cell in `crates/rdlt-connector-rest/tests/auth.rs` (Debug + error
  renderings contain no secret substring); existing conformance cells
  green.
- [ ] T003 Paginator seam:
  `crates/rdlt-connector-rest/src/source/read/paginate.rs` — public
  `Paginator` trait (bounded response summary → `PageDecision`),
  the frozen three families (none/page/offset) rewired through it with
  the same-request hash guard + `max_pages` (typed error naming stream
  + state), and `crates/rdlt-connector-rest/src/source/read/mod.rs`
  owning the loop; EXISTING conformance cells green unchanged (the
  net); guard cells (same-request loop, max_pages) in
  `crates/rdlt-connector-rest/tests/pagination.rs`.

**Checkpoint**: layout + seams in place, zero behavior change proven.

## Phase 3: User Story 1 — Any real API, declaratively (P1) 🎯 MVP

**Goal**: the full pagination/auth/selector/action/POST surface,
validated typed, proven against wiremock.

**Independent test**: a mock API per pagination family × auth scheme ×
action reads to exact totals through the engine; invalid configs fail
typed at parse.

- [ ] T004 [US1] New pagination families in
  `crates/rdlt-connector-rest/src/source/read/paginate.rs` +
  `config.rs`: `cursor` (body selector → param), `header_cursor`,
  `next_url` (body selector → absolute/relative URL), `link_header`
  (RFC5988 rel=next, hand-rolled ~20 lines), plus `total_pages_path`/
  `total_count_path` stops on page/offset — termination cells per
  family (empty page / absent cursor / absent link / count reached) in
  `crates/rdlt-connector-rest/tests/pagination.rs` (wiremock,
  engine-driven totals).
- [ ] T005 [P] [US1] JSONPath-subset selector in
  `crates/rdlt-connector-rest/src/source/read/extract.rs`: dot paths +
  `[*]` + `[N]` parsed into a typed `Selector` at config validation
  (typed error naming the supported subset); records extraction keeps
  the no-selector passthrough byte-identical (RS5); no-match errors
  name path + response top-level keys; unit cells in-file + wiremock
  cells in `crates/rdlt-connector-rest/tests/actions.rs`.
- [ ] T006 [P] [US1] Auth additions in
  `crates/rdlt-connector-rest/src/source/client/auth.rs`: `api_key`
  (header/query) and `oauth2_client_credentials` (token URL POST via
  the same classify path, lazy single-flight cache, expiry margin, ONE
  401 re-fetch then fatal); wiremock token-server cells in
  `crates/rdlt-connector-rest/tests/auth.rs` (fetch, cache-hit,
  expiry refresh, 401-refetch-then-fatal, 5xx-token-fetch transient);
  Secret applied to key/client_secret/token.
- [ ] T007 [US1] Request shape + response actions:
  `method: get|post` + `body` templates (pagination params into query
  for GET / declared body params for POST-cursor, R5) in
  `crates/rdlt-connector-rest/src/source/{config.rs,read/mod.rs}`;
  `response_actions` (`{status?, content_contains?, action:
  ignore|end_stream|error}`, first-match, 64KiB content bound) applied
  in the read loop; cells in
  `crates/rdlt-connector-rest/tests/actions.rs` (404→end_stream,
  content-match ignore, undeclared 4xx stays typed, POST body + body
  pagination).

**Checkpoint**: US1 delivers the declarative surface; config docs parse
old documents unchanged (alias cells prove it).

## Phase 4: User Story 2 — Incremental + politeness + crash (P2)

**Goal**: start/end param binding, Retry-After + pacing behavior,
fail points swept.

**Independent test**: resume re-requests only the tail; 429 waits and
succeeds; sweep pins fire.

- [ ] T008 [US2] Incremental block
  (`{cursor_field, start_param, end_param?, initial_value?}`) in
  `crates/rdlt-connector-rest/src/source/config.rs` +
  `read/mod.rs`, with old `cursor_field`/`cursor_param` as frozen
  parsing aliases; resume/window cells in
  `crates/rdlt-connector-rest/tests/conformance.rs` (tail-only
  re-request asserted via wiremock request logs; end_param closes the
  window; alias round-trip).
- [ ] T009 [P] [US2] Politeness cells: Retry-After honored within
  `retry_after_cap_secs` (wiremock 429→success sequence, wait
  observed), beyond-cap surfaces RateLimited to the engine budget;
  pacing observable (request-timestamp spacing ≥ declared interval) —
  `crates/rdlt-connector-rest/tests/conformance.rs`.
- [ ] T010 [US2] Crash points: `rest.request`, `rest.decode`,
  `rest.checkpoint` in
  `crates/rdlt-connector-rest/src/source/read/mod.rs` + `FAIL_POINTS`
  registry in `src/source/mod.rs`; extend
  `crates/rdlt-engine/tests/crash_sweep.rs` with the rest-source arm
  (wiremock-backed source, armed-fire pins, crash/rerun exact totals).

## Phase 5: User Story 3 — The composition layer (P3)

**Goal**: parent-child + the public-pieces-only composed example +
PokeAPI live proof.

**Independent test**: the example connector (no raw reqwest) reads a
nested mock API through the engine; PokeAPI cell passes under
RDLT_NET=1.

- [ ] T011 [US3] Parent-child resolution in
  `crates/rdlt-connector-rest/src/source/read/resolve.rs` +
  `config.rs`: `parent: {stream, placeholders, include}` — validated
  (declared parent, collision-typed `_parent_*` fields), parent read
  first, bounded value buffering, one paginated child sequence per
  parent record, failures naming resolved values, child checkpoints at
  feed end only; cells in
  `crates/rdlt-connector-rest/tests/children.rs` (wiremock nested API,
  engine totals, include fields, failure naming).
- [ ] T012 [P] [US3] The composed example
  `crates/rdlt-connector-rest/examples/composed_api.rs`: a mini named-
  API connector built ONLY from public pieces (config generator + a
  custom `Paginator` impl for an API quirk + the standard client) —
  compiles in CI as the seam proof; a test in
  `crates/rdlt-connector-rest/tests/children.rs` runs it against
  wiremock through the engine.
- [ ] T013 [US3] PokeAPI live cell
  `crates/rdlt-connector-rest/tests/pokeapi_live.rs` (FR-013):
  `RDLT_NET=1`-gated (skip, not fail, without it); declarative config
  only — `/api/v2/pokemon?limit=100` via next_url pagination + a
  parent-child detail stream `/api/v2/pokemon/{name}` (first N
  parents); 100ms pacing; structural asserts (pagination terminated,
  children resolved, records landed through the engine).

## Phase 6: Polish & close-out

- [ ] T014 [P] Traceability matrix
  `specs/014-rest-completeness/matrix.md`: every config row (source,
  auth, stream, pagination, incremental, actions, parent) → cells,
  zero uncited (011 rules; gap cells land WITH this task); dlt parity
  record `specs/014-rest-completeness/dlt-parity.md` (paginator/auth
  mapping from R2/R3; deliberate deviations: no auto-detection, no
  OAuth JWT yet, callables→seam).
- [ ] T015 Close-out: coverage re-measure to the ≥80% floor with
  classified exclusions recorded in `benches/RESULTS.md`; gated
  REST→PG bar re-measured (`TARGET=rest-pg-100k make bench`, in-band);
  README (`crates/rdlt-connector-rest/README.md` — comprehensive
  full-options reference, the 013 README standard); config-schema
  round-trip tests extended; `make check` + doc-tests + semver ("no
  update required") green; quickstart.md walked verbatim.

## Dependencies

- T001 → T002 → T003 (strictly sequential: weld → hygiene/client →
  seam)
- US1: T004 after T003; T005/T006 [P] after T002; T007 after T004+T005
- US2: T008 after T007; T009 [P] after T002; T010 after T008
- US3: T011 after T007; T012 [P] after T011 (uses the seam); T013
  after T011 (needs next_url + parent)
- T014 [P] after all cells exist; T015 last
- Parallel: T005+T006; T009 beside T008; T012 beside T013

## Implementation strategy

MVP = Phases 1–3 (the declarative surface on the proven skeleton).
The non-negotiable at every stage: the pre-existing conformance cells
pass UNCHANGED — if a refactor step needs to edit one beyond import
paths, stop, that's a behavior change. The bench re-measure (T015)
needs the quiet-machine discipline; schedule it once, at close-out.
