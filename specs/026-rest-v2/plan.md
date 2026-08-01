# rdlt-connector-rest-v2 Implementation Plan

The REST source, second generation — the same playbook that shipped
`specs/025-postgres-v2` (postgres second generation, since swapped in):
a NEW crate written greenfield, no code copied from generation 1, every
identifier renamed from first principles under 025's seven naming rules,
every operator-visible surface frozen and verified identical. Generation 1
(`crates/rdlt-connector-rest`) stays UNTOUCHED and fully gated while both
coexist; the owner decides the swap.

## STATUS — COMPLETE (2026-08-01)

DONE, on branch `rest-v2` off main @ d2b7600e (the postgres swap-in
merge), 7 commits — including three review rounds (record at the bottom
of this file); the ADVERSARIAL REVIEW LOOP is closed. Final state:

- Crate `rdlt-connector-rest-v2` complete: 22 lib tests, 86/86
  integration (every generation-1 assertion passed as written on the
  first run) + the 2 review-round pins, 2/2 sweep (crash/rerun
  convergence ×3 points, registry-vs-sources), doctest, mock_api
  example. No containers needed — the suites are wiremock-backed.
- Zero-warning bar: clippy --all-targets --all-features, rustdoc
  -D warnings, fmt — all clean.
- Review rounds 1-3 (record below): NO drift vs generation 1; three
  inherited defects found and fixed parity-safe, each pinned; one layout
  and ~23 comment/naming items fixed; the round-3 meta-review confirmed
  every fix correct and its two process gaps (a missing pin, this STATUS
  block's staleness) are closed by the round-3 commit and this text.
- Makefile wired: v2 sweep line under TARGET=sweep alongside
  generation 1's.
- FULL GATE TWICE CLEAN, untouched runs on one commit (1069/1069
  workspace tests, 2 named instrument skips, all sweeps, semver no
  update required, perf gate 0 regressed, cold start 23.7 / 23.8 ms
  vs the 40 ms bar, exit 0 both) — and then, after review rounds 2 and 3
  changed source, ONCE MORE on the review-complete tree: 1070/1070 (the
  round-3 pin is the +1), same skips, perf 0 regressed, cold 23.5 ms,
  exit 0.
  - Gate #2's FIRST attempt failed on the recorded podman
    rootlessport flake, NOT this crate: cell
    `rdlt-connector-file::s3_live::range_read_returns_the_tail`, port
    38981, mechanism 1 (inter-run residue — the run started straight
    after gate #1; 48 residual containers + 1,501 TIME_WAIT sockets
    measured at failure). Remedy per the recorded procedure: `make
    reclaim` + drain to zero, rerun — clean.
- Generation 1 UNTOUCHED and still fully gated (D1). NOT merged, NOT
  wired into the facade; swap/rename/publish are the owner's calls. The
  swap checklist mirrors postgres-v2's: point the facade `rest` feature
  at v2, port `pipeline_spec`/CLI spellings (`RestSource`→`source::Rest`
  per the ledger), retire the gen-1 Makefile line, keep the `sweep`
  binary name, move the bench fixture's mock_api reference.

## SWAP-IN — EXECUTED (2026-08-01, owner decision)

The owner took the standing calls: generation 1 DELETED, this crate
renamed to `rdlt-connector-rest` (its `-v2` suffix existed only to
coexist), branch merged to main. What the swap rewired, all in one
commit — a much smaller blast radius than the postgres swap, because the
facade re-exports the crate root and no other crate consumed the REST
API directly:

- Package: name `rdlt-connector-rest`, `publish = false` dropped, docs.rs
  `documentation` key restored, "second generation" dropped from the
  description.
- Facade (`rdlt`): the `rest` feature mapping is unchanged (no feature
  split to rename); `pipeline_spec.rs` ports `RestSource::from_yaml/
  from_json` to `source::Rest::…` per the ledger. The YAML vocabulary is
  UNCHANGED (frozen surface 1).
- Gate wiring: the two per-generation Makefile sweep lines collapsed to
  one; the binary name `sweep` and the crash-point registry check carry
  over unchanged (the scanner selfcheck's rest row still holds — 3
  directly-armed names at the same path).
- `examples/mock_api.rs` keeps its name and package (D5): the bench
  teardown's `pkill -f mock_api` and any spawn-by-package reference
  resolve exactly as before.
- README banner rewritten from coexistence to the swapped-in reality.

## Decision record

- D1. NEW crate `rdlt-connector-rest-v2` (`publish = false`), generation 1
  untouched. Swap/rename/publish are the owner's calls, exactly as they
  were for postgres (owner later took all three at once there).
- D2. The 025 naming rules apply verbatim (module path is part of the
  name; no ad-hoc truncations; verbs for actions, predicates read as
  assertions; role-named parameters; one name per concept with the ledger
  below; errors named by what failed).
- D3. Source-only crate, so no feature split: the crate has ONE compiled
  shape (plus `failpoints`). The `source/` module wrapper stays — the
  family convention (`rdlt-connector-postgres::source::…`) is what the
  facade and the ledger rename onto.
- D4. The JSONPath-subset selector is HOISTED to a substrate module
  (`source::select`): generation 1's config validation reached into
  `read::extract::Selector` — a config→read layering inversion this
  layout removes. Same syntax, same semantics, byte-identical error
  needles.
- D5. `examples/mock_api.rs` is KEPT as-is in spirit (rewritten, same
  CLI, same shape, same defaults): `rdlt-bench`'s REST fixture spawns it
  by name and the benchmark measures the clients, not this process.
- D6. The generation-1 doc string on `request_timeout_secs` says "end to
  end" while the build code deliberately uses `read_timeout` (per-read
  reset, so a stalling server fails but a progressing transfer never
  dies). The CODE is the contract; v2's config doc describes the
  read-timeout semantics correctly. Behavior unchanged.

## Frozen surfaces (operator/engine-visible — the parity bar)

1. THE CONFIG DOCUMENT, every serde spelling and default:
   - Field names as generation 1 spells them (`base_url`, `auth`,
     `headers`, `params`, `max_concurrency`, `min_request_interval_ms`,
     `retry_after_cap_secs`, `request_timeout_secs`, `max_pages`,
     `streams[].{name,path,method,body,params,headers,records_path,
     pagination,incremental,cursor_field,cursor_param,response_actions,
     parent,primary_key,type_hints}`).
   - `auth:` accepts BOTH the singleton-map form and the pre-014 YAML
     tagged form (`auth: !bearer`); serializes as singleton-map. Scheme
     spellings: `none|bearer|header|basic|api_key|
     oauth2_client_credentials` (snake_case).
   - `pagination:` internally tagged on `type`, spellings
     `none|page|offset|cursor|header_cursor|next_url|link_header` —
     `type: cursor` is the BODY-cursor family's frozen document spelling.
   - Legacy flat aliases `cursor_field`/`cursor_param` parse unchanged
     (set together, never mixed with the `incremental` block).
   - `response_actions[].action` is the document key for the action kind
     (`ignore|end_stream|error`).
   - Defaults: `page` param starting at 1, `offset`/`limit` params,
     `max_concurrency` 1, `retry_after_cap_secs` 300,
     `request_timeout_secs` 300, `max_pages` 10 000,
     `expiry_margin_secs` 60, api-key `location: header`.
   - Validation refuses the same documents for the same reasons, keeping
     every message needle the ported tests pin (`auth:` steering for the
     13 reserved credential-header names, "must be set together",
     "mutually exclusive"-family messages, "needs an object `body`",
     selector syntax messages, response-action matcher/status-range
     messages, parent matrix messages, zero `request_timeout_secs`/
     `max_concurrency` refusals, "at least one stream").
   - `config_schema()` stays GENERATED from the config structs (schema
     and parser cannot drift) and must accept/reject the same documents;
     its `$defs` type names follow the v2 Rust names (the postgres-v2
     precedent: validation behavior is the frozen thing, not schema
     bytes).
2. ERROR CLASSIFICATION, verbatim: transport failure → Transient;
   HTTP 429 → `RateLimited` carrying the server's Retry-After UNCLAMPED;
   5xx → Transient; other non-success → Fatal — unless a DECLARED
   response action matches first (first match wins; matchers are typed
   status equality and `content_contains` over the first 64 KiB).
   In-source waits bounded by construction: at most ONE Retry-After wait
   (429/503, within `retry_after_cap_secs`, delta-seconds and HTTP-date
   forms, past date = zero wait not absence) and at most ONE 401
   credential re-fetch per send. OAuth2: lazy single-flight cache with
   expiry margin, generation counter so a stale 401 never evicts a fresh
   token; token endpoint shares the read deadline but skips pacing and
   default headers; endpoint 5xx transient, 429 rate-limited carrying the
   server's Retry-After (the round-1 amendment), other 4xx fatal naming
   credentials, missing `access_token` fatal.
3. CRASH-POINT IDS verbatim: `rest.request`, `rest.decode`,
   `rest.checkpoint`; the registry lists exactly these and the sweep
   pins armed-fire + crash/rerun convergence to exact totals.
4. CURSOR SEMANTICS: max-observed string value (strings and numbers
   rendered; anything else skipped, never guessed), lexicographic
   ordering as the documented field constraint; seeded from the committed
   cursor or `initial_value` so an empty page can never move it
   backwards; parentless streams checkpoint per page AFTER the rows they
   cover, child streams once at feed end; every child window starts at
   the committed resume point, never a sibling's in-flight progress;
   `start_param`/`end_param`+`end_value` bindings ride every request of
   the sequence.
5. WIRE BEHAVIOR: stream params/headers merge OVER source-level defaults;
   auth/stream-set values win over defaults of the same name; page
   params ride the query for GET (and body-less POST) and are set INTO
   the JSON object body for paginated POST; base-url join collapses the
   boundary slash; relative next-urls resolve against `base_url`;
   `{placeholder}` path substitution percent-encodes the VALUE (RFC 3986
   unreserved passthrough) with the `.`/`..` dot-segment escape, while
   query/body substitution stays verbatim (reqwest/serde encode those);
   `_parent_<field>` embedding spelling with typed collision refusal;
   the same-request loop guard fingerprints url + page params + extra
   params, and `max_pages` bounds every sequence; Link headers parse the
   RFC 5988 subset tolerating commas inside URLs and junk members.
6. PAGINATION TERMINATION per family, verbatim: single page; page-number
   stops on zero records or declared totals (totals checked BEFORE an
   extra empty-page request; `total_pages_path` and `total_count_path`
   mutually exclusive); offset stops on a short page or declared total;
   body cursor on absent/null/empty-string (non-scalar typed fatal);
   header cursor on absent/empty; next-url on absent/null/empty
   (non-string typed fatal); link-header on no `rel="next"`. An
   `ignore`d page still hands body-driven paginators its body (an
   unparseable one ends the chain cleanly). A closed records channel
   means cancellation: return Ok, never an error.
7. SPI SURFACE: connector name `rest` + crate version in `spec()`;
   `config_schema` declared; `streams()` carries primary key, effective
   cursor field, type hints; fan-out capped by `max_concurrency` via one
   bounded forwarding channel (capacity 2× the cap); child failures name
   the stream and the resolved placeholder values WITHOUT changing the
   error's classification (transient stays transient, rate-limited keeps
   its retry_after).

## The crate

```
crates/rdlt-connector-rest-v2/
  src/lib.rs               — crate docs + TOC (pub mod source; no root re-exports)
  src/source/
    mod.rs                 — TOC; canonical spellings hoisted (source::Rest, source::Config)
    connector.rs           — source::Rest: SPI (spec/streams/read) + from_yaml/from_json/from_value/new
    fail_points.rs         — FAIL_POINTS registry (frozen IDs)
    config/
      mod.rs               — TOC (pub use vocabulary::…)
      vocabulary.rs        — Config, Stream, Auth, KeyLocation, Method, Pagination,
                             Incremental, ResponseAction, ActionKind, Parent, TypeHint,
                             ConfigError, config_schema, the auth compat serde module
      validate.rs          — the ONE validation gate every entry point runs
    select.rs              — Selector: the JSONPath subset (dot paths, [*], [N])
    http/
      mod.rs               — TOC
      client.rs            — Client: the send loop (pacing floor, default merge,
                             bounded Retry-After wait, bounded 401 re-fetch)
      credentials.rs       — Credentials: scheme attachment + OAuth2 token cache
                             (single-flight, expiry margin, generation counter)
      classify.rs          — transport/status classification + Retry-After parsing
    read/
      mod.rs               — TOC + read::deliver (the per-stream entry)
      cursor.rs            — max-observed cursor tracking + scalar rendering
      sequence.rs          — Sequence: one paginated request sequence (request build,
                             response actions, loop guards, crash points)
      paginate.rs          — the PUBLIC seam: Paginator trait, Context, Decision,
                             Error + the seven config-backed families
      extract.rs           — Page: records extraction (byte passthrough fast path,
                             selector extraction, one parse per page)
      resolve.rs           — placeholder substitution + parent-field embedding
      fanout.rs            — parent collection + bounded child fan-out
  tests/integration.rs + tests/cases/…   — house layout (one binary + cases)
  tests/sweep.rs           — crash sweep + registry-vs-sources check (own binary)
  examples/mock_api.rs     — the benchmark mock server (D5)
  README.md
```

## Rename ledger (old → v2)

| Old | v2 | Rule |
|---|---|---|
| crate `rdlt-connector-rest` | `rdlt-connector-rest-v2` | D1 |
| `RestSource` | `source::Rest` | 1 |
| `RestConfig` | `source::Config` | 1 |
| `RestStream` | `config::Stream` | 1 |
| `HintType` | `config::TypeHint` | 5 (postgres alignment) |
| `HttpMethod` | `config::Method` | 2 |
| `ApiKeyLocation` | `config::KeyLocation` | 2 |
| `RestClient` | `http::Client` | 1, 2 |
| `AuthProvider` | `http::Credentials` | 1 (named by what it holds) |
| `classify_reqwest` / `classify_status` | `classify::transport` / `classify::status` | 3 |
| `retry_after` (free fn) | `classify::server_retry_after` | 3 |
| `SequenceDriver` | `read::Sequence` | 2 |
| `PageContext` / `PageDecision` / `PaginatorError` | `paginate::Context` / `paginate::Decision` / `paginate::Error` | 2 |
| `paginate::from_config` | `paginate::build` | 3 |
| `Extracted` | `extract::Page` | 1 |
| `extract_records` | `extract::page` | 3 |
| `read_stream` (free fn) | `read::deliver` | 3 |
| `update_max_cursor` | `cursor::observe` | 3 |
| `collect_parent_values` | `resolve::parent_values` | 3 |
| `embed_parent_fields` | `resolve::embed` | 3 |
| crate-root re-exports (`RestSource`, …) | REMOVED — one spelling per item, module paths canonical | 5 |

Config field/variant RUST names track their document spellings (frozen
surface 1), so most keep their names; the document is the contract.

## Test plan

Port every generation-1 suite with assertions, YAML documents, and mock
choreography byte-identical (the postgres-v2 method — a ported assertion
that fails is a v2 bug found, not a test to adapt):

- `tests/integration.rs` + `cases/`: `test_actions` (19), `test_auth`
  (8), `test_children` (6 incl. the composed-seam proof and the fan-out
  concurrency floor), `test_config_schema` (4), `test_conformance` (6
  incl. the SPI conformance kit), `test_pagination` (10),
  `test_robustness` (7), `test_pokeapi_live` (env-gated `RDLT_NET=1`).
- `tests/sweep.rs` stays its OWN binary (the Makefile selects
  `binary(sweep)`): crash/rerun convergence ×3 points + the
  registry-matches-sources check via `rdlt_testkit`.
- Lib unit tests rewritten per module (selector parse/select, link
  parsing, path-encoding, credential generations, config refusals,
  passthrough ptr-equality pin).
- Shared harness `tests/cases/common.rs` (read_stream/read_ok/read_err/
  stream_yaml) — same helper surface.

## Gate wiring

- Workspace member + `rdlt-connector-rest-v2` sweep line in the Makefile
  sweep target (alongside generation 1's, exactly as postgres-v2 ran).
- Zero-warning bar: clippy --all-targets -D warnings, rustdoc -D
  warnings, fmt, for the crate's one compiled shape (± failpoints).
- Full `make check` TWICE CLEAN untouched before declaring done.

## Verification checklist (close-out)

- [ ] Every generation-1 test ported and green as written.
- [ ] Passthrough fast path pinned byte-identical (ptr equality).
- [ ] RS1–RS8 re-read against v2; deviations recorded here, never silent.
- [ ] Frozen surfaces 1–7 verified by the ported suites.
- [ ] Naming audit against the seven rules (src + tests).
- [ ] Gate twice clean; review rounds recorded below.

## REVIEW ROUNDS (running record)

Round 1 (three parallel lenses, 2026-08-01):

- DRIFT vs generation 1: NO DRIFT FOUND — every function pair walked on
  both sides; the only byte-level difference is `config_schema()`'s
  `$defs` type names, declared in Frozen surfaces item 1.
- BUG SCAN: two CONFIRMED defects, both INHERITED from generation 1
  verbatim (the drift lens proves the code paths are identical there),
  both fixed here with regression pins — the postgres-v2 precedent for
  parity-safe inherited fixes. Generation 1 stays untouched (D1).
  1. `resolve::substitute` applied one `.replace()` pass PER placeholder
     over the accumulated output, so a parent value (remote data) whose
     text looked like another declared `{token}` had that other field's
     value injected into it (BTreeMap order decided the corruption).
     Fixed: `replace_tokens` does ONE left-to-right pass over the
     template and never rescans substituted text; `substitute_path` now
     routes through the same pass over pre-encoded values. Pin:
     `a_value_resembling_another_token_is_not_resubstituted`.
  2. The OAuth2 token endpoint classified a 429 as FATAL (its hand-rolled
     success/5xx/else split predated the shared rulebook), aborting the
     whole run over an ordinary shared-issuer rate limit — while the
     identical status on the data path is `RateLimited`. Fixed: a 429 arm
     returning `RateLimited` with the endpoint named and the server's
     Retry-After attached. Pin: `oauth2_token_endpoint_429_is_rate_limited`.
     (Frozen surfaces item 2's "endpoint 5xx transient, 4xx fatal" is
     hereby amended: 429 → RateLimited, other 4xx fatal.)
- NAMING/COMMENT AUDIT: no unsanctioned rule violations; ~15 polish items
  all applied — test-import aliases resurrecting `RestConfig` dropped
  (one spelling per concept), six comments made self-contained (no
  generation references or spec-number citations in code), README's
  pacing and cursor-family claims corrected to match the code,
  `done`→`ended` and `need_values`→`needs_values` (booleans as
  assertions), the `ParentValues` instance spelled `parent_values` at
  every seam, `reserved_auth_header`→`credential_header_refusal` (named
  by what it builds). Recorded, not changed: `select::Selector` /
  `paginate::Paginator` repeat their module noun (sanctioned in the
  layout above); `paginate::build`'s public `String` error (matches the
  seam shape the composed-example contract exercises).

Round 2 (the five-lens review protocol on the finished tree, 2026-08-01):

- CLAUDE.md COMPLIANCE: one layout item — `read/mod.rs` carried three
  functions instead of being a pure TOC. Fixed: the per-stream loop moved
  to `read/delivery.rs`; `mod.rs` is a TOC re-exporting
  `delivery::{deliver, parse_selector}`. Also tightened the crate-docs
  doctest to the refusal's exact needle. Everything else verified
  compliant (Principle V/VI, no unsafe, test layout, gate wiring, D1's
  zero-diff on generation 1).
- SHALLOW BUG SCAN: no findings; both round-1 pins verified to fail on
  the pre-fix code; `replace_tokens` edge cases (adjacent/empty/unclosed/
  nested tokens) traced sound.
- HISTORY/LESSONS: every hard-won generation-1 fix (the 014 review ten,
  020's Retry-After date form and dot-segment escape, 017's
  classification-preserving child context) and every deliberate
  NON-feature (no pagination auto-detection, no synthesized "now")
  verified present in v2 with its regression test. No lesson lost.
- RECORDED-GUIDANCE RECURRENCE: one hit of the postgres-v2 double-framing
  class — `fanout::with_parent_context` re-wrapped a rendered
  `SourceError` in the same variant, printing the classification frame
  twice. Fixed: context wraps the variant's INNER cause; the
  non_exhaustive wildcard arm keeps full rendering and goes fatal (the
  safe direction for an unknown variant). Classes 2–5 (substring
  matching, truncations, serde-name leaks, feature-combo warnings —
  the last verified empirically) all clean.
- COMMENT-CODE CONSISTENCY: four wrong comments fixed (NextUrl resolves
  against `base_url` not the current URL; README's token-endpoint row
  now carries the round-1 429 amendment; the "one parse per page, total"
  claim scoped to record consumers — a body-driven paginator's cursor
  parse is pagination's, and the double-parse itself is kept as-is for
  parity; `Client::send`'s `Err` description widened to everything
  without a data-response status). Four minor inaccuracies fixed
  (fingerprint contents include the fixed incremental bindings; a
  beyond-cap 503 surfaces Transient not RateLimited; the loop guard
  catches non-adjacent cycles too; `Secret`'s home crate named).

Round 3 (meta-review of round 2's fixes, 2026-08-01): every fix verified
correct by direct trace (TOC split paths/visibilities, single-frame
rendering against `SourceError`'s Display impls, each corrected comment
against its code). Two process gaps found and closed: the double-framing
fix had no regression pin (added:
`parent_context_renders_the_classification_frame_once` — one frame per
classification, retry_after survives wrapping) and the STATUS block
predated round 2's tree (amended, with the post-review gate recorded).
Final gate on the review-complete tree: 1070/1070, perf 0 regressed,
cold 23.5 ms, exit 0.
