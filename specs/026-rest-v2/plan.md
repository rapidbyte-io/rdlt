# rdlt-connector-rest-v2 Implementation Plan

The REST source, second generation — the same playbook that shipped
`specs/025-postgres-v2` (postgres second generation, since swapped in):
a NEW crate written greenfield, no code copied from generation 1, every
identifier renamed from first principles under 025's seven naming rules,
every operator-visible surface frozen and verified identical. Generation 1
(`crates/rdlt-connector-rest`) stays UNTOUCHED and fully gated while both
coexist; the owner decides the swap.

## STATUS — IN PROGRESS (started 2026-08-01)

Branch `rest-v2` off main @ d2b7600e (the postgres swap-in merge).

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
   default headers; endpoint 5xx transient, 4xx fatal naming credentials,
   missing `access_token` fatal.
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

(none yet)
