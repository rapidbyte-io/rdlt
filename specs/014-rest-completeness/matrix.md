# Parameter Traceability Matrix (014 — REST source)

Every user-settable parameter → the cells that prove it (011 rules,
zero uncited rows). Citations are `file::test`
(nextest: `-E 'test(<name>)'`). Class: unit (in-src `::tests`) |
mock (wiremock, `crates/rdlt-connector-rest/tests/`) | live
(`RDLT_NET=1`, PokeAPI) | sweep (`--features failpoints`). Schema
round-trips for the WHOLE document (every family/scheme/block below in
one corpus) ride `config_schema.rs::schema_valid_corpus_parses` and
`::documented_example_validates_and_parses`; unknown-field rejection:
`config_schema.rs::unknown_fields_fail_schema_and_parser_identically`.

## Source — top level

| parameter | default | behaviors proven | validation proven | cells | class |
|---|---|---|---|---|---|
| `base_url` | required | joined with stream paths; relative `next_url` pages resolve against it | missing typed (serde) | every mock cell; pagination.rs::next_url_follows_absolute_and_relative | mock |
| `auth` | `none` | six schemes attach (see Auth) | YAML singleton-map + JSON forms parse | auth.rs (all); config_schema.rs::schema_valid_corpus_parses | mock+unit |
| `headers` | `{}` | ride every request, merged UNDER stream headers (same name → stream wins, sent once) | invalid header name/value typed at send | auth.rs::headers_and_params_merge_stream_over_source | mock |
| `params` | `{}` | ride every request, merged UNDER stream params | — | auth.rs::headers_and_params_merge_stream_over_source | mock |
| `max_concurrency` | `1` | child fan-out overlaps up to the limit (3×400ms children ≪ sequential floor, exact totals); 1 = strictly sequential | `0` typed at parse | children.rs::children_fan_out_concurrently_within_the_limit; children.rs::zero_max_concurrency_rejected_at_parse | mock |
| `min_request_interval_ms` | `0` | pacing floor observed across consecutive requests | — | conformance.rs::pacing_floor_is_observed; pokeapi_live.rs (100ms politeness) | mock+live |
| `retry_after_cap_secs` | `300` | within-cap Retry-After honored ONCE in-source then success; persistent 429 surfaces `RateLimited` (+header value) to the engine budget | — | conformance.rs::retry_after_within_cap_waits_and_succeeds; conformance.rs::error_classification_matches_the_contract | mock |
| `max_pages` | `10000` | guard fires typed, naming stream + paginator state | — | pagination.rs::max_pages_guard_fires_typed | mock |
| `streams` | required | — | empty list typed | config_schema.rs::empty_streams_rejected | unit |

## Auth schemes

| scheme | fields | behaviors proven | cells | class |
|---|---|---|---|---|
| `none` (default) | — | requests go bare | every unauthenticated cell | mock |
| `bearer` | `token` | `Authorization: Bearer` on EVERY page of a paginated read | pagination.rs::auth_rides_every_page | mock |
| `header` | `name`, `value` | exact header attached | auth.rs::basic_and_header_auth_attach | mock |
| `basic` | `username`, `password` | `Authorization: Basic <base64>` attached | auth.rs::basic_and_header_auth_attach | mock |
| `api_key` | `name`, `key`, `location` (`header`\|`query`, default header) | both locations attach | auth.rs::api_key_attaches_header_and_query | mock |
| `oauth2_client_credentials` | `token_url`, `client_id`, `client_secret`, `scopes` (`[]`), `audience` (absent), `expiry_margin_secs` (60) | lazy fetch, ONE token POST for a multi-page read (cache), grant/scope form fields asserted; 401 → cache dropped, ONE re-fetch, second 401 fatal; token-endpoint 5xx transient (engine budget), 4xx fatal | auth.rs::oauth2_fetches_once_and_caches; auth.rs::oauth2_refreshes_once_on_401_then_fatal; auth.rs::oauth2_token_endpoint_5xx_is_transient | mock |

Secret hygiene (all schemes): config Debug + source Debug + every error
rendering contain no secret substring —
auth.rs::secrets_never_render_anywhere; unit redaction/transparency:
`src/source/client/secret.rs::tests`.

## Stream entry

| parameter | default | behaviors proven | validation proven | cells | class |
|---|---|---|---|---|---|
| `name` | required | root table name through the engine | — | children.rs::parent_child_reads_through_the_engine (tables `repos`/`issues` land) | mock |
| `path` | required | joined; `{placeholder}` substitution under `parent` | placeholders without `parent` typed | children.rs (all); children.rs::parent_validation_matrix | mock |
| `method` | `get` | `post` sends the body; pagination params go INTO the body for POST-cursor | — | actions.rs::post_body_with_cursor_pagination | mock |
| `body` | absent | JSON template; `{token}` substitution under `parent` | requires `method: post`, typed | actions.rs::post_body_with_cursor_pagination; actions.rs::body_requires_post | mock |
| `params` | `{}` | ride every request of the stream; merged OVER source params | — | pokeapi_live.rs::pokeapi_list_and_details_through_the_engine (`limit`); auth.rs::headers_and_params_merge_stream_over_source | live+mock |
| `headers` | `{}` | merged OVER source headers | — | auth.rs::headers_and_params_merge_stream_over_source | mock |
| `records_path` | absent | absent = body streams through BYTE-IDENTICAL (perf path); dot + `[*]` + `[N]` selection; single-array-match unwraps; wildcard matches are records | unsupported syntax typed AT PARSE naming the subset; no-match typed naming path + response top-level keys | src/source/read/extract.rs::tests::passthrough_is_byte_identical + selector_subset_parses_and_rejects + selection_flattens_wildcards + index_segments_select_and_non_array_shapes_are_typed + no_match_names_path_and_shape; actions.rs::wildcard_selector_extracts_nested; actions.rs::selector_no_match_is_typed; actions.rs::invalid_selector_fails_at_parse | unit+mock |
| `pagination` | `none` | see Pagination | eager selector validation on all `*_path` fields | actions.rs::invalid_selector_fails_at_parse (records_path arm; same validate loop covers pagination paths) | mock |
| `incremental` | absent | see Incremental | — | — | — |
| `cursor_field` / `cursor_param` | absent | FROZEN pre-014 aliases; identical behavior to the block | set together; never mixed with the block — typed | conformance.rs::paginates_and_checkpoints_max_cursor + resume_sends_cursor_param_and_skips_completed_ranges (alias spellings, unchanged pre-014 cells); actions.rs::incremental_block_and_aliases_are_exclusive; actions.rs::validation_matrix_covers_remaining_arms (one alias half alone) | mock |
| `response_actions` | `[]` | see Response actions | — | — | — |
| `parent` | absent | see Parent-child | — | — | — |
| `primary_key` | absent | declared key rides `StreamSpec` into the engine (merge identity) | — | conformance.rs::rest_source_is_conformant (spec carries it); sweep.rs::rest_read_path_survives_crash_sweep (exactly-once convergence keyed on it) | mock+sweep |
| `type_hints` | `{}` | per-column logical types override inference | — | conformance.rs::rest_source_is_conformant; config_schema.rs::documented_example_validates_and_parses | mock+unit |

## Pagination families

Universal guards, every family: a repeated identical request is typed
(naming stream + params) — pagination.rs::same_request_loop_guard_fires_typed;
`max_pages` — pagination.rs::max_pages_guard_fires_typed. Frozen pre-014
spellings parse unchanged — pagination.rs::pre_014_pagination_spellings_parse.

| family | fields (defaults) | termination proven | cells | class |
|---|---|---|---|---|
| `none` (default) | — | single request | auth.rs::api_key_attaches_header_and_query (and every single-page cell) | mock |
| `page` | `page_param` (page), `start` (1), `total_pages_path` / `total_count_path` (absent, mutually exclusive — declaring both typed) | empty page; declared total reached WITHOUT an extra request (`expect(1)`) | children.rs::parent_child_reads_through_the_engine (empty-page stop); pagination.rs::page_with_total_pages_stops_at_total; actions.rs::validation_matrix_covers_remaining_arms (both-stops rejection) | mock |
| `offset` | `offset_param` (offset), `limit_param` (limit), `page_size` (required), `total_count_path` (absent) | short page; declared count reached | pagination.rs::offset_with_total_count_stops_at_total | mock |
| `cursor` | `cursor_path`, `cursor_param` | absent/null cursor ends; value chains into the param (query for GET, body for POST) | pagination.rs::body_cursor_chains_and_terminates; actions.rs::post_body_with_cursor_pagination | mock |
| `header_cursor` | `header`, `cursor_param` | absent header ends; header value chains | pagination.rs::header_cursor_chains_and_terminates | mock |
| `next_url` | `next_url_path` | absent/null ends; absolute followed verbatim, relative resolved against `base_url`; live chain terminates naturally | pagination.rs::next_url_follows_absolute_and_relative; pokeapi_live.rs::pokeapi_next_url_chain_terminates | mock+live |
| `link_header` | — | RFC5988 `rel="next"` followed; no next link ends | pagination.rs::link_header_follows_rel_next; unit parse: src/source/read/paginate.rs::tests | mock+unit |

## Incremental block

| parameter | default | behaviors proven | validation proven | cells | class |
|---|---|---|---|---|---|
| `cursor_field` | required | max-observed value checkpointed AFTER its rows (S2); resume skips completed ranges | empty typed; exclusivity with aliases typed | conformance.rs::paginates_and_checkpoints_max_cursor; conformance.rs::resume_sends_cursor_param_and_skips_completed_ranges; actions.rs::incremental_block_and_aliases_are_exclusive; actions.rs::validation_matrix_covers_remaining_arms (empty cursor_field) | mock |
| `start_param` | absent | resume cursor bound onto every request | — | actions.rs::incremental_start_and_end_params_bind (resume `5` → `since=5`) | mock |
| `end_param` + `end_value` | absent | closed window bound onto every request; checkpoint still max-observed | `end_param` without `end_value` typed | actions.rs::incremental_start_and_end_params_bind (`until=9`, checkpoint `7`); actions.rs::end_param_requires_end_value | mock |
| `initial_value` | absent | first-run lower bound (seeds the cursor; an empty page never lowers it) | — | sweep.rs::rest_read_path_survives_crash_sweep (`since` honored across crash/rerun) | sweep |

## Response actions

| parameter | behaviors proven | validation proven | cells | class |
|---|---|---|---|---|
| `status` → `end_stream` | declared 404 ends cleanly, totals = rows so far; UNDECLARED 4xx stays typed (allow-list posture) | — | actions.rs::action_404_end_stream; actions.rs::undeclared_4xx_stays_typed | mock |
| `content_contains` → `ignore` | matching page contributes nothing; pagination still terminates; 64KiB match bound (in-code constant) | — | actions.rs::action_content_ignore | mock |
| matcher-less action | — | rejected at parse (would swallow everything) | actions.rs::unconditional_action_rejected_at_parse | mock |
| all three actions × all three crash points | crash/rerun exactly-once totals through a real destination | — | sweep.rs::rest_read_path_survives_crash_sweep (3 points × 3 actions, armed-fire pin exact) | sweep |

## Parent-child

| parameter | behaviors proven | validation proven | cells | class |
|---|---|---|---|---|
| `stream` | parent read first (fresh pass), one child sequence per parent record, engine-landed totals exact | undeclared parent, self-parent, nested (2-level) child — typed | children.rs::parent_child_reads_through_the_engine; children.rs::parent_validation_matrix | mock |
| `placeholders` | `{token}` substituted into path (scalars only; dot-path parent fields); child FAILURES name the resolved values | unused placeholder typed; empty map typed; non-scalar/missing parent field typed | children.rs::child_failure_names_resolved_values; src/source/read/resolve.rs::tests::collects_and_substitutes + missing_field_and_non_scalar_are_typed; children.rs::parent_validation_matrix | mock+unit |
| `include` | `_parent_<field>` embedded on every child record | collision with an existing child field typed | children.rs::parent_child_reads_through_the_engine (`_parent_name`, `_parent_stars`); src/source/read/resolve.rs::tests::embeds_parent_fields_and_detects_collisions | mock+unit |
| (composition seam) | a named-API wrapper built ONLY from public pieces reads through the engine; live list+detail fan-out | — | children.rs::composed_example_runs_through_the_engine; pokeapi_live.rs::pokeapi_list_and_details_through_the_engine | mock+live |

## Cross-cutting

| concern | proven | cells | class |
|---|---|---|---|
| SPI conformance | source passes the shared conformance harness | conformance.rs::rest_source_is_conformant | mock |
| config entry points | `from_yaml` / `from_json` / `from_value` share one validation path | actions.rs::validation_matrix_covers_remaining_arms (from_json); config_schema.rs (from_value, all cells); every YAML cell | mock+unit |
| error classification | network/5xx transient, 429 `RateLimited` (+`Retry-After` value), 4xx fatal | conformance.rs::error_classification_matches_the_contract | mock |
| crash points | `rest.request` / `rest.decode` / `rest.checkpoint` armed-fire, exactly-once totals | sweep.rs::rest_read_path_survives_crash_sweep | sweep |
| live proof | PokeAPI: bounded engine run + natural termination, structural asserts, 100ms pacing | pokeapi_live.rs (both cells; skip-not-fail without `RDLT_NET=1`) | live |
