# Parameter Traceability Matrix (contract PM1)

Every user-settable parameter → the cells that prove it. Citations are
`file::test` (nextest: `-E 'test(<name>)'`). Class: unit | live
(container) | sweep (`--features failpoints`) | heavy (`RDLT_HEAVY=1`).
Unit config/schema cells live in `src/.../config.rs::tests`,
`tests/config_schema.rs`, `src/tls.rs::tests`, and the CLI's
`src/main.rs::tests`. Zero GAP rows (close-out state).

## Source — top level

| parameter | default | behaviors proven | validation proven | cells | class |
|---|---|---|---|---|---|
| `conn` | required | parse gate up front; portability (see Conn-string table) | empty/unparseable typed; never the retry path | config.rs::conn_parse_gate_and_tls_policy; tls.rs::tls_trio_extracts_from_both_libpq_forms; tls_matrix::common_connect_failures_carry_the_server_message | unit+live |
| `schema` | `public` | custom scope resolves bare names; default used pervasively | empty rejected | reflect_live::reflects_schema_shape_pks_views_and_type_policies; config.rs::empty_selections_rejected | live |
| `include_views` | `false` | false: views excluded from discovery; true: views become streams; named view always included | — | reflect_live::reflects_schema_shape_pks_views_and_type_policies; conformance::schema_wide_discovery_and_views | live |
| `tables` | absent = discover all | discover-all; listed order; partition/INHERITS leaves excluded with explicit-listing override; foreign tables never | present-but-empty, dup names, qualified names, empty name — all typed | conformance::schema_wide_discovery_and_views; conformance::partitioned_tables_load_once_via_parent; conformance::inherits_children_load_once_via_parent; config.rs::qualified_table_name_rejected; config.rs::empty_selections_rejected | live+unit |
| `queries` | `[]` | see Query streams | — | — | — |
| `tls` | absent = `prefer` | see TLS | — | — | — |
| `cdc` | absent | see CDC | — | — | — |
| `batch_target_bytes` | 8 MiB | byte target observably cuts batches (commit count) | zero rejected | incremental::batch_knobs_cut_batches_observably; config.rs::empty_selections_rejected | live+unit |
| `batch_max_rows` | 65536 | row cap observably cuts batches; mid-stream checkpoints exist because of it | zero rejected | incremental::batch_knobs_cut_batches_observably; crash_sweep::sweep_postgres_source | live+sweep |

## Source — table entry

| parameter | default | behaviors proven | validation proven | cells | class |
|---|---|---|---|---|---|
| `name` | required | bare-name resolution in `schema` | dup/qualified/empty typed | config.rs::qualified_table_name_rejected + empty_selections_rejected; every live suite | unit |
| `cursor` | absent = snapshot | snapshot streams re-read fully; never checkpoint | — | conformance::run_to_duckdb; incremental (all) | live |
| `primary_key` | reflected PK | override drives merge identity; PK-less → row-hash dedup; CDC: wins under FULL, mismatch typed under default identity | empty list typed | incremental::merge_by_declared_key_converges_and_keyless_is_rejected; incremental::pkless_table_dedups_via_row_hash; cdc::declared_primary_key_override_keys_the_stream_under_full; cdc::declared_key_mismatch_under_default_identity_is_typed; config.rs::empty_selections_rejected | live |
| `included_columns` / `excluded_columns` | all / none | selection applies incl. hostile identifiers; cursor column must survive selection | mutual exclusion, empty list typed | conformance::hostile_identifiers_and_column_selection; incremental::cursor_column_must_survive_selection; config.rs::include_exclude_mutually_exclusive + empty_selections_rejected | live+unit |
| `type_hints` | `{}` | see Type hints | unknown column / undefined pair typed at open | conformance::hint_matrix_covers_every_documented_pair | live |

## Source — cursor block

| parameter | default | behaviors proven | validation proven | cells | class |
|---|---|---|---|---|---|
| `column` | required | cursor-ordered reads; mid-table checkpointed resume | missing-from-selection / not-capable typed | incremental::delta_loads_and_closed_boundary_dedup; crash_sweep::transient_mid_copy_resumes_within_one_run; incremental::cursor_column_must_survive_selection | live+sweep |
| `initial_value` | absent = full load | first-run lower bound | bad literal typed | incremental::initial_and_end_value_window; cursor.rs unit parse cells | live+unit |
| `boundary` | `closed` | closed: watermark-equal re-fetch + keyed dedup; open: strict `>`, no dedup | lag×open typed at parse AND open | incremental::delta_loads_and_closed_boundary_dedup; incremental::open_boundary_skips_watermark_equal_rows; config.rs::lag_with_open_boundary_dies_at_config_parse; incremental::lag_rejections_are_typed_and_early | live+unit |
| `direction` | `max` | max ascending (pervasive); min: watermark = minimum, resumes below | — | incremental::delta_loads…; incremental::direction_min_descends_and_resumes | live |
| `end_value` | absent | upper bound as read filter, never resume state | bad literal typed | incremental::initial_and_end_value_window | live |
| `end_bound` | `exclusive` | inclusive loads boundary rows exactly once | — | incremental::inclusive_end_bound_loads_boundary_rows_exactly_once | live |
| `nulls` | `exclude` | exclude filters; include re-fetches every run; error = typed data-contract failure naming stream+column; old policies unchanged | — | incremental::null_cursor_policies; incremental::null_cursor_error_policy_fails_typed_and_old_policies_unchanged | live |
| `lag` | absent | duration family (timestamps) captures late rows, exact totals under merge; magnitude family (integers) ditto; window row source-side dedup; watermark never lowered | vocabulary, family mismatches, date whole-days, no-PK — typed and early | incremental::lag_captures_late_arrivals_with_exact_totals_under_merge; incremental::magnitude_lag_for_integer_cursors; incremental::lag_rejections_are_typed_and_early; incremental::regressing_clock_never_moves_watermark_backward; config.rs::lag unit cells; cursor.rs::sql_delta cells | live+unit |

Cursor-capable-type rows (uuid/text cursors): incremental::uuid_cursor_end_to_end; incremental::text_cursor_mixed_case_byte_order (live).

## Source — query streams

| parameter | default | behaviors proven | validation proven | cells | class |
|---|---|---|---|---|---|
| `name` | required | stream naming | uniqueness across tables+queries, empty typed | config.rs::json_and_value_entry_points… + validate cells; query_streams suite | unit+live |
| `sql` | required | server-described schema; wrapped read-only; joins/aggregates | write statements rejected typed+early | query_streams::join_query_lands_with_described_schema_and_incremental_works; query_streams::rejections_are_typed_and_early | live |
| `cursor` | absent | full incremental parity on queries | — | query_streams::join_query_lands… | live |
| `primary_key` | none | declared key drives dedup/merge | empty typed | query_streams::join_query_lands…; config.rs unit | live+unit |
| `type_hints` | `{}` | same closed table as tables | — | (shared machinery: conformance::hint_matrix…; synthesized_table_config path) | live |

## Source — type hints (12 values)

All documented pairs proven end-to-end (typed landing + values):
conformance::type_hints_end_to_end (text→timestamp_tz, numeric→decimal,
failing cast typed) + conformance::hint_matrix_covers_every_documented_pair
(text→{bool,int64,float64,timestamp_naive,date,time,uuid,json,binary},
int→{bool,float64}, numeric→float64, timestamp→date, date→timestamp_tz;
undefined pair = typed closed-table rejection). Vocabulary + shape at the
schema layer: config_schema.rs cells; utf8-always + cursor-capability
flips: types.rs unit cells. Class: live+unit.

## Source — CDC block

| parameter | default | behaviors proven | validation proven | cells | class |
|---|---|---|---|---|---|
| `slot` | required | single consumer; peek non-consuming; lifecycle | missing/foreign-plugin/invalidated/held — each distinguished | cdc::create_if_missing_is_idempotent…, missing_resources…, foreign_plugin…, wal_retention_overrun…, concurrent_consumer…, peek_is_nonconsuming…, recreated_slot_with_resuming_cursor_is_typed_never_a_gap | live |
| `publication` | required | coverage preflight | missing / gap typed naming tables | cdc::publication_gap_names_publication_and_missing_tables; missing_resources… | live |
| `create_if_missing` | `false` | idempotent creation; never drops | absent-resource hints | cdc::create_if_missing_is_idempotent… | live |
| `mode` | `catchup` | catchup finishes at target; tail: bursts without restart, quiet idle, clean cancel, exact resume | vocabulary at schema | cdc::us1_equality_cycle…; cdc::tail_applies_bursts_cancels_cleanly_and_resumes; config_schema.rs::cdc_block… | live+unit |
| `idle_wait` | `"1s"` | tail wakes from idle (sleep-spanning burst) | duration-only vocabulary both layers | cdc::tail_applies_bursts… (quiet window + second burst); config.rs::cdc_validation_matrix; config_schema.rs | live+unit |
| `flag_column` | `_rdlt_deleted` | custom name flows end-to-end incl. hard-delete composition | collision typed | cdc::custom_flag_column_flows_end_to_end; cdc::identity_preflight_matrix_is_typed_per_table | live |
| `ack` | `auto` | auto: once per run, destination-committed floors only, partial run acks nothing; off: data flows, position never advances | vocabulary at schema | cdc::ack_never_exceeds_the_least_committed_cursor; cdc::ack_off_never_advances_the_slot | live |

CDC interactions: cdc×cursor exclusivity (config.rs::cdc_validation_matrix,
cdc::identity_preflight_matrix…); boundary overlap (cdc::boundary_overlap…);
TOAST policy both halves (cdc::toast_*); GUC independence
(cdc::session_gucs…); replica-identity matrix incl. dropped-index and
mid-stream drop (cdc::identity_preflight…, dropped_identity_index…,
identity_dropped_mid_stream…); recommended-composition warning legs (CLI
main.rs::cdc_composition_warning_matrix); crash discipline
(cdc_crash_sweep::*, sweep+heavy); lag observability
(cdc::replication_lag_lands_on_the_dedicated_target).

## TLS block + conn-string surface

| parameter | default | behaviors proven | validation proven | cells | class |
|---|---|---|---|---|---|
| `mode` (5) | `prefer` | full matrix source-side against TLS-only server; prefer plaintext fallback; destination same path | contradictions typed naming both | tls_matrix::source_matrix_against_tls_only_server; prefer_falls_back_on_plaintext_server_and_conn_sslmode_flows; destination_uses_the_same_policy_path; tls.rs::policy_resolution_and_contradictions | live+unit |
| `root_cert` | platform store | path + inline PEM load; `sslrootcert=system` = platform store; sslrootcert URL form syncs | material errors name the input | tls.rs::rcgen_root_loads_inline_and_from_path; tls.rs::sslrootcert_system_selects_the_platform_store; tls_matrix::sslrootcert_url_syncs_and_application_name_is_set; tls.rs::root_errors_are_typed_and_name_the_input | unit+live |
| `client_cert`/`client_key` | absent | mTLS matrix against cert-auth server; offered-but-unused syncs; composes with every verifying mode; ClientCert failure distinguished | both-or-neither, material errors typed+early | tls_matrix::client_cert_matrix_against_cert_auth_server; credential_offered_but_unused_still_syncs; tls.rs::credential_shape_rules…, credential_material_errors…, credentials_compose…, rustls_classification_distinguishes_client_cert_rejection | live+unit |
| conn-string portability | — | libpq trio extraction both forms; application_name default+yield; percent-escapes strict | unsupported params rejected BY NAME | tls.rs::tls_trio_extracts…, application_name_defaults_and_yields, malformed_percent_escapes…, rejected_parameters_are_named_never_bare | unit+live |

## Destination — connection + options

| parameter | default | behaviors proven | validation proven | cells | class |
|---|---|---|---|---|---|
| `conn` | required | same gate as source | same | tls_matrix::destination_uses_the_same_policy_path | live |
| `dataset` | `public` | custom schema created+used (pervasive); omitted → tables observed in `public` | — | dest_conformance (pervasive); param_matrix::default_dataset_is_public | live |
| `tls` | `prefer` | same policy type/path both directions | — | tls_matrix::destination_uses_the_same_policy_path | live |
| `merge_strategy` (dest-wide + per-table) | unset (`delete_insert`) | delete_insert (pervasive); upsert converges in place, unique index auto, 23505 typed; scd2 history; EXPLICIT config under append/replace typed, default never rejects (R5) | shredded upsert/scd2 typed | dest_conformance::keyed_structured_merge…, upsert_converges…, duplicate_keys_under_upsert…, shredded_upsert_is_rejected…; scd2 suite; param_matrix::explicit_strategy_under_non_merge_mode_is_typed | live |
| `hard_delete` | absent | bool flag `IS TRUE`; non-bool `IS NOT NULL`; survivor's flag decides; root-only | child typed; missing column typed; scd2 combo typed | dest_conformance::upsert_converges_and_hard_delete_removes_keys, flagged_then_recreated_root_keeps_its_subtree, child_hard_delete_is_rejected_typed; param_matrix::non_bool_hard_delete_flag_uses_is_not_null; refinements::dedup_sort_survivor_drives_hard_delete; config.rs::validation_matrix | live+unit |
| `dedup_sort` | absent = last-wins | desc/asc survivors vs arrival; FR-002 absence unchanged; NULL/tie policy; drives hard-delete/upsert/scd2 | column existence/collisions/key-column/shredded/non-merge — each typed | refinements::dedup_sort_orders_survivors_not_arrival, dedup_sort_null_and_tie_policy_is_deterministic, dedup_sort_survivor_drives_scd2_change_detection, refinement_options_validate_typed_at_open, refinement_options_reject_shredded_streams; config.rs shape cells | live+unit |
| `merge_key` | absent | scope replacement; untouched scopes; scope-moves; NULL not a scope; auto index; per-table single-unit rule + empty-unit tolerance; composes with upsert+hard_delete+dedup_sort | split feed typed; existence/collision/shredded/scd2/non-merge typed | refinements::merge_key_* (5 cells), scoped_feed_in_a_later_unit…, merge_key_composes…; dest_crash_sweep::sweep_postgres_destination_refined_merge | live+sweep |
| `scd2.valid_from`/`valid_to` | `_rdlt_valid_from`/`_rdlt_valid_to` | history + point-in-time on defaults; CUSTOM names flow end-to-end | collision/same-name typed | scd2::three_rounds_produce_correct_history_and_point_in_time; scd2::custom_validity_column_names_flow_end_to_end; scd2::rejections_are_typed_at_ensure | live |
| `scd2.absent` | `keep` | keep leaves active (three_rounds); retire closes missing keys; redelivery adds zero versions; per-table single-unit rule + empty-unit tolerance | split feed typed (S6) | scd2::absent_retire_closes_missing_keys, redelivery_adds_zero_versions, absent_retire_rejects_multi_unit_loads; refinements::scd2_retire_shares_the_per_table_single_unit_rule | live |

## CLI pipeline spec

| parameter | default | behaviors proven | validation proven | cells | class |
|---|---|---|---|---|---|
| `pipeline` | required | ids engine state (resume across runs — pervasive in every two-run cell) | — | e.g. incremental::delta_loads… run pairs | live |
| `workdir` | `.rdlt` | custom parses; default applied downstream | — | main.rs::pipeline_spec_forms_parse | unit |
| `write_mode` | `append` | all three forms + absent-default parse; merge behavioral everywhere | — | main.rs::pipeline_spec_forms_parse; every merge suite | unit+live |
| `source.postgres` | required | inline document = natural form, validated via from_value gate; `{config: path}` file form; C1 holds inline | mixing forms = loud error | main.rs::inline_postgres_source_parses_and_validates | unit |
| `destination.*` | required | postgres rows above; duckdb/parquet kinds parse (scope: parse-only per spec) | — | main.rs::pipeline_spec_forms_parse; cdc_composition_warning_matrix (duckdb) | unit |
| refinement passthrough | — | per-table options ride the YAML | — | main.rs::refinement_options_pass_through_the_yaml | unit |

## Mismatches found & resolved (PM6)

1. `merge_strategy` silently unused under append/replace (recorded 010
   footnote) → R5 typed rejection for EXPLICIT configuration;
   `PgDestOptions.merge_strategy` now `Option` (default never rejects).
   Cell: param_matrix::explicit_strategy_under_non_merge_mode_is_typed.
   README + 008 contract amended.
2. No behavior/doc mismatches found in the audited rows beyond #1 — two
   test-expectation errors during cell writing (lag-window replay is
   source-side deduped; incremental streams always emit ≥2 commits) were
   documentation-of-tests issues, corrected in the cells themselves.
