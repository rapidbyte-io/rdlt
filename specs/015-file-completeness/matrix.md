# Parameter Traceability Matrix (015 — the file family)

Every user-settable parameter → the cells that prove it (011 rules, zero
uncited rows). Citations are `file::test` (nextest: `-E 'test(<name>)'`).
Class: unit (in-src `::tests`) | local (tests/, no container) | live
(RUSTFS container, skip-not-fail) | sweep (`--features failpoints`).
Schema round-trips: `config_schema.rs::documented_example_validates_and_parses`
+ `::schema_valid_corpus_parses` (source) and
`::dest_schema_valid_corpus_parses` (dest);
`::unknown_fields_fail_schema_and_parser_identically` (both reject
unknown fields). The preservation net (FF1) is
`preservation.rs` — pre-015 cursor/commit-log/config documents keep
their exact meaning.

## Source — stream entry

| parameter | default | behaviors proven | validation proven | cells | class |
|---|---|---|---|---|---|
| `name` | required | stream/table naming through the engine | — | e2e_duckdb.rs::incremental_jsonl_to_duckdb | local |
| `format` | required | jsonl (record fast path), parquet (structured, S7), csv (record via NDJSON, R4) | explicit — no extension magic (pre-015 rule) | jsonl.rs (all); e2e_copy.rs::parquet_to_parquet_copy_preserves_types_and_resumes; csv.rs (all) | local |
| `path` | required | explicit path, glob (lexicographic order), existing-file-literal-even-with-metacharacters; empty glob = success; named-missing = typed; partial listing never passes | — | jsonl.rs::full_glob_load_in_path_order + empty_glob_is_an_empty_success + missing_named_file_is_an_error + literal_path_with_glob_metacharacters_is_taken_literally + unreadable_directory_in_glob_is_an_error_not_a_partial_list | local |
| `location` | absent = local | S3-compatible reading: deterministic listing w/ etags, COMPLETE across pagination (1100 keys), prefix+glob keys, range-read tails, engine-driven exact totals + delta by read accounting | `location` block must name a kind; endpoint/bucket eager typed; unreachable endpoint + wrong credentials typed NAMING endpoint/bucket (empty prefix stays success); credential values never render | s3_live.rs::seeded_bucket_lists_deterministically + listing_survives_pagination + missing_named_object_is_typed_and_empty_prefix_is_success + wrong_credentials_are_typed + unreachable_endpoint_is_typed + range_read_returns_the_tail + seeded_bucket_loads_and_delta_runs_through_the_engine; src/location/mod.rs::tests (validation + grep-proof) | live+unit |
| `csv` (options block) | `{delimiter: ",", header: true, quote: "\""}` | delimiter/quote/headerless matrix (`c0..cN`) | block only with `format: csv` (typed); non-ASCII delimiter/quote typed | csv.rs::options_matrix; csv.rs::csv_block_requires_csv_format | local |
| `primary_key` | absent | rides StreamSpec (merge identity downstream; the sweep converges on it) | parquet + primary_key typed (S7, pre-015 rule) | sweep.rs::file_source_read_path_survives_crash_sweep; preservation.rs::pre_015_source_config_spellings_parse | sweep+local |
| `type_hints` | `{}` | jsonl: stream-spec hints (pre-015); csv: OVERRIDE inference — parse-checked bool/int64/float64, json parsed, string-shaped passthrough; violations typed naming file+row+column; landed types queryable (timestamptz arithmetic in duckdb) | — | csv.rs::hints_override_and_violations_are_typed; s3_live.rs::quickstart_shape_csv_gz_with_hints_and_delta | local+live |
| `validate` (jsonl) | `true` | skim-parse per line; malformed typed naming file + byte offset | — | jsonl.rs::malformed_line_fails_naming_file_and_offset | local |

## Source — formats & codecs

| behavior | proven | cells | class |
|---|---|---|---|
| CSV inference lattice | bool→int64→float64→utf8 over the WHOLE file; empty = null; mixed column widens | csv.rs::inference_lattice_and_nulls | local |
| CSV malformed row | typed naming file + 1-based row (ragged row) | csv.rs::malformed_row_is_typed | local |
| CSV empty file | header-only = zero rows, success | csv.rs::header_only_file_is_empty_success | local |
| gzip/zstd decode | transparent by extension (jsonl + csv), exact totals | csv.rs::compressed_jsonl_reads_and_skips_when_complete + compressed_csv_reads | local |
| codec mismatch | magic-byte check, typed naming the file | csv.rs::codec_extension_mismatch_is_typed | local |
| compressed parquet | rejected at parse (parquet owns its codecs) | csv.rs::compressed_parquet_rejected_at_parse | local |
| parquet structured reads | Arrow batches, row-group cursor units, types preserved end-to-end | e2e_copy.rs::parquet_to_parquet_copy_preserves_types_and_resumes; s3_live.rs::parquet_objects_load_through_the_engine (temp-fetch path + completed-skip) | local+live |

## Source — incremental (one cursor rulebook, FF3)

| rule | proven | cells | class |
|---|---|---|---|
| completed files skipped | unchanged second run reads nothing (local); completed objects skipped (bucket) | jsonl.rs::unchanged_second_run_reads_nothing; s3_live.rs::parquet_objects_load_through_the_engine | local+live |
| grown tails resumed (plain jsonl) | tail-only re-request local + object (delta by read accounting) | jsonl.rs::resume_reads_only_appended_tail_and_new_files; s3_live.rs::seeded_bucket_loads_and_delta_runs_through_the_engine | local+live |
| shrunk = typed naming the file | local + the pre-015 pin | jsonl.rs::shrunk_file_fails_naming_it; preservation.rs::pre_015_cursor_document_parses_and_plans_identically | local |
| rewritten-in-place tripwire | mtime (local) AND etag (object, same size different etag) | jsonl.rs::same_size_rewrite_fails_loudly; s3_live.rs::same_size_rewrite_is_typed_by_etag | local+live |
| mid-record offset guard | growth after an unterminated final line typed | jsonl.rs::growth_after_unterminated_final_line_fails_loudly | local |
| whole-file units (csv/compressed) | complete+unchanged skips; size change typed (never grow in place); csv.gz delta loads only new objects | csv.rs::compressed_jsonl_reads_and_skips_when_complete; s3_live.rs::quickstart_shape_csv_gz_with_hints_and_delta; src/source/cursor.rs::tests | local+live+unit |
| pre-015 cursor documents | parse + drive the full plan matrix identically (format_version 1, additive etag field) | preservation.rs::pre_015_cursor_document_parses_and_plans_identically | local |

## Destination — config (`FileDestConfig` / CLI `file:` + frozen `parquet:`)

| parameter | default | behaviors proven | validation proven | cells | class |
|---|---|---|---|---|---|
| `path` | required | output dir (local) / key prefix (bucket) | empty typed | dest_options.rs::dest_config_validation_is_typed; every dest cell | local |
| `location` | absent = local | staged PUT → COPY+DELETE finalize; commit-atomic visibility (the FF5 probe: a concurrent lister through a 20k-row run observes ONLY final part names; staging fully consumed); exact bucket totals | shared vocabulary (see source `location` row) | s3_live.rs::dest_publishes_atomically_to_the_bucket | live |
| `format` | `parquet` | parquet (pre-015 protocol byte-identical); jsonl parts = valid NDJSON with same staging/rename protocol and totals parity | — | dest_options.rs::jsonl_format_writes_ndjson_parts; s3_live.rs::dest_partitions_jsonl_in_the_bucket; preservation.rs::pre_015_artifact_names_and_commit_log_shape_are_frozen | local+live |
| `partition_by` | absent | one prefix per rendered value, rows in exactly one partition file set, NULL → `__null__` — local and bucket | empty column name typed; column missing from schema typed NAMING it at write | dest_options.rs::partition_by_splits_rows + missing_partition_column_is_typed; s3_live.rs::dest_partitions_jsonl_in_the_bucket | local+live |
| (frozen `parquet:` spelling) | — | `ParquetDir::open(dir)` ≡ local parquet; part naming, `_rdlt_state`/`_rdlt_commits` scope-hashed names, commit-log shape, D3 receipt dedup from PLANTED pre-015 bytes | — | preservation.rs::pre_015_artifact_names_and_commit_log_shape_are_frozen + pre_015_commit_log_fixture_drives_receipt_dedup; recovery.rs (all three); dest_conformance.rs (all) | local |

## Cross-cutting

| concern | proven | cells | class |
|---|---|---|---|
| SPI conformance | source + destination pass the shared harnesses; merge still rejected by capability | conformance.rs::file_source_is_conformant; dest_conformance.rs::parquet_destination_is_conformant + merge_still_rejected_by_capability_even_with_declared_key | local |
| crash discipline (FF7) | source points (file.list/file.read) ×3 actions local; dest object points (file.stage.put/finalize.copy/finalize.delete) ×3 actions in the bucket; armed-fire pins exact, exactly-once totals; pq.* preserved set swept by the engine crash_sweep | sweep.rs::file_source_read_path_survives_crash_sweep + file_dest_s3_path_survives_crash_sweep; rdlt-engine/tests/crash_sweep.rs | sweep |
| recovery protocol | replace-once-per-load durable guard; final names independent of cross-table arrival; pipeline scoping under a shared dir | recovery.rs::replace_recovery_session_keeps_prior_commits_of_same_load + final_names_independent_of_cross_table_arrival_order + open_does_not_destroy_another_pipelines_staging_or_state | local |
| flagship path | jsonl → duckdb incremental engine e2e (the bench shape) | e2e_duckdb.rs::incremental_jsonl_to_duckdb | local |
| CLI spellings | `parquet:` parses unchanged; `file:` carries the full vocabulary | rdlt-cli/src/main.rs::tests (spec parse cells); preservation.rs::pre_015_source_config_spellings_parse | unit |
