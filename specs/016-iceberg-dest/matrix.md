# Parameter Traceability Matrix (016 — Iceberg destination)

Every user-settable parameter → the cells that prove it (011 rules, zero
uncited rows). Citations are `file::test` (nextest: `-E 'test(<name>)'`).
Class: unit (in-src `::tests`) | local (tests/, no container) | live
(Polaris + RUSTFS containers, skip-not-fail) | sweep (`--features
failpoints`) | deep (`make test TARGET=deep`, RDLT_DEEP-gated Spark).
Schema round-trips: `config_schema.rs::schema_valid_corpus_parses` +
`::unknown_fields_fail_schema_and_parser_identically` (schema and parser
cannot drift; unknown fields rejected identically).

## Config — catalog

| parameter | default | behaviors proven | validation proven | cells | class |
|---|---|---|---|---|---|
| `catalog.uri` | required | REST catalog connection end-to-end; unreachable endpoint typed naming it | http(s) scheme eager typed | catalog_live.rs::engine_append_exact_totals_one_snapshot_per_commit + open_unreachable_catalog_is_typed; config_schema.rs::validation_matrix | live+local |
| `catalog.warehouse` | required | provider warehouse selection | non-empty eager; nonexistent warehouse typed NAMING it live | catalog_live.rs::open_failures_are_typed_against_live_catalog; config_schema.rs::validation_matrix | live+local |
| `catalog.auth.oauth2_client_credentials` | one-of | client-credentials leg against Polaris (id/secret/scopes; token_url defaults to the catalog's endpoint); wrong secret typed | exactly-one-scheme eager (both/none typed); secret never renders | every live cell (fixture auth); catalog_live.rs::open_failures_are_typed_against_live_catalog; config_schema.rs::validation_matrix + secrets_never_render | live+local |
| `catalog.auth.bearer` | one-of | static token attached as `Authorization: Bearer` through a full engine run + receipt oracle (the header path any bearer catalog sees; UC OSS write leg RECORDED read-only at T001 — live UC arm deferred, research R8 addendum) | same exactly-one rule; token never renders | providers.rs::bearer_auth_against_live_catalog; config_schema.rs::validation_matrix + secrets_never_render | live+local |
| `catalog.props` | `{}` | verbatim passthrough into catalog properties (escape hatch; wins over generated props by insertion order) | — | src/dest/commit.rs::connect (construction); config_schema.rs::schema_valid_corpus_parses | local |

## Config — namespace & storage

| parameter | default | behaviors proven | validation proven | cells | class |
|---|---|---|---|---|---|
| `namespace` | required | dot-separated levels → multi-level NamespaceIdent | non-empty levels eager typed | config_schema.rs::validation_matrix + helper_lookups; every live cell | live+local |
| `create_namespace` | `false` | true: created iff missing (concurrent creator tolerated); false: missing namespace typed with the remedy | — | commit.rs::ensure_namespace (all live cells run with true); catalog_live.rs fixture path | live |
| `storage` (absent) | delegation | catalog config-defaults carry the S3 credentials — NO user keys in the dest config (the verified Polaris delegation path) | — | providers.rs::vended_credentials_no_storage_block | live |
| `storage.s3.{endpoint,region,access_key,secret_key,path_style}` | absent | explicit override end-to-end; PRECEDENCE proven: a wrong override FAILS (never silently ignored for vended defaults), nothing lands, secret never renders | storage block must name a kind (eager); keys never render | providers.rs::explicit_storage_override_works + wrong_storage_override_fails_not_silently_ignored; config_schema.rs::validation_matrix + secrets_never_render | live+local |

## Config — tables & partitioning

| parameter | default | behaviors proven | validation proven | cells | class |
|---|---|---|---|---|---|
| `tables.<stream>.name` | stream name | rename lookup | non-empty typed | config_schema.rs::helper_lookups + validation_matrix | local |
| `tables.<stream>.partition_by` | `[]` | identity + temporal transforms → partition spec at CREATE (field-ids 1000+, spec-id explicit — Polaris parses strictly, probed live); fanout writes = one data file per partition value per commit; spec visible in raw metadata AND to pyiceberg | unknown column typed at ensure; unknown transform cannot parse (closed enum); spec FIXED at creation — live-table mismatch typed | src/dest/schema.rs::partition_tests (identity_and_temporal_transforms_build + empty_partition_by_is_none + unknown_partition_column_is_typed); partitioning.rs::partitioned_writes_fan_out_and_spec_is_visible + partition_spec_mismatch_is_typed; interop.rs::pyiceberg_reads_partitioned | unit+live |
| reserved `_rdlt_state` | — | stream table colliding with the state marker table typed-rejected | — | src/dest/dest.rs (ensure_table guard); exercised by every state cell via the marker table | live |

## Write modes (ID5 — no silent degradation)

| mode | behavior | cells | class |
|---|---|---|---|
| Append | fast-append snapshots, exact totals, one snapshot per non-empty commit, empty window publishes NOTHING | catalog_live.rs::engine_append_exact_totals_one_snapshot_per_commit + engine_empty_commit_publishes_no_snapshot | live |
| Replace | typed "not supported by this release" at ensure_table — the RECORDED T001/T008 narrowing (iceberg-rust 0.10 has no overwrite transaction; emulation forbidden) | exactly_once.rs::replace_rejected_against_live_catalog; spec FR-008 | live |
| Merge | rejected by capability (`merge: false`) with a typed error | exactly_once.rs::replace_rejected_against_live_catalog (second arm) | live |

## Exactly-once & state (ID2)

| rule | proven | cells | class |
|---|---|---|---|
| commit identity in snapshot summaries | rdlt.pipeline/load-id/commit-seq on every snapshot, readable via the raw-catalog oracle AND pyiceberg | catalog_live.rs::engine_append_exact_totals_one_snapshot_per_commit; interop.rs::pyiceberg_reads_plain_append | live |
| replay publishes nothing | fresh session re-committing a landed (load, seq): snapshot count unchanged, receipt returned, new identities append on top | exactly_once.rs::replayed_commit_publishes_nothing | live |
| data-file names unique across sessions | session nonce in file names — a recovery session never overwrites a committed file (found by the T009 sweep; window counter survives re-ensure) | sweep.rs::iceberg_dest_survives_crash_sweep (regression trigger) | sweep |
| state doc on the `_rdlt_state` marker table | written state-LAST after per-table commits; second run resumes from the persisted cursor, zero re-reads, no snapshot | exactly_once.rs::incremental_run_resumes_from_catalog_state | live |
| state home rationale | namespace properties REJECTED live (iceberg-catalog-rest 0.10 update_namespace = FeatureUnsupported); per-stream properties fail recovery (not enumerable) — research R3 addendum | design record; behavior pinned by the resume cell above | — |

## Conflict retry (ID3)

| rule | proven | cells | class |
|---|---|---|---|
| bounded refresh→rebuild→commit | 3 injected conflicts land on attempt 4 (exactly COMMIT_ATTEMPTS update_table calls) | src/dest/commit.rs::tests::commit_retries_through_transient_conflicts | unit |
| exhaustion typed | names table + attempt bound, never an unbounded loop | src/dest/commit.rs::tests::commit_exhaustion_is_typed | unit |
| no foreign snapshot lost | two live writers hammering one table: every commit is a snapshot, both identity sets complete, exact totals | conflict.rs::competing_writers_lose_no_snapshots | live |
| state-property commits conflict-retried | same bounded loop around update_table_properties | src/dest/commit.rs::write_state (construction); exercised by every multi-run live cell | live |

## Schema mapping & drift (ID6)

| rule | proven | cells | class |
|---|---|---|---|
| closed scalar table | every LogicalType → Iceberg type (Json → string documented) | src/dest/schema.rs::tests::closed_table_maps_every_scalar | unit |
| nested shapes | struct/scalar-list recursion, unique gapless field ids | src/dest/schema.rs::tests::nested_shapes_map_recursively_with_unique_ids | unit |
| additive drift | new nullable column added via UpdateSchema in one transaction; evolved schema + full count readable by pyiceberg and Spark | interop.rs::pyiceberg_reads_after_additive_drift; spark_deep.rs::spark_reads_all_three_shapes | live+deep |
| type conflicts | typed naming the column (reconcile) | src/dest/commit.rs::reconcile (construction); classification pinned by errors unit cells | unit |
| batch alignment | by-name column matching, arrow-cast where representations differ, missing column typed | src/dest/dest.rs::align (every live engine cell drives it) | live |

## Errors (typed, FR-009)

| rule | proven | cells | class |
|---|---|---|---|
| classification matrix | Unexpected→transient, CatalogCommitConflicts→fatal-with-context, config/data fatal naming subject, unknown kinds LOUD fatal | src/dest/errors.rs::tests::classification_matrix + conflict_detection | unit |
| unreachable / unauthorized / missing-warehouse | typed naming endpoint / subject / warehouse | catalog_live.rs::open_unreachable_catalog_is_typed + open_failures_are_typed_against_live_catalog | live |
| credential expiry mid-run | GAP (recorded): the fixture cannot simulate token expiry; the classification unit cells stand for the transient posture | src/dest/errors.rs::tests::classification_matrix | unit |

## Crash discipline (ID7)

| rule | proven | cells | class |
|---|---|---|---|
| `ice.files.write` / `ice.commit` / `ice.receipt.visible` ×3 actions | armed-fire pinned (all 9 combos fire), crash armed twice + recover disarmed = exact totals, duplicate-free identity history | sweep.rs::iceberg_dest_survives_crash_sweep | sweep |
| registry | `ICE_FAIL_POINTS` = the three points, asserted by the sweep loop | sweep.rs (registry-driven) | sweep |

## Interop (ID8)

| oracle | shapes | cells | class |
|---|---|---|---|
| pyiceberg 0.11.1 (pinned venv) | plain ×2 commits (count/columns/snapshots/receipts), partitioned (spec), post-drift (evolved schema) | interop.rs (all three) | live |
| Spark 3.5.3 + iceberg-runtime 1.7.1 | the same three shapes (counts, types, drifted column) | spark_deep.rs::spark_reads_all_three_shapes | deep |
| raw-catalog receipt oracle | snapshot summaries independent of the crate | tests/common/mod.rs::snapshot_summaries (used by every live cell) | live |

## CLI & facade

| surface | proven | cells | class |
|---|---|---|---|
| `destination: iceberg:` pipeline block | full vocabulary parses from YAML, validation typed at spec load, secret redacted | rdlt-cli main.rs::tests::iceberg_spec_parses_from_the_yaml | local |
| `rdlt::connector::iceberg` + feature `iceberg` | facade re-export compiles into the CLI build (feature enabled) | rdlt-cli build (feature list) | local |
| bench cell `iceberg-polaris-200k` | scoreboard, subprocess mode, verify 200k rows; no dlt pair (RECORDED — dlt's Iceberg leg needs pyiceberg+filesystem wiring outside the competitor harness's shape) | benches/cells/e2e.toml; artifacts under benches/artifacts/ | bench |

## Recorded deferrals (phase-2 doors, not gaps in this release)

- Glue / SigV4: no per-request signing seam in iceberg-catalog-rest 0.10
  (T001 verdict d); doors recorded in research R4.
- UC OSS live write leg: Iceberg REST surface is read-only (T001 verdict
  e); bearer proven live against Polaris instead (R8 addendum).
- Credential expiry mid-run: unit-level classification only (above).
- Iceberg maintenance (compaction/expire-snapshots) and merge-on-read:
  out of scope by spec; see dlt-parity.md.
