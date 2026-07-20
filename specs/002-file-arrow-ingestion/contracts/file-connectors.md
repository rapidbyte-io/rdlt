# Contract: File Connectors (feature 002)

**Audience**: users of `rdlt-source-file` / `rdlt-dest-parquet`; both connectors are
bound by the full feature-001 SPI contract and certified by the public conformance
suites.

## `rdlt-source-file` configuration (YAML/TOML-friendly)

```yaml
streams:
  - name: events
    format: jsonl            # jsonl | parquet (explicit; no extension magic)
    path: "data/events-*.jsonl"   # explicit file or glob
    # jsonl streams may additionally use feature-001 record-stream options
    # (primary_key, type_hints); parquet streams are structured (S7) and may not
    # declare primary_key (no per-row identity).
```

Behavioral contract (beyond the S-clauses):

| Rule | Behavior |
|---|---|
| Selection | Glob resolved once per run; lexicographic path order; empty glob ⇒ empty stream (success), explicitly named missing file ⇒ fatal error naming it. |
| Resume | Cursor = per-file progress (data-model §1). Completed ranges are never re-read; appended tails resume at the recorded offset (only when the consumed range ended at a record boundary — growth after an unterminated final line is a fatal error, never a mid-record read); shrunk files, and same-size files whose mtime moved (rewritten in place), are a fatal, file-naming error. |
| Checkpoints | After every pushed slab / row-group batch and at each file boundary. |
| Malformed input | Invalid JSON line / non-UTF-8 / corrupt parquet ⇒ fatal error naming the file (and byte offset for JSONL). Never skip silently. |
| Format semantics | `jsonl` streams are record streams (full lineage, Merge allowed). `parquet` streams are structured (S7/E7/B4 apply). |

## `rdlt-dest-parquet` contract

- Capabilities: `merge: false`, `structs: true`, `scalar_lists: true`,
  `decimal: true`, `json_type: false`.
- Layout & publication: data-model §4 (temp-dir staging, atomic renames,
  `_rdlt_state.json` / `_rdlt_commits.json` with `format_version`).
- Honors D1–D6 and passes the destination conformance suite. Known bound,
  documented: multi-file publication is not atomic as a SET; recovery re-commits
  converge because staged file names are deterministic per
  `(load_id, commit_seq, table, n)` (research R18).
- Out of scope v1: merge, partitioning, compaction, object stores.
