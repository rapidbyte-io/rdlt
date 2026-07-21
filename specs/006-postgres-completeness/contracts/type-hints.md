# Contract: Per-Column Type Hints (amends 005 type-mapping.md)

**Feature**: 006-postgres-completeness | **Date**: 2026-07-20

`tables[].type_hints` / `queries[].type_hints`: `{column: hint}`, hint
vocabulary shared with the rest/file sources. A hint compiles to a
server-side cast in the projection + the matching wire decode — the
binary stream still carries only the 005 lossless decode set.

## The closed conversion table

| Source type (reflected/described) | Allowed hints | Cast rule |
|---|---|---|
| ANY | `utf8` | `(col)::text` — canonical text, always available |
| text family (text/varchar/bpchar/name/citext) | `int64`, `float64`, `decimal(p,s)`, `timestamp_tz`, `timestamp_naive`, `date`, `time`, `uuid`, `bool`, `json`, `binary` | strict server cast to the target (`::int8`, `::timestamptz`, `::jsonb`, `::bytea` — binary parses the text as bytea input, e.g. `\\x…` hex) |
| int2/int4/int8 | `float64`, `decimal(p,s)`, `bool` (0/1 only), `utf8` | numeric widening casts; bool via `<>0` is NOT implied — `::bool` strictness applies |
| float4/float8 | `decimal(p,s)`, `utf8` | `::numeric(p,s)` (server rounds per SQL rules — [documented-lossy]) |
| numeric (any) | `float64` ([documented-lossy]), `decimal(p,s)` re-shape, `utf8` | explicit casts |
| timestamptz ↔ timestamp | each other ([documented-lossy]: zone semantics), `date`, `utf8` | explicit casts |
| date | `timestamp_tz`, `timestamp_naive`, `utf8` | midnight expansion |
| uuid | `utf8` | text form |
| json/jsonb | `utf8` | text form |
| anything else | `utf8` only | textual fallback row |

Any (source, hint) pair NOT in this table is a **typed config error at
open** — no best-effort casting. Additions to the table are contract
changes (test + doc in the same change).

## Semantics

- Cast failures (e.g. text `"abc"` hinted `int64`) abort the stream
  with a typed copy-phase error naming the column. Engine clause E7
  gives structured streams no value-level discard, so the schema-policy
  outcome for a bad value IS the typed error — never silent coercion,
  never a dropped value.
- Hinted cursor columns must remain cursor-capable post-hint.
- Representation-changing hints ([documented-lossy] rows) join the
  lossy-visibility surface (one warn per column per read).
