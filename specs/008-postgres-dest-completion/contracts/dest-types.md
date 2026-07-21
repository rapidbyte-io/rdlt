# Contract: Destination Native Types

Extends the 005 destination behavior; the source-side type-mapping
contract (`specs/005-postgres-source/contracts/type-mapping.md`) is
unchanged — this contract governs what the DESTINATION creates and how
values land.

## Rules

| # | Rule |
|---|---|
| T1 | Decimal-typed columns create `NUMERIC(p,s)` with the stream's declared precision/scale; loaded values are exact — `SUM()` over the destination equals the source total with zero float involvement. The engine's `decimal` capability is declared true; lowering no longer converts decimals to text for this destination. |
| T2 | Json-typed columns create `JSONB`; documents queryable by JSON path. Documents the binary JSON type rejects (NUL escapes, invalid surrogates) fail typed NAMING the column — never truncated or silently texted. `json_type` capability true. |
| T3 | Uuid-typed columns create `UUID`; equality joins against uuid literals work. (No engine capability involved — the destination recognizes the logical type.) |
| T4 | Columns with `nullable: false` create `NOT NULL` — at CREATE time only. Migrations remain additive: NOT NULL is never added to existing columns. |
| T5 | All native values ride the SAME binary bulk path as before (no per-row fallback); the wire encoders are exact mirrors of the source's decoders, proven by encode→decode round-trip property tests over the full value range incl. NULLs and precision edges. |
| T6 | Column-type decisions come from the table SCHEMA's logical types, never from raw arrow types — a plain text column and a json/uuid-logical column (same arrow representation) can never confuse. |
| T7 | Pre-008 tables with text columns where a native type would now be chosen: the column is NEVER silently retyped; values continue landing as text exactly as before; ONE `tracing::warn!` on the `rdlt::lossy` target per column per session names the table, column, wanted type, and the documented migration paths (fresh table via a new name + Replace, or manual `ALTER … USING`). |
| T8 | The type matrix conformance suite extends to the native types against a real server: catalog type assertions + value round-trips incl. NULL rows and boundary values. |

## Non-rules (explicitly out)

Geometry/PostGIS types; automatic retyping of existing columns;
text/CSV COPY fallback paths.
