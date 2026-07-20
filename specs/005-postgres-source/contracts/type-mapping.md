# Contract: Postgres → LogicalType Mapping

**Feature**: 005-postgres-source | **Date**: 2026-07-20

The binding correspondence from reflected Postgres types to
`rdlt_core::LogicalType`. Every cell is conformance-tested (round-trip
through a real Postgres). Rules marked **[documented-lossy]** change
representation, never value content, and are called out in user docs.
Silent coercion is forbidden — any type not covered below falls to the
**textual fallback** rule, never to inference.

> **Structured-path constraint (discovered against the engine at
> implement time)**: structured streams derive logical types purely
> from Arrow `DataType`s (engine clause E7; `passthrough.rs::
> column_type_from_arrow`) — Arrow carries no uuid/json types, so a
> structured source CANNOT produce `LogicalType::Uuid`/`Json`. The
> rows below therefore land those values as `Utf8` carrying their
> canonical text (uuid: 36-char lowercase; json/jsonb/arrays/
> composites/ranges: canonical JSON text) — the "opaque, never
> shredded" semantics hold; only the logical label differs from the
> original draft of this contract. Logical-type fidelity for
> structured sources (e.g. Arrow field metadata honored by the
> passthrough) is a recorded backlog item — an engine-contract change
> this feature does not make (FR-012).

## Scalar mappings (lossless)

| Postgres | LogicalType | Wire decode note |
|---|---|---|
| `bool` | `Bool` | 1 byte |
| `int2`, `int4`, `int8` | `Int64` | widened |
| `float4`, `float8` | `Float64` | widened; NaN/±inf pass through |
| `numeric(p,s)`, p ≤ 38 | `Decimal{p,s}` | binary NBASE-10000 digits → i128 |
| `text`, `varchar(n)`, `char(n)`, `name` | `Utf8` | `char(n)` keeps pad spaces as stored |
| `bytea` | `Binary` | |
| `timestamptz` | `TimestampTz` | µs since PG epoch, rebased to Unix epoch |
| `timestamp` | `TimestampNaive` | µs, rebased |
| `date` | `Date` | days, rebased |
| `time` | `Time` | µs |
| `uuid` | `Utf8` | 36-char lowercase canonical text (structured-path constraint above) |
| `json`, `jsonb` | `Utf8` (canonical JSON text) | jsonb: version byte stripped; NOT shredded (structured-path constraint above) |

## Policy mappings [documented-lossy: representation changes, values survive]

| Postgres | LogicalType | Rule |
|---|---|---|
| `numeric` unconstrained or p > 38 | `Utf8` | canonical text — no precision loss ever |
| enum types | `Utf8` | label text |
| arrays (any element) | `Utf8` (canonical JSON text) | server-side `to_jsonb(col)::text` rendering |
| composite / row types | `Utf8` (canonical JSON text) | server-side `to_jsonb(col)::text` |
| range / multirange | `Utf8` (canonical JSON text) | server-side `to_jsonb(col)::text` |
| `timetz` | `Utf8` | no tz-aware time type in the lattice |
| `interval` | `Utf8` | ISO-8601 duration text |
| `inet`, `cidr`, `macaddr(8)` | `Utf8` | canonical text |
| `money` | `Utf8` | locale-dependent semantics; text preserves what PG returns |
| domains | base type's rule | reflected through to the base |
| anything else | `Utf8` | **textual fallback**: value's canonical text; run report notes the column |

## Special values

- `NULL` → Arrow null (nullability from reflection; a NOT NULL column
  with a wire NULL is a typed decode error — drift, not data).
- `timestamp[tz]`/`date` **±infinity** → saturate to the type's
  min/max representable instant **[documented-lossy]**; never NULL,
  never an error — extremes remain visible and sortable.
- `NaN` in `numeric` → typed decode error under `Decimal` (Arrow
  decimal cannot hold it); under the Utf8 numeric rule it passes as
  text. Conformance-tested both ways.

## Cursor-capable types

`Int64`, `Decimal`, `Utf8` (incl. uuid-as-text), `TimestampTz`,
`TimestampNaive`, `Date`, `Time` — the set the source-config contract
accepts for `cursor.column`. Cursor JSON rendering (state round-trip):
Int64 as number; Decimal/uuid/temporal as canonical strings;
property-tested `decode(encode(v)) == v`.

## Conformance obligations

1. Every row of both tables above has a round-trip test against real
   Postgres (seed → extract → assert Arrow type + value).
2. The differential property test (R8) covers every mapping the
   generator can produce — binary decoder ≡ driver-row reference.
3. Additions to this table are additive contract changes: new rows
   require a test and a doc entry in the same change.
