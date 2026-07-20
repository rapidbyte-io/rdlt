# Contract: Query Streams

**Feature**: 006-postgres-completeness | **Date**: 2026-07-20

```yaml
queries:
  - name: order_totals            # unique across tables AND queries
    sql: |
      SELECT o.id, o.updated_at, sum(i.amount) AS total
      FROM orders o JOIN order_items i ON i.order_id = o.id
      GROUP BY o.id, o.updated_at
    cursor: { column: updated_at }   # column must exist in the OUTPUT
    primary_key: [id]                # required for merge; optional otherwise
    type_hints: { total: decimal(14,2) }
```

## Rules

1. The user SQL is ALWAYS executed as `SELECT * FROM ( <sql> ) AS q` —
   this single wrapper enforces read-only (the database rejects
   data-modifying statements/CTEs in a subquery, before any data
   moves), hosts the incremental predicate/ORDER BY, and gives the
   statement-level snapshot tables get.
2. Schema is DESCRIBED, not reflected: column names + types from the
   database's own description of the wrapped statement, mapped by the
   005 type-mapping contract. Precision/scale are not described —
   numerics take the textual policy row unless hinted. Nullability is
   not described — all columns nullable.
3. Cursor configuration behaves exactly as on tables (boundaries,
   dedup, mid-stream checkpoints, watermark monotonicity) and requires
   the cursor column in the described output; `primary_key` (declared,
   nothing to reflect) feeds dedup keys and merge exactly as a table's
   reflected key does.
4. Typed config errors at open: duplicate/colliding names, cursor
   column absent from output, description failure (syntax error,
   mutating statement), hint violations per the hints contract.
5. Every run re-describes (discovery is once per run, as for tables);
   described-schema drift between runs follows the existing
   schema-evolution policies.
