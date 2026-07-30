//! Per-column type observation with value-checked widening.
//!
//! Drives `rdlt_core::widen` and layers the *value* checks the pure lattice cannot
//! know about: an `Int64` beyond ±2^53 meeting `Float64` escalates the column to
//! `Utf8` — losslessness is enforced at runtime, never assumed.
//!
//! Generic over [`JsonView`]: the tree and streaming paths
//! observe through the SAME logic — one lattice, one escalation rule.

use rdlt_core::types::{LogicalType, int64_fits_in_f64, widen};
use rdlt_core::{ColumnDef, ColumnType, Provenance};

use super::canon::parse_timestamp_tz;
use super::view::{JsonView, ValueKind};

/// Observation state for a scalar position (column, struct field, or list item).
#[derive(Debug, Default, Clone)]
pub(crate) struct ScalarState {
    /// `None` until the first non-null value.
    ty: Option<LogicalType>,
    /// Fixed by a per-column hint: inference must not widen it.
    pinned: bool,
    saw_inexact_int: bool,
    saw_float: bool,
}

impl ScalarState {
    pub(crate) fn pinned(ty: LogicalType) -> Self {
        Self {
            ty: Some(ty),
            pinned: true,
            ..Self::default()
        }
    }

    /// Was this column's type declared rather than inferred? A declared type is
    /// never rewritten by an observation, whatever shape the value has.
    pub(crate) fn is_pinned(&self) -> bool {
        self.pinned
    }

    pub(crate) fn observe<'a, V: JsonView<'a>>(&mut self, value: V) {
        if self.pinned {
            return;
        }
        let observed = match value.kind() {
            ValueKind::Null => return,
            ValueKind::Bool(_) => LogicalType::Bool,
            ValueKind::Int(i) => {
                if !int64_fits_in_f64(i) {
                    self.saw_inexact_int = true;
                }
                LogicalType::Int64
            }
            // A `u64` above `i64::MAX` has no exact Int64 OR Float64
            // representation, so text is the only type on the lattice that can
            // carry its digits. Observing Utf8 directly is what makes that
            // happen: `widen`'s catch-all sends every non-Binary/Json pairing to
            // Utf8, so the column resolves to text whichever order the values
            // arrive in. Deciding per value — text only above i64::MAX — would
            // make the resolved type depend on arrival order, which is the bug
            // class this lattice exists to prevent.
            ValueKind::UInt(_) => LogicalType::Utf8,
            ValueKind::Float(_) => {
                self.saw_float = true;
                LogicalType::Float64
            }
            ValueKind::Str(s) => {
                if parse_timestamp_tz(s).is_some() {
                    LogicalType::TimestampTz
                } else {
                    LogicalType::Utf8
                }
            }
            // Non-scalars reaching a scalar position are a shape conflict → Json.
            ValueKind::Object | ValueKind::Array => LogicalType::Json,
        };
        let joined = match self.ty {
            None => observed,
            Some(current) => widen(current, observed),
        };
        // Value-checked escalation: Float64 cannot exactly hold every observed Int64.
        self.ty = Some(if joined == LogicalType::Float64 && self.saw_inexact_int {
            LogicalType::Utf8
        } else {
            joined
        });
    }

    /// Resolved type; `Utf8` for never-observed (all-null) positions.
    pub(crate) fn resolve(&self) -> LogicalType {
        self.ty.unwrap_or(LogicalType::Utf8)
    }
}

/// Observation state for one column position, tracking shape as well as type.
#[derive(Debug, Clone)]
pub(crate) enum ColumnState {
    /// Only nulls seen so far.
    Unknown,
    Scalar(ScalarState),
    /// Nested object: fields in first-seen order (order is part of the schema hash).
    Struct(Vec<(String, ColumnState)>),
    ScalarList(ScalarState),
    /// List of objects — rows live in a child table; the column itself vanishes.
    ChildTable,
    /// Irreconcilable shapes; values preserved verbatim.
    Json,
}

impl ColumnState {
    pub(crate) fn observe<'a, V: JsonView<'a>>(&mut self, value: V, lists_as_columns: bool) {
        if value.is_null() {
            return;
        }
        match self {
            ColumnState::Json => {}
            ColumnState::Unknown => {
                *self = Self::fresh(value, lists_as_columns);
            }
            // A shape conflict widens an inferred column to Json — but a PINNED
            // column was declared by the user, and a value that does not fit the
            // declaration must not silently rewrite it. The pinned column keeps
            // its type here and the offending value is nulled and counted at
            // build time.
            //
            // Mutation note: this guard is REDUNDANT with `observe`, and
            // measurably so — forcing it either way is an equivalent mutant.
            // `ScalarState::observe` already returns early when pinned AND maps
            // `Object | Array` to `Json`, so both arms reach the same resolved
            // type by different routes. Kept because it states the rule where
            // the rule matters, rather than leaving a reader to find it inside
            // the scalar observer.
            ColumnState::Scalar(state) => match value.kind() {
                ValueKind::Object | ValueKind::Array if !state.is_pinned() => *self = ColumnState::Json,
                _ => state.observe(value),
            },
            ColumnState::Struct(fields) => match value.kind() {
                ValueKind::Object => {
                    for (key, item) in value.obj_entries() {
                        match fields.iter_mut().find(|(name, _)| name == key) {
                            Some((_, state)) => state.observe(item, lists_as_columns),
                            None => {
                                let mut state = ColumnState::Unknown;
                                state.observe(item, lists_as_columns);
                                fields.push((key.to_owned(), state));
                            }
                        }
                    }
                }
                _ => *self = ColumnState::Json,
            },
            ColumnState::ScalarList(item_state) => match value.kind() {
                ValueKind::Array => {
                    if value.arr_items().any(|item| item.is_object()) {
                        // scalars-then-objects in the same list position: irreconcilable
                        *self = ColumnState::Json;
                    } else {
                        for item in value.arr_items() {
                            if item.is_array() {
                                *self = ColumnState::Json; // nested lists: v1 escape hatch
                                return;
                            }
                            item_state.observe(item);
                        }
                    }
                }
                _ => *self = ColumnState::Json,
            },
            ColumnState::ChildTable => {
                // Empty lists and further lists (of objects or scalars — scalar items
                // become {"value": …} child rows) are fine. A non-array or a mixed
                // list is a shape conflict: the column becomes Json for conflicting
                // values; the child table stops growing.
                if !matches!(
                    array_shape(value),
                    ArrayShape::Objects | ArrayShape::Scalars | ArrayShape::Empty
                ) {
                    *self = ColumnState::Json;
                }
            }
        }
    }

    /// Decide an initial state from the FIRST non-null value.
    ///
    /// Null is deliberately absent from this match: `observe` — the only caller
    /// — returns early on a null, so a null cannot reach here. An arm for it
    /// would be unreachable code, and its mutant unkillable for that reason
    /// rather than for lack of a test. A null leaves the column `Unknown` by
    /// never calling this at all, which is the same outcome by a shorter route.
    fn fresh<'a, V: JsonView<'a>>(value: V, lists_as_columns: bool) -> Self {
        match value.kind() {
            ValueKind::Object => {
                let mut fields = Vec::new();
                for (key, item) in value.obj_entries() {
                    let mut state = ColumnState::Unknown;
                    state.observe(item, lists_as_columns);
                    fields.push((key.to_owned(), state));
                }
                ColumnState::Struct(fields)
            }
            ValueKind::Array => match array_shape(value) {
                ArrayShape::Empty => ColumnState::Unknown, // decide when data arrives
                ArrayShape::Objects => ColumnState::ChildTable,
                ArrayShape::Scalars => {
                    if lists_as_columns {
                        let mut item_state = ScalarState::default();
                        for item in value.arr_items() {
                            item_state.observe(item);
                        }
                        ColumnState::ScalarList(item_state)
                    } else {
                        ColumnState::ChildTable
                    }
                }
                ArrayShape::Mixed => ColumnState::Json,
            },
            _ => {
                let mut state = ScalarState::default();
                state.observe(value);
                ColumnState::Scalar(state)
            }
        }
    }

    /// Resolve to a schema column type; `None` for positions that are not columns
    /// (child tables) or never got data.
    pub(crate) fn resolve(&self) -> Option<ColumnType> {
        match self {
            ColumnState::Unknown | ColumnState::ChildTable => None,
            ColumnState::Json => Some(ColumnType::scalar(LogicalType::Json)),
            ColumnState::Scalar(s) => Some(ColumnType::scalar(s.resolve())),
            ColumnState::ScalarList(item) => Some(ColumnType::ScalarList {
                item: item.resolve(),
            }),
            ColumnState::Struct(fields) => {
                let resolved: Vec<ColumnDef> = fields
                    .iter()
                    .filter_map(|(name, state)| {
                        state.resolve().map(|ty| ColumnDef {
                            name: name.clone(),
                            column_type: ty,
                            nullable: true,
                            provenance: Provenance::Inferred,
                        })
                    })
                    .collect();
                if resolved.is_empty() {
                    None // an object whose every field is still unknown
                } else {
                    Some(ColumnType::Struct { fields: resolved })
                }
            }
        }
    }

    pub(crate) fn is_child_table(&self) -> bool {
        matches!(self, ColumnState::ChildTable)
    }
}

#[derive(Debug, PartialEq, Eq)]
enum ArrayShape {
    Empty,
    Objects,
    Scalars,
    Mixed,
}

fn array_shape<'a, V: JsonView<'a>>(value: V) -> ArrayShape {
    if !value.is_array() {
        return ArrayShape::Mixed;
    }
    let mut objects = 0usize;
    let mut non_null = 0usize;
    for item in value.arr_items() {
        if item.is_null() {
            continue;
        }
        non_null += 1;
        if item.is_object() {
            objects += 1;
        }
    }
    if non_null == 0 {
        ArrayShape::Empty
    } else if objects == non_null {
        ArrayShape::Objects
    } else if objects == 0 {
        ArrayShape::Scalars
    } else {
        ArrayShape::Mixed
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::{Value, json};

    fn observe_all(values: &[Value]) -> ColumnState {
        let mut state = ColumnState::Unknown;
        for v in values {
            state.observe(v, true);
        }
        state
    }

    /// A leading NULL must not DECIDE the column. `fresh` maps a null to
    /// `Unknown` precisely so the first real value picks the type; without that
    /// arm a null falls through to the scalar branch and the column is decided
    /// by the absence of data.
    #[test]
    fn a_leading_null_leaves_the_column_undecided() {
        let state = observe_all(&[json!(null)]);
        assert_eq!(state.resolve(), None, "null alone decides nothing");

        // …and the first real value still gets to choose, from either order.
        let state = observe_all(&[json!(null), json!(7)]);
        assert_eq!(
            state.resolve(),
            Some(ColumnType::scalar(LogicalType::Int64))
        );
        let state = observe_all(&[json!(null), json!("text")]);
        assert_eq!(state.resolve(), Some(ColumnType::scalar(LogicalType::Utf8)));

        // The CONTAINER case is what actually distinguishes `Unknown` from a
        // scalar state that merely resolves to nothing: `Unknown` re-runs
        // `fresh` on the next value and gets a Struct, while a scalar state
        // treats the object as a shape conflict and escapes to Json. Following
        // a null with scalars alone cannot tell those apart.
        let state = observe_all(&[json!(null), json!({"a": 1})]);
        assert!(
            matches!(state.resolve(), Some(ColumnType::Struct { .. })),
            "a null then an object must infer a STRUCT, not the Json escape hatch: {:?}",
            state.resolve()
        );
    }

    /// A list of scalars stays a list. Dropping the array arm sends it to the
    /// Json escape hatch instead — the column still loads, but every row is
    /// stored as opaque text rather than a typed list, and nothing downstream
    /// can index it.
    #[test]
    fn a_scalar_list_stays_a_scalar_list() {
        let state = observe_all(&[json!([1, 2]), json!([3])]);
        assert_eq!(
            state.resolve(),
            Some(ColumnType::ScalarList {
                item: LogicalType::Int64
            }),
            "repeated scalar arrays stay a typed list"
        );
        // Widening within the list still happens on the item lattice.
        let state = observe_all(&[json!([1]), json!(["x"])]);
        assert_eq!(
            state.resolve(),
            Some(ColumnType::ScalarList {
                item: LogicalType::Utf8
            })
        );
        // A list that later holds objects is irreconcilable and escapes to Json.
        let state = observe_all(&[json!([1]), json!([{"a": 1}])]);
        assert_eq!(state.resolve(), Some(ColumnType::scalar(LogicalType::Json)));
    }

    /// An INFERRED scalar that later sees a container widens to Json; a PINNED
    /// one keeps its declared type and the offending value is nulled and counted
    /// at build time. Both halves matter: forcing `is_pinned` true freezes every
    /// column and inference stops working, and forcing the guard false rewrites
    /// a type the user declared.
    #[test]
    fn shape_conflict_widens_inferred_columns_but_never_pinned_ones() {
        // Inferred: an object arriving after an int escalates to Json.
        let state = observe_all(&[json!(1), json!({"a": 1})]);
        assert_eq!(
            state.resolve(),
            Some(ColumnType::scalar(LogicalType::Json)),
            "an inferred scalar widens on a shape conflict"
        );
        let state = observe_all(&[json!(1), json!([1, 2])]);
        assert_eq!(state.resolve(), Some(ColumnType::scalar(LogicalType::Json)));

        // Pinned: the declared type survives the same conflicts untouched. This
        // is the half that a `!state.is_pinned()` guard forced false would
        // destroy — the object would rewrite a type the user declared.
        let mut pinned = ColumnState::Scalar(ScalarState::pinned(LogicalType::Int64));
        pinned.observe(&json!({"a": 1}), true);
        pinned.observe(&json!([1, 2]), true);
        pinned.observe(&json!("text"), true);
        assert_eq!(
            pinned.resolve(),
            Some(ColumnType::scalar(LogicalType::Int64)),
            "a declared type is never rewritten by an observation"
        );
    }

    #[test]
    fn value_checked_escalation_beyond_2_53() {
        let state = observe_all(&[json!(10.5), json!(9007199254740993i64)]);
        assert_eq!(state.resolve(), Some(ColumnType::scalar(LogicalType::Utf8)));
        // Pure ints beyond 2^53 with no float stay Int64 — nothing lossy happened.
        let state = observe_all(&[json!(9007199254740993i64), json!(1)]);
        assert_eq!(
            state.resolve(),
            Some(ColumnType::scalar(LogicalType::Int64))
        );
        // Order-insensitive: big int first, float later.
        let state = observe_all(&[json!(9007199254740993i64), json!(0.5)]);
        assert_eq!(state.resolve(), Some(ColumnType::scalar(LogicalType::Utf8)));
    }

    #[test]
    fn timestamps_detect_strictly_and_widen_to_text() {
        let state = observe_all(&[json!("2026-07-19T10:00:00Z")]);
        assert_eq!(
            state.resolve(),
            Some(ColumnType::scalar(LogicalType::TimestampTz))
        );
        let state = observe_all(&[json!("2026-07-19T10:00:00Z"), json!("not a timestamp")]);
        assert_eq!(state.resolve(), Some(ColumnType::scalar(LogicalType::Utf8)));
    }

    #[test]
    fn shape_conflicts_go_to_json() {
        let state = observe_all(&[json!({"a": 1}), json!(5)]);
        assert_eq!(state.resolve(), Some(ColumnType::scalar(LogicalType::Json)));
        let state = observe_all(&[json!([1, 2]), json!("x")]);
        assert_eq!(state.resolve(), Some(ColumnType::scalar(LogicalType::Json)));
    }

    #[test]
    fn pinned_hint_never_widens() {
        let mut state = ScalarState::pinned(LogicalType::TimestampTz);
        state.observe(&json!("definitely not a timestamp"));
        assert_eq!(state.resolve(), LogicalType::TimestampTz);
    }
}
