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
use super::view::{JsonView, Kind};

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
            Kind::Null => return,
            Kind::Bool(_) => LogicalType::Bool,
            Kind::Int(i) => {
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
            Kind::UInt(_) => LogicalType::Utf8,
            Kind::Float(_) => {
                self.saw_float = true;
                LogicalType::Float64
            }
            Kind::Str(s) => {
                if parse_timestamp_tz(s).is_some() {
                    LogicalType::TimestampTz
                } else {
                    LogicalType::Utf8
                }
            }
            // Non-scalars reaching a scalar position are a shape conflict → Json.
            Kind::Object | Kind::Array => LogicalType::Json,
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
pub(crate) enum ColState {
    /// Only nulls seen so far.
    Unknown,
    Scalar(ScalarState),
    /// Nested object: fields in first-seen order (order is part of the schema hash).
    Struct(Vec<(String, ColState)>),
    ScalarList(ScalarState),
    /// List of objects — rows live in a child table; the column itself vanishes.
    ChildTable,
    /// Irreconcilable shapes; values preserved verbatim.
    Json,
}

impl ColState {
    pub(crate) fn observe<'a, V: JsonView<'a>>(&mut self, value: V, lists_as_columns: bool) {
        if value.is_null() {
            return;
        }
        match self {
            ColState::Json => {}
            ColState::Unknown => {
                *self = Self::fresh(value, lists_as_columns);
            }
            // A shape conflict widens an inferred column to Json — but a PINNED
            // column was declared by the user, and a value that does not fit the
            // declaration must not silently rewrite it. The pinned column keeps
            // its type here and the offending value is nulled and counted at
            // build time.
            ColState::Scalar(state) => match value.kind() {
                Kind::Object | Kind::Array if !state.is_pinned() => *self = ColState::Json,
                _ => state.observe(value),
            },
            ColState::Struct(fields) => match value.kind() {
                Kind::Object => {
                    for (key, item) in value.obj_entries() {
                        match fields.iter_mut().find(|(name, _)| name == key) {
                            Some((_, state)) => state.observe(item, lists_as_columns),
                            None => {
                                let mut state = ColState::Unknown;
                                state.observe(item, lists_as_columns);
                                fields.push((key.to_owned(), state));
                            }
                        }
                    }
                }
                _ => *self = ColState::Json,
            },
            ColState::ScalarList(item_state) => match value.kind() {
                Kind::Array => {
                    if value.arr_items().any(|item| item.is_object()) {
                        // scalars-then-objects in the same list position: irreconcilable
                        *self = ColState::Json;
                    } else {
                        for item in value.arr_items() {
                            if item.is_array() {
                                *self = ColState::Json; // nested lists: v1 escape hatch
                                return;
                            }
                            item_state.observe(item);
                        }
                    }
                }
                _ => *self = ColState::Json,
            },
            ColState::ChildTable => {
                // Empty lists and further lists (of objects or scalars — scalar items
                // become {"value": …} child rows) are fine. A non-array or a mixed
                // list is a shape conflict: the column becomes Json for conflicting
                // values; the child table stops growing.
                if !matches!(
                    array_shape(value),
                    ArrayShape::Objects | ArrayShape::Scalars | ArrayShape::Empty
                ) {
                    *self = ColState::Json;
                }
            }
        }
    }

    fn fresh<'a, V: JsonView<'a>>(value: V, lists_as_columns: bool) -> Self {
        match value.kind() {
            Kind::Null => ColState::Unknown,
            Kind::Object => {
                let mut fields = Vec::new();
                for (key, item) in value.obj_entries() {
                    let mut state = ColState::Unknown;
                    state.observe(item, lists_as_columns);
                    fields.push((key.to_owned(), state));
                }
                ColState::Struct(fields)
            }
            Kind::Array => match array_shape(value) {
                ArrayShape::Empty => ColState::Unknown, // decide when data arrives
                ArrayShape::Objects => ColState::ChildTable,
                ArrayShape::Scalars => {
                    if lists_as_columns {
                        let mut item_state = ScalarState::default();
                        for item in value.arr_items() {
                            item_state.observe(item);
                        }
                        ColState::ScalarList(item_state)
                    } else {
                        ColState::ChildTable
                    }
                }
                ArrayShape::Mixed => ColState::Json,
            },
            _ => {
                let mut state = ScalarState::default();
                state.observe(value);
                ColState::Scalar(state)
            }
        }
    }

    /// Resolve to a schema column type; `None` for positions that are not columns
    /// (child tables) or never got data.
    pub(crate) fn resolve(&self) -> Option<ColumnType> {
        match self {
            ColState::Unknown | ColState::ChildTable => None,
            ColState::Json => Some(ColumnType::scalar(LogicalType::Json)),
            ColState::Scalar(s) => Some(ColumnType::scalar(s.resolve())),
            ColState::ScalarList(item) => Some(ColumnType::ScalarList {
                item: item.resolve(),
            }),
            ColState::Struct(fields) => {
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
        matches!(self, ColState::ChildTable)
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

    fn observe_all(values: &[Value]) -> ColState {
        let mut state = ColState::Unknown;
        for v in values {
            state.observe(v, true);
        }
        state
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
