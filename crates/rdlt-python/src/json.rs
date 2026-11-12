//! Python rows as JSON lines, written on the thread that holds the GIL, within what the engine
//! holds a push to: its bytes, and how deep a value nests.
//!
//! Values are never quoted in errors.

#[cfg(test)]
mod tests;

use std::mem;

use bytes::Bytes;
use pyo3::sync::PyOnceLock;
use pyo3::types::{
    PyAnyMethods as _, PyBool, PyBoolMethods as _, PyDict, PyDictMethods as _, PyFloat,
    PyFloatMethods as _, PyInt, PyIterator, PyList, PyString, PyStringMethods as _, PyTuple,
    PyType, PyTypeMethods as _,
};
use pyo3::{Bound, Py, PyAny, PyErr, Python};
use rdlt_connector::ConnectorError;

use crate::errors;

/// What a push of rows keeps within.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct JsonLimits {
    /// Bytes one push holds at most.
    pub(crate) push_bytes: usize,
    /// Levels a value nests at most, the row itself the first.
    pub(crate) depth: usize,
}

impl JsonLimits {
    /// The JSON limits of a read whose engine allows `push_bytes` a push and `depth` levels: the
    /// `json_push_bytes` and `nesting_depth` of what `Emitter::limits` answers, taken by their
    /// fields, since this crate may not name `rdlt-wire`'s type.
    pub(crate) fn new(push_bytes: u64, depth: u64) -> Self {
        Self {
            push_bytes: usize::try_from(push_bytes).unwrap_or(usize::MAX),
            depth: usize::try_from(depth).unwrap_or(usize::MAX),
        }
    }
}

/// Why rows could not be written.
#[derive(Debug, thiserror::Error)]
pub(crate) enum Unwritten {
    /// A value of a type with no JSON form.
    #[error("row {row}: {path}: a value of type {type_name} has no JSON form")]
    Unsupported {
        row: usize,
        path: String,
        type_name: String,
    },
    /// A mapping's key that is not text.
    #[error("row {row}: {path}: a key of type {type_name} is not text")]
    Key {
        row: usize,
        path: String,
        type_name: String,
    },
    /// A float or decimal that is not finite.
    #[error("row {row}: {path}: a number that is not finite has no JSON form")]
    NotFinite { row: usize, path: String },
    /// Text that is not valid Unicode, as a lone surrogate.
    #[error("row {row}: {path}: text that is not valid Unicode")]
    NotText { row: usize, path: String },
    /// A row nesting deeper than the limit.
    #[error("row {row}: nests deeper than {limit} levels")]
    Deep { row: usize, limit: usize },
    /// A row larger than a whole push.
    #[error("row {row}: its JSON reaches {bytes} bytes, past a push's {limit}")]
    Large {
        row: usize,
        bytes: usize,
        limit: usize,
    },
    /// A row that is not a mapping.
    #[error("row {row}: is not a mapping")]
    NotObject { row: usize },
    /// Python raised while a value was read; its type alone is said.
    #[error("reading a row raised {0}")]
    Raised(String),
}

impl Unwritten {
    /// The data error the source fails with.
    pub(crate) fn connector_error(&self) -> ConnectorError {
        let code = match self {
            Self::Unsupported { .. } => "json_unsupported_type",
            Self::Key { .. } => "json_key_not_text",
            Self::NotFinite { .. } => "json_not_finite",
            Self::NotText { .. } => "json_not_text",
            Self::Deep { .. } => "json_too_deep",
            Self::Large { .. } => "json_push_bytes",
            Self::NotObject { .. } => "json_not_object",
            Self::Raised(_) => "json_raised",
        };
        ConnectorError::data(self.to_string()).with_code(code)
    }
}

fn raised(py: Python<'_>, error: &PyErr) -> Unwritten {
    Unwritten::Raised(errors::type_name(py, error))
}

fn type_name(value: &Bound<'_, PyAny>) -> String {
    value
        .get_type()
        .name()
        .map_or_else(|_| "?".to_owned(), |name| name.to_string())
}

/// Python's types that take a text form: dates and times (ISO 8601), decimals (their digits, as
/// a number), UUIDs (their hyphenated text).
struct Typed<'py> {
    date: Bound<'py, PyType>,
    time: Bound<'py, PyType>,
    decimal: Bound<'py, PyType>,
    uuid: Bound<'py, PyType>,
}

impl<'py> Typed<'py> {
    fn of(py: Python<'py>) -> Result<Self, PyErr> {
        static DATE: PyOnceLock<Py<PyType>> = PyOnceLock::new();
        static TIME: PyOnceLock<Py<PyType>> = PyOnceLock::new();
        static DECIMAL: PyOnceLock<Py<PyType>> = PyOnceLock::new();
        static UUID: PyOnceLock<Py<PyType>> = PyOnceLock::new();
        Ok(Self {
            date: DATE.import(py, "datetime", "date")?.clone(),
            time: TIME.import(py, "datetime", "time")?.clone(),
            decimal: DECIMAL.import(py, "decimal", "Decimal")?.clone(),
            uuid: UUID.import(py, "uuid", "UUID")?.clone(),
        })
    }
}

/// One row's writer: where it is in the row, for errors, and the bytes it wrote.
struct Writer<'a, 'py> {
    py: Python<'py>,
    typed: &'a Typed<'py>,
    row: usize,
    depth: usize,
    limit: usize,
    path: String,
    out: Vec<u8>,
}

impl Writer<'_, '_> {
    /// Writes a string, refused first if its length alone passes the limit: escaping can make
    /// it six times as long.
    fn text(&mut self, text: &str) -> Result<(), Unwritten> {
        let bytes = self.out.len().saturating_add(text.len());
        if bytes > self.limit {
            return Err(Unwritten::Large {
                row: self.row,
                bytes,
                limit: self.limit,
            });
        }
        serde_json::to_writer(&mut self.out, text).expect("writing to a vector does not fail");
        Ok(())
    }

    fn here(&self) -> (usize, String) {
        (self.row, self.path.clone())
    }

    fn value(&mut self, value: &Bound<'_, PyAny>, level: usize) -> Result<(), Unwritten> {
        // Refused here, since repeated references can make a row far larger than Python holds.
        if self.out.len() > self.limit {
            return Err(Unwritten::Large {
                row: self.row,
                bytes: self.out.len(),
                limit: self.limit,
            });
        }
        if value.is_none() {
            self.out.extend_from_slice(b"null");
        } else if let Ok(flag) = value.cast::<PyBool>() {
            self.out
                .extend_from_slice(if flag.is_true() { b"true" } else { b"false" });
        } else if value.is_instance_of::<PyInt>() {
            self.integer(value)?;
        } else if let Ok(float) = value.cast::<PyFloat>() {
            let float = float.value();
            if !float.is_finite() {
                let (row, path) = self.here();
                return Err(Unwritten::NotFinite { row, path });
            }
            serde_json::to_writer(&mut self.out, &float).expect("a finite float writes");
        } else if let Ok(text) = value.cast::<PyString>() {
            let Ok(text) = text.to_str() else {
                let (row, path) = self.here();
                return Err(Unwritten::NotText { row, path });
            };
            self.text(text)?;
        } else if let Ok(mapping) = value.cast::<PyDict>() {
            self.object(mapping, level + 1)?;
        } else if value.is_instance_of::<PyList>() || value.is_instance_of::<PyTuple>() {
            self.array(value, level + 1)?;
        } else {
            self.typed(value)?;
        }
        Ok(())
    }

    /// Writes an integer; wider than 64 bits, as Python's digits, read through `int` itself so
    /// a subclass's `__str__` cannot write anything else here.
    fn integer(&mut self, value: &Bound<'_, PyAny>) -> Result<(), Unwritten> {
        if let Ok(small) = value.extract::<i64>() {
            self.out.extend_from_slice(small.to_string().as_bytes());
            return Ok(());
        }
        let digits = self
            .py
            .get_type::<PyInt>()
            .call_method1("__repr__", (value,))
            .map_err(|error| raised(self.py, &error))?;
        self.out.extend_from_slice(digits.to_string().as_bytes());
        Ok(())
    }

    fn deeper(&self, level: usize) -> Result<(), Unwritten> {
        if level > self.depth {
            return Err(Unwritten::Deep {
                row: self.row,
                limit: self.depth,
            });
        }
        Ok(())
    }

    fn object(&mut self, mapping: &Bound<'_, PyDict>, level: usize) -> Result<(), Unwritten> {
        self.deeper(level)?;
        self.out.push(b'{');
        let at = self.path.len();
        for (index, (key, value)) in mapping.iter().enumerate() {
            let Some(key) = key
                .cast::<PyString>()
                .ok()
                .and_then(|key| key.to_str().ok().map(str::to_owned))
            else {
                let (row, path) = self.here();
                return Err(Unwritten::Key {
                    row,
                    path,
                    type_name: type_name(&key),
                });
            };
            if index > 0 {
                self.out.push(b',');
            }
            self.text(&key)?;
            self.out.push(b':');
            self.path.push('.');
            self.path.push_str(&key);
            self.value(&value, level)?;
            self.path.truncate(at);
        }
        self.out.push(b'}');
        Ok(())
    }

    fn array(&mut self, items: &Bound<'_, PyAny>, level: usize) -> Result<(), Unwritten> {
        self.deeper(level)?;
        self.out.push(b'[');
        let at = self.path.len();
        let iterator = items.try_iter().map_err(|error| raised(self.py, &error))?;
        for (index, item) in iterator.enumerate() {
            let item = item.map_err(|error| raised(self.py, &error))?;
            if index > 0 {
                self.out.push(b',');
            }
            self.path.push('[');
            self.path.push_str(&index.to_string());
            self.path.push(']');
            self.value(&item, level)?;
            self.path.truncate(at);
        }
        self.out.push(b']');
        Ok(())
    }

    fn typed(&mut self, value: &Bound<'_, PyAny>) -> Result<(), Unwritten> {
        let instance = |kind: &Bound<'_, PyType>| value.is_instance(kind).unwrap_or(false);
        if instance(&self.typed.decimal) {
            self.decimal(value)?;
        } else if instance(&self.typed.date) || instance(&self.typed.time) {
            let iso = value
                .call_method0("isoformat")
                .map_err(|error| raised(self.py, &error))?;
            self.text(&iso.to_string())?;
        } else if instance(&self.typed.uuid) {
            let text = value.str().map_err(|error| raised(self.py, &error))?;
            self.text(&text.to_string())?;
        } else {
            let (row, path) = self.here();
            return Err(Unwritten::Unsupported {
                row,
                path,
                type_name: type_name(value),
            });
        }
        Ok(())
    }

    /// Writes a decimal as a number, read through `Decimal` itself so a subclass's methods
    /// cannot write anything else here.
    fn decimal(&mut self, value: &Bound<'_, PyAny>) -> Result<(), Unwritten> {
        let decimal = &self.typed.decimal;
        let finite = decimal
            .call_method1("is_finite", (value,))
            .and_then(|finite| finite.extract::<bool>())
            .map_err(|error| raised(self.py, &error))?;
        if !finite {
            let (row, path) = self.here();
            return Err(Unwritten::NotFinite { row, path });
        }
        let digits = decimal
            .call_method1("__str__", (value,))
            .map_err(|error| raised(self.py, &error))?;
        self.out.extend_from_slice(digits.to_string().as_bytes());
        Ok(())
    }
}

/// One row, `number` of its yield, as a JSON line.
fn line(
    py: Python<'_>,
    typed: &Typed<'_>,
    row: &Bound<'_, PyAny>,
    number: usize,
    limits: JsonLimits,
) -> Result<Vec<u8>, Unwritten> {
    let Ok(mapping) = row.cast::<PyDict>() else {
        return Err(Unwritten::NotObject { row: number });
    };
    let mut writer = Writer {
        py,
        typed,
        row: number,
        depth: limits.depth,
        limit: limits.push_bytes,
        path: "$".to_owned(),
        out: Vec::new(),
    };
    writer.object(mapping, 1)?;
    writer.out.push(b'\n');
    Ok(writer.out)
}

/// The rows of one yield, as JSON lines cut into pushes of at most `limits.push_bytes`, written
/// a push at a time: no row is read past the first that does not fit the push being filled.
pub(crate) struct Rows {
    /// The rows not yet read; none once every row has been.
    left: Option<Left>,
    /// The rows read.
    read: usize,
    limits: JsonLimits,
    filling: Vec<u8>,
}

/// The rows of a yield not yet read.
enum Left {
    /// A mapping yielded alone.
    One(Py<PyDict>),
    /// A list's or a tuple's.
    Many(Py<PyIterator>),
}

impl Rows {
    /// The rows `rows` holds: a mapping, or a list or tuple of mappings.
    ///
    /// # Errors
    ///
    /// [`Unwritten::NotObject`] for anything else, and [`Unwritten::Raised`] where a list's
    /// iteration raised.
    pub(crate) fn new(
        py: Python<'_>,
        rows: &Bound<'_, PyAny>,
        limits: JsonLimits,
    ) -> Result<Self, Unwritten> {
        let left = if let Ok(row) = rows.cast::<PyDict>() {
            Left::One(row.clone().unbind())
        } else if rows.is_instance_of::<PyList>() || rows.is_instance_of::<PyTuple>() {
            let iterator = rows.try_iter().map_err(|error| raised(py, &error))?;
            Left::Many(iterator.unbind())
        } else {
            return Err(Unwritten::NotObject { row: 1 });
        };
        Ok(Self {
            left: Some(left),
            read: 0,
            limits,
            filling: Vec::new(),
        })
    }

    /// The next push: a full one, or the last, which the rows ran out filling; none after it.
    ///
    /// # Errors
    ///
    /// An [`Unwritten`] for the first row that is no mapping, holds a value with no JSON form,
    /// nests past `limits.depth`, or is larger than a whole push.
    pub(crate) fn next(&mut self, py: Python<'_>) -> Result<Option<Bytes>, Unwritten> {
        if self.left.is_some() {
            let typed = Typed::of(py).map_err(|error| raised(py, &error))?;
            while let Some(row) = self.row(py)? {
                self.read += 1;
                let line = line(py, &typed, &row, self.read, self.limits)?;
                if line.len() > self.limits.push_bytes {
                    return Err(Unwritten::Large {
                        row: self.read,
                        bytes: line.len(),
                        limit: self.limits.push_bytes,
                    });
                }
                if self.filling.len() + line.len() > self.limits.push_bytes {
                    let full = mem::replace(&mut self.filling, line);
                    return Ok(Some(Bytes::from(full)));
                }
                self.filling.extend_from_slice(&line);
            }
        }
        if self.filling.is_empty() {
            return Ok(None);
        }
        Ok(Some(Bytes::from(mem::take(&mut self.filling))))
    }

    /// The next row, none once every row has been read.
    fn row<'py>(&mut self, py: Python<'py>) -> Result<Option<Bound<'py, PyAny>>, Unwritten> {
        let row = match &self.left {
            None => None,
            Some(Left::One(row)) => {
                let row = row.bind(py).clone().into_any();
                self.left = None;
                Some(row)
            }
            Some(Left::Many(iterator)) => {
                let next = iterator.bind(py).clone().next();
                let row = next.transpose().map_err(|error| raised(py, &error))?;
                if row.is_none() {
                    self.left = None;
                }
                row
            }
        };
        Ok(row)
    }
}
