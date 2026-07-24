//! Hand-rolled pgoutput (logical replication) message parser — protocol
//! version 1, the format `pg_logical_slot_peek_binary_changes` emits with
//! `proto_version '1'`. Same house discipline as the binary-COPY decoder:
//! no dependencies, typed errors, no panics on
//! malformed input, fuzzed (`pg_pgoutput_decode`).
//!
//! Message reference: PostgreSQL docs, "Logical Replication Message
//! Formats". Tuple values arrive TEXT-form under proto v1.

/// Typed parse failure — never a panic (fuzz-pinned).
#[derive(Debug)]
pub struct PgoutputError(pub String);

impl std::fmt::Display for PgoutputError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "pgoutput: {}", self.0)
    }
}

fn err<T>(message: impl Into<String>) -> Result<T, PgoutputError> {
    Err(PgoutputError(message.into()))
}

/// One column value inside a tuple.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TupleValue {
    Null,
    /// Unchanged out-of-line (TOAST) value — the update did not carry it
    /// (the row-building layer decides what happens next).
    UnchangedToast,
    /// Text-form value bytes (proto v1 sends text).
    Text(Vec<u8>),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TupleData {
    pub values: Vec<TupleValue>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RelationColumn {
    pub name: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Relation {
    pub id: u32,
    pub namespace: String,
    pub name: String,
    pub columns: Vec<RelationColumn>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum Message {
    /// Transaction boundary. The wire carries the commit LSN and xid; the
    /// consumer checkpoints from the peek row's own LSN and does not read
    /// them.
    Begin,
    /// Transaction boundary. The wire carries the commit/end LSNs; the
    /// consumer checkpoints from the peek row's own LSN and does not read
    /// them.
    Commit,
    Relation(Relation),
    Insert {
        rel: u32,
        new: TupleData,
    },
    Update {
        rel: u32,
        /// Old key ('K') or full old tuple ('O'), when present.
        old: Option<TupleData>,
        new: TupleData,
    },
    Delete {
        rel: u32,
        /// Old key ('K') or full old tuple ('O').
        old: TupleData,
    },
    Truncate {
        rels: Vec<u32>,
    },
    /// Parsed and carried for completeness; the consumer ignores them.
    Origin,
    Type,
}

struct Reader<'a> {
    bytes: &'a [u8],
    pos: usize,
}

impl<'a> Reader<'a> {
    fn take(&mut self, n: usize) -> Result<&'a [u8], PgoutputError> {
        let end = self
            .pos
            .checked_add(n)
            .filter(|e| *e <= self.bytes.len())
            .ok_or_else(|| PgoutputError("message truncated".into()))?;
        let out = &self.bytes[self.pos..end];
        self.pos = end;
        Ok(out)
    }

    fn u8(&mut self) -> Result<u8, PgoutputError> {
        Ok(self.take(1)?[0])
    }

    fn u16(&mut self) -> Result<u16, PgoutputError> {
        Ok(u16::from_be_bytes(self.take(2)?.try_into().expect("2")))
    }

    fn u32(&mut self) -> Result<u32, PgoutputError> {
        Ok(u32::from_be_bytes(self.take(4)?.try_into().expect("4")))
    }

    fn i32(&mut self) -> Result<i32, PgoutputError> {
        Ok(i32::from_be_bytes(self.take(4)?.try_into().expect("4")))
    }

    fn u64(&mut self) -> Result<u64, PgoutputError> {
        Ok(u64::from_be_bytes(self.take(8)?.try_into().expect("8")))
    }

    fn cstr(&mut self) -> Result<String, PgoutputError> {
        let rest = &self.bytes[self.pos..];
        let nul = rest
            .iter()
            .position(|b| *b == 0)
            .ok_or_else(|| PgoutputError("unterminated string".into()))?;
        let s = std::str::from_utf8(&rest[..nul])
            .map_err(|_| PgoutputError("non-UTF8 identifier".into()))?
            .to_owned();
        self.pos += nul + 1;
        Ok(s)
    }

    fn done(&self) -> bool {
        self.pos == self.bytes.len()
    }
}

fn tuple(r: &mut Reader<'_>) -> Result<TupleData, PgoutputError> {
    let ncols = r.u16()? as usize;
    // Bound: each column needs at least 1 byte — a hostile count cannot
    // allocate more than the message could carry.
    if ncols > r.bytes.len() {
        return err("tuple column count exceeds message");
    }
    let mut values = Vec::with_capacity(ncols);
    for _ in 0..ncols {
        match r.u8()? {
            b'n' => values.push(TupleValue::Null),
            b'u' => values.push(TupleValue::UnchangedToast),
            b't' => {
                let len = r.u32()? as usize;
                values.push(TupleValue::Text(r.take(len)?.to_vec()));
            }
            b'b' => {
                // Binary form only appears under options we do not request;
                // reject rather than misinterpret.
                return err("binary tuple value (unrequested option)");
            }
            other => return err(format!("unknown tuple value kind {other:#04x}")),
        }
    }
    Ok(TupleData { values })
}

/// Parse ONE pgoutput message (one `data` cell from the peek function).
pub fn parse(bytes: &[u8]) -> Result<Message, PgoutputError> {
    let mut r = Reader { bytes, pos: 0 };
    let tag = r.u8()?;
    let message = match tag {
        b'B' => {
            let _final_lsn = r.u64()?;
            let _commit_ts = r.u64()?;
            let _xid = r.u32()?;
            Message::Begin
        }
        b'C' => {
            let _flags = r.u8()?;
            let _commit_lsn = r.u64()?;
            let _end_lsn = r.u64()?;
            let _commit_ts = r.u64()?;
            Message::Commit
        }
        b'O' => {
            let _lsn = r.u64()?;
            let _name = r.cstr()?;
            Message::Origin
        }
        b'R' => {
            let id = r.u32()?;
            let namespace = r.cstr()?;
            let name = r.cstr()?;
            // replica-identity setting ('d'/'n'/'f'/'i'); identity is read
            // from the catalog at preflight, not from the wire.
            let _replident = r.u8()?;
            let ncols = r.u16()? as usize;
            if ncols > bytes.len() {
                return err("relation column count exceeds message");
            }
            let mut columns = Vec::with_capacity(ncols);
            for _ in 0..ncols {
                // Wire order per column: flags (identity-key bit), name,
                // type OID, type modifier. Only the name is retained — column
                // mapping is by name; type/identity facts come from the
                // catalog.
                let _flags = r.u8()?;
                let name = r.cstr()?;
                let _type_oid = r.u32()?;
                let _typmod = r.i32()?;
                columns.push(RelationColumn { name });
            }
            Message::Relation(Relation {
                id,
                namespace,
                name,
                columns,
            })
        }
        b'Y' => {
            let _oid = r.u32()?;
            let _namespace = r.cstr()?;
            let _name = r.cstr()?;
            Message::Type
        }
        b'I' => {
            let rel = r.u32()?;
            match r.u8()? {
                b'N' => Message::Insert {
                    rel,
                    new: tuple(&mut r)?,
                },
                other => return err(format!("insert: expected 'N', got {other:#04x}")),
            }
        }
        b'U' => {
            let rel = r.u32()?;
            let mut old = None;
            let marker = r.u8()?;
            let new = match marker {
                b'K' | b'O' => {
                    old = Some(tuple(&mut r)?);
                    match r.u8()? {
                        b'N' => tuple(&mut r)?,
                        other => return err(format!("update: expected 'N', got {other:#04x}")),
                    }
                }
                b'N' => tuple(&mut r)?,
                other => return err(format!("update: unknown marker {other:#04x}")),
            };
            Message::Update { rel, old, new }
        }
        b'D' => {
            let rel = r.u32()?;
            match r.u8()? {
                b'K' | b'O' => Message::Delete {
                    rel,
                    old: tuple(&mut r)?,
                },
                other => return err(format!("delete: unknown marker {other:#04x}")),
            }
        }
        b'T' => {
            let nrels = r.u32()? as usize;
            if nrels > bytes.len() {
                return err("truncate relation count exceeds message");
            }
            let _flags = r.u8()?;
            let mut rels = Vec::with_capacity(nrels);
            for _ in 0..nrels {
                rels.push(r.u32()?);
            }
            Message::Truncate { rels }
        }
        other => return err(format!("unknown message tag {other:#04x}")),
    };
    if !r.done() {
        return err("trailing bytes after message");
    }
    Ok(message)
}

#[cfg(test)]
mod tests {
    use proptest::prelude::*;

    use super::*;

    fn cstr(s: &str) -> Vec<u8> {
        let mut v = s.as_bytes().to_vec();
        v.push(0);
        v
    }

    fn text_tuple(values: &[Option<&str>]) -> Vec<u8> {
        let mut v = (values.len() as u16).to_be_bytes().to_vec();
        for value in values {
            match value {
                None => v.push(b'n'),
                Some(text) => {
                    v.push(b't');
                    v.extend((text.len() as u32).to_be_bytes());
                    v.extend(text.as_bytes());
                }
            }
        }
        v
    }

    #[test]
    fn round_trips_the_message_set() {
        // Begin
        let mut b = vec![b'B'];
        b.extend(7u64.to_be_bytes());
        b.extend(0i64.to_be_bytes());
        b.extend(42u32.to_be_bytes());
        assert_eq!(parse(&b).unwrap(), Message::Begin);

        // Commit
        let mut c = vec![b'C', 0];
        c.extend(7u64.to_be_bytes());
        c.extend(8u64.to_be_bytes());
        c.extend(0i64.to_be_bytes());
        assert_eq!(parse(&c).unwrap(), Message::Commit);

        // Relation with one key column.
        let mut r = vec![b'R'];
        r.extend(99u32.to_be_bytes());
        r.extend(cstr("public"));
        r.extend(cstr("orders"));
        r.push(b'd');
        r.extend(2u16.to_be_bytes());
        r.push(1); // key column
        r.extend(cstr("id"));
        r.extend(20u32.to_be_bytes()); // int8
        r.extend((-1i32).to_be_bytes());
        r.push(0);
        r.extend(cstr("name"));
        r.extend(25u32.to_be_bytes()); // text
        r.extend((-1i32).to_be_bytes());
        let parsed = parse(&r).unwrap();
        match parsed {
            Message::Relation(rel) => {
                assert_eq!(rel.name, "orders");
                assert_eq!(rel.columns.len(), 2);
                assert_eq!(rel.columns[0].name, "id");
                assert_eq!(rel.columns[1].name, "name");
            }
            other => panic!("{other:?}"),
        }

        // Insert
        let mut i = vec![b'I'];
        i.extend(99u32.to_be_bytes());
        i.push(b'N');
        i.extend(text_tuple(&[Some("1"), Some("ada")]));
        match parse(&i).unwrap() {
            Message::Insert { rel: 99, new } => {
                assert_eq!(new.values[0], TupleValue::Text(b"1".to_vec()));
            }
            other => panic!("{other:?}"),
        }

        // Update with old key + unchanged TOAST in the new image.
        let mut u = vec![b'U'];
        u.extend(99u32.to_be_bytes());
        u.push(b'K');
        u.extend(text_tuple(&[Some("1"), None]));
        u.push(b'N');
        let mut new = 2u16.to_be_bytes().to_vec();
        new.push(b't');
        new.extend(1u32.to_be_bytes());
        new.push(b'1');
        new.push(b'u'); // unchanged toast
        u.extend(new);
        match parse(&u).unwrap() {
            Message::Update {
                rel: 99,
                old: Some(_),
                new,
            } => assert_eq!(new.values[1], TupleValue::UnchangedToast),
            other => panic!("{other:?}"),
        }

        // Delete by key.
        let mut d = vec![b'D'];
        d.extend(99u32.to_be_bytes());
        d.push(b'K');
        d.extend(text_tuple(&[Some("1"), None]));
        assert!(matches!(
            parse(&d).unwrap(),
            Message::Delete { rel: 99, .. }
        ));

        // Truncate.
        let mut t = vec![b'T'];
        t.extend(1u32.to_be_bytes());
        t.push(0);
        t.extend(99u32.to_be_bytes());
        assert_eq!(parse(&t).unwrap(), Message::Truncate { rels: vec![99] });
    }

    #[test]
    fn malformed_inputs_are_typed_errors_never_panics() {
        for bad in [
            &[][..],
            b"B",
            &[b'B', 0, 0],
            &[b'I', 0, 0, 0, 9, b'X'],
            &[b'U', 0, 0, 0, 9, b'Q'],
            &[b'Z', 1, 2, 3],
            // hostile counts
            &[b'I', 0, 0, 0, 9, b'N', 0xFF, 0xFF],
            &[b'T', 0xFF, 0xFF, 0xFF, 0xFF, 0],
            // trailing garbage
            &[b'O', 0, 0, 0, 0, 0, 0, 0, 0, 0, 0xAA],
        ] {
            assert!(parse(bad).is_err(), "{bad:?}");
        }
    }

    proptest! {
        #![proptest_config(ProptestConfig { cases: 4096, ..ProptestConfig::default() })]

        /// The fuzz property in miniature: arbitrary bytes never panic.
        #[test]
        fn arbitrary_bytes_never_panic(bytes in proptest::collection::vec(any::<u8>(), 0..512)) {
            let _ = parse(&bytes);
        }
    }
}
