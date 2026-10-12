//! Logs rewritten as whoever can write their directory rewrites them.

use serde::{Deserialize, Deserializer};
use serde_json::value::RawValue;

/// The member a frame's payload holds at a path of names, and what it is set to.
pub(crate) type Edit = (&'static [&'static str], serde_json::Value);

/// Rewrites every chunk of the logs under `base`, setting the member `rewrite` names for each
/// frame's kind, every other byte of the payload as it was, its checksum made to match again, as
/// whoever can write the logs' directory can.
pub(crate) fn rewritten(base: &std::path::Path, rewrite: &dyn Fn(u8) -> Option<Edit>) -> usize {
    let mut chunks = 0;
    for entry in std::fs::read_dir(base).expect("a directory") {
        let path = entry.expect("an entry").path();
        if path.is_dir() {
            chunks += rewritten(&path, rewrite);
            continue;
        }
        if path.extension().and_then(|extension| extension.to_str()) != Some("wal") {
            continue;
        }
        let bytes = std::fs::read(&path).expect("a chunk");
        let mut out = bytes[..14].to_vec();
        let mut at = 14;
        while at + 9 <= bytes.len() {
            let kind = bytes[at];
            let len = usize::try_from(u32::from_le_bytes(
                bytes[at + 1..at + 5].try_into().expect("four bytes"),
            ))
            .expect("a length");
            let mut payload = bytes[at + 9..at + 9 + len].to_vec();
            if let Some((path, value)) = rewrite(kind) {
                payload = edited(&payload, path, &value);
            }
            let len = u32::try_from(payload.len())
                .expect("a short payload")
                .to_le_bytes();
            let mut head = vec![kind];
            head.extend_from_slice(&len);
            let check = crc32c::crc32c_append(crc32c::crc32c(&head), &payload);
            out.extend_from_slice(&head);
            out.extend_from_slice(&check.to_le_bytes());
            out.extend_from_slice(&payload);
            at += 9 + len_of(&bytes[at..]);
        }
        std::fs::write(&path, out).expect("the rewritten chunk");
        chunks += 1;
    }
    chunks
}

/// The JSON object `payload` with the member at `path` set to `value`, every other member's
/// text as it was.
fn edited(payload: &[u8], path: &[&str], value: &serde_json::Value) -> Vec<u8> {
    let Members(members) = serde_json::from_slice(payload).expect("a JSON object");
    let mut found = false;
    let mut out = b"{".to_vec();
    for (index, (name, member)) in members.iter().enumerate() {
        if index > 0 {
            out.push(b',');
        }
        out.extend(serde_json::to_vec(name).expect("a name encodes"));
        out.push(b':');
        if name == path[0] {
            found = true;
            if path.len() == 1 {
                out.extend(serde_json::to_vec(value).expect("JSON encodes"));
            } else {
                out.extend(edited(member.get().as_bytes(), &path[1..], value));
            }
        } else {
            out.extend_from_slice(member.get().as_bytes());
        }
    }
    assert!(found, "the payload holds {path:?}");
    out.push(b'}');
    out
}

/// A JSON object's members in the order they are written, each as its text.
struct Members(Vec<(String, Box<RawValue>)>);

impl<'de> Deserialize<'de> for Members {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        deserializer.deserialize_map(MembersVisitor)
    }
}

/// Reads a JSON object's members in order.
struct MembersVisitor;

impl<'de> serde::de::Visitor<'de> for MembersVisitor {
    type Value = Members;

    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("a JSON object")
    }

    fn visit_map<A: serde::de::MapAccess<'de>>(self, mut map: A) -> Result<Members, A::Error> {
        let mut members = Vec::new();
        while let Some(member) = map.next_entry()? {
            members.push(member);
        }
        Ok(Members(members))
    }
}

/// The length of the payload of the frame `bytes` starts with.
fn len_of(bytes: &[u8]) -> usize {
    usize::try_from(u32::from_le_bytes(
        bytes[1..5].try_into().expect("four bytes"),
    ))
    .expect("a length")
}
