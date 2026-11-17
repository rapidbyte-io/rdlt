//! The extension pip reads a file's name by: Python's `posixpath.splitext`, and a `.tar` before it
//! taken in too, in any case.

#[cfg(test)]
mod tests;

/// The extension of `path` as pip's `splitext` reads it: from the last `.` of its last segment,
/// unless only dots come before it there, and from the `.tar`, in any case, that ends what comes
/// before.
pub(super) fn extension(path: &str) -> &str {
    let segment = path.rfind('/').map_or(0, |slash| slash + 1);
    let split = match path.rfind('.') {
        Some(dot) if dot >= segment && path[segment..dot].bytes().any(|byte| byte != b'.') => dot,
        _ => path.len(),
    };
    let base = &path.as_bytes()[..split];
    let tar = base.len() >= ".tar".len()
        && base[base.len() - ".tar".len()..].eq_ignore_ascii_case(b".tar");
    if tar {
        &path[split - ".tar".len()..]
    } else {
        &path[split..]
    }
}
