//! SQLite 4's variable-length integers (<https://sqlite.org/src4/doc/trunk/www/varint.wiki>),
//! whose bytes compare as the numbers they encode: one to nine bytes, the first saying how many
//! follow, so numbers written one after another read back alike and compare as their tuple does.
//!
//! The encoding is written here rather than taken from a crate, since ids stored in tables hold
//! it: it must not change with a dependency's release.

#[cfg(test)]
mod tests;

/// Appends `value`'s bytes to `out`.
pub(crate) fn write(value: u64, out: &mut Vec<u8>) {
    match len(value) {
        1 => out.push(value.to_be_bytes()[7]),
        2 => {
            let [.., high, low] = (value - 240).to_be_bytes();
            out.extend([241 + high, low]);
        }
        3 => {
            let [.., high, low] = (value - 2288).to_be_bytes();
            out.extend([249, high, low]);
        }
        bytes => {
            let width = bytes - 1;
            out.push(WIDE[width - 3]);
            out.extend_from_slice(&value.to_be_bytes()[8 - width..]);
        }
    }
}

/// How many bytes `value` takes.
pub(crate) const fn len(value: u64) -> usize {
    match value {
        0..=240 => 1,
        241..=2287 => 2,
        2288..=67_823 => 3,
        67_824..=0xff_ffff => 4,
        0x100_0000..=0xffff_ffff => 5,
        0x1_0000_0000..=0xff_ffff_ffff => 6,
        0x100_0000_0000..=0xffff_ffff_ffff => 7,
        0x1_0000_0000_0000..=0xff_ffff_ffff_ffff => 8,
        _ => 9,
    }
}

/// The first byte of a number of three to eight big-endian bytes, by their count less three.
const WIDE: [u8; 6] = [250, 251, 252, 253, 254, 255];
