//! The type rulebook and the page-size rule.
//!
//! Both exist because the wire has hard edges: Oracle's types do not
//! map to the SPI's vocabulary one-for-one (research R3.1), and a
//! query reply must fit ONE SDU packet (T003) — so the page size is
//! DERIVED from the columns the server described, never guessed.

use oracle_rs::constants::OracleType;
use oracle_rs::statement::ColumnInfo;
use rdlt_connector_sdk::spi::core::LogicalType;

/// The SPI type one Oracle column carries, or a refusal naming it.
///
/// The rules that are NOT obvious (R3.1, each deliberate):
/// - Oracle `DATE` CARRIES TIME (second precision, no zone), so it is
///   a naive TIMESTAMP, never a `Date`.
/// - `NUMBER(p,s)` with a declared scale is exact `Decimal{p,s}`;
///   BARE `NUMBER` has FLOATING scale and no exact SPI counterpart,
///   so it travels as text (full precision preserved — probed exact
///   at 38 digits) rather than silently rounding through f64.
/// - `TIMESTAMP WITH LOCAL TIME ZONE` is normalized by the session,
///   which the client pins to UTC at connect.
pub(crate) fn logical_type(column: &ColumnInfo) -> Result<LogicalType, String> {
    // Oracle's NEGATIVE scales (-127 for FLOAT and floating-scale
    // NUMBER, or a declared NUMBER(10,-2)) describe a value that is
    // rounded ABOVE the decimal point. `LogicalType::Decimal` has no
    // way to say that — its scale is unsigned — so these keep their
    // exact digits as text rather than being declared a decimal whose
    // scale would be nonsense.
    if column.scale < 0 {
        return Ok(LogicalType::Utf8);
    }
    Ok(match column.oracle_type {
        OracleType::Number => {
            if column.scale > 0 && column.precision > 0 {
                LogicalType::Decimal {
                    precision: column.precision as u8,
                    scale: column.scale as u8,
                }
            } else if column.scale == 0 && column.precision > 0 && column.precision <= 18 {
                LogicalType::Int64
            } else {
                // Bare NUMBER, or a precision beyond i64: exact text.
                LogicalType::Utf8
            }
        }
        OracleType::BinaryFloat | OracleType::BinaryDouble => LogicalType::Float64,
        OracleType::Varchar | OracleType::Char | OracleType::Long | OracleType::Clob => {
            LogicalType::Utf8
        }
        OracleType::Raw | OracleType::LongRaw | OracleType::Blob => LogicalType::Binary,
        OracleType::Date | OracleType::Timestamp => LogicalType::TimestampNaive,
        OracleType::TimestampTz | OracleType::TimestampLtz => LogicalType::TimestampTz,
        OracleType::Boolean => LogicalType::Bool,
        OracleType::Json => LogicalType::Json,
        OracleType::Rowid | OracleType::Urowid => LogicalType::Utf8,
        other => {
            return Err(format!(
                "column `{}` has Oracle type {other:?}, which this connector does not read — \
                 project it with a supported expression (TO_CHAR, TO_CLOB) or exclude it",
                column.name
            ));
        }
    })
}

/// Is this column read through a LOB LOCATOR rather than inline?
/// (Probed at every size: CLOB/BLOB always arrive as locators.)
pub(crate) fn is_lob(column: &ColumnInfo) -> bool {
    matches!(column.oracle_type, OracleType::Clob | OracleType::Blob)
}

/// The SDU a connection negotiates by default; pages are sized to fit
/// ONE of these (T003 — a reply spanning packets cannot be read).
/// The live value comes from `Tuning::sdu_bytes`; this is the number
/// that default matches, and what the page-size pins measure against.
#[cfg(test)]
const DEFAULT_SDU_BYTES: u32 = 8192;

/// The share of the SDU a page may claim. The rest absorbs the
/// message framing, the column metadata echo, and the terminator —
/// deliberately generous, because the failure mode of guessing high
/// is a refused read, and the cost of guessing low is one extra
/// round trip.
const PAGE_BUDGET: f64 = 0.55;

/// The wire width the ROWID column adds to EVERY page row.
///
/// The describe is taken over `SELECT t.*`, but each executed page
/// also projects `ROWIDTOCHAR(t.ROWID)` — 18 characters, on every
/// row, unconditionally. Sizing the page from the describe alone left
/// that unbudgeted, which is how a narrow table could derive a page
/// whose reply exceeded the packet.
const ROWID_BYTES: u32 = 18;

/// How many rows may be requested per round trip, DERIVED from the
/// widths the server described — or `None` when a SINGLE row cannot
/// fit, which no page size can rescue.
///
/// A LOB column contributes its locator, not its content (the content
/// is read separately), so a table of CLOBs still pages sanely.
///
/// Returning `None` rather than clamping to 1 is the point. The old
/// floor of 1 abandoned the invariant the whole design rests on — one
/// reply, one packet — and handed the failure to the wire, where it
/// surfaced as a buffer underflow on a read that can never succeed.
pub(crate) fn rows_per_page(columns: &[ColumnInfo], sdu_bytes: u32) -> Option<u32> {
    const LOCATOR_BYTES: u32 = 128;
    const PER_COLUMN_OVERHEAD: u32 = 2;

    let row_bytes: u32 = columns
        .iter()
        .map(|c| {
            let width = if is_lob(c) {
                LOCATOR_BYTES
            } else {
                // `buffer_size` is the width THE SERVER ALLOCATED, in
                // bytes. `data_size` is the DECLARED size, which for a
                // character-semantics column is a CHARACTER count —
                // `VARCHAR2(1000 CHAR)` describes as 1000 while the
                // server allocates 4000. Budgeting from the declared
                // number under-counted by up to 4x on any multibyte
                // database, deriving pages that could not fit.
                c.buffer_size.max(c.data_size).max(1)
            };
            // Saturating: these are server-supplied ub4s, and a sum
            // that wrapped would produce a tiny row width and a
            // catastrophically large page.
            width.saturating_add(PER_COLUMN_OVERHEAD)
        })
        .fold(ROWID_BYTES + PER_COLUMN_OVERHEAD, u32::saturating_add)
        .max(1);
    let budget = (f64::from(sdu_bytes) * PAGE_BUDGET) as u32;
    match budget / row_bytes {
        0 => None,
        rows => Some(rows),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn column(name: &str, oracle_type: OracleType, data_size: u32) -> ColumnInfo {
        let mut info = ColumnInfo::new(name, oracle_type);
        info.data_size = data_size;
        info.buffer_size = data_size;
        info
    }

    /// The rules that are easy to get wrong, pinned: DATE is a
    /// TIMESTAMP (it carries time), bare NUMBER is text (no exact SPI
    /// counterpart), scaled NUMBER is exact Decimal.
    #[test]
    fn the_type_rules_that_bite_are_pinned() {
        assert_eq!(
            logical_type(&column("d", OracleType::Date, 7)).expect("date"),
            LogicalType::TimestampNaive,
            "Oracle DATE carries time-of-day"
        );
        assert_eq!(
            logical_type(&column("n", OracleType::Number, 22)).expect("bare number"),
            LogicalType::Utf8,
            "bare NUMBER has floating scale — exact text, never lossy f64"
        );
        let mut scaled = column("m", OracleType::Number, 22);
        scaled.precision = 10;
        scaled.scale = 2;
        assert_eq!(
            logical_type(&scaled).expect("scaled"),
            LogicalType::Decimal {
                precision: 10,
                scale: 2
            }
        );
        let mut counter = column("c", OracleType::Number, 22);
        counter.precision = 10;
        assert_eq!(logical_type(&counter).expect("int"), LogicalType::Int64);
        assert_eq!(
            logical_type(&column("t", OracleType::TimestampLtz, 11)).expect("ltz"),
            LogicalType::TimestampTz,
            "LOCAL TIME ZONE is normalized by the UTC-pinned session"
        );
    }

    /// An unreadable type refuses by NAME with an actionable
    /// suggestion, never a silent drop.
    #[test]
    fn an_unsupported_type_refuses_naming_the_column() {
        let err = logical_type(&column("shape", OracleType::Vector, 100)).expect_err("refused");
        assert!(
            err.contains("column `shape`") && err.contains("TO_CHAR"),
            "{err}"
        );
    }

    /// Pages are derived from described widths so a reply cannot
    /// exceed one SDU packet (T003's standing limit), with LOBs
    /// counted as locators.
    #[test]
    fn page_size_follows_the_described_widths() {
        let narrow = [
            column("id", OracleType::Number, 22),
            column("v", OracleType::Varchar, 40),
        ];
        let wide = [
            column("id", OracleType::Number, 22),
            column("pad", OracleType::Varchar, 4000),
        ];
        let narrow_page = rows_per_page(&narrow, DEFAULT_SDU_BYTES).expect("narrow fits");
        let wide_page = rows_per_page(&wide, DEFAULT_SDU_BYTES).expect("a 4 KB row still fits");
        assert!(narrow_page > wide_page, "{narrow_page} vs {wide_page}");
        assert_eq!(wide_page, 1, "a 4 KB row leaves room for exactly one");
        // Every page must fit the budget it was derived from — and
        // the ROWID column every page projects is part of the row.
        for (columns, page) in [(&narrow[..], narrow_page), (&wide[..], wide_page)] {
            let row_bytes: u32 =
                columns.iter().map(|c| c.data_size + 2).sum::<u32>() + ROWID_BYTES + 2;
            assert!(
                page * row_bytes <= DEFAULT_SDU_BYTES,
                "page {page} × {row_bytes} exceeds the SDU"
            );
        }
        // A table of LOBs pages by locator width, not content.
        let lobs = [column("doc", OracleType::Clob, 4000)];
        assert!(rows_per_page(&lobs, DEFAULT_SDU_BYTES).expect("locators are small") > 20);
    }

    /// A row wider than the whole packet is REFUSED, not clamped to a
    /// one-row page that cannot be read either.
    #[test]
    fn a_row_wider_than_the_packet_has_no_page_size() {
        let huge = [
            column("a", OracleType::Varchar, 4000),
            column("b", OracleType::Varchar, 4000),
        ];
        assert_eq!(
            rows_per_page(&huge, DEFAULT_SDU_BYTES),
            None,
            "8 KB of declared width cannot fit a 4.5 KB budget at any page size"
        );
    }

    /// Character-semantics columns are budgeted by the SERVER'S byte
    /// width, not the declared character count — the two differ by up
    /// to 4x on a multibyte database.
    #[test]
    fn the_budget_uses_the_servers_byte_width() {
        // VARCHAR2(1000 CHAR) on AL32UTF8: declared 1000, allocated 4000.
        let mut col = ColumnInfo::new("txt", OracleType::Varchar);
        col.data_size = 1000;
        col.buffer_size = 4000;
        let page = rows_per_page(&[col], DEFAULT_SDU_BYTES).expect("one row fits");
        assert_eq!(
            page, 1,
            "budgeting by the declared 1000 would have derived 4 rows of up to 16 KB"
        );
    }
}
