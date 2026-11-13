//! datetime ↔ epoch-millis conversion.
//!
//! Mirrors `millis_from_datetime`/`decode_datetime` in `_cbsonmodule.c` and
//! the helpers in `bson/datetime_ms.py`: naive datetime fields are treated
//! as UTC (the `timegm` interpretation).

/// Days since 1970-01-01 for a civil date (Howard Hinnant's algorithm).
fn days_from_civil(y: i64, m: u32, d: u32) -> i64 {
    let y = if m <= 2 { y - 1 } else { y };
    let era = if y >= 0 { y } else { y - 399 } / 400;
    let yoe = y - era * 400; // [0, 399]
    let mp = (m as i64 + 9) % 12; // [0, 11]
    let doy = (153 * mp + 2) / 5 + d as i64 - 1; // [0, 365]
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146097 + doe - 719468
}

/// Inverse of `days_from_civil`: days since 1970-01-01 → (year, month, day).
fn civil_from_days(z: i64) -> (i64, u32, u32) {
    let z = z + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = z - era * 146_097; // [0, 146096]
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365; // [0, 399]
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100); // [0, 365]
    let mp = (5 * doy + 2) / 153; // [0, 11]
    let d = doy - (153 * mp + 2) / 5 + 1; // [1, 31]
    let m = if mp < 10 { mp + 3 } else { mp - 9 }; // [1, 12]
    (y + if m <= 2 { 1 } else { 0 }, m as u32, d as u32)
}

/// Computes epoch millis for naive datetime fields interpreted as UTC.
///
/// Mirrors `millis_from_datetime`: `timegm(fields) * 1000 + microsecond / 1000`.
pub fn millis_from_fields(
    year: i32,
    month: u8,
    day: u8,
    hour: u8,
    minute: u8,
    second: u8,
    microsecond: u32,
) -> i64 {
    let days = days_from_civil(year as i64, month as u32, day as u32);
    let secs = days * 86_400 + hour as i64 * 3600 + minute as i64 * 60 + second as i64;
    secs * 1000 + (microsecond / 1000) as i64
}

/// Decomposes epoch millis into UTC datetime fields.
///
/// Microseconds are the millisecond remainder times 1000 (BSON datetimes
/// have millisecond resolution).
pub fn millis_to_fields(millis: i64) -> (i32, u8, u8, u8, u8, u8, u32) {
    let days = millis.div_euclid(86_400_000);
    let rem = millis.rem_euclid(86_400_000);
    let (y, m, d) = civil_from_days(days);
    let hour = (rem / 3_600_000) as u8;
    let minute = (rem % 3_600_000 / 60_000) as u8;
    let second = (rem % 60_000 / 1000) as u8;
    let microsecond = (rem % 1000 * 1000) as u32;
    (
        y as i32,
        m as u8,
        d as u8,
        hour,
        minute,
        second,
        microsecond,
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn epoch() {
        assert_eq!(millis_from_fields(1970, 1, 1, 0, 0, 0, 0), 0);
        assert_eq!(millis_to_fields(0), (1970, 1, 1, 0, 0, 0, 0));
    }

    #[test]
    fn known_moments() {
        // 2026-10-20T14:30:00Z
        assert_eq!(
            millis_from_fields(2026, 10, 20, 14, 30, 0, 0),
            1_792_506_600_000
        );
        assert_eq!(
            millis_to_fields(1_792_506_600_000),
            (2026, 10, 20, 14, 30, 0, 0)
        );
        // One millisecond before the epoch.
        assert_eq!(millis_from_fields(1969, 12, 31, 23, 59, 59, 999_000), -1);
        assert_eq!(millis_to_fields(-1), (1969, 12, 31, 23, 59, 59, 999_000));
        // Leap years.
        assert_eq!(
            millis_from_fields(2024, 2, 29, 0, 0, 0, 0),
            1_709_164_800_000
        );
        assert_eq!(
            millis_to_fields(1_709_164_800_000),
            (2024, 2, 29, 0, 0, 0, 0)
        );
    }

    #[test]
    fn round_trips() {
        for millis in [
            -2_082_481_920_000_i64, // year 1
            -1,
            0,
            1,
            999,
            1000,
            1_792_494_600_123,
            2_534_023_008_799_999, // 9999-12-31T23:59:59.999Z
        ] {
            let fields = millis_to_fields(millis);
            let round_tripped = millis_from_fields(
                fields.0, fields.1, fields.2, fields.3, fields.4, fields.5, fields.6,
            );
            assert_eq!(round_tripped, millis, "millis={millis}");
        }
    }
}
