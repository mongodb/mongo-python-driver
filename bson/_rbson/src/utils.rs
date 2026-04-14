// Copyright 2025-present MongoDB, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Utility functions for BSON operations

use pyo3::prelude::*;
use pyo3::types::PyAny;

use crate::types::TYPE_CACHE;

/// Convert Python datetime to milliseconds since epoch UTC
/// This is equivalent to Python's bson.datetime_ms._datetime_to_millis()
pub(crate) fn datetime_to_millis(py: Python, dtm: &Bound<'_, PyAny>) -> PyResult<i64> {
    // Get datetime components
    let year: i32 = dtm.getattr("year")?.extract()?;
    let month: i32 = dtm.getattr("month")?.extract()?;
    let day: i32 = dtm.getattr("day")?.extract()?;
    let hour: i32 = dtm.getattr("hour")?.extract()?;
    let minute: i32 = dtm.getattr("minute")?.extract()?;
    let second: i32 = dtm.getattr("second")?.extract()?;
    let microsecond: i32 = dtm.getattr("microsecond")?.extract()?;

    // Check if datetime has timezone offset
    let utcoffset = dtm.call_method0("utcoffset")?;
    let offset_seconds: i64 = if !utcoffset.is_none() {
        // Get total_seconds() from timedelta
        let total_seconds: f64 = utcoffset.call_method0("total_seconds")?.extract()?;
        total_seconds as i64
    } else {
        0
    };

    // Calculate seconds since epoch using the same algorithm as Python's calendar.timegm
    // This is: (year - 1970) * 365.25 days + month/day adjustments + time
    // We'll use Python's calendar.timegm for accuracy
    let timegm = TYPE_CACHE.get_calendar_timegm(py)?;

    // Create a time tuple (year, month, day, hour, minute, second, weekday, yearday, isdst)
    // We need timetuple() method
    let timetuple = dtm.call_method0("timetuple")?;
    let seconds_since_epoch: i64 = timegm.bind(py).call1((timetuple,))?.extract()?;

    // Adjust for timezone offset (subtract to get UTC)
    let utc_seconds = seconds_since_epoch - offset_seconds;

    // Convert to milliseconds and add microseconds
    let millis = utc_seconds * 1000 + (microsecond / 1000) as i64;

    Ok(millis)
}

/// Convert Python regex flags (int) to BSON regex options (string)
pub(crate) fn int_flags_to_str(flags: i32) -> String {
    let mut options = String::new();

    // Python re module flags to BSON regex options:
    // re.IGNORECASE = 2 -> 'i'
    // re.MULTILINE = 8 -> 'm'
    // re.DOTALL = 16 -> 's'
    // re.VERBOSE = 64 -> 'x'
    // Note: re.LOCALE and re.UNICODE are Python-specific

    if flags & 2 != 0 {
        options.push('i');
    }
    if flags & 4 != 0 {
        options.push('l'); // Preserved for round-trip compatibility
    }
    if flags & 8 != 0 {
        options.push('m');
    }
    if flags & 16 != 0 {
        options.push('s');
    }
    if flags & 32 != 0 {
        options.push('u'); // Preserved for round-trip compatibility
    }
    if flags & 64 != 0 {
        options.push('x');
    }

    options
}

/// Convert BSON regex options (string) to Python regex flags (int)
pub(crate) fn str_flags_to_int(options: &str) -> i32 {
    let mut flags = 0;

    for ch in options.chars() {
        match ch {
            'i' => flags |= 2,  // re.IGNORECASE
            'l' => flags |= 4,  // re.LOCALE
            'm' => flags |= 8,  // re.MULTILINE
            's' => flags |= 16, // re.DOTALL
            'u' => flags |= 32, // re.UNICODE
            'x' => flags |= 64, // re.VERBOSE
            _ => {}             // Ignore unknown flags
        }
    }

    flags
}

/// Validate a document key
pub(crate) fn validate_key(key: &str, check_keys: bool) -> PyResult<()> {
    // Check for null bytes (always invalid)
    if key.contains('\0') {
        return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
            "Key names must not contain the NULL byte"
        ));
    }

    // Check keys if requested (but not for _id)
    if check_keys && key != "_id" {
        if key.starts_with('$') {
            return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
                format!("key '{}' must not start with '$'", key)
            ));
        }
        if key.contains('.') {
            return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
                format!("key '{}' must not contain '.'", key)
            ));
        }
    }

    Ok(())
}

/// Write a C-style null-terminated string
pub(crate) fn write_cstring(buf: &mut Vec<u8>, s: &str) {
    buf.extend_from_slice(s.as_bytes());
    buf.push(0);
}

/// Write a BSON string (int32 length + string + null terminator)
pub(crate) fn write_string(buf: &mut Vec<u8>, s: &str) {
    let len = (s.len() + 1) as i32; // +1 for null terminator
    buf.extend_from_slice(&len.to_le_bytes());
    buf.extend_from_slice(s.as_bytes());
    buf.push(0);
}
