//! Growable little-endian byte buffer for BSON encoding.
//!
//! Mirrors the semantics of `bson/buffer.c`: append-only writes with the
//! ability to backpatch previously written 4-byte length fields. Like the
//! C's `buffer_assure_space`, cumulative growth beyond `INT_MAX` (the BSON
//! document size limit) fails instead of wrapping the length fields the
//! buffer's own consumers backpatch and decode, and allocation failure
//! raises instead of aborting (the C's MemoryError).

use pyo3::exceptions::{PyMemoryError, PyValueError};
use pyo3::prelude::*;

/// A write could not complete.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReserveError {
    /// The cumulative write would exceed the signed-int32 BSON document
    /// size limit (mirrors the C's `buffer_assure_space` ValueError).
    Overflow,
    /// The allocation failed (mirrors the C's MemoryError).
    Allocation,
}

impl std::fmt::Display for ReserveError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ReserveError::Overflow => f.write_str("Document would overflow BSON size limit"),
            ReserveError::Allocation => f.write_str("could not allocate memory for the document"),
        }
    }
}

impl std::error::Error for ReserveError {}

impl From<ReserveError> for PyErr {
    fn from(err: ReserveError) -> PyErr {
        match err {
            ReserveError::Overflow => {
                // Mirrors the C's buffer_assure_space: a plain ValueError.
                PyValueError::new_err("Document would overflow BSON size limit")
            }
            // Mirrors the C's buffer_grow: a plain MemoryError.
            ReserveError::Allocation => PyMemoryError::new_err(()),
        }
    }
}

/// Growable byte buffer with BSON-oriented write helpers.
pub struct Buffer {
    buf: Vec<u8>,
}

impl Buffer {
    #[inline]
    pub fn new() -> Self {
        // BSON minimum size is 5 bytes; most documents are larger.
        Buffer {
            buf: Vec::with_capacity(256),
        }
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.buf.len()
    }

    /// Reserves `additional` bytes, failing when the cumulative size would
    /// exceed `i32::MAX` (checked before any allocation, mirroring the C's
    /// `buffer_assure_space`) or when the allocation fails (the C's
    /// MemoryError; `try_reserve` raises instead of aborting).
    #[inline]
    fn reserve(&mut self, additional: usize) -> Result<(), ReserveError> {
        match self.buf.len().checked_add(additional) {
            Some(total) if total <= i32::MAX as usize => {}
            _ => return Err(ReserveError::Overflow),
        }
        self.buf
            .try_reserve(additional)
            .map_err(|_| ReserveError::Allocation)
    }

    #[inline]
    pub fn write_u8(&mut self, value: u8) -> Result<(), ReserveError> {
        self.reserve(1)?;
        self.buf.push(value);
        Ok(())
    }

    #[inline]
    pub fn write_u32_le(&mut self, value: u32) -> Result<(), ReserveError> {
        self.reserve(4)?;
        self.buf.extend_from_slice(&value.to_le_bytes());
        Ok(())
    }

    #[inline]
    pub fn write_i32_le(&mut self, value: i32) -> Result<(), ReserveError> {
        self.reserve(4)?;
        self.buf.extend_from_slice(&value.to_le_bytes());
        Ok(())
    }

    #[inline]
    pub fn write_i64_le(&mut self, value: i64) -> Result<(), ReserveError> {
        self.reserve(8)?;
        self.buf.extend_from_slice(&value.to_le_bytes());
        Ok(())
    }

    #[inline]
    pub fn write_f64_le(&mut self, value: f64) -> Result<(), ReserveError> {
        self.reserve(8)?;
        self.buf.extend_from_slice(&value.to_bits().to_le_bytes());
        Ok(())
    }

    /// Writes raw bytes without any framing.
    #[inline]
    pub fn write_bytes(&mut self, bytes: &[u8]) -> Result<(), ReserveError> {
        self.reserve(bytes.len())?;
        self.buf.extend_from_slice(bytes);
        Ok(())
    }

    /// Returns the current offset, for later `backpatch_u32` calls.
    #[inline]
    pub fn position(&self) -> usize {
        self.buf.len()
    }

    /// Overwrites 4 bytes at `at` with `value` (little-endian).
    #[inline]
    pub fn backpatch_u32(&mut self, at: usize, value: u32) {
        self.buf[at..at + 4].copy_from_slice(&value.to_le_bytes());
    }

    /// Consumes the buffer, returning the written bytes.
    #[inline]
    pub fn into_vec(self) -> Vec<u8> {
        self.buf
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn writes_past_the_bson_size_limit_fail() {
        let mut buf = Buffer::new();
        // Huge additions fail the cap check before any allocation: the
        // check is pure math, so even usize::MAX cannot abort or allocate.
        assert_eq!(buf.reserve(usize::MAX), Err(ReserveError::Overflow));
        assert_eq!(
            buf.reserve((i32::MAX as usize) + 1),
            Err(ReserveError::Overflow)
        );
        assert_eq!(buf.len(), 0);
        // A failed write leaves nothing behind; later writes still succeed.
        buf.write_u8(1).unwrap();
        assert_eq!(buf.len(), 1);
        // Exactly INT_MAX total bytes is the largest legal document. Grow
        // in 512 MiB chunks from one reused source so the test peaks at
        // ~2.5 GiB (the capped buffer plus one chunk) rather than twice
        // that; the last write lands exactly on the boundary, and one more
        // byte overflows.
        let chunk = vec![0u8; 1 << 29];
        for _ in 0..3 {
            buf.write_bytes(&chunk).unwrap();
        }
        buf.write_bytes(&chunk[..((i32::MAX as usize) - 1) % chunk.len()])
            .unwrap();
        assert_eq!(buf.len(), i32::MAX as usize);
        assert_eq!(buf.write_u8(0), Err(ReserveError::Overflow));
        assert_eq!(buf.len(), i32::MAX as usize);
    }

    #[test]
    fn length_backpatching() {
        let mut buf = Buffer::new();
        buf.write_u32_le(0).unwrap(); // placeholder
        buf.write_bytes(b"a\x00").unwrap();
        buf.write_u8(0).unwrap();
        let total = buf.len() as u32;
        buf.backpatch_u32(0, total);
        let mut expected = total.to_le_bytes().to_vec();
        expected.extend_from_slice(b"a\x00\x00");
        assert_eq!(buf.into_vec(), expected);
    }

    #[test]
    fn scalar_writes_are_little_endian() {
        let mut buf = Buffer::new();
        buf.write_i32_le(-1).unwrap();
        buf.write_f64_le(1.0).unwrap();
        assert_eq!(
            buf.into_vec(),
            [
                0xff, 0xff, 0xff, 0xff, // -1
                0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xf0, 0x3f, // 1.0f64
            ]
        );
    }
}
