//! Growable little-endian byte buffer for BSON encoding.
//!
//! Mirrors the semantics of `bson/buffer.c`: append-only writes with the
//! ability to backpatch previously written 4-byte length fields.

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

    #[inline]
    pub fn write_u8(&mut self, value: u8) {
        self.buf.push(value);
    }

    #[inline]
    pub fn write_u32_le(&mut self, value: u32) {
        self.buf.extend_from_slice(&value.to_le_bytes());
    }

    #[inline]
    pub fn write_i32_le(&mut self, value: i32) {
        self.buf.extend_from_slice(&value.to_le_bytes());
    }

    #[inline]
    pub fn write_i64_le(&mut self, value: i64) {
        self.buf.extend_from_slice(&value.to_le_bytes());
    }

    #[inline]
    pub fn write_f64_le(&mut self, value: f64) {
        self.buf.extend_from_slice(&value.to_bits().to_le_bytes());
    }

    /// Writes raw bytes without any framing.
    #[inline]
    pub fn write_bytes(&mut self, bytes: &[u8]) {
        self.buf.extend_from_slice(bytes);
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
    fn length_backpatching() {
        let mut buf = Buffer::new();
        buf.write_u32_le(0); // placeholder
        buf.write_bytes(b"a\x00");
        buf.write_u8(0);
        let total = buf.len() as u32;
        buf.backpatch_u32(0, total);
        let mut expected = total.to_le_bytes().to_vec();
        expected.extend_from_slice(b"a\x00\x00");
        assert_eq!(buf.into_vec(), expected);
    }

    #[test]
    fn scalar_writes_are_little_endian() {
        let mut buf = Buffer::new();
        buf.write_i32_le(-1);
        buf.write_f64_le(1.0);
        assert_eq!(
            buf.into_vec(),
            [
                0xff, 0xff, 0xff, 0xff, // -1
                0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xf0, 0x3f, // 1.0f64
            ]
        );
    }
}
