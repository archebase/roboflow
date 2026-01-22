// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! CDR cursor for reading CDR-encoded data with proper alignment.
//!
//! Based on the TypeScript implementation at:
//! https://github.com/emulated-devices/rtps-cdr/blob/main/src/CdrReader.ts

use crate::CodecError;
use crate::Result as CoreResult;

/// Size of the CDR encapsulation header (4 bytes).
pub const CDR_HEADER_SIZE: usize = 4;

/// CDR cursor that tracks position and origin for proper alignment.
///
/// The cursor is used for reading CDR-encoded data. It tracks:
/// - `offset`: Current read position in the buffer
/// - `origin`: Alignment reference point (0 for top-level, reset for nested structs)
/// - `origin_stack`: Stack of saved origins for nested struct scopes
///
/// Key concept: Alignment is calculated as `(offset - origin) % size`, not `offset % size`.
/// This matches the DDS-XTypes specification for proper alignment in nested structures.
///
/// # Example
///
/// ```no_run
/// # fn main() -> Result<(), Box<dyn std::error::Error>> {
/// use robocodec::encoding::cdr::cursor::CdrCursor;
///
/// let data = vec![0x00, 0x01, 0x00, 0x00, 0x2A, 0x00, 0x00, 0x00]; // CDR header + value
/// let mut cursor = CdrCursor::new(&data)?;
/// assert_eq!(cursor.read_u32()?, 42);
/// # Ok(())
/// # }
/// ```
pub struct CdrCursor<'a> {
    /// The data buffer (includes CDR header)
    data: &'a [u8],
    /// Current read position
    offset: usize,
    /// Origin offset for alignment calculation (0 for top-level, reset for nested structs)
    origin: usize,
    /// Stack of saved origins for nested struct scopes
    origin_stack: Vec<usize>,
    /// Whether the data uses little endian encoding
    little_endian: bool,
    /// Whether this is ROS1 encoded data (affects array alignment)
    is_ros1: bool,
}

impl<'a> CdrCursor<'a> {
    /// Create a new CDR cursor from CDR-encoded data.
    ///
    /// # Arguments
    ///
    /// * `data` - The CDR-encoded data (must include 4-byte header)
    ///
    /// # CDR Header Format
    ///
    /// The CDR header is 4 bytes:
    /// - Byte 0: Unused (always 0)
    /// - Byte 1: Encapsulation kind (endianness flag)
    /// - Bytes 2-3: Options (unused, set to 0)
    pub fn new(data: &'a [u8]) -> CoreResult<Self> {
        if data.len() < CDR_HEADER_SIZE {
            return Err(CodecError::Other(format!(
                "Invalid CDR data size {}, must contain at least a 4-byte header",
                data.len()
            )));
        }

        // Check endianness flag in byte 1
        // 0 = big endian, 1 = little endian
        let little_endian = data[1] == 1;

        // Start after the 4-byte CDR header
        // Origin is set to 4 (after CDR header) so alignment is calculated
        // relative to the start of the serialized payload, matching CDR spec.
        let origin = CDR_HEADER_SIZE;
        let offset = CDR_HEADER_SIZE;

        Ok(Self {
            data,
            offset,
            origin,
            origin_stack: Vec::new(),
            little_endian,
            is_ros1: false,
        })
    }

    /// Create a new CDR cursor from data without a CDR header.
    ///
    /// This is used for ROS1 bag messages which store message data without
    /// the CDR encapsulation header. The cursor starts at position 0 with
    /// origin at 0, since there's no header to skip.
    ///
    /// # Arguments
    ///
    /// * `data` - The CDR-encoded binary data WITHOUT 4-byte header
    /// * `little_endian` - Whether the data uses little endian encoding
    pub fn new_headerless(data: &'a [u8], little_endian: bool) -> Self {
        Self {
            data,
            offset: 0,
            origin: 0,
            origin_stack: Vec::new(),
            little_endian,
            is_ros1: false,
        }
    }

    /// Create a new CDR cursor for ROS1 bag data.
    ///
    /// ROS1 bags have data that includes a CDR header, but the header
    /// may incorrectly indicate big-endian encoding when the data is
    /// actually little-endian. This method creates a cursor starting
    /// after the CDR header and forces little-endian encoding.
    ///
    /// # Arguments
    ///
    /// * `data` - The CDR-encoded binary data INCLUDING 4-byte header
    pub fn new_ros1(data: &'a [u8]) -> CoreResult<Self> {
        if data.len() < CDR_HEADER_SIZE {
            return Err(CodecError::Other(format!(
                "Invalid CDR data size {}, must contain at least a 4-byte header",
                data.len()
            )));
        }

        // Start after the 4-byte CDR header
        // Force little-endian encoding (ROS1 quirk - header often wrong)
        Ok(Self {
            data,
            offset: CDR_HEADER_SIZE,
            origin: CDR_HEADER_SIZE,
            origin_stack: Vec::new(),
            little_endian: true,
            is_ros1: true,
        })
    }

    /// Get the current position relative to the data start.
    #[inline]
    pub fn position(&self) -> usize {
        self.offset
    }

    /// Get the remaining bytes available to read.
    #[inline]
    pub fn remaining(&self) -> usize {
        self.data.len().saturating_sub(self.offset)
    }

    /// Check if at end of buffer.
    #[inline]
    pub fn is_at_end(&self) -> bool {
        self.offset >= self.data.len()
    }

    /// Check if this cursor is for ROS1 encoded data.
    #[inline]
    pub fn is_ros1(&self) -> bool {
        self.is_ros1
    }

    /// Align to the specified boundary, relative to the origin.
    ///
    /// This matches the TypeScript implementation: `(offset - origin) % size`
    ///
    /// # Arguments
    ///
    /// * `size` - The alignment boundary (e.g., 4 for 4-byte alignment)
    pub fn align(&mut self, size: usize) -> CoreResult<()> {
        let alignment = (self.offset - self.origin) % size;
        if alignment > 0 {
            let padding = size - alignment;
            if self.offset + padding > self.data.len() {
                return Err(CodecError::buffer_too_short(
                    padding,
                    self.remaining(),
                    self.offset as u64,
                ));
            }
            self.offset += padding;
        }
        Ok(())
    }

    /// Push the current origin onto the stack and reset origin to current offset.
    ///
    /// This is called when entering a nested struct. The origin is reset so that
    /// alignment is calculated relative to the start of the nested struct.
    /// The previous origin is saved and can be restored with `pop_origin()`.
    pub fn push_origin(&mut self) {
        self.origin_stack.push(self.origin);
        self.origin = self.offset;
    }

    /// Pop the previous origin from the stack.
    ///
    /// This is called when exiting a nested struct. The origin is restored to
    /// the value it had before entering the nested struct, so that subsequent
    /// fields are aligned correctly relative to the parent message.
    pub fn pop_origin(&mut self) {
        if let Some(prev_origin) = self.origin_stack.pop() {
            self.origin = prev_origin;
        }
    }

    /// Reset the origin to the current offset (for nested structs).
    ///
    /// This matches the TypeScript `resetOrigin()` function.
    /// When entering a nested struct, the origin is reset so that alignment
    /// is calculated relative to the start of the nested struct, not the
    /// start of the entire message.
    ///
    /// Note: For proper nested struct handling, use `push_origin()` and `pop_origin()` instead.
    pub fn reset_origin(&mut self) {
        self.origin = self.offset;
    }

    /// Read a single byte.
    pub fn read_u8(&mut self) -> CoreResult<u8> {
        if self.offset >= self.data.len() {
            return Err(CodecError::buffer_too_short(1, 0, self.offset as u64));
        }
        let value = self.data[self.offset];
        self.offset += 1;
        Ok(value)
    }

    /// Read a signed byte.
    pub fn read_i8(&mut self) -> CoreResult<i8> {
        Ok(self.read_u8()? as i8)
    }

    /// Read a u16 value.
    pub fn read_u16(&mut self) -> CoreResult<u16> {
        self.align(2)?;
        if self.offset + 2 > self.data.len() {
            return Err(CodecError::buffer_too_short(
                2,
                self.remaining(),
                self.offset as u64,
            ));
        }
        let bytes = [self.data[self.offset], self.data[self.offset + 1]];
        self.offset += 2;
        Ok(if self.little_endian {
            u16::from_le_bytes(bytes)
        } else {
            u16::from_be_bytes(bytes)
        })
    }

    /// Read an i16 value.
    pub fn read_i16(&mut self) -> CoreResult<i16> {
        Ok(self.read_u16()? as i16)
    }

    /// Read a u32 value.
    pub fn read_u32(&mut self) -> CoreResult<u32> {
        self.align(4)?;
        if self.offset + 4 > self.data.len() {
            return Err(CodecError::buffer_too_short(
                4,
                self.remaining(),
                self.offset as u64,
            ));
        }
        let bytes = [
            self.data[self.offset],
            self.data[self.offset + 1],
            self.data[self.offset + 2],
            self.data[self.offset + 3],
        ];
        self.offset += 4;
        Ok(if self.little_endian {
            u32::from_le_bytes(bytes)
        } else {
            u32::from_be_bytes(bytes)
        })
    }

    /// Read an i32 value.
    pub fn read_i32(&mut self) -> CoreResult<i32> {
        Ok(self.read_u32()? as i32)
    }

    /// Read a u64 value.
    pub fn read_u64(&mut self) -> CoreResult<u64> {
        self.align(8)?;
        if self.offset + 8 > self.data.len() {
            return Err(CodecError::buffer_too_short(
                8,
                self.remaining(),
                self.offset as u64,
            ));
        }
        let bytes = [
            self.data[self.offset],
            self.data[self.offset + 1],
            self.data[self.offset + 2],
            self.data[self.offset + 3],
            self.data[self.offset + 4],
            self.data[self.offset + 5],
            self.data[self.offset + 6],
            self.data[self.offset + 7],
        ];
        self.offset += 8;
        Ok(if self.little_endian {
            u64::from_le_bytes(bytes)
        } else {
            u64::from_be_bytes(bytes)
        })
    }

    /// Read an i64 value.
    pub fn read_i64(&mut self) -> CoreResult<i64> {
        Ok(self.read_u64()? as i64)
    }

    /// Read an f32 value.
    pub fn read_f32(&mut self) -> CoreResult<f32> {
        self.align(4)?;
        if self.offset + 4 > self.data.len() {
            return Err(CodecError::buffer_too_short(
                4,
                self.remaining(),
                self.offset as u64,
            ));
        }
        let bytes = [
            self.data[self.offset],
            self.data[self.offset + 1],
            self.data[self.offset + 2],
            self.data[self.offset + 3],
        ];
        self.offset += 4;
        Ok(if self.little_endian {
            f32::from_le_bytes(bytes)
        } else {
            f32::from_be_bytes(bytes)
        })
    }

    /// Read an f64 value.
    pub fn read_f64(&mut self) -> CoreResult<f64> {
        self.align(8)?;
        if self.offset + 8 > self.data.len() {
            return Err(CodecError::buffer_too_short(
                8,
                self.remaining(),
                self.offset as u64,
            ));
        }
        let bytes = [
            self.data[self.offset],
            self.data[self.offset + 1],
            self.data[self.offset + 2],
            self.data[self.offset + 3],
            self.data[self.offset + 4],
            self.data[self.offset + 5],
            self.data[self.offset + 6],
            self.data[self.offset + 7],
        ];
        self.offset += 8;
        Ok(if self.little_endian {
            f64::from_le_bytes(bytes)
        } else {
            f64::from_be_bytes(bytes)
        })
    }

    /// Read a byte slice.
    pub fn read_bytes(&mut self, count: usize) -> CoreResult<&'a [u8]> {
        if count > self.remaining() {
            return Err(CodecError::buffer_too_short(
                count,
                self.remaining(),
                self.offset as u64,
            ));
        }
        let start = self.offset;
        self.offset += count;
        Ok(&self.data[start..self.offset])
    }

    /// Skip bytes.
    pub fn skip(&mut self, count: usize) -> CoreResult<()> {
        if count > self.remaining() {
            return Err(CodecError::buffer_too_short(
                count,
                self.remaining(),
                self.offset as u64,
            ));
        }
        self.offset += count;
        Ok(())
    }

    /// Peek at the next byte without advancing the position.
    pub fn peek(&self) -> Option<u8> {
        if self.offset < self.data.len() {
            Some(self.data[self.offset])
        } else {
            None
        }
    }

    /// Read a f32 value without alignment (for primitive arrays).
    pub fn read_f32_unaligned(&mut self) -> CoreResult<f32> {
        if self.offset + 4 > self.data.len() {
            return Err(CodecError::buffer_too_short(
                4,
                self.remaining(),
                self.offset as u64,
            ));
        }
        let bytes = [
            self.data[self.offset],
            self.data[self.offset + 1],
            self.data[self.offset + 2],
            self.data[self.offset + 3],
        ];
        self.offset += 4;
        Ok(if self.little_endian {
            f32::from_le_bytes(bytes)
        } else {
            f32::from_be_bytes(bytes)
        })
    }

    /// Read a f64 value without alignment (for primitive arrays).
    pub fn read_f64_unaligned(&mut self) -> CoreResult<f64> {
        if self.offset + 8 > self.data.len() {
            return Err(CodecError::buffer_too_short(
                8,
                self.remaining(),
                self.offset as u64,
            ));
        }
        let bytes = [
            self.data[self.offset],
            self.data[self.offset + 1],
            self.data[self.offset + 2],
            self.data[self.offset + 3],
            self.data[self.offset + 4],
            self.data[self.offset + 5],
            self.data[self.offset + 6],
            self.data[self.offset + 7],
        ];
        self.offset += 8;
        Ok(if self.little_endian {
            f64::from_le_bytes(bytes)
        } else {
            f64::from_be_bytes(bytes)
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cursor_new() {
        let data = vec![0x00, 0x01, 0x00, 0x00]; // CDR header (little-endian)
        let cursor = CdrCursor::new(&data).unwrap();
        assert_eq!(cursor.position(), 4);
        assert_eq!(cursor.remaining(), 0);
        assert!(cursor.is_at_end());
    }

    #[test]
    fn test_cursor_too_short() {
        let data = vec![0x00, 0x01]; // Only 2 bytes
        let result = CdrCursor::new(&data);
        assert!(result.is_err());
    }

    #[test]
    fn test_read_u8() {
        let data = vec![0x00, 0x01, 0x00, 0x00, 0x42, 0xFF]; // header + data
        let mut cursor = CdrCursor::new(&data).unwrap();
        assert_eq!(cursor.read_u8().unwrap(), 0x42);
        assert_eq!(cursor.read_u8().unwrap(), 0xFF);
    }

    #[test]
    fn test_read_u32() {
        let mut data = vec![0x00, 0x01, 0x00, 0x00]; // CDR header
        data.extend_from_slice(&42u32.to_le_bytes());
        let mut cursor = CdrCursor::new(&data).unwrap();
        assert_eq!(cursor.read_u32().unwrap(), 42);
    }

    #[test]
    fn test_read_u64() {
        let mut data = vec![0x00, 0x01, 0x00, 0x00]; // CDR header
                                                     // With origin = 4, the first field at position 4 is already aligned to 8
                                                     // because (4 - 4) % 8 = 0, no padding needed
        data.extend_from_slice(&0x123456789ABCDEF0u64.to_le_bytes());
        let mut cursor = CdrCursor::new(&data).unwrap();
        assert_eq!(cursor.read_u64().unwrap(), 0x123456789ABCDEF0);
    }

    #[test]
    fn test_read_f32() {
        let mut data = vec![0x00, 0x01, 0x00, 0x00]; // CDR header
        data.extend_from_slice(&1.0f32.to_le_bytes());
        let mut cursor = CdrCursor::new(&data).unwrap();
        assert!((cursor.read_f32().unwrap() - 1.0).abs() < f32::EPSILON);
    }

    #[test]
    fn test_read_f64() {
        let mut data = vec![0x00, 0x01, 0x00, 0x00]; // CDR header
                                                     // With origin = 4, the first field at position 4 is already aligned to 8
                                                     // because (4 - 4) % 8 = 0, no padding needed
        data.extend_from_slice(&1.0f64.to_le_bytes());
        let mut cursor = CdrCursor::new(&data).unwrap();
        assert!((cursor.read_f64().unwrap() - 1.0).abs() < f64::EPSILON);
    }

    #[test]
    fn test_alignment() {
        // Test alignment calculation: (offset - origin) % size
        let mut data = vec![0x00, 0x01, 0x00, 0x00]; // CDR header, origin = 4
        data.push(0x01); // offset = 5, (5 - 4) % 4 = 1, need 3 bytes padding
                         // Add 3 padding bytes
        data.extend_from_slice(&[0x00, 0x00, 0x00]);
        data.extend_from_slice(&42u32.to_le_bytes()); // value to read after alignment

        let mut cursor = CdrCursor::new(&data).unwrap();
        cursor.read_u8().unwrap(); // offset = 5
        assert_eq!(cursor.position(), 5);

        cursor.align(4).unwrap(); // Should add 3 bytes padding
        assert_eq!(cursor.position(), 8); // 5 + 3 = 8

        cursor.read_u32().unwrap(); // offset = 12
        assert_eq!(cursor.position(), 12);
    }

    #[test]
    fn test_reset_origin() {
        let mut data = vec![0x00, 0x01, 0x00, 0x00]; // CDR header, origin = 4
        data.extend_from_slice(&1u32.to_le_bytes()); // offset = 8
        data.push(0x01); // offset = 9
                         // Add 3 bytes padding for the align(4) call
        data.extend_from_slice(&[0x00, 0x00, 0x00]);

        let mut cursor = CdrCursor::new(&data).unwrap();
        cursor.read_u32().unwrap(); // offset = 8
        cursor.reset_origin(); // origin = 8

        cursor.read_u8().unwrap(); // offset = 9, (9 - 8) % 4 = 1
        cursor.align(4).unwrap(); // Should add 3 bytes padding
        assert_eq!(cursor.position(), 12); // 9 + 3 = 12
    }

    #[test]
    fn test_peek() {
        let data = vec![0x00, 0x01, 0x00, 0x00, 0x42]; // header + data
        let mut cursor = CdrCursor::new(&data).unwrap();
        assert_eq!(cursor.peek(), Some(0x42));
        assert_eq!(cursor.position(), 4); // Position not advanced
        assert_eq!(cursor.read_u8().unwrap(), 0x42);
    }

    #[test]
    fn test_skip() {
        let data = vec![0x00, 0x01, 0x00, 0x00, 0x01, 0x02, 0x03, 0x04]; // header + 4 bytes
        let mut cursor = CdrCursor::new(&data).unwrap();
        cursor.skip(2).unwrap();
        assert_eq!(cursor.position(), 6);
        assert_eq!(cursor.read_u16().unwrap(), 0x0403); // little-endian
    }

    #[test]
    fn test_little_endian_detection() {
        // Test little-endian (byte 1 = 1)
        let data_le = vec![0x00, 0x01, 0x00, 0x00, 0x01, 0x00]; // header + 0x0001 LE
        let mut cursor_le = CdrCursor::new(&data_le).unwrap();
        assert_eq!(cursor_le.read_u16().unwrap(), 0x0001);

        // Test big-endian (byte 1 = 0)
        let data_be = vec![0x00, 0x00, 0x00, 0x00, 0x00, 0x01]; // header + 0x0001 BE
        let mut cursor_be = CdrCursor::new(&data_be).unwrap();
        assert_eq!(cursor_be.read_u16().unwrap(), 0x0001);
    }

    // Comprehensive cursor tests

    #[test]
    fn test_read_i8() {
        let data = vec![0x00, 0x01, 0x00, 0x00, 0xFF, 0x7F]; // header + data
        let mut cursor = CdrCursor::new(&data).unwrap();
        assert_eq!(cursor.read_i8().unwrap(), -1);
        assert_eq!(cursor.read_i8().unwrap(), 127);
    }

    #[test]
    fn test_read_i16_min_max() {
        let mut data = vec![0x00, 0x01, 0x00, 0x00];
        // i16::MIN = -32768 = 0x8000
        data.extend_from_slice(&[0x00, 0x80]);
        // i16::MAX = 32767 = 0x7FFF
        data.extend_from_slice(&[0xFF, 0x7F]);

        let mut cursor = CdrCursor::new(&data).unwrap();
        assert_eq!(cursor.read_i16().unwrap(), i16::MIN);
        assert_eq!(cursor.read_i16().unwrap(), i16::MAX);
    }

    #[test]
    fn test_read_i32_min_max() {
        let mut data = vec![0x00, 0x01, 0x00, 0x00];
        data.extend_from_slice(&i32::MIN.to_le_bytes());
        data.extend_from_slice(&i32::MAX.to_le_bytes());

        let mut cursor = CdrCursor::new(&data).unwrap();
        assert_eq!(cursor.read_i32().unwrap(), i32::MIN);
        assert_eq!(cursor.read_i32().unwrap(), i32::MAX);
    }

    #[test]
    fn test_read_i64_min_max() {
        let mut data = vec![0x00, 0x01, 0x00, 0x00];
        // With origin = 4, the first field at position 4 is already aligned to 8
        // No padding needed for the first i64
        data.extend_from_slice(&i64::MIN.to_le_bytes());
        data.extend_from_slice(&i64::MAX.to_le_bytes());

        let mut cursor = CdrCursor::new(&data).unwrap();
        assert_eq!(cursor.read_i64().unwrap(), i64::MIN);
        assert_eq!(cursor.read_i64().unwrap(), i64::MAX);
    }

    #[test]
    fn test_read_bytes() {
        let data = vec![0x00, 0x01, 0x00, 0x00, 0x01, 0x02, 0x03, 0x04];
        let mut cursor = CdrCursor::new(&data).unwrap();
        let bytes = cursor.read_bytes(4).unwrap();
        assert_eq!(bytes, &[0x01, 0x02, 0x03, 0x04]);
    }

    #[test]
    fn test_read_bytes_empty() {
        let data = vec![0x00, 0x01, 0x00, 0x00];
        let mut cursor = CdrCursor::new(&data).unwrap();
        let bytes = cursor.read_bytes(0).unwrap();
        assert!(bytes.is_empty());
    }

    #[test]
    fn test_read_bytes_too_short() {
        let data = vec![0x00, 0x01, 0x00, 0x00, 0x01, 0x02];
        let mut cursor = CdrCursor::new(&data).unwrap();
        let result = cursor.read_bytes(10);
        assert!(result.is_err());
    }

    #[test]
    fn test_skip_partial() {
        let data = vec![0x00, 0x01, 0x00, 0x00, 0x01, 0x02, 0x03, 0x04];
        let mut cursor = CdrCursor::new(&data).unwrap();
        cursor.skip(3).unwrap();
        assert_eq!(cursor.position(), 7);
        assert_eq!(cursor.read_u8().unwrap(), 0x04);
    }

    #[test]
    fn test_skip_too_far() {
        let data = vec![0x00, 0x01, 0x00, 0x00, 0x01];
        let mut cursor = CdrCursor::new(&data).unwrap();
        let result = cursor.skip(10);
        assert!(result.is_err());
    }

    #[test]
    fn test_remaining() {
        let data = vec![0x00, 0x01, 0x00, 0x00, 0x01, 0x02, 0x03];
        let mut cursor = CdrCursor::new(&data).unwrap();
        assert_eq!(cursor.remaining(), 3);
        cursor.read_u8().unwrap();
        assert_eq!(cursor.remaining(), 2);
    }

    #[test]
    fn test_is_at_end() {
        let data = vec![0x00, 0x01, 0x00, 0x00, 0x01];
        let mut cursor = CdrCursor::new(&data).unwrap();
        assert!(!cursor.is_at_end());
        cursor.read_u8().unwrap();
        assert!(cursor.is_at_end());
    }

    #[test]
    fn test_align_to_1() {
        let data = vec![0x00, 0x01, 0x00, 0x00, 0x01];
        let mut cursor = CdrCursor::new(&data).unwrap();
        cursor.align(1).unwrap(); // Should not add any padding
        assert_eq!(cursor.position(), 4);
    }

    #[test]
    fn test_align_to_2() {
        let data = vec![0x00, 0x01, 0x00, 0x00, 0x01, 0x00, 0x02, 0x00];
        let mut cursor = CdrCursor::new(&data).unwrap();
        cursor.read_u8().unwrap(); // offset = 5
        cursor.align(2).unwrap(); // Should add 1 byte padding
        assert_eq!(cursor.position(), 6);
        assert_eq!(cursor.read_u16().unwrap(), 0x0002);
    }

    #[test]
    fn test_align_to_8_already_aligned() {
        // With origin = 4, position 4 is already 8-byte aligned: (4-4) % 8 = 0
        let mut data = vec![0x00, 0x01, 0x00, 0x00];
        data.extend_from_slice(&0x12345678u32.to_le_bytes());
        let mut cursor = CdrCursor::new(&data).unwrap();
        cursor.align(8).unwrap(); // Already aligned, no padding needed
        assert_eq!(cursor.position(), 4);
    }

    #[test]
    fn test_align_no_padding_needed() {
        let mut data = vec![0x00, 0x01, 0x00, 0x00];
        data.extend_from_slice(&0x12345678u32.to_le_bytes());
        let mut cursor = CdrCursor::new(&data).unwrap();
        cursor.align(4).unwrap(); // offset = 4, already aligned
        assert_eq!(cursor.position(), 4);
        assert_eq!(cursor.read_u32().unwrap(), 0x12345678);
    }

    #[test]
    fn test_align_buffer_too_short() {
        let data = vec![0x00, 0x01, 0x00, 0x00, 0x01];
        let mut cursor = CdrCursor::new(&data).unwrap();
        cursor.read_u8().unwrap(); // offset = 5
        let result = cursor.align(4); // Need 3 more bytes but only 1 available
        assert!(result.is_err());
    }

    #[test]
    fn test_read_u8_buffer_too_short() {
        let data = vec![0x00, 0x01, 0x00, 0x00];
        let mut cursor = CdrCursor::new(&data).unwrap();
        let result = cursor.read_u8();
        assert!(result.is_err());
    }

    #[test]
    fn test_read_u16_buffer_too_short() {
        let data = vec![0x00, 0x01, 0x00, 0x00, 0x01];
        let mut cursor = CdrCursor::new(&data).unwrap();
        let result = cursor.read_u16();
        assert!(result.is_err());
    }

    #[test]
    fn test_read_u32_buffer_too_short() {
        let data = vec![0x00, 0x01, 0x00, 0x00, 0x01, 0x02];
        let mut cursor = CdrCursor::new(&data).unwrap();
        let result = cursor.read_u32();
        assert!(result.is_err());
    }

    #[test]
    fn test_read_u64_buffer_too_short() {
        // With origin = 4, position 4 is already 8-byte aligned: (4 - 4) % 8 = 0
        // Only 4 bytes available after header, need 8
        let data = vec![0x00, 0x01, 0x00, 0x00, 0x01, 0x02, 0x03, 0x04];
        let mut cursor = CdrCursor::new(&data).unwrap();
        let result = cursor.read_u64();
        assert!(result.is_err());
    }

    #[test]
    fn test_read_f32_buffer_too_short() {
        let data = vec![0x00, 0x01, 0x00, 0x00, 0x01, 0x02];
        let mut cursor = CdrCursor::new(&data).unwrap();
        let result = cursor.read_f32();
        assert!(result.is_err());
    }

    #[test]
    fn test_read_f64_buffer_too_short() {
        let data = vec![0x00, 0x01, 0x00, 0x00, 0x01, 0x02];
        let mut cursor = CdrCursor::new(&data).unwrap();
        let result = cursor.read_f64();
        assert!(result.is_err());
    }

    #[test]
    fn test_big_endian_read_u32() {
        let data = vec![0x00, 0x00, 0x00, 0x00, 0x12, 0x34, 0x56, 0x78];
        let mut cursor = CdrCursor::new(&data).unwrap();
        assert_eq!(cursor.read_u32().unwrap(), 0x12345678);
    }

    #[test]
    fn test_big_endian_read_u64() {
        // With origin = 4, position 4 is already 8-byte aligned: (4 - 4) % 8 = 0
        let data = vec![
            0x00, 0x00, 0x00, 0x00, 0x12, 0x34, 0x56, 0x78, 0x9A, 0xBC, 0xDE, 0xF0,
        ];
        let mut cursor = CdrCursor::new(&data).unwrap();
        assert_eq!(cursor.read_u64().unwrap(), 0x123456789ABCDEF0);
    }

    #[test]
    fn test_multiple_resets() {
        let mut data = vec![0x00, 0x01, 0x00, 0x00];
        data.extend_from_slice(&1u32.to_le_bytes());
        data.extend_from_slice(&2u32.to_le_bytes());
        data.extend_from_slice(&3u32.to_le_bytes());

        let mut cursor = CdrCursor::new(&data).unwrap();
        cursor.read_u32().unwrap(); // offset = 8

        cursor.reset_origin(); // origin = 8
        cursor.read_u32().unwrap(); // offset = 12
        assert_eq!(cursor.position(), 12);

        cursor.reset_origin(); // origin = 12
        cursor.read_u32().unwrap(); // offset = 16
        assert_eq!(cursor.position(), 16);
    }

    #[test]
    fn test_read_all_types() {
        let mut data = vec![0x00, 0x01, 0x00, 0x00]; // header (little-endian)
        data.push(0x01); // u8 (offset = 5)
        data.extend_from_slice(&[0x00]); // padding to align u16 to 2 (offset = 6)
        data.extend_from_slice(&[0x02, 0x03]); // u16, little-endian 0x0302 (offset = 8)
        data.extend_from_slice(&[0x04, 0x05, 0x06, 0x07]); // u32, little-endian 0x07060504 (offset = 12)
                                                           // With origin = 4, offset 12 is already 8-byte aligned: (12 - 4) % 8 = 0
        data.extend_from_slice(&[0x08, 0x09, 0x0A, 0x0B, 0x0C, 0x0D, 0x0E, 0x0F]); // u64 (offset = 20)
        data.extend_from_slice(&1.5f64.to_le_bytes()); // f64 (offset = 28, already 8-byte aligned: (28-4) % 8 = 0)

        let mut cursor = CdrCursor::new(&data).unwrap();
        assert_eq!(cursor.read_u8().unwrap(), 0x01);
        assert_eq!(cursor.read_u16().unwrap(), 0x0302);
        assert_eq!(cursor.read_u32().unwrap(), 0x07060504);
        assert_eq!(cursor.read_u64().unwrap(), 0x0F0E0D0C0B0A0908);
        assert!((cursor.read_f64().unwrap() - 1.5).abs() < f64::EPSILON);
    }

    #[test]
    fn test_cursor_header_validation() {
        // Exactly 4 bytes - should work
        let data = vec![0x00, 0x01, 0x00, 0x00];
        assert!(CdrCursor::new(&data).is_ok());

        // Less than 4 bytes - should fail
        let data = vec![0x00, 0x01, 0x00];
        assert!(CdrCursor::new(&data).is_err());

        // Empty data - should fail
        let data: Vec<u8> = vec![];
        assert!(CdrCursor::new(&data).is_err());
    }
}
