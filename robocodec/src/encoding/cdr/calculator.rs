// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! CDR size calculator for computing the size of CDR-encoded data.
//!
//! Based on the TypeScript implementation at:
//! https://github.com/emulated-devices/rtps-cdr/blob/main/src/CdrSizeCalculator.ts

/// CDR size calculator.
///
/// This calculator computes the size of CDR-encoded data before actually
/// encoding it. This is useful for pre-allocating buffers.
///
/// # Example
///
/// ```no_run
/// # fn main() {
/// use robocodec::encoding::cdr::calculator::CdrCalculator;
///
/// let mut calc = CdrCalculator::new();
/// calc.int32();    // 4 bytes
/// calc.int32();    // 4 bytes
/// calc.string(5);  // 4 (length) + 5 + 1 (null) = 10 bytes
/// assert_eq!(calc.size(), 18);
/// # }
/// ```
#[derive(Debug, Clone)]
pub struct CdrCalculator {
    /// Current size offset (starts at 4 after CDR header)
    offset: usize,
    /// Origin offset for alignment calculation (4 for proper field alignment after CDR header)
    origin: usize,
}

impl Default for CdrCalculator {
    fn default() -> Self {
        Self::new()
    }
}

impl CdrCalculator {
    /// Create a new calculator.
    ///
    /// The offset starts at 4, representing the size of the CDR header.
    /// The origin starts at 4 (after CDR header) for proper field alignment.
    #[must_use]
    pub fn new() -> Self {
        Self {
            offset: 4,
            origin: 4,
        }
    }

    /// Get the current calculated size.
    #[must_use]
    pub const fn size(&self) -> usize {
        self.offset
    }

    /// Calculate size for an 8-bit signed/unsigned integer.
    pub fn int8(&mut self) -> usize {
        self.increment_and_return(1)
    }

    /// Calculate size for an 8-bit signed/unsigned integer.
    pub fn uint8(&mut self) -> usize {
        self.int8()
    }

    /// Calculate size for a 16-bit signed/unsigned integer.
    pub fn int16(&mut self) -> usize {
        self.increment_and_return(2)
    }

    /// Calculate size for a 16-bit signed/unsigned integer.
    pub fn uint16(&mut self) -> usize {
        self.int16()
    }

    /// Calculate size for a 32-bit signed/unsigned integer.
    pub fn int32(&mut self) -> usize {
        self.increment_and_return(4)
    }

    /// Calculate size for a 32-bit signed/unsigned integer.
    pub fn uint32(&mut self) -> usize {
        self.int32()
    }

    /// Calculate size for a 64-bit signed/unsigned integer.
    pub fn int64(&mut self) -> usize {
        self.increment_and_return(8)
    }

    /// Calculate size for a 64-bit signed/unsigned integer.
    pub fn uint64(&mut self) -> usize {
        self.int64()
    }

    /// Calculate size for a 32-bit float.
    pub fn float32(&mut self) -> usize {
        self.increment_and_return(4)
    }

    /// Calculate size for a 64-bit double.
    pub fn float64(&mut self) -> usize {
        self.increment_and_return(8)
    }

    /// Calculate size for a string.
    ///
    /// # Arguments
    ///
    /// * `length` - The length of the string content (not including null terminator)
    pub fn string(&mut self, length: usize) -> usize {
        self.uint32();
        self.offset += length + 1; // Add one for the null terminator
        self.offset
    }

    /// Calculate size for a sequence length prefix.
    pub fn sequence_length(&mut self) -> usize {
        self.uint32()
    }

    /// Calculate size for an array of fixed-size elements.
    ///
    /// # Arguments
    ///
    /// * `count` - Number of elements in the array
    /// * `element_size` - Size of each element in bytes
    /// * `element_alignment` - Alignment requirement of each element
    pub fn array(&mut self, count: usize, element_size: usize, element_alignment: usize) -> usize {
        self.sequence_length();
        for _ in 0..count {
            self.align(element_alignment);
            self.offset += element_size;
        }
        self.offset
    }

    /// Add padding for alignment.
    ///
    /// # Arguments
    ///
    /// * `byte_count` - The byte width to align to (e.g., 4 for 4-byte alignment)
    pub fn align(&mut self, byte_count: usize) {
        let alignment = (self.offset - self.origin) % byte_count;
        if alignment > 0 {
            self.offset += byte_count - alignment;
        }
    }

    /// Reset the calculator to its initial state.
    pub fn reset(&mut self) {
        self.offset = 4;
        self.origin = 4;
    }

    /// Reset the origin to the current offset (for nested structs).
    ///
    /// This matches the TypeScript `resetOrigin()` function.
    pub fn reset_origin(&mut self) {
        self.origin = self.offset;
    }

    /// Increment the offset by `byte_count` and any required padding bytes,
    /// then return the new offset.
    ///
    /// This matches the TypeScript `incrementAndReturn` method.
    fn increment_and_return(&mut self, byte_count: usize) -> usize {
        self.align(byte_count);
        self.offset += byte_count;
        self.offset
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_calculator_new() {
        let calc = CdrCalculator::new();
        assert_eq!(calc.size(), 4); // Starts at 4 (CDR header)
    }

    #[test]
    fn test_calculator_int8() {
        let mut calc = CdrCalculator::new();
        calc.int8();
        assert_eq!(calc.size(), 5);
        calc.uint8();
        assert_eq!(calc.size(), 6);
    }

    #[test]
    fn test_calculator_int16() {
        let mut calc = CdrCalculator::new();
        calc.int16();
        assert_eq!(calc.size(), 6); // 4 + 2
    }

    #[test]
    fn test_calculator_int32() {
        let mut calc = CdrCalculator::new();
        calc.int32();
        assert_eq!(calc.size(), 8); // 4 + 4
    }

    #[test]
    fn test_calculator_int64() {
        let mut calc = CdrCalculator::new();
        calc.int64();
        // With origin = 4, position 4 is already 8-byte aligned: (4 - 4) % 8 = 0
        assert_eq!(calc.size(), 12); // 4 (header) + 8 (int64)
    }

    #[test]
    fn test_calculator_float32() {
        let mut calc = CdrCalculator::new();
        calc.float32();
        assert_eq!(calc.size(), 8); // 4 + 4
    }

    #[test]
    fn test_calculator_float64() {
        let mut calc = CdrCalculator::new();
        calc.float64();
        // With origin = 4, position 4 is already 8-byte aligned: (4 - 4) % 8 = 0
        assert_eq!(calc.size(), 12); // 4 (header) + 8 (float64)
    }

    #[test]
    fn test_calculator_string() {
        let mut calc = CdrCalculator::new();
        calc.string(5); // "hello" + null
                        // 4 (header) + 4 (length) + 5 + 1 (null) = 14
        assert_eq!(calc.size(), 14);
    }

    #[test]
    fn test_calculator_string_empty() {
        let mut calc = CdrCalculator::new();
        calc.string(0);
        // 4 (header) + 4 (length) + 0 + 1 (null) = 9
        assert_eq!(calc.size(), 9);
    }

    #[test]
    fn test_calculator_sequence_length() {
        let mut calc = CdrCalculator::new();
        calc.sequence_length();
        assert_eq!(calc.size(), 8); // 4 + 4
    }

    #[test]
    fn test_calculator_alignment() {
        let mut calc = CdrCalculator::new();
        calc.int8(); // offset = 5, (5-4) % 4 = 1
        calc.align(4); // Should add 3 bytes
        assert_eq!(calc.size(), 8);
    }

    #[test]
    fn test_calculator_multiple_int32() {
        let mut calc = CdrCalculator::new();
        calc.int32(); // offset = 8
        calc.int32(); // offset = 12
        calc.int32(); // offset = 16
        assert_eq!(calc.size(), 16);
    }

    #[test]
    fn test_calculator_int8_then_int32() {
        let mut calc = CdrCalculator::new();
        calc.int8(); // offset = 5
        calc.int32(); // Should align to 8, then add 4
        assert_eq!(calc.size(), 12);
    }

    #[test]
    fn test_calculator_array() {
        let mut calc = CdrCalculator::new();
        calc.array(3, 4, 4); // 3 int32s
                             // 4 (header) + 4 (length) + 3*4 (elements) = 20
        assert_eq!(calc.size(), 20);
    }

    #[test]
    fn test_calculator_complex_message() {
        let mut calc = CdrCalculator::new();
        // Simulate: std_msgs/Header
        // uint32 sec
        calc.uint32();
        // uint32 nsec
        calc.uint32();
        // string frame_id
        calc.string(9); // "base_link"

        // 4 (header) + 4 (sec) + 4 (nsec) + 4 (len) + 9 + 1 (null)
        // But string needs alignment after the uint32s...
        // After nsec: offset = 12, (12-4) % 4 = 0, no padding
        // After string length: offset = 16, (16-4) % 4 = 0, no padding
        // Total: 4 + 4 + 4 + 4 + 9 + 1 = 26

        assert_eq!(calc.size(), 26);
    }

    #[test]
    fn test_calculator_reset() {
        let mut calc = CdrCalculator::new();
        calc.int32();
        calc.int32();
        assert_eq!(calc.size(), 12);
        calc.reset();
        assert_eq!(calc.size(), 4);
    }

    #[test]
    fn test_calculator_align_no_padding_needed() {
        let mut calc = CdrCalculator::new();
        calc.int32(); // offset = 8, (8-4) % 4 = 0
        calc.align(4); // No padding needed
        assert_eq!(calc.size(), 8);
    }

    #[test]
    fn test_calculator_align_with_padding() {
        let mut calc = CdrCalculator::new();
        calc.int8(); // offset = 5, (5-4) % 4 = 1
        calc.align(4); // 3 bytes padding
        assert_eq!(calc.size(), 8);
    }

    #[test]
    fn test_calculator_8_byte_alignment() {
        let mut calc = CdrCalculator::new();
        calc.int32(); // offset = 8, (8-4) % 8 = 4, needs padding
        calc.align(8); // Adds 4 bytes padding
        assert_eq!(calc.size(), 12);
    }

    // Comprehensive calculator tests

    #[test]
    fn test_calculator_uint16() {
        let mut calc = CdrCalculator::new();
        calc.uint16();
        assert_eq!(calc.size(), 6); // 4 + 2
    }

    #[test]
    fn test_calculator_uint64() {
        let mut calc = CdrCalculator::new();
        calc.uint64();
        // With origin = 4, position 4 is already 8-byte aligned: (4 - 4) % 8 = 0
        assert_eq!(calc.size(), 12); // 4 + 8
    }

    #[test]
    fn test_calculator_multiple_types() {
        let mut calc = CdrCalculator::new();
        calc.int8(); // offset = 5
        calc.int16(); // align to 2: (5-4) % 2 = 1, +1 pad, +2 = 8
        calc.int32(); // align to 4: (8-4) % 4 = 0, +4 = 12
        calc.int64(); // align to 8: (12-4) % 8 = 0, +8 = 20
        assert_eq!(calc.size(), 20);
    }

    #[test]
    fn test_calculator_string_alignment() {
        let mut calc = CdrCalculator::new();
        calc.int8(); // offset = 5
        calc.string(5); // align to 4, offset = 5 -> 8, + 4 + 5 + 1 = 18
        assert_eq!(calc.size(), 18);
    }

    #[test]
    fn test_calculator_string_after_string() {
        let mut calc = CdrCalculator::new();
        calc.string(5); // offset = 4, + 4 + 5 + 1 = 14
        calc.string(10); // offset = 14, align to 4, offset = 14 -> 16, + 4 + 10 + 1 = 31
        assert_eq!(calc.size(), 31);
    }

    #[test]
    fn test_calculator_array_single_element() {
        let mut calc = CdrCalculator::new();
        calc.array(1, 4, 4); // 1 int32
        assert_eq!(calc.size(), 12); // 4 + 4 (length) + 4 (data)
    }

    #[test]
    fn test_calculator_array_multiple_elements() {
        let mut calc = CdrCalculator::new();
        calc.array(5, 4, 4); // 5 int32s
        assert_eq!(calc.size(), 28); // 4 + 4 + 5*4 = 28
    }

    #[test]
    fn test_calculator_array_with_alignment() {
        let mut calc = CdrCalculator::new();
        calc.int8(); // offset = 5, origin = 4
        calc.array(2, 8, 8); // 2 int64s
                             // After int8: offset = 5, origin = 4
                             // sequence_length (align to 4): (5-4)%4=1, +3 pad -> offset = 8, +4 = 12
                             // First int64: align to 8, (12-4)%8=0, no pad, +8 = 20
                             // Second int64: align to 8, (20-4)%8=0, +8 = 28
        assert_eq!(calc.size(), 28);
    }

    #[test]
    fn test_calculator_float64_after_int8() {
        let mut calc = CdrCalculator::new();
        calc.int8(); // offset = 5, origin = 4
        calc.float64(); // align to 8, (5-4)%8=1, +7 pad -> offset = 12, +8 = 20
        assert_eq!(calc.size(), 20);
    }

    #[test]
    fn test_calculator_reset_origin() {
        let mut calc = CdrCalculator::new();
        calc.int32(); // offset = 8
        calc.reset_origin(); // origin = 8
        calc.int8(); // offset = 9, (9-8)%4=1
        calc.align(4); // + 3 padding, offset = 12
        assert_eq!(calc.size(), 12);
    }

    #[test]
    fn test_calculator_complex_nested() {
        let mut calc = CdrCalculator::new();
        // Outer struct
        calc.int32(); // offset = 8
        calc.reset_origin(); // origin = 8, enter nested struct
        calc.int32(); // offset = 12
        calc.float64(); // (12-8)%8=4, + 4 padding, offset = 16, + 8 = 24
        assert_eq!(calc.size(), 24);
    }

    #[test]
    fn test_calculator_empty_array() {
        let mut calc = CdrCalculator::new();
        calc.array(0, 4, 4);
        assert_eq!(calc.size(), 8); // 4 + 4 (length) + 0 (data)
    }

    #[test]
    fn test_calculator_multiple_resets() {
        let mut calc = CdrCalculator::new();
        calc.int32(); // offset = 8
        calc.reset_origin(); // origin = 8
        calc.int32(); // offset = 12
        calc.reset_origin(); // origin = 12
        calc.int32(); // offset = 16
        assert_eq!(calc.size(), 16);
    }

    #[test]
    fn test_calculator_alignment_boundary() {
        let mut calc = CdrCalculator::new();
        calc.uint8(); // offset = 5
        calc.uint8(); // offset = 6
        calc.uint8(); // offset = 7
        calc.uint8(); // offset = 8
        calc.uint32(); // offset = 8, already aligned, + 4 = 12
        assert_eq!(calc.size(), 12);
    }

    #[test]
    fn test_calculator_int8_then_float64() {
        let mut calc = CdrCalculator::new();
        calc.int8(); // offset = 5, origin = 4
        calc.float64(); // align to 8, (5-4)%8=1, +7 pad -> offset = 12, +8 = 20
        assert_eq!(calc.size(), 20);
    }

    #[test]
    fn test_calculator_multiple_float64() {
        let mut calc = CdrCalculator::new();
        calc.float64(); // offset = 4, origin = 4, (4-4)%8=0, no pad, +8 = 12
        calc.float64(); // offset = 12, origin = 4, (12-4)%8=0, +8 = 20
        calc.float64(); // offset = 20, origin = 4, (20-4)%8=0, +8 = 28
        assert_eq!(calc.size(), 28);
    }

    #[test]
    fn test_calculator_string_then_int64() {
        let mut calc = CdrCalculator::new();
        calc.string(4); // offset = 4, +4(len)+4(data)+1(null) = 13, origin = 4
        calc.int64(); // align to 8, (13-4)%8=1, +7 pad -> offset = 20, +8 = 28
        assert_eq!(calc.size(), 28);
    }

    #[test]
    fn test_calculator_matches_encoder_int32() {
        let mut calc = CdrCalculator::new();
        calc.int32();

        let mut encoder = crate::encoding::CdrEncoder::new();
        encoder.int32(42).unwrap();
        assert_eq!(calc.size(), encoder.size());
    }

    #[test]
    fn test_calculator_matches_encoder_float64() {
        let mut calc = CdrCalculator::new();
        calc.float64();

        let mut encoder = crate::encoding::CdrEncoder::new();
        encoder.float64(1.0).unwrap();
        assert_eq!(calc.size(), encoder.size());
    }

    #[test]
    fn test_calculator_matches_encoder_string() {
        let mut calc = CdrCalculator::new();
        calc.string(5);

        let mut encoder = crate::encoding::CdrEncoder::new();
        encoder.string("hello").unwrap();
        assert_eq!(calc.size(), encoder.size());
    }
}
