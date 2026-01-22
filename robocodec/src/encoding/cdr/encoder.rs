// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! CDR encoder for writing CDR-encoded data.
//!
//! Based on the TypeScript implementation at:
//! https://github.com/emulated-devices/rtps-cdr/blob/main/src/CdrWriter.ts

use super::{calculator::CdrCalculator, CDR_HEADER_SIZE};
use crate::core::Result as CoreResult;
use crate::core::{CodecValue, DecodedMessage};
use crate::schema::{FieldType, MessageSchema, PrimitiveType as IdlPrimitiveType};

/// Default initial capacity for the encoder buffer.
const DEFAULT_CAPACITY: usize = 16;

/// Nanoseconds per second for time/duration conversion.
const NANOS_PER_SEC: i64 = 1_000_000_000;

/// CDR encapsulation kind.
///
/// Defines the encoding format and endianness of the CDR data.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
#[repr(u8)]
pub enum EncapsulationKind {
    /// CDR, Little Endian
    #[default]
    CdrLe = 0x01,
    /// CDR, Big Endian
    CdrBe = 0x00,
    /// CDR2, Little Endian
    Cdr2Le = 0x02,
    /// CDR2, Big Endian
    Cdr2Be = 0x03,
    /// PL CDR, Little Endian
    PlCdrLe = 0x04,
    /// PL CDR, Big Endian
    PlCdrBe = 0x05,
    /// PL CDR2, Little Endian
    PlCdr2Le = 0x06,
    /// PL CDR2, Big Endian
    PlCdr2Be = 0x07,
    /// Delimited CDR2, Little Endian
    DelimitedCdr2Le = 0x08,
    /// Delimited CDR2, Big Endian
    DelimitedCdr2Be = 0x09,
}

impl EncapsulationKind {
    /// Check if this encapsulation uses CDR2 encoding rules.
    #[must_use]
    pub const fn is_cdr2(self) -> bool {
        matches!(
            self,
            Self::Cdr2Le
                | Self::Cdr2Be
                | Self::PlCdr2Le
                | Self::PlCdr2Be
                | Self::DelimitedCdr2Le
                | Self::DelimitedCdr2Be
        )
    }

    /// Check if this encapsulation uses little endian byte order.
    #[must_use]
    pub const fn is_little_endian(self) -> bool {
        matches!(
            self,
            Self::CdrLe | Self::Cdr2Le | Self::PlCdrLe | Self::PlCdr2Le | Self::DelimitedCdr2Le
        )
    }

    /// Get the 8-byte alignment for this encapsulation.
    /// CDR1 uses 8-byte alignment for 64-bit values, CDR2 uses 4-byte alignment.
    #[must_use]
    pub const fn eight_byte_alignment(self) -> usize {
        if self.is_cdr2() {
            4
        } else {
            8
        }
    }
}

/// CDR encoder for writing CDR-encoded data.
///
/// This encoder handles all the complexity of CDR encoding including:
/// - Proper alignment (relative to origin for nested structs)
/// - Endianness handling
/// - CDR1 vs CDR2 encoding differences
///
/// # Example
///
/// ```no_run
/// # fn main() {
/// use robocodec::encoding::cdr::encoder::CdrEncoder;
///
/// let mut encoder = CdrEncoder::new();
/// encoder.int32(42);
/// encoder.string("hello");
/// let data = encoder.finish();
/// # }
/// ```
pub struct CdrEncoder {
    /// Output buffer
    buffer: Vec<u8>,
    /// Current write position
    offset: usize,
    /// Origin offset for alignment calculation
    origin: usize,
    /// Encapsulation kind
    kind: EncapsulationKind,
    /// Whether to use little endian encoding
    little_endian: bool,
    /// 8-byte alignment (4 for CDR2, 8 for CDR1)
    eight_byte_alignment: usize,
}

impl Default for CdrEncoder {
    fn default() -> Self {
        Self::new()
    }
}

impl CdrEncoder {
    /// Create a new encoder with default settings (CDR, little-endian).
    #[must_use]
    pub fn new() -> Self {
        Self::with_kind(EncapsulationKind::default())
    }

    /// Create a new encoder with the specified encapsulation kind.
    #[must_use]
    pub fn with_kind(kind: EncapsulationKind) -> Self {
        let little_endian = kind.is_little_endian();
        let eight_byte_alignment = kind.eight_byte_alignment();

        let mut buffer = Vec::with_capacity(DEFAULT_CAPACITY);
        // Write CDR header
        buffer.push(0); // Unused
        buffer.push(kind as u8); // Encapsulation kind
        buffer.push(0); // Options (unused)
        buffer.push(0); // Options (unused)

        Self {
            buffer,
            offset: CDR_HEADER_SIZE,
            origin: CDR_HEADER_SIZE, // Align relative to position after CDR header (matches cursor behavior)
            kind,
            little_endian,
            eight_byte_alignment,
        }
    }

    /// Create a new encoder with the specified initial capacity.
    #[must_use]
    pub fn with_capacity(capacity: usize) -> Self {
        let mut encoder = Self::new();
        encoder
            .buffer
            .reserve(capacity.saturating_sub(DEFAULT_CAPACITY));
        encoder
    }

    /// Get the encapsulation kind.
    #[must_use]
    pub const fn kind(&self) -> EncapsulationKind {
        self.kind
    }

    /// Get the current size of the encoded data.
    #[must_use]
    pub const fn size(&self) -> usize {
        self.offset
    }

    /// Get a reference to the encoded data.
    #[must_use]
    pub fn data(&self) -> &[u8] {
        &self.buffer[..self.offset]
    }

    /// Consume the encoder and return the encoded data.
    #[must_use]
    pub fn finish(self) -> Vec<u8> {
        self.buffer
    }

    /// Reset the encoder to write a new message.
    ///
    /// Keeps the allocated buffer but resets the position.
    pub fn reset(&mut self) {
        self.offset = CDR_HEADER_SIZE;
        self.origin = CDR_HEADER_SIZE; // Align relative to position after CDR header (matches cursor behavior)
    }

    /// Reset the origin to the current offset (for nested structs).
    ///
    /// This matches the TypeScript `resetOrigin()` function.
    pub fn reset_origin(&mut self) {
        self.origin = self.offset;
    }

    /// Ensure there's enough capacity for additional bytes.
    fn reserve(&mut self, additional: usize) {
        let needed = self.offset + additional;
        if needed > self.buffer.len() {
            self.buffer.resize(needed.max(self.buffer.len() * 2), 0);
        }
    }

    /// Align to the specified boundary, writing padding bytes.
    ///
    /// # Arguments
    ///
    /// * `size` - The alignment boundary (e.g., 4 for 4-byte alignment)
    /// * `bytes_to_write` - Optional hint for how many bytes will be written after alignment
    fn align(&mut self, size: usize, bytes_to_write: usize) {
        let alignment = (self.offset - self.origin) % size;
        if alignment > 0 {
            let padding = size - alignment;
            self.reserve(padding + bytes_to_write);
            // Write zero padding bytes
            for _ in 0..padding {
                self.buffer[self.offset] = 0;
                self.offset += 1;
            }
        } else {
            self.reserve(bytes_to_write);
        }
    }

    /// Write an 8-bit signed integer.
    pub fn int8(&mut self, value: i8) -> CoreResult<&mut Self> {
        self.reserve(1);
        self.buffer[self.offset] = value as u8;
        self.offset += 1;
        Ok(self)
    }

    /// Write an 8-bit unsigned integer.
    pub fn uint8(&mut self, value: u8) -> CoreResult<&mut Self> {
        self.reserve(1);
        self.buffer[self.offset] = value;
        self.offset += 1;
        Ok(self)
    }

    /// Write a 16-bit signed integer.
    pub fn int16(&mut self, value: i16) -> CoreResult<&mut Self> {
        self.align(2, 2);
        let bytes = if self.little_endian {
            value.to_le_bytes()
        } else {
            value.to_be_bytes()
        };
        self.write_bytes(&bytes, 2);
        Ok(self)
    }

    /// Write a 16-bit unsigned integer.
    pub fn uint16(&mut self, value: u16) -> CoreResult<&mut Self> {
        self.align(2, 2);
        let bytes = if self.little_endian {
            value.to_le_bytes()
        } else {
            value.to_be_bytes()
        };
        self.write_bytes(&bytes, 2);
        Ok(self)
    }

    /// Write a 32-bit signed integer.
    pub fn int32(&mut self, value: i32) -> CoreResult<&mut Self> {
        self.align(4, 4);
        let bytes = if self.little_endian {
            value.to_le_bytes()
        } else {
            value.to_be_bytes()
        };
        self.write_bytes(&bytes, 4);
        Ok(self)
    }

    /// Write a 32-bit unsigned integer.
    pub fn uint32(&mut self, value: u32) -> CoreResult<&mut Self> {
        self.align(4, 4);
        let bytes = if self.little_endian {
            value.to_le_bytes()
        } else {
            value.to_be_bytes()
        };
        self.write_bytes(&bytes, 4);
        Ok(self)
    }

    /// Write a 64-bit signed integer.
    pub fn int64(&mut self, value: i64) -> CoreResult<&mut Self> {
        self.align(self.eight_byte_alignment, 8);
        let bytes = if self.little_endian {
            value.to_le_bytes()
        } else {
            value.to_be_bytes()
        };
        self.write_bytes(&bytes, 8);
        Ok(self)
    }

    /// Write a 64-bit unsigned integer.
    pub fn uint64(&mut self, value: u64) -> CoreResult<&mut Self> {
        self.align(self.eight_byte_alignment, 8);
        let bytes = if self.little_endian {
            value.to_le_bytes()
        } else {
            value.to_be_bytes()
        };
        self.write_bytes(&bytes, 8);
        Ok(self)
    }

    /// Write a 16-bit unsigned integer in big-endian byte order.
    pub fn uint16_be(&mut self, value: u16) -> CoreResult<&mut Self> {
        self.align(2, 2);
        let bytes = value.to_be_bytes();
        self.write_bytes(&bytes, 2);
        Ok(self)
    }

    /// Write a 32-bit unsigned integer in big-endian byte order.
    pub fn uint32_be(&mut self, value: u32) -> CoreResult<&mut Self> {
        self.align(4, 4);
        let bytes = value.to_be_bytes();
        self.write_bytes(&bytes, 4);
        Ok(self)
    }

    /// Write a 64-bit unsigned integer in big-endian byte order.
    pub fn uint64_be(&mut self, value: u64) -> CoreResult<&mut Self> {
        self.align(8, 8);
        let bytes = value.to_be_bytes();
        self.write_bytes(&bytes, 8);
        Ok(self)
    }

    /// Write a 32-bit float.
    pub fn float32(&mut self, value: f32) -> CoreResult<&mut Self> {
        self.align(4, 4);
        let bytes = if self.little_endian {
            value.to_le_bytes()
        } else {
            value.to_be_bytes()
        };
        self.write_bytes(&bytes, 4);
        Ok(self)
    }

    /// Write a 64-bit double.
    pub fn float64(&mut self, value: f64) -> CoreResult<&mut Self> {
        self.align(self.eight_byte_alignment, 8);
        let bytes = if self.little_endian {
            value.to_le_bytes()
        } else {
            value.to_be_bytes()
        };
        self.write_bytes(&bytes, 8);
        Ok(self)
    }

    /// Write a string.
    ///
    /// # Arguments
    ///
    /// * `value` - The string to write
    /// * `write_length` - Whether to write the length prefix (default: true)
    pub fn string(&mut self, value: &str) -> CoreResult<&mut Self> {
        let strlen = value.len();
        self.uint32((strlen + 1) as u32)?; // Add one for null terminator
        self.reserve(strlen + 1);
        // Write string bytes
        let bytes = value.as_bytes();
        self.buffer[self.offset..self.offset + strlen].copy_from_slice(bytes);
        // Write null terminator
        self.buffer[self.offset + strlen] = 0;
        self.offset += strlen + 1;
        Ok(self)
    }

    /// Write a sequence length (for dynamic arrays).
    pub fn sequence_length(&mut self, value: usize) -> CoreResult<&mut Self> {
        self.uint32(value as u32)?;
        Ok(self)
    }

    /// Write raw bytes.
    pub fn bytes(&mut self, data: &[u8]) -> CoreResult<&mut Self> {
        self.reserve(data.len());
        self.buffer[self.offset..self.offset + data.len()].copy_from_slice(data);
        self.offset += data.len();
        Ok(self)
    }

    /// Write an array of 8-bit signed integers.
    pub fn int8_array(&mut self, values: &[i8], write_length: bool) -> CoreResult<&mut Self> {
        if write_length {
            self.sequence_length(values.len())?;
        }
        self.reserve(values.len());
        for &v in values {
            self.buffer[self.offset] = v as u8;
            self.offset += 1;
        }
        Ok(self)
    }

    /// Write an array of 8-bit unsigned integers.
    pub fn uint8_array(&mut self, values: &[u8], write_length: bool) -> CoreResult<&mut Self> {
        if write_length {
            self.sequence_length(values.len())?;
        }
        self.bytes(values)?;
        Ok(self)
    }

    /// Write an array of 32-bit unsigned integers.
    pub fn uint32_array(&mut self, values: &[u32], write_length: bool) -> CoreResult<&mut Self> {
        if write_length {
            self.sequence_length(values.len())?;
        }
        for &v in values {
            self.uint32(v)?;
        }
        Ok(self)
    }

    /// Write an array of 64-bit doubles.
    pub fn float64_array(&mut self, values: &[f64], write_length: bool) -> CoreResult<&mut Self> {
        if write_length {
            self.sequence_length(values.len())?;
        }
        for &v in values {
            self.float64(v)?;
        }
        Ok(self)
    }

    /// Encode a `DecodedMessage` using a schema.
    ///
    /// This is the primary method for re-encoding decoded messages back to CDR.
    /// It handles:
    /// - Primitive types (int, float, string, bool)
    /// - Arrays (fixed and dynamic)
    /// - Nested structs
    ///
    /// # Arguments
    ///
    /// * `message` - The decoded message to encode
    /// * `schema` - The parsed schema defining the message structure
    /// * `type_name` - The name of the type to encode (must exist in schema)
    ///
    /// # Example
    ///
    /// ```no_run
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// use robocodec::encoding::cdr::encoder::CdrEncoder;
    /// use robocodec::schema::parse_schema;
    /// use robocodec::DecodedMessage;
    /// use robocodec::encoding::cdr::decoder::CdrDecoder;
    ///
    /// let schema = parse_schema("TestMsg", "int32 value\nfloat64 data")?;
    /// # let raw_data = vec![0u8; 100];
    /// let decoded = CdrDecoder::new().decode(&schema, &raw_data, Some("TestMsg"))?;
    ///
    /// let mut encoder = CdrEncoder::new();
    /// encoder.encode_message(&decoded, &schema, "TestMsg")?;
    /// let normalized_data = encoder.finish();
    /// # Ok(())
    /// # }
    /// ```
    pub fn encode_message(
        &mut self,
        message: &DecodedMessage,
        schema: &MessageSchema,
        type_name: &str,
    ) -> CoreResult<()> {
        self.encode_message_internal(message, schema, type_name, 0)?;
        Ok(())
    }

    /// Internal recursive encoding with depth tracking.
    fn encode_message_internal(
        &mut self,
        message: &DecodedMessage,
        schema: &MessageSchema,
        type_name: &str,
        depth: usize,
    ) -> CoreResult<()> {
        const MAX_DEPTH: usize = 32;

        if depth > MAX_DEPTH {
            return Err(crate::core::CodecError::encode(
                "CDR",
                format!(
                    "Maximum encoding depth exceeded ({MAX_DEPTH}), possible circular reference"
                ),
            ));
        }

        // Get the type definition from schema
        let msg_type = schema.types.get(type_name).ok_or_else(|| {
            crate::core::CodecError::encode(
                "CDR",
                format!("Type '{type_name}' not found in schema"),
            )
        })?;

        let saved_origin = self.origin;
        self.origin = self.offset;

        // Encode each field in order
        for field in &msg_type.fields {
            if let Some(value) = message.get(&field.name) {
                self.encode_value(value, &field.type_name, schema, depth + 1)?;
            }
            // Skip missing fields (optional fields not set)
        }

        self.origin = saved_origin;
        Ok(())
    }

    /// Encode a single `CodecValue` based on its type.
    fn encode_value(
        &mut self,
        value: &CodecValue,
        field_type: &FieldType,
        schema: &MessageSchema,
        depth: usize,
    ) -> CoreResult<()> {
        match field_type {
            FieldType::Primitive(prim) => {
                self.encode_primitive(value, *prim)?;
            }
            FieldType::Array { base_type, size } => {
                self.encode_array(value, base_type, *size, schema, depth)?;
            }
            FieldType::Nested(type_name) => {
                self.encode_nested(value, type_name, schema, depth)?;
            }
        }
        Ok(())
    }

    /// Encode a primitive value with automatic type coercion.
    ///
    /// This method handles automatic type coercion from Python's native types:
    /// - Python int (Int64) -> int8/int16/int32/uint8/uint16/uint32 with bounds checking
    /// - Python float (Float64) -> float32 with precision loss
    fn encode_primitive(&mut self, value: &CodecValue, prim: IdlPrimitiveType) -> CoreResult<()> {
        match prim {
            IdlPrimitiveType::Bool => {
                if let CodecValue::Bool(b) = value {
                    self.uint8(if *b { 1 } else { 0 })?;
                } else {
                    return self.type_mismatch("bool", value);
                }
            }
            IdlPrimitiveType::Int8 => {
                let i = self.coerce_to_i8(value)?;
                self.int8(i)?;
            }
            IdlPrimitiveType::Int16 => {
                let i = self.coerce_to_i16(value)?;
                self.int16(i)?;
            }
            IdlPrimitiveType::Int32 => {
                let i = self.coerce_to_i32(value)?;
                self.int32(i)?;
            }
            IdlPrimitiveType::Int64 => {
                let i = self.coerce_to_i64(value)?;
                self.int64(i)?;
            }
            IdlPrimitiveType::UInt8 | IdlPrimitiveType::Byte => {
                let u = self.coerce_to_u8(value)?;
                self.uint8(u)?;
            }
            IdlPrimitiveType::UInt16 => {
                let u = self.coerce_to_u16(value)?;
                self.uint16(u)?;
            }
            IdlPrimitiveType::UInt32 => {
                let u = self.coerce_to_u32(value)?;
                self.uint32(u)?;
            }
            IdlPrimitiveType::UInt64 => {
                let u = self.coerce_to_u64(value)?;
                self.uint64(u)?;
            }
            IdlPrimitiveType::Float32 => {
                let f = self.coerce_to_f32(value)?;
                self.float32(f)?;
            }
            IdlPrimitiveType::Float64 => {
                let f = self.coerce_to_f64(value)?;
                self.float64(f)?;
            }
            IdlPrimitiveType::String | IdlPrimitiveType::WString => {
                if let CodecValue::String(s) = value {
                    self.string(s)?;
                } else {
                    return self.type_mismatch("string", value);
                }
            }
            IdlPrimitiveType::Char => {
                let i = self.coerce_to_i8(value)?;
                self.int8(i)?;
            }
            IdlPrimitiveType::Time => {
                if let CodecValue::Timestamp(nanos) = value {
                    // Convert nanoseconds to sec/nsec
                    let sec = nanos / NANOS_PER_SEC;
                    let nsec = (nanos % NANOS_PER_SEC).abs();
                    self.int32(sec as i32)?;
                    self.uint32(nsec as u32)?;
                } else {
                    return self.type_mismatch("time", value);
                }
            }
            IdlPrimitiveType::Duration => {
                if let CodecValue::Duration(nanos) = value {
                    // Convert nanoseconds to sec/nsec
                    let sec = nanos / NANOS_PER_SEC;
                    let nsec = if *nanos < 0 {
                        // For negative durations, nsec is stored as positive
                        (nanos % NANOS_PER_SEC).abs()
                    } else {
                        nanos % NANOS_PER_SEC
                    };
                    self.int32(sec as i32)?;
                    self.uint32(nsec as u32)?;
                } else {
                    return self.type_mismatch("duration", value);
                }
            }
        }
        Ok(())
    }

    /// Coerce a CodecValue to i8 with bounds checking.
    fn coerce_to_i8(&self, value: &CodecValue) -> CoreResult<i8> {
        match value {
            CodecValue::Int8(i) => Ok(*i),
            CodecValue::Int16(i) => {
                i8::try_from(*i).map_err(|_| self.overflow_error("int8", value))
            }
            CodecValue::Int32(i) => {
                i8::try_from(*i).map_err(|_| self.overflow_error("int8", value))
            }
            CodecValue::Int64(i) => {
                i8::try_from(*i).map_err(|_| self.overflow_error("int8", value))
            }
            CodecValue::UInt8(u) => {
                i8::try_from(*u).map_err(|_| self.overflow_error("int8", value))
            }
            CodecValue::UInt16(u) => {
                i8::try_from(*u).map_err(|_| self.overflow_error("int8", value))
            }
            CodecValue::UInt32(u) => {
                i8::try_from(*u).map_err(|_| self.overflow_error("int8", value))
            }
            CodecValue::UInt64(u) => {
                i8::try_from(*u).map_err(|_| self.overflow_error("int8", value))
            }
            _ => Err(self.coerce_error("int8", value)),
        }
    }

    /// Coerce a CodecValue to i16 with bounds checking.
    fn coerce_to_i16(&self, value: &CodecValue) -> CoreResult<i16> {
        match value {
            CodecValue::Int8(i) => Ok(i16::from(*i)),
            CodecValue::Int16(i) => Ok(*i),
            CodecValue::Int32(i) => {
                i16::try_from(*i).map_err(|_| self.overflow_error("int16", value))
            }
            CodecValue::Int64(i) => {
                i16::try_from(*i).map_err(|_| self.overflow_error("int16", value))
            }
            CodecValue::UInt8(u) => Ok(i16::from(*u)),
            CodecValue::UInt16(u) => {
                i16::try_from(*u).map_err(|_| self.overflow_error("int16", value))
            }
            CodecValue::UInt32(u) => {
                i16::try_from(*u).map_err(|_| self.overflow_error("int16", value))
            }
            CodecValue::UInt64(u) => {
                i16::try_from(*u).map_err(|_| self.overflow_error("int16", value))
            }
            _ => Err(self.coerce_error("int16", value)),
        }
    }

    /// Coerce a CodecValue to i32 with bounds checking.
    fn coerce_to_i32(&self, value: &CodecValue) -> CoreResult<i32> {
        match value {
            CodecValue::Int8(i) => Ok(i32::from(*i)),
            CodecValue::Int16(i) => Ok(i32::from(*i)),
            CodecValue::Int32(i) => Ok(*i),
            CodecValue::Int64(i) => {
                i32::try_from(*i).map_err(|_| self.overflow_error("int32", value))
            }
            CodecValue::UInt8(u) => Ok(i32::from(*u)),
            CodecValue::UInt16(u) => Ok(i32::from(*u)),
            CodecValue::UInt32(u) => {
                i32::try_from(*u).map_err(|_| self.overflow_error("int32", value))
            }
            CodecValue::UInt64(u) => {
                i32::try_from(*u).map_err(|_| self.overflow_error("int32", value))
            }
            _ => Err(self.coerce_error("int32", value)),
        }
    }

    /// Coerce a CodecValue to i64.
    fn coerce_to_i64(&self, value: &CodecValue) -> CoreResult<i64> {
        match value {
            CodecValue::Int8(i) => Ok(i64::from(*i)),
            CodecValue::Int16(i) => Ok(i64::from(*i)),
            CodecValue::Int32(i) => Ok(i64::from(*i)),
            CodecValue::Int64(i) => Ok(*i),
            CodecValue::UInt8(u) => Ok(i64::from(*u)),
            CodecValue::UInt16(u) => Ok(i64::from(*u)),
            CodecValue::UInt32(u) => Ok(i64::from(*u)),
            CodecValue::UInt64(u) => {
                i64::try_from(*u).map_err(|_| self.overflow_error("int64", value))
            }
            _ => Err(self.coerce_error("int64", value)),
        }
    }

    /// Coerce a CodecValue to u8 with bounds checking.
    fn coerce_to_u8(&self, value: &CodecValue) -> CoreResult<u8> {
        match value {
            CodecValue::UInt8(u) => Ok(*u),
            CodecValue::UInt16(u) => {
                u8::try_from(*u).map_err(|_| self.overflow_error("uint8", value))
            }
            CodecValue::UInt32(u) => {
                u8::try_from(*u).map_err(|_| self.overflow_error("uint8", value))
            }
            CodecValue::UInt64(u) => {
                u8::try_from(*u).map_err(|_| self.overflow_error("uint8", value))
            }
            CodecValue::Int8(i) => {
                u8::try_from(*i).map_err(|_| self.overflow_error("uint8", value))
            }
            CodecValue::Int16(i) => {
                u8::try_from(*i).map_err(|_| self.overflow_error("uint8", value))
            }
            CodecValue::Int32(i) => {
                u8::try_from(*i).map_err(|_| self.overflow_error("uint8", value))
            }
            CodecValue::Int64(i) => {
                u8::try_from(*i).map_err(|_| self.overflow_error("uint8", value))
            }
            _ => Err(self.coerce_error("uint8", value)),
        }
    }

    /// Coerce a CodecValue to u16 with bounds checking.
    fn coerce_to_u16(&self, value: &CodecValue) -> CoreResult<u16> {
        match value {
            CodecValue::UInt8(u) => Ok(u16::from(*u)),
            CodecValue::UInt16(u) => Ok(*u),
            CodecValue::UInt32(u) => {
                u16::try_from(*u).map_err(|_| self.overflow_error("uint16", value))
            }
            CodecValue::UInt64(u) => {
                u16::try_from(*u).map_err(|_| self.overflow_error("uint16", value))
            }
            CodecValue::Int8(i) => {
                u16::try_from(*i).map_err(|_| self.overflow_error("uint16", value))
            }
            CodecValue::Int16(i) => {
                u16::try_from(*i).map_err(|_| self.overflow_error("uint16", value))
            }
            CodecValue::Int32(i) => {
                u16::try_from(*i).map_err(|_| self.overflow_error("uint16", value))
            }
            CodecValue::Int64(i) => {
                u16::try_from(*i).map_err(|_| self.overflow_error("uint16", value))
            }
            _ => Err(self.coerce_error("uint16", value)),
        }
    }

    /// Coerce a CodecValue to u32 with bounds checking.
    fn coerce_to_u32(&self, value: &CodecValue) -> CoreResult<u32> {
        match value {
            CodecValue::UInt8(u) => Ok(u32::from(*u)),
            CodecValue::UInt16(u) => Ok(u32::from(*u)),
            CodecValue::UInt32(u) => Ok(*u),
            CodecValue::UInt64(u) => {
                u32::try_from(*u).map_err(|_| self.overflow_error("uint32", value))
            }
            CodecValue::Int8(i) => {
                u32::try_from(*i).map_err(|_| self.overflow_error("uint32", value))
            }
            CodecValue::Int16(i) => {
                u32::try_from(*i).map_err(|_| self.overflow_error("uint32", value))
            }
            CodecValue::Int32(i) => {
                u32::try_from(*i).map_err(|_| self.overflow_error("uint32", value))
            }
            CodecValue::Int64(i) => {
                u32::try_from(*i).map_err(|_| self.overflow_error("uint32", value))
            }
            _ => Err(self.coerce_error("uint32", value)),
        }
    }

    /// Coerce a CodecValue to u64 with bounds checking.
    fn coerce_to_u64(&self, value: &CodecValue) -> CoreResult<u64> {
        match value {
            CodecValue::UInt8(u) => Ok(u64::from(*u)),
            CodecValue::UInt16(u) => Ok(u64::from(*u)),
            CodecValue::UInt32(u) => Ok(u64::from(*u)),
            CodecValue::UInt64(u) => Ok(*u),
            CodecValue::Int8(i) => {
                u64::try_from(*i).map_err(|_| self.overflow_error("uint64", value))
            }
            CodecValue::Int16(i) => {
                u64::try_from(*i).map_err(|_| self.overflow_error("uint64", value))
            }
            CodecValue::Int32(i) => {
                u64::try_from(*i).map_err(|_| self.overflow_error("uint64", value))
            }
            CodecValue::Int64(i) => {
                u64::try_from(*i).map_err(|_| self.overflow_error("uint64", value))
            }
            _ => Err(self.coerce_error("uint64", value)),
        }
    }

    /// Coerce a CodecValue to f32.
    fn coerce_to_f32(&self, value: &CodecValue) -> CoreResult<f32> {
        match value {
            CodecValue::Float32(f) => Ok(*f),
            CodecValue::Float64(f) => Ok(*f as f32), // Allow precision loss
            CodecValue::Int8(i) => Ok(*i as f32),
            CodecValue::Int16(i) => Ok(*i as f32),
            CodecValue::Int32(i) => Ok(*i as f32),
            CodecValue::Int64(i) => Ok(*i as f32),
            CodecValue::UInt8(u) => Ok(*u as f32),
            CodecValue::UInt16(u) => Ok(*u as f32),
            CodecValue::UInt32(u) => Ok(*u as f32),
            CodecValue::UInt64(u) => Ok(*u as f32),
            _ => Err(self.coerce_error("float32", value)),
        }
    }

    /// Coerce a CodecValue to f64.
    fn coerce_to_f64(&self, value: &CodecValue) -> CoreResult<f64> {
        match value {
            CodecValue::Float32(f) => Ok(f64::from(*f)),
            CodecValue::Float64(f) => Ok(*f),
            CodecValue::Int8(i) => Ok(f64::from(*i)),
            CodecValue::Int16(i) => Ok(f64::from(*i)),
            CodecValue::Int32(i) => Ok(f64::from(*i)),
            CodecValue::Int64(i) => Ok(*i as f64),
            CodecValue::UInt8(u) => Ok(f64::from(*u)),
            CodecValue::UInt16(u) => Ok(f64::from(*u)),
            CodecValue::UInt32(u) => Ok(f64::from(*u)),
            CodecValue::UInt64(u) => Ok(*u as f64),
            _ => Err(self.coerce_error("float64", value)),
        }
    }

    /// Create an overflow error for type coercion.
    fn overflow_error(&self, expected: &str, actual: &CodecValue) -> crate::core::CodecError {
        crate::core::CodecError::encode(
            "CDR",
            format!("Value {actual:?} overflows target type {expected}"),
        )
    }

    /// Create a coercion error for incompatible types.
    fn coerce_error(&self, expected: &str, actual: &CodecValue) -> crate::core::CodecError {
        crate::core::CodecError::encode("CDR", format!("Cannot coerce {actual:?} to {expected}"))
    }

    /// Encode an array value.
    fn encode_array(
        &mut self,
        value: &CodecValue,
        base_type: &FieldType,
        fixed_size: Option<usize>,
        schema: &MessageSchema,
        depth: usize,
    ) -> CoreResult<()> {
        let elements = if let CodecValue::Array(arr) = value {
            arr
        } else {
            return Err(crate::core::CodecError::encode(
                "CDR",
                format!("Expected array, got {value:?}"),
            ));
        };

        // Write sequence length for dynamic arrays
        if fixed_size.is_none() {
            self.sequence_length(elements.len())?;
        }

        // Encode each element
        for elem in elements {
            self.encode_value(elem, base_type, schema, depth)?;
        }

        Ok(())
    }

    /// Encode a nested message value.
    fn encode_nested(
        &mut self,
        value: &CodecValue,
        type_name: &str,
        schema: &MessageSchema,
        depth: usize,
    ) -> CoreResult<()> {
        let nested_msg = if let CodecValue::Struct(msg) = value {
            msg
        } else {
            return Err(crate::core::CodecError::encode(
                "CDR",
                format!("Expected struct for type '{type_name}', got {value:?}"),
            ));
        };

        // Save origin for nested struct alignment
        let saved_origin = self.origin;
        self.origin = self.offset;

        // Get the nested type definition with variant resolution (handles / vs ::)
        let msg_type = schema.get_type_variants(type_name).ok_or_else(|| {
            crate::core::CodecError::encode(
                "CDR",
                format!("Nested type '{type_name}' not found in schema"),
            )
        })?;

        // Encode each field
        for field in &msg_type.fields {
            if let Some(field_value) = nested_msg.get(&field.name) {
                self.encode_value(field_value, &field.type_name, schema, depth)?;
            }
        }

        self.origin = saved_origin;
        Ok(())
    }

    /// Create a type mismatch error.
    fn type_mismatch(&self, expected: &str, actual: &CodecValue) -> CoreResult<()> {
        Err(crate::core::CodecError::encode(
            "CDR",
            format!("Type mismatch: expected {expected}, got {actual:?}"),
        ))
    }

    /// Write bytes to the buffer.
    fn write_bytes(&mut self, bytes: &[u8], count: usize) {
        debug_assert_eq!(bytes.len(), count);
        self.buffer[self.offset..self.offset + count].copy_from_slice(bytes);
        self.offset += count;
    }

    /// Create a calculator to pre-calculate the size of a message.
    #[must_use]
    pub fn calculator() -> CdrCalculator {
        CdrCalculator::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_encoder_new() {
        let encoder = CdrEncoder::new();
        assert_eq!(encoder.size(), 4); // CDR header
        assert_eq!(encoder.data(), &[0x00, 0x01, 0x00, 0x00]);
    }

    #[test]
    fn test_encoder_int8() {
        let mut encoder = CdrEncoder::new();
        encoder.int8(-1).unwrap();
        encoder.int8(127).unwrap();
        assert_eq!(encoder.data(), &[0x00, 0x01, 0x00, 0x00, 0xFF, 0x7F]);
    }

    #[test]
    fn test_encoder_uint8() {
        let mut encoder = CdrEncoder::new();
        encoder.uint8(0x42).unwrap();
        assert_eq!(encoder.data(), &[0x00, 0x01, 0x00, 0x00, 0x42]);
    }

    #[test]
    fn test_encoder_int16() {
        let mut encoder = CdrEncoder::new();
        encoder.int16(-300).unwrap();
        // -300 in little-endian: 0xD4, 0xFE
        assert_eq!(encoder.data()[4..6], [0xD4, 0xFE]);
    }

    #[test]
    fn test_encoder_int32() {
        let mut encoder = CdrEncoder::new();
        encoder.int32(42).unwrap();
        assert_eq!(encoder.data()[4..8], 42u32.to_le_bytes());
    }

    #[test]
    fn test_encoder_int64() {
        let mut encoder = CdrEncoder::new();
        encoder.int64(-7000000001).unwrap();
        let data = encoder.data();
        // With origin = 4, position 4 is already 8-byte aligned: (4 - 4) % 8 = 0
        // After header (4 bytes) + value (8 bytes) = 12 bytes
        assert_eq!(data.len(), 12);
    }

    #[test]
    fn test_encoder_float32() {
        let mut encoder = CdrEncoder::new();
        encoder.float32(1.0).unwrap();
        let data = encoder.data();
        // 1.0f32 in little-endian: 0x00, 0x00, 0x80, 0x3F
        assert_eq!(data[4..8], [0x00, 0x00, 0x80, 0x3F]);
    }

    #[test]
    fn test_encoder_float64() {
        let mut encoder = CdrEncoder::new();
        encoder.float64(1.0).unwrap();
        let data = encoder.data();
        // With origin = 4, position 4 is already 8-byte aligned: (4 - 4) % 8 = 0
        // After header (4) + value (8) = 12
        assert_eq!(data.len(), 12);
    }

    #[test]
    fn test_encoder_string() {
        let mut encoder = CdrEncoder::new();
        encoder.string("hello").unwrap();
        let data = encoder.data();
        // 4 (header) + 4 (length) + 5 + 1 (null) = 14
        assert_eq!(data.len(), 14);
    }

    #[test]
    fn test_encoder_empty_string() {
        let mut encoder = CdrEncoder::new();
        encoder.string("").unwrap();
        let data = encoder.data();
        // 4 (header) + 4 (length = 1) + 0 + 1 (null) = 9
        assert_eq!(data.len(), 9);
    }

    #[test]
    fn test_encoder_alignment() {
        let mut encoder = CdrEncoder::new();
        encoder.uint8(1).unwrap(); // offset = 5
        encoder.uint32(2).unwrap(); // Should align to 8
        assert_eq!(encoder.size(), 12);
    }

    #[test]
    fn test_encoder_uint32_array() {
        let mut encoder = CdrEncoder::new();
        encoder.uint32_array(&[1, 2, 3], true).unwrap();
        assert_eq!(encoder.size(), 20); // 4 (header) + 4 (len) + 12 (data)
    }

    #[test]
    fn test_encapsulation_kind_cdr2() {
        let encoder = CdrEncoder::with_kind(EncapsulationKind::Cdr2Le);
        assert_eq!(encoder.data()[1], 0x02);
        assert!(encoder.kind().is_cdr2());
    }

    #[test]
    fn test_encapsulation_kind_big_endian() {
        let encoder = CdrEncoder::with_kind(EncapsulationKind::CdrBe);
        assert_eq!(encoder.data()[1], 0x00);
        assert!(!encoder.kind().is_little_endian());
    }

    #[test]
    fn test_round_trip_int32() {
        let mut encoder = CdrEncoder::new();
        encoder.int32(42).unwrap();
        let data = encoder.data();

        // Decode using cursor
        let mut cursor = super::super::cursor::CdrCursor::new(data).unwrap();
        assert_eq!(cursor.read_i32().unwrap(), 42);
    }

    #[test]
    fn test_round_trip_string() {
        let mut encoder = CdrEncoder::new();
        encoder.string("hello").unwrap();
        let data = encoder.data();

        // Decode using cursor
        let mut cursor = super::super::cursor::CdrCursor::new(data).unwrap();
        let len = cursor.read_u32().unwrap();
        assert_eq!(len, 6); // "hello" + null
        let bytes = cursor.read_bytes(5).unwrap();
        assert_eq!(bytes, b"hello");
        cursor.read_u8().unwrap(); // null terminator
    }

    #[test]
    fn test_round_trip_complex_message() {
        // Simulate: geometry_msgs/TransformStamped
        let mut encoder = CdrEncoder::new();

        // sequenceLength(1) for the array
        encoder.sequence_length(1).unwrap();

        // std_msgs/Header header
        // uint32 sec
        encoder.uint32(1490149580).unwrap();
        // uint32 nsec
        encoder.uint32(117017840).unwrap();
        // string frame_id
        encoder.string("base_link").unwrap();
        // string child_frame_id
        encoder.string("radar").unwrap();

        // geometry_msgs/Transform transform
        // geometry_msgs/Vector3 translation
        encoder.float64(3.835).unwrap(); // x
        encoder.float64(0.0).unwrap(); // y
        encoder.float64(0.0).unwrap(); // z

        // geometry_msgs/Quaternion rotation
        encoder.float64(0.0).unwrap(); // x
        encoder.float64(0.0).unwrap(); // y
        encoder.float64(0.0).unwrap(); // z
        encoder.float64(1.0).unwrap(); // w

        let data = encoder.data();
        // With origin = 4, the first float64 at offset 42 only needs 2 bytes padding
        // Total: 4 + 4 + 4 + 4 + 14 + 2(pad) + 10 + 2(pad) + 8*7 = 100
        assert_eq!(data.len(), 100);
    }

    #[test]
    fn test_reset() {
        let mut encoder = CdrEncoder::new();
        encoder.int32(42).unwrap();
        assert_eq!(encoder.size(), 8);
        encoder.reset();
        assert_eq!(encoder.size(), 4);
        assert_eq!(encoder.data(), &[0x00, 0x01, 0x00, 0x00]);
    }

    // Comprehensive encoder tests

    #[test]
    fn test_encoder_uint16() {
        let mut encoder = CdrEncoder::new();
        encoder.uint16(0x1234).unwrap();
        assert_eq!(encoder.data()[4..6], [0x34, 0x12]); // little-endian
    }

    #[test]
    fn test_encoder_uint64() {
        let mut encoder = CdrEncoder::new();
        encoder.uint64(0x123456789ABCDEF0).unwrap();
        let data = encoder.data();
        // With origin = 4, position 4 is already 8-byte aligned: (4 - 4) % 8 = 0
        // After header (4) + value (8) = 12
        assert_eq!(data.len(), 12);
        // Check the value bytes (starts at position 4)
        assert_eq!(
            &data[4..12],
            &[0xF0, 0xDE, 0xBC, 0x9A, 0x78, 0x56, 0x34, 0x12]
        );
    }

    #[test]
    fn test_encoder_int8_min_max() {
        let mut encoder = CdrEncoder::new();
        encoder.int8(i8::MIN).unwrap(); // -128
        encoder.int8(i8::MAX).unwrap(); // 127
        let data = encoder.data();
        assert_eq!(data[4], 0x80); // -128 = 0x80
        assert_eq!(data[5], 0x7F); // 127 = 0x7F
    }

    #[test]
    fn test_encoder_int16_min_max() {
        let mut encoder = CdrEncoder::new();
        encoder.int16(i16::MIN).unwrap(); // -32768
        encoder.int16(i16::MAX).unwrap(); // 32767
        let data = encoder.data();
        // -32768 in little-endian: 0x00, 0x80
        assert_eq!(data[4..6], [0x00, 0x80]);
        // 32767 in little-endian: 0xFF, 0x7F
        assert_eq!(data[6..8], [0xFF, 0x7F]);
    }

    #[test]
    fn test_encoder_int32_min_max() {
        let mut encoder = CdrEncoder::new();
        encoder.int32(i32::MIN).unwrap();
        encoder.int32(i32::MAX).unwrap();
        let data = encoder.data();
        // i32::MIN in little-endian: 0x00, 0x00, 0x00, 0x80
        assert_eq!(data[4..8], [0x00, 0x00, 0x00, 0x80]);
        // i32::MAX in little-endian: 0xFF, 0xFF, 0xFF, 0x7F
        assert_eq!(data[8..12], [0xFF, 0xFF, 0xFF, 0x7F]);
    }

    #[test]
    fn test_encoder_int64_min_max() {
        let mut encoder = CdrEncoder::new();
        encoder.int64(i64::MIN).unwrap();
        let data = encoder.data();
        // With origin = 4, position 4 is already 8-byte aligned: (4 - 4) % 8 = 0
        assert_eq!(data.len(), 12); // header + value
                                    // i64::MIN in little-endian (starts at position 4)
        assert_eq!(
            &data[4..12],
            &[0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x80]
        );
    }

    #[test]
    fn test_encoder_float32_special_values() {
        let mut encoder = CdrEncoder::new();
        encoder.float32(f32::INFINITY).unwrap();
        encoder.float32(f32::NEG_INFINITY).unwrap();
        encoder.float32(0.0).unwrap();
        encoder.float32(-0.0).unwrap();
        let data = encoder.data();
        // Check that special values are encoded correctly
        assert_eq!(data.len(), 4 + 4 * 4); // header + 4 floats
    }

    #[test]
    fn test_encoder_float64_special_values() {
        let mut encoder = CdrEncoder::new();
        encoder.float64(f64::INFINITY).unwrap();
        let data = encoder.data();
        // With origin = 4, position 4 is already 8-byte aligned: (4 - 4) % 8 = 0
        // header + value
        assert_eq!(data.len(), 12);
    }

    #[test]
    fn test_encoder_string_unicode() {
        let mut encoder = CdrEncoder::new();
        encoder.string("hello 世界").unwrap();
        let data = encoder.data();
        // 4 (header) + 4 (length) + 12 (UTF-8 bytes: 5 + 1 + 2 + 3 + 1) + 1 (null)
        // "hello" = 5 bytes, " " = 1 byte, "世界" = 6 bytes (2 UTF-8 chars * 3 bytes each)
        // Total content = 12 bytes, plus null = 13
        assert_eq!(data.len(), 4 + 4 + 12 + 1);
    }

    #[test]
    fn test_encoder_string_long() {
        let mut encoder = CdrEncoder::new();
        let long_string = "a".repeat(1000);
        encoder.string(&long_string).unwrap();
        let data = encoder.data();
        // 4 (header) + 4 (length) + 1000 + 1 (null)
        assert_eq!(data.len(), 1009);
    }

    #[test]
    fn test_encoder_string_with_null() {
        let mut encoder = CdrEncoder::new();
        encoder.string("hello\0world").unwrap();
        let data = encoder.data();
        // Should still add null terminator at the end
        assert_eq!(data[data.len() - 1], 0);
    }

    #[test]
    fn test_encoder_int8_array() {
        let mut encoder = CdrEncoder::new();
        encoder.int8_array(&[1, 2, 3, 4, 5], true).unwrap();
        assert_eq!(encoder.size(), 4 + 4 + 5); // header + length + data
    }

    #[test]
    fn test_encoder_uint8_array() {
        let mut encoder = CdrEncoder::new();
        encoder.uint8_array(&[0x10, 0x20, 0x30], true).unwrap();
        assert_eq!(encoder.size(), 4 + 4 + 3); // header + length + data
    }

    #[test]
    fn test_encoder_float64_array() {
        let mut encoder = CdrEncoder::new();
        encoder.float64_array(&[1.0, 2.0, 3.0], true).unwrap();
        // With origin = 4:
        // - After header: offset = 4, origin = 4
        // - Write length (4 bytes): offset = 8
        // - First f64 at offset 8: (8-4) % 8 = 4, needs 4 bytes padding
        // - Total: 4 (header) + 4 (length) + 4 (padding) + 24 (3*8) = 36
        assert_eq!(encoder.size(), 36);
    }

    #[test]
    fn test_encoder_empty_array() {
        let mut encoder = CdrEncoder::new();
        encoder.uint32_array(&[], true).unwrap();
        assert_eq!(encoder.size(), 8); // header + length (0)
    }

    #[test]
    fn test_encoder_array_without_length() {
        let mut encoder = CdrEncoder::new();
        encoder.uint32_array(&[1, 2, 3], false).unwrap();
        assert_eq!(encoder.size(), 4 + 12); // header + data only
    }

    #[test]
    fn test_encoder_big_endian_int32() {
        let mut encoder = CdrEncoder::with_kind(EncapsulationKind::CdrBe);
        encoder.int32(0x12345678).unwrap();
        let data = encoder.data();
        // Big-endian: 0x12, 0x34, 0x56, 0x78
        assert_eq!(data[4..8], [0x12, 0x34, 0x56, 0x78]);
    }

    #[test]
    fn test_encoder_big_endian_int64() {
        let mut encoder = CdrEncoder::with_kind(EncapsulationKind::CdrBe);
        encoder.int64(0x123456789ABCDEF0).unwrap();
        let data = encoder.data();
        // With origin = 4, position 4 is already 8-byte aligned: (4 - 4) % 8 = 0
        // Big-endian: 0x12, 0x34, 0x56, 0x78, 0x9A, 0xBC, 0xDE, 0xF0 (starts at position 4)
        assert_eq!(
            data[4..12],
            [0x12, 0x34, 0x56, 0x78, 0x9A, 0xBC, 0xDE, 0xF0]
        );
    }

    #[test]
    fn test_encoder_cdr2_float64_alignment() {
        // CDR2 uses 4-byte alignment for 64-bit values
        let mut encoder = CdrEncoder::with_kind(EncapsulationKind::Cdr2Le);
        encoder.uint32(1).unwrap();
        encoder.float64(2.0).unwrap();
        // After uint32: offset = 8
        // float64 in CDR2: align to 4, (8-0)%4=0, no padding, write 8 bytes
        // Total: 4 (header) + 4 (uint32) + 8 (float64) = 16
        assert_eq!(encoder.size(), 16);
    }

    #[test]
    fn test_encoder_cdr1_vs_cdr2_alignment() {
        // With origin = 4, after two uint32s (8 bytes each from offset 4),
        // the offset is 12, which is 8-byte aligned: (12-4) % 8 = 0
        // So CDR1 and CDR2 produce the same size in this case
        let mut encoder_cdr1 = CdrEncoder::new();
        encoder_cdr1.uint32(1).unwrap(); // offset = 8
        encoder_cdr1.uint32(2).unwrap(); // offset = 12
        encoder_cdr1.float64(3.0).unwrap(); // offset = 12, align to 8: (12-4)%8=0, no padding, +8 = 20
        let size_cdr1 = encoder_cdr1.size();

        // CDR2: 4-byte alignment for float64
        let mut encoder_cdr2 = CdrEncoder::with_kind(EncapsulationKind::Cdr2Le);
        encoder_cdr2.uint32(1).unwrap(); // offset = 8
        encoder_cdr2.uint32(2).unwrap(); // offset = 12
        encoder_cdr2.float64(3.0).unwrap(); // offset = 12, align to 4: (12-4)%4=0, +8 = 20
        let size_cdr2 = encoder_cdr2.size();

        // Both are the same size since offset 12 is aligned for both 4 and 8
        assert_eq!(size_cdr1, 20); // header + 4 + 4 + 8
        assert_eq!(size_cdr2, 20); // header + 4 + 4 + 8
    }

    #[test]
    fn test_encoder_bytes() {
        let mut encoder = CdrEncoder::new();
        encoder.bytes(&[0x01, 0x02, 0x03, 0x04]).unwrap();
        let data = encoder.data();
        assert_eq!(data[4..8], [0x01, 0x02, 0x03, 0x04]);
    }

    #[test]
    fn test_encoder_sequence_length() {
        let mut encoder = CdrEncoder::new();
        encoder.sequence_length(42).unwrap();
        let data = encoder.data();
        assert_eq!(data[4..8], 42u32.to_le_bytes());
    }

    #[test]
    fn test_encoder_multiple_float64() {
        let mut encoder = CdrEncoder::new();
        encoder.float64(1.0).unwrap();
        encoder.float64(2.0).unwrap();
        encoder.float64(3.0).unwrap();
        // With origin = 4, position 4 is 8-byte aligned: (4-4) % 8 = 0
        // header (4) + 8 + 8 + 8 = 28 (no padding needed)
        assert_eq!(encoder.size(), 28);
    }

    #[test]
    fn test_encoder_mixed_types() {
        let mut encoder = CdrEncoder::new();
        encoder.uint8(1).unwrap();
        encoder.uint16(2).unwrap();
        encoder.uint32(3).unwrap();
        encoder.uint64(4).unwrap();
        encoder.float32(5.0).unwrap();
        encoder.float64(6.0).unwrap();
        // Verify size calculation
        assert!(encoder.size() > 4);
    }

    #[test]
    fn test_encoder_big_endian_uint16() {
        let mut encoder = CdrEncoder::new();
        encoder.uint16_be(0x1234).unwrap();
        let data = encoder.data();
        // Big-endian: 0x12, 0x34
        assert_eq!(data[4..6], [0x12, 0x34]);
    }

    #[test]
    fn test_encoder_big_endian_uint32() {
        let mut encoder = CdrEncoder::new();
        encoder.uint32_be(0x12345678).unwrap();
        let data = encoder.data();
        assert_eq!(data[4..8], [0x12, 0x34, 0x56, 0x78]);
    }

    #[test]
    fn test_encoder_big_endian_uint64() {
        let mut encoder = CdrEncoder::new();
        encoder.uint64_be(0x123456789ABCDEF0).unwrap();
        let data = encoder.data();
        // With origin = 4, position 4 is already 8-byte aligned: (4 - 4) % 8 = 0
        assert_eq!(
            data[4..12],
            [0x12, 0x34, 0x56, 0x78, 0x9A, 0xBC, 0xDE, 0xF0]
        );
    }

    #[test]
    fn test_encoder_finish() {
        let mut encoder = CdrEncoder::new();
        encoder.int32(42).unwrap();
        let data = encoder.finish();
        assert_eq!(data.len(), 8);
    }

    #[test]
    fn test_encoder_with_capacity() {
        let mut encoder = CdrEncoder::with_capacity(100);
        encoder.int32(42).unwrap();
        assert_eq!(encoder.size(), 8);
    }

    #[test]
    fn test_encoder_string_alignment_after() {
        let mut encoder = CdrEncoder::new();
        encoder.uint8(1).unwrap();
        encoder.string("test").unwrap();
        encoder.uint32(2).unwrap();
        // uint8 at offset 5
        // string: align to 4, (5-0)%4=1, +3 padding = 8, write length (4) = 12, write "test\0" (5) = 17
        // uint32: align to 4, (17-0)%4=1, +3 padding = 20, write 4 = 24
        assert_eq!(encoder.size(), 24);
    }

    // Round-trip tests with cursor

    #[test]
    fn test_round_trip_uint16() {
        let mut encoder = CdrEncoder::new();
        encoder.uint16(0x1234).unwrap();
        let data = encoder.data();

        let mut cursor = super::super::cursor::CdrCursor::new(data).unwrap();
        assert_eq!(cursor.read_u16().unwrap(), 0x1234);
    }

    #[test]
    fn test_round_trip_uint64() {
        let mut encoder = CdrEncoder::new();
        encoder.uint64(0x123456789ABCDEF0).unwrap();
        let data = encoder.data();

        let mut cursor = super::super::cursor::CdrCursor::new(data).unwrap();
        assert_eq!(cursor.read_u64().unwrap(), 0x123456789ABCDEF0);
    }

    #[test]
    fn test_round_trip_int8_min_max() {
        let values = [i8::MIN, i8::MAX, 0, -1, 1];
        for &v in &values {
            let mut encoder = CdrEncoder::new();
            encoder.int8(v).unwrap();
            let data = encoder.data();

            let mut cursor = super::super::cursor::CdrCursor::new(data).unwrap();
            assert_eq!(cursor.read_i8().unwrap(), v);
        }
    }

    #[test]
    fn test_round_trip_int16_min_max() {
        let values = [i16::MIN, i16::MAX, 0, -1, 1];
        for &v in &values {
            let mut encoder = CdrEncoder::new();
            encoder.int16(v).unwrap();
            let data = encoder.data();

            let mut cursor = super::super::cursor::CdrCursor::new(data).unwrap();
            assert_eq!(cursor.read_i16().unwrap(), v);
        }
    }

    #[test]
    fn test_round_trip_int32_min_max() {
        let values = [i32::MIN, i32::MAX, 0, -1, 1];
        for &v in &values {
            let mut encoder = CdrEncoder::new();
            encoder.int32(v).unwrap();
            let data = encoder.data();

            let mut cursor = super::super::cursor::CdrCursor::new(data).unwrap();
            assert_eq!(cursor.read_i32().unwrap(), v);
        }
    }

    #[test]
    fn test_round_trip_int64_min_max() {
        let values = [i64::MIN, i64::MAX, 0, -1, 1];
        for &v in &values {
            let mut encoder = CdrEncoder::new();
            encoder.int64(v).unwrap();
            let data = encoder.data();

            let mut cursor = super::super::cursor::CdrCursor::new(data).unwrap();
            assert_eq!(cursor.read_i64().unwrap(), v);
        }
    }

    #[test]
    fn test_round_trip_float32() {
        let values = [
            0.0,
            1.0,
            -1.0,
            std::f32::consts::PI,
            f32::INFINITY,
            f32::NEG_INFINITY,
        ];
        for &v in &values {
            let mut encoder = CdrEncoder::new();
            encoder.float32(v).unwrap();
            let data = encoder.data();

            let mut cursor = super::super::cursor::CdrCursor::new(data).unwrap();
            let decoded = cursor.read_f32().unwrap();
            if v.is_infinite() {
                assert_eq!(decoded.is_infinite(), v.is_infinite());
                assert_eq!(decoded.is_sign_positive(), v.is_sign_positive());
            } else {
                assert!((decoded - v).abs() < f32::EPSILON);
            }
        }
    }

    #[test]
    fn test_round_trip_float64() {
        let values = [
            0.0,
            1.0,
            -1.0,
            std::f64::consts::PI,
            f64::INFINITY,
            f64::NEG_INFINITY,
        ];
        for &v in &values {
            let mut encoder = CdrEncoder::new();
            encoder.float64(v).unwrap();
            let data = encoder.data();

            let mut cursor = super::super::cursor::CdrCursor::new(data).unwrap();
            let decoded = cursor.read_f64().unwrap();
            if v.is_infinite() {
                assert_eq!(decoded.is_infinite(), v.is_infinite());
                assert_eq!(decoded.is_sign_positive(), v.is_sign_positive());
            } else {
                assert!((decoded - v).abs() < f64::EPSILON);
            }
        }
    }

    #[test]
    fn test_round_trip_empty_string() {
        let mut encoder = CdrEncoder::new();
        encoder.string("").unwrap();
        let data = encoder.data();

        let mut cursor = super::super::cursor::CdrCursor::new(data).unwrap();
        let len = cursor.read_u32().unwrap();
        assert_eq!(len, 1); // Length includes null terminator
        let bytes = cursor.read_bytes(0).unwrap();
        assert_eq!(bytes, b"");
        cursor.read_u8().unwrap(); // null terminator
    }

    #[test]
    fn test_round_trip_unicode_string() {
        let s = "hello 世界 🌍";
        let mut encoder = CdrEncoder::new();
        encoder.string(s).unwrap();
        let data = encoder.data();

        let mut cursor = super::super::cursor::CdrCursor::new(data).unwrap();
        let len = cursor.read_u32().unwrap();
        let bytes = cursor.read_bytes(len as usize - 1).unwrap();
        cursor.read_u8().unwrap(); // null terminator
        assert_eq!(std::str::from_utf8(bytes).unwrap(), s);
    }

    #[test]
    fn test_round_trip_big_endian_int32() {
        let mut encoder = CdrEncoder::with_kind(EncapsulationKind::CdrBe);
        encoder.int32(0x12345678).unwrap();
        let data = encoder.data();

        let mut cursor = super::super::cursor::CdrCursor::new(data).unwrap();
        assert_eq!(cursor.read_i32().unwrap(), 0x12345678);
    }

    // Note: test_round_trip_cdr2_float64_alignment removed because cursor doesn't support CDR2 alignment
    // CDR2 uses 4-byte alignment for 64-bit values, while cursor always uses 8-byte alignment

    #[test]
    fn test_round_trip_multiple_values() {
        let mut encoder = CdrEncoder::new();
        encoder.uint8(1).unwrap();
        encoder.uint16(2).unwrap();
        encoder.uint32(3).unwrap();
        encoder.uint64(4).unwrap();
        encoder.float32(5.0).unwrap();
        encoder.float64(6.0).unwrap();
        encoder.string("test").unwrap();
        let data = encoder.data();

        let mut cursor = super::super::cursor::CdrCursor::new(data).unwrap();
        assert_eq!(cursor.read_u8().unwrap(), 1);
        assert_eq!(cursor.read_u16().unwrap(), 2);
        assert_eq!(cursor.read_u32().unwrap(), 3);
        assert_eq!(cursor.read_u64().unwrap(), 4);
        assert!((cursor.read_f32().unwrap() - 5.0).abs() < f32::EPSILON);
        assert!((cursor.read_f64().unwrap() - 6.0).abs() < f64::EPSILON);
        // Read string
        let len = cursor.read_u32().unwrap();
        let bytes = cursor.read_bytes(len as usize - 1).unwrap();
        cursor.read_u8().unwrap();
        assert_eq!(std::str::from_utf8(bytes).unwrap(), "test");
    }
}
