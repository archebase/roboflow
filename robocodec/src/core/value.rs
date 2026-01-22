// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Codec value type system.
//!
//! Provides a unified value representation for decoded messages from CDR,
//! Protobuf, and JSON formats. All variants are serde-serializable.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Type alias for decoded message as field name -> value mapping.
pub type DecodedMessage = HashMap<String, CodecValue>;

/// Unified value type for decoded robotics data.
///
/// This enum represents values that can be decoded from CDR (ROS1/ROS2),
/// Protobuf, or JSON message formats. It is serde-serializable and designed
/// for easy conversion to other value types.
///
/// # Design Principles
///
/// - **Serde support**: All variants are serializable for downstream processing
/// - **Owned types**: Uses owned `String` and `Vec<u8>` for clarity and simplicity
/// - **Codec-focused**: Emphasizes type operations over domain-specific semantics
/// - **Comprehensive**: Covers all robotics data types including temporal types
///
/// # Memory Layout
///
/// The enum uses a discriminant (1 byte) plus the largest variant size.
/// For containers (Array, Struct), the HashMap/Vec dominates memory usage.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum CodecValue {
    // Boolean
    Bool(bool),

    // Signed integers
    Int8(i8),
    Int16(i16),
    Int32(i32),
    Int64(i64),

    // Unsigned integers
    UInt8(u8),
    UInt16(u16),
    UInt32(u32),
    UInt64(u64),

    // Floating point
    Float32(f32),
    Float64(f64),

    // String (UTF-8)
    String(String),

    // Binary data (image frames, point clouds, serialized messages)
    Bytes(Vec<u8>),

    // Timestamp as nanoseconds since Unix epoch
    /// Can represent dates from 1677-09-21 to 2262-04-11 with nanosecond precision
    Timestamp(i64),

    // Duration as nanoseconds (can be negative)
    Duration(i64),

    // Array of values
    Array(Vec<CodecValue>),

    // Nested message/struct
    Struct(DecodedMessage),

    // Null value for optional fields
    Null,
}

impl CodecValue {
    // ========================================================================
    // Type Checking Predicates
    // ========================================================================

    /// Check if this value is a numeric type (integers or floats).
    pub fn is_numeric(&self) -> bool {
        matches!(
            self,
            CodecValue::Int8(_)
                | CodecValue::Int16(_)
                | CodecValue::Int32(_)
                | CodecValue::Int64(_)
                | CodecValue::UInt8(_)
                | CodecValue::UInt16(_)
                | CodecValue::UInt32(_)
                | CodecValue::UInt64(_)
                | CodecValue::Float32(_)
                | CodecValue::Float64(_)
        )
    }

    /// Check if this value is an integer type (signed or unsigned).
    pub fn is_integer(&self) -> bool {
        matches!(
            self,
            CodecValue::Int8(_)
                | CodecValue::Int16(_)
                | CodecValue::Int32(_)
                | CodecValue::Int64(_)
                | CodecValue::UInt8(_)
                | CodecValue::UInt16(_)
                | CodecValue::UInt32(_)
                | CodecValue::UInt64(_)
        )
    }

    /// Check if this value is a signed integer.
    pub fn is_signed_integer(&self) -> bool {
        matches!(
            self,
            CodecValue::Int8(_)
                | CodecValue::Int16(_)
                | CodecValue::Int32(_)
                | CodecValue::Int64(_)
        )
    }

    /// Check if this value is an unsigned integer.
    pub fn is_unsigned_integer(&self) -> bool {
        matches!(
            self,
            CodecValue::UInt8(_)
                | CodecValue::UInt16(_)
                | CodecValue::UInt32(_)
                | CodecValue::UInt64(_)
        )
    }

    /// Check if this value is a floating-point type.
    pub fn is_float(&self) -> bool {
        matches!(self, CodecValue::Float32(_) | CodecValue::Float64(_))
    }

    /// Check if this value is a temporal type (timestamp or duration).
    pub fn is_temporal(&self) -> bool {
        matches!(self, CodecValue::Timestamp(_) | CodecValue::Duration(_))
    }

    /// Check if this value is a container type (array or struct).
    pub fn is_container(&self) -> bool {
        matches!(self, CodecValue::Array(_) | CodecValue::Struct(_))
    }

    /// Check if this value is null.
    pub fn is_null(&self) -> bool {
        matches!(self, CodecValue::Null)
    }

    // ========================================================================
    // Type Conversion Methods
    // ========================================================================

    /// Try to convert this value to f64 (for numeric values only).
    pub fn as_f64(&self) -> Option<f64> {
        match self {
            CodecValue::Int8(v) => Some(*v as f64),
            CodecValue::Int16(v) => Some(*v as f64),
            CodecValue::Int32(v) => Some(*v as f64),
            CodecValue::Int64(v) => Some(*v as f64),
            CodecValue::UInt8(v) => Some(*v as f64),
            CodecValue::UInt16(v) => Some(*v as f64),
            CodecValue::UInt32(v) => Some(*v as f64),
            CodecValue::UInt64(v) => Some(*v as f64),
            CodecValue::Float32(v) => Some(*v as f64),
            CodecValue::Float64(v) => Some(*v),
            _ => None,
        }
    }

    /// Try to convert this value to i64 (for integer types only).
    pub fn as_i64(&self) -> Option<i64> {
        match self {
            CodecValue::Int8(v) => Some(*v as i64),
            CodecValue::Int16(v) => Some(*v as i64),
            CodecValue::Int32(v) => Some(*v as i64),
            CodecValue::Int64(v) => Some(*v),
            CodecValue::UInt8(v) => Some(*v as i64),
            CodecValue::UInt16(v) => Some(*v as i64),
            CodecValue::UInt32(v) => Some(*v as i64),
            CodecValue::UInt64(v) => {
                if *v <= i64::MAX as u64 {
                    Some(*v as i64)
                } else {
                    None
                }
            }
            _ => None,
        }
    }

    /// Try to convert this value to u64 (for unsigned integer types only).
    pub fn as_u64(&self) -> Option<u64> {
        match self {
            CodecValue::UInt8(v) => Some(*v as u64),
            CodecValue::UInt16(v) => Some(*v as u64),
            CodecValue::UInt32(v) => Some(*v as u64),
            CodecValue::UInt64(v) => Some(*v),
            CodecValue::Int8(v) => {
                if *v >= 0 {
                    Some(*v as u64)
                } else {
                    None
                }
            }
            CodecValue::Int16(v) => {
                if *v >= 0 {
                    Some(*v as u64)
                } else {
                    None
                }
            }
            CodecValue::Int32(v) => {
                if *v >= 0 {
                    Some(*v as u64)
                } else {
                    None
                }
            }
            CodecValue::Int64(v) => {
                if *v >= 0 {
                    Some(*v as u64)
                } else {
                    None
                }
            }
            _ => None,
        }
    }

    /// Try to get the inner string value.
    pub fn as_str(&self) -> Option<&str> {
        match self {
            CodecValue::String(s) => Some(s),
            _ => None,
        }
    }

    /// Try to get the inner bytes.
    pub fn as_bytes(&self) -> Option<&[u8]> {
        match self {
            CodecValue::Bytes(b) => Some(b),
            _ => None,
        }
    }

    /// Try to get the inner struct.
    pub fn as_struct(&self) -> Option<&DecodedMessage> {
        match self {
            CodecValue::Struct(s) => Some(s),
            _ => None,
        }
    }

    /// Try to get a mutable reference to the inner struct.
    pub fn as_struct_mut(&mut self) -> Option<&mut DecodedMessage> {
        match self {
            CodecValue::Struct(s) => Some(s),
            _ => None,
        }
    }

    /// Try to get the inner array.
    pub fn as_array(&self) -> Option<&[CodecValue]> {
        match self {
            CodecValue::Array(arr) => Some(arr),
            _ => None,
        }
    }

    /// Try to get a mutable reference to the inner array.
    pub fn as_array_mut(&mut self) -> Option<&mut Vec<CodecValue>> {
        match self {
            CodecValue::Array(arr) => Some(arr),
            _ => None,
        }
    }

    /// Get the timestamp value as nanoseconds.
    pub fn as_timestamp_nanos(&self) -> Option<i64> {
        match self {
            CodecValue::Timestamp(nanos) => Some(*nanos),
            _ => None,
        }
    }

    /// Get the duration value as nanoseconds.
    pub fn as_duration_nanos(&self) -> Option<i64> {
        match self {
            CodecValue::Duration(nanos) => Some(*nanos),
            _ => None,
        }
    }

    // ========================================================================
    // Codec-Specific Helpers
    // ========================================================================

    /// Get the type name of this value as a string.
    pub fn type_name(&self) -> &'static str {
        match self {
            CodecValue::Bool(_) => "bool",
            CodecValue::Int8(_) => "int8",
            CodecValue::Int16(_) => "int16",
            CodecValue::Int32(_) => "int32",
            CodecValue::Int64(_) => "int64",
            CodecValue::UInt8(_) => "uint8",
            CodecValue::UInt16(_) => "uint16",
            CodecValue::UInt32(_) => "uint32",
            CodecValue::UInt64(_) => "uint64",
            CodecValue::Float32(_) => "float32",
            CodecValue::Float64(_) => "float64",
            CodecValue::String(_) => "string",
            CodecValue::Bytes(_) => "bytes",
            CodecValue::Timestamp(_) => "timestamp",
            CodecValue::Duration(_) => "duration",
            CodecValue::Array(_) => "array",
            CodecValue::Struct(_) => "struct",
            CodecValue::Null => "null",
        }
    }

    /// Estimate the in-memory size of this value in bytes.
    ///
    /// This is an approximation for memory usage tracking.
    /// Does not include HashMap overhead for structs.
    pub fn size_hint(&self) -> usize {
        match self {
            CodecValue::Bool(_) | CodecValue::Int8(_) | CodecValue::UInt8(_) => 1,
            CodecValue::Int16(_) | CodecValue::UInt16(_) => 2,
            CodecValue::Int32(_) | CodecValue::UInt32(_) | CodecValue::Float32(_) => 4,
            CodecValue::Int64(_) | CodecValue::UInt64(_) | CodecValue::Float64(_) => 8,
            CodecValue::Timestamp(_) | CodecValue::Duration(_) => 8,
            CodecValue::String(s) => s.len(),
            CodecValue::Bytes(b) => b.len(),
            CodecValue::Null => 0,
            CodecValue::Array(arr) => {
                arr.iter().map(|v| v.size_hint()).sum::<usize>() + (arr.len() * 8)
            }
            CodecValue::Struct(map) => map.values().map(|v| v.size_hint()).sum::<usize>(),
        }
    }

    // ========================================================================
    // Convenience Constructors
    // ========================================================================

    /// Create a timestamp from seconds and nanoseconds (unsigned).
    ///
    /// Common in ROS1 time representation.
    pub fn timestamp_from_secs_nanos(secs: u32, nanos: u32) -> Self {
        let total_nanos = (secs as i64) * 1_000_000_000 + (nanos as i64);
        CodecValue::Timestamp(total_nanos)
    }

    /// Create a timestamp from signed seconds and unsigned nanoseconds.
    ///
    /// Common in ROS2 time representation (builtin_interfaces/Time).
    pub fn timestamp_from_signed_secs_nanos(secs: i32, nanos: u32) -> Self {
        let total_nanos = (secs as i64) * 1_000_000_000 + (nanos as i64);
        CodecValue::Timestamp(total_nanos)
    }

    /// Create a duration from signed seconds and nanoseconds.
    ///
    /// Supports negative durations.
    pub fn duration_from_secs_nanos(secs: i32, nanos: i32) -> Self {
        let total_nanos = (secs as i64) * 1_000_000_000 + (nanos as i64);
        CodecValue::Duration(total_nanos)
    }

    // ========================================================================
    // ROS-Specific Convenience Methods (matching strata-core's API)
    // ========================================================================

    /// Create a Timestamp from ROS1 time (secs: u32, nsecs: u32).
    ///
    /// ROS1 time uses unsigned 32-bit seconds and nanoseconds.
    pub fn from_ros1_time(secs: u32, nsecs: u32) -> Self {
        Self::timestamp_from_secs_nanos(secs, nsecs)
    }

    /// Create a Timestamp from ROS2 Time (sec: i32, nanosec: u32).
    ///
    /// ROS2 builtin_interfaces/Time uses signed 32-bit seconds
    /// and unsigned 32-bit nanoseconds.
    pub fn from_ros2_time(sec: i32, nanosec: u32) -> Self {
        Self::timestamp_from_signed_secs_nanos(sec, nanosec)
    }

    /// Create a Duration from ROS1 duration (secs: i32, nsecs: i32).
    ///
    /// ROS1 duration uses signed 32-bit seconds and nanoseconds.
    pub fn from_ros1_duration(secs: i32, nsecs: i32) -> Self {
        Self::duration_from_secs_nanos(secs, nsecs)
    }

    /// Create a Duration from ROS2 Duration (sec: i32, nanosec: u32).
    ///
    /// ROS2 builtin_interfaces/Duration uses signed 32-bit seconds
    /// and unsigned 32-bit nanoseconds.
    pub fn from_ros2_duration(sec: i32, nanosec: u32) -> Self {
        let total_nanos = (sec as i64) * 1_000_000_000 + (nanosec as i64);
        CodecValue::Duration(total_nanos)
    }
}

impl fmt::Display for CodecValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            CodecValue::Bool(v) => write!(f, "{v}"),
            CodecValue::Int8(v) => write!(f, "{v}"),
            CodecValue::Int16(v) => write!(f, "{v}"),
            CodecValue::Int32(v) => write!(f, "{v}"),
            CodecValue::Int64(v) => write!(f, "{v}"),
            CodecValue::UInt8(v) => write!(f, "{v}"),
            CodecValue::UInt16(v) => write!(f, "{v}"),
            CodecValue::UInt32(v) => write!(f, "{v}"),
            CodecValue::UInt64(v) => write!(f, "{v}"),
            CodecValue::Float32(v) => write!(f, "{v}"),
            CodecValue::Float64(v) => write!(f, "{v}"),
            CodecValue::String(v) => write!(f, "\"{v}\""),
            CodecValue::Bytes(v) => write!(f, "<{} bytes>", v.len()),
            CodecValue::Timestamp(v) => write!(f, "Timestamp({v}ns)"),
            CodecValue::Duration(v) => write!(f, "Duration({v}ns)"),
            CodecValue::Array(v) => write!(f, "[{} elements]", v.len()),
            CodecValue::Struct(v) => write!(f, "{{{} fields}}", v.len()),
            CodecValue::Null => write!(f, "null"),
        }
    }
}

// Use std::fmt instead of bare fmt
use std::fmt;

// =============================================================================
// Primitive Type Enum
// =============================================================================

/// Primitive type identifiers for codec schemas.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum PrimitiveType {
    /// Boolean
    Bool,
    /// 8-bit signed integer
    Int8,
    /// 16-bit signed integer
    Int16,
    /// 32-bit signed integer
    Int32,
    /// 64-bit signed integer
    Int64,
    /// 8-bit unsigned integer
    UInt8,
    /// 16-bit unsigned integer
    UInt16,
    /// 32-bit unsigned integer
    UInt32,
    /// 64-bit unsigned integer
    UInt64,
    /// 32-bit float
    Float32,
    /// 64-bit float
    Float64,
    /// String
    String,
    /// Byte (alias for UInt8)
    Byte,
}

impl PrimitiveType {
    /// Get the alignment requirement for this primitive type in bytes.
    pub const fn alignment(self) -> u64 {
        match self {
            PrimitiveType::Bool
            | PrimitiveType::Int8
            | PrimitiveType::UInt8
            | PrimitiveType::Byte => 1,
            PrimitiveType::Int16 | PrimitiveType::UInt16 => 2,
            PrimitiveType::Int32 | PrimitiveType::UInt32 | PrimitiveType::Float32 => 4,
            PrimitiveType::Int64 | PrimitiveType::UInt64 | PrimitiveType::Float64 => 8,
            PrimitiveType::String => 4, // Length prefix is 4-byte aligned
        }
    }

    /// Get the size in bytes for this primitive type, if fixed.
    pub const fn size(self) -> Option<usize> {
        match self {
            PrimitiveType::Bool => Some(1),
            PrimitiveType::Int8 | PrimitiveType::UInt8 | PrimitiveType::Byte => Some(1),
            PrimitiveType::Int16 | PrimitiveType::UInt16 => Some(2),
            PrimitiveType::Int32 | PrimitiveType::UInt32 | PrimitiveType::Float32 => Some(4),
            PrimitiveType::Int64 | PrimitiveType::UInt64 | PrimitiveType::Float64 => Some(8),
            PrimitiveType::String => None, // Variable length
        }
    }

    /// Parse a primitive type from a string.
    pub fn try_from_str(s: &str) -> Option<Self> {
        match s {
            "bool" => Some(PrimitiveType::Bool),
            "int8" => Some(PrimitiveType::Int8),
            "int16" => Some(PrimitiveType::Int16),
            "int32" => Some(PrimitiveType::Int32),
            "int64" => Some(PrimitiveType::Int64),
            "uint8" => Some(PrimitiveType::UInt8),
            "uint16" => Some(PrimitiveType::UInt16),
            "uint32" => Some(PrimitiveType::UInt32),
            "uint64" => Some(PrimitiveType::UInt64),
            "float32" => Some(PrimitiveType::Float32),
            "float64" => Some(PrimitiveType::Float64),
            "string" => Some(PrimitiveType::String),
            "byte" | "char" => Some(PrimitiveType::Byte),
            _ => None,
        }
    }
}

impl fmt::Display for PrimitiveType {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            PrimitiveType::Bool => write!(f, "bool"),
            PrimitiveType::Int8 => write!(f, "int8"),
            PrimitiveType::Int16 => write!(f, "int16"),
            PrimitiveType::Int32 => write!(f, "int32"),
            PrimitiveType::Int64 => write!(f, "int64"),
            PrimitiveType::UInt8 => write!(f, "uint8"),
            PrimitiveType::UInt16 => write!(f, "uint16"),
            PrimitiveType::UInt32 => write!(f, "uint32"),
            PrimitiveType::UInt64 => write!(f, "uint64"),
            PrimitiveType::Float32 => write!(f, "float32"),
            PrimitiveType::Float64 => write!(f, "float64"),
            PrimitiveType::String => write!(f, "string"),
            PrimitiveType::Byte => write!(f, "byte"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_type_checking() {
        assert!(CodecValue::Int32(42).is_numeric());
        assert!(CodecValue::Int32(42).is_integer());
        assert!(CodecValue::Float64(2.5).is_numeric());
        assert!(CodecValue::Float64(2.5).is_float());
        assert!(!CodecValue::Float64(2.5).is_integer());
        assert!(!CodecValue::String("hello".to_string()).is_numeric());
        assert!(CodecValue::Timestamp(123456).is_temporal());
        assert!(CodecValue::Duration(-1000).is_temporal());
        assert!(CodecValue::Null.is_null());
    }

    #[test]
    fn test_as_f64() {
        assert_eq!(CodecValue::Int32(42).as_f64(), Some(42.0));
        assert_eq!(CodecValue::Float32(2.5).as_f64(), Some(2.5f32 as f64));
        assert_eq!(CodecValue::String("hello".to_string()).as_f64(), None);
    }

    #[test]
    fn test_as_i64() {
        assert_eq!(CodecValue::Int32(42).as_i64(), Some(42));
        assert_eq!(CodecValue::UInt32(42).as_i64(), Some(42));
        assert_eq!(CodecValue::Float64(2.5).as_i64(), None);
    }

    #[test]
    fn test_timestamp_constructors() {
        let ts = CodecValue::timestamp_from_secs_nanos(1704067200, 500_000_000);
        assert_eq!(ts.as_timestamp_nanos(), Some(1_704_067_200_500_000_000));

        let ts2 = CodecValue::timestamp_from_signed_secs_nanos(-1, 999_999_999);
        assert_eq!(
            ts2.as_timestamp_nanos(),
            Some(-1_000_000_000i64 + 999_999_999i64)
        );
    }

    #[test]
    fn test_size_hint() {
        assert_eq!(CodecValue::Int32(42).size_hint(), 4);
        assert_eq!(CodecValue::String("hello".to_string()).size_hint(), 5);
        assert_eq!(CodecValue::Bytes(vec![1, 2, 3]).size_hint(), 3);
        assert_eq!(CodecValue::Null.size_hint(), 0);
    }

    #[test]
    fn test_primitive_type_alignment() {
        assert_eq!(PrimitiveType::Bool.alignment(), 1);
        assert_eq!(PrimitiveType::Int16.alignment(), 2);
        assert_eq!(PrimitiveType::Int32.alignment(), 4);
        assert_eq!(PrimitiveType::Int64.alignment(), 8);
        assert_eq!(PrimitiveType::String.alignment(), 4);
    }

    #[test]
    fn test_primitive_type_size() {
        assert_eq!(PrimitiveType::Bool.size(), Some(1));
        assert_eq!(PrimitiveType::Int32.size(), Some(4));
        assert_eq!(PrimitiveType::String.size(), None);
    }

    #[test]
    fn test_primitive_type_from_str() {
        assert_eq!(
            PrimitiveType::try_from_str("int32"),
            Some(PrimitiveType::Int32)
        );
        assert_eq!(
            PrimitiveType::try_from_str("float64"),
            Some(PrimitiveType::Float64)
        );
        assert_eq!(PrimitiveType::try_from_str("unknown"), None);
    }

    #[test]
    fn test_serialization() {
        let value = CodecValue::Int32(42);
        let json = serde_json::to_string(&value).unwrap();
        let decoded: CodecValue = serde_json::from_str(&json).unwrap();
        assert_eq!(decoded, value);
    }
}
