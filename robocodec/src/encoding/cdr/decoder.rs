// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! CDR (Common Data Representation) decoder implementation.
//!
//! Decodes CDR-encoded binary data using a schema-driven approach.

use std::collections::HashMap;

use crate::core::{CodecError, CodecValue, DecodedMessage, PrimitiveType, Result as CoreResult};
use crate::schema::{FieldType, MessageSchema, PrimitiveType as IdlPrimitiveType};

use super::cursor::{CdrCursor, CDR_HEADER_SIZE};
use super::plan::{DecodeOp, DecodePlan, ElementType};

/// Maximum allowed array length to prevent OOM attacks.
const MAX_ARRAY_LENGTH: usize = 10_000_000;

/// Default CDR alignment for length-prefixed types.
const DEFAULT_CDR_ALIGNMENT: u64 = 4;

/// Nanoseconds per second for time/duration conversion.
const NANOS_PER_SEC: i64 = 1_000_000_000;

/// CDR decoder for ROS1/ROS2 messages.
///
/// Uses schema information to decode CDR-encoded binary data.
pub struct CdrDecoder {
    /// Cached decode plans
    plan_cache: std::sync::Mutex<HashMap<String, DecodePlan>>,
}

impl CdrDecoder {
    /// Create a new CDR decoder.
    pub fn new() -> Self {
        Self {
            plan_cache: std::sync::Mutex::new(HashMap::new()),
        }
    }

    /// Decode a CDR-encoded message.
    ///
    /// # Arguments
    ///
    /// * `schema` - The parsed message schema
    /// * `data` - The CDR-encoded binary data (includes 4-byte header)
    /// * `type_name` - The type name to decode (optional, uses schema name if None)
    pub fn decode(
        &self,
        schema: &MessageSchema,
        data: &[u8],
        type_name: Option<&str>,
    ) -> CoreResult<DecodedMessage> {
        let type_name = type_name.unwrap_or(&schema.name);

        // Get or generate decode plan
        let plan = self.get_or_generate_plan(schema, type_name)?;

        // Create CDR cursor (handles CDR header internally)
        let mut cursor = CdrCursor::new(data)?;

        // Execute the decode plan with the cursor
        self.execute_plan(&plan, &mut cursor, schema)
    }

    /// Decode CDR data without a header (for ROS1 bag messages).
    ///
    /// This is used for ROS1 bag messages which store message data without
    /// the CDR encapsulation header. The cursor starts at position 0 with
    /// origin at 0, since there's no header to skip.
    ///
    /// # Arguments
    ///
    /// * `schema` - The parsed message schema
    /// * `data` - The CDR-encoded binary data WITHOUT 4-byte header
    /// * `type_name` - The type name to decode (optional, uses schema name if None)
    pub fn decode_headerless(
        &self,
        schema: &MessageSchema,
        data: &[u8],
        type_name: Option<&str>,
    ) -> CoreResult<DecodedMessage> {
        let type_name = type_name.unwrap_or(&schema.name);

        // Get or generate decode plan
        let plan = self.get_or_generate_plan(schema, type_name)?;

        // Create cursor for headerless data (ROS1 bag format - no CDR header)
        // ROS1 uses little-endian encoding by default
        let mut cursor = CdrCursor::new_headerless(data, true);

        // Execute the decode plan with the cursor
        self.execute_plan(&plan, &mut cursor, schema)
    }

    /// Decode CDR data with ROS1 bag format handling.
    ///
    /// ROS1 bags have a 12-byte record header followed by CDR data.
    /// The CDR header in ROS1 bags is sometimes incorrectly set to big-endian
    /// even when the data is little-endian, so we override this.
    ///
    /// # Arguments
    ///
    /// * `schema` - The parsed message schema
    /// * `data` - The full message data including 12-byte ROS record header
    /// * `type_name` - The type name to decode (optional, uses schema name if None)
    pub fn decode_ros1(
        &self,
        schema: &MessageSchema,
        data: &[u8],
        type_name: Option<&str>,
    ) -> CoreResult<DecodedMessage> {
        let type_name = type_name.unwrap_or(&schema.name);

        // Skip only the 12-byte ROS record header (4 bytes seq + 8 bytes time)
        // The CDR message starts at offset 12 (including the 4-byte CDR header)
        if data.len() < 16 {
            return Err(CodecError::Other("ROS1 message data too short".to_string()));
        }

        // Get or generate decode plan
        let plan = self.get_or_generate_plan(schema, type_name)?;

        // ROS1 bags have a CDR header at offset 12, but it's often incorrectly
        // set to big-endian. We need to start at offset 16 (after CDR header)
        // but force little-endian encoding.
        let mut cursor = CdrCursor::new_headerless(&data[16..], true);

        // Execute the decode plan with the cursor
        self.execute_plan(&plan, &mut cursor, schema)
    }

    /// Decode CDR data with ROS1 bag format handling, including the CDR header.
    ///
    /// This variant keeps the CDR header at offset 12 and forces little-endian.
    ///
    /// # Arguments
    ///
    /// * `schema` - The parsed message schema
    /// * `data` - The full message data including 12-byte ROS record header
    /// * `type_name` - The type name to decode (optional, uses schema name if None)
    pub fn decode_ros1_with_header(
        &self,
        schema: &MessageSchema,
        data: &[u8],
        type_name: Option<&str>,
    ) -> CoreResult<DecodedMessage> {
        let type_name = type_name.unwrap_or(&schema.name);

        // Skip only the 12-byte ROS record header
        if data.len() < 12 + CDR_HEADER_SIZE {
            return Err(CodecError::Other("ROS1 message data too short".to_string()));
        }

        // Get or generate decode plan
        let plan = self.get_or_generate_plan(schema, type_name)?;

        // Use the new_ros1 method which handles the incorrect big-endian header
        let mut cursor = CdrCursor::new_ros1(&data[12..])?;

        // Execute the decode plan with the cursor
        self.execute_plan(&plan, &mut cursor, schema)
    }

    /// Get a cached plan or generate a new one.
    fn get_or_generate_plan(
        &self,
        schema: &MessageSchema,
        type_name: &str,
    ) -> CoreResult<DecodePlan> {
        // Check cache first
        {
            let cache = self.lock_cache()?;
            if let Some(plan) = cache.get(type_name) {
                return Ok(plan.clone());
            }
        }

        // Generate new plan
        let plan = self.generate_plan(schema, type_name)?;

        // Cache it
        {
            let mut cache = self.lock_cache()?;
            cache.insert(type_name.to_string(), plan.clone());
        }

        Ok(plan)
    }

    /// Lock the plan cache, centralizing error handling.
    fn lock_cache(&self) -> CoreResult<std::sync::MutexGuard<'_, HashMap<String, DecodePlan>>> {
        self.plan_cache
            .lock()
            .map_err(|e| CodecError::Other(format!("Plan cache lock poisoned: {e}")))
    }

    /// Generate a decode plan for a message type.
    fn generate_plan(&self, schema: &MessageSchema, type_name: &str) -> CoreResult<DecodePlan> {
        let msg_type = schema
            .get_type_variants(type_name)
            .ok_or_else(|| CodecError::type_not_found(type_name))?;

        let mut plan = DecodePlan::new(msg_type.name.clone());

        for (idx, field) in msg_type.fields.iter().enumerate() {
            let is_first_field = idx == 0;
            self.generate_field(
                &mut plan,
                schema,
                &field.name,
                &field.type_name,
                "",
                is_first_field,
                true,
            )?;
        }

        Ok(plan)
    }

    /// Generate decode operations for a field.
    #[allow(clippy::too_many_arguments)]
    fn generate_field(
        &self,
        plan: &mut DecodePlan,
        schema: &MessageSchema,
        field_name: &str,
        field_type: &FieldType,
        path_prefix: &str,
        is_first_field: bool,
        parent_is_first_field: bool,
    ) -> CoreResult<()> {
        let field_path = if path_prefix.is_empty() {
            field_name.to_string()
        } else {
            format!("{path_prefix}.{field_name}")
        };

        // Calculate alignment for this field type
        let alignment = self.field_alignment(field_type, schema);

        // Add alignment operation:
        // - For the first field of the top-level message: align to max alignment
        // - For subsequent fields: align to the field's alignment requirement
        // - For first field of a nested type: align to that field's alignment
        if is_first_field && parent_is_first_field {
            // First field of top-level message - align to max alignment
            if alignment > 1 {
                plan.add_op(DecodeOp::Align { alignment });
            }
        } else if !is_first_field || !parent_is_first_field {
            // Subsequent fields OR nested fields - align to field's alignment (CDR spec)
            // This ensures proper padding between fields and alignment for nested types
            plan.add_op(DecodeOp::Align { alignment });
        }

        match field_type {
            FieldType::Primitive(prim) => {
                // String types need special handling (length-prefixed)
                if Self::is_string_type(prim) {
                    plan.add_op(DecodeOp::ReadString { field_path });
                } else if matches!(prim, IdlPrimitiveType::Time) {
                    plan.add_op(DecodeOp::ReadTime { field_path });
                } else if matches!(prim, IdlPrimitiveType::Duration) {
                    plan.add_op(DecodeOp::ReadDuration { field_path });
                } else {
                    let core_prim = prim.to_core();
                    plan.add_op(DecodeOp::ReadPrimitive {
                        field_path,
                        type_name: core_prim,
                    });
                }
            }

            FieldType::Array { base_type, size } => {
                let element_type = self.element_type(base_type.as_ref(), schema)?;

                plan.add_op(DecodeOp::ReadArray {
                    field_path,
                    element_type,
                    count: *size,
                });
            }

            FieldType::Nested(type_name) => {
                plan.add_op(DecodeOp::DecodeNested {
                    field_path: field_path.clone(),
                    type_name: type_name.clone(),
                });

                // Recursively generate nested field operations
                if let Some(nested_type) = schema.get_type_variants(type_name) {
                    for (idx, nested_field) in nested_type.fields.iter().enumerate() {
                        self.generate_field(
                            plan,
                            schema,
                            &nested_field.name,
                            &nested_field.type_name,
                            &field_path,
                            idx == 0,
                            false,
                        )?;
                    }
                    plan.add_op(DecodeOp::EndScope);
                }
            }
        }

        Ok(())
    }

    /// Get the alignment requirement for a field type.
    fn field_alignment(&self, field_type: &FieldType, schema: &MessageSchema) -> u64 {
        match field_type {
            FieldType::Primitive(prim) => {
                let core_prim = prim.to_core();
                core_prim.alignment()
            }
            FieldType::Array { base_type, size } => {
                if size.is_none() {
                    DEFAULT_CDR_ALIGNMENT // Length prefix is 4-byte aligned
                } else {
                    self.element_type(base_type.as_ref(), schema)
                        .map(|t| t.alignment())
                        .unwrap_or(1)
                }
            }
            FieldType::Nested(type_name) => schema
                .get_type_variants(type_name)
                .map(|t| t.max_alignment)
                .unwrap_or(DEFAULT_CDR_ALIGNMENT),
        }
    }

    /// Check if a primitive type is a string type (String or WString).
    fn is_string_type(prim: &IdlPrimitiveType) -> bool {
        matches!(prim, IdlPrimitiveType::String | IdlPrimitiveType::WString)
    }

    /// Get the element type for an array.
    fn element_type(
        &self,
        base_type: &FieldType,
        schema: &MessageSchema,
    ) -> CoreResult<ElementType> {
        match base_type {
            FieldType::Primitive(prim) => {
                // String types need special handling (length-prefixed)
                if Self::is_string_type(prim) {
                    Ok(ElementType::String)
                } else {
                    Ok(ElementType::Primitive(prim.to_core()))
                }
            }
            FieldType::Array { .. } => {
                // Multi-dimensional arrays not yet supported
                Ok(ElementType::Primitive(PrimitiveType::UInt8))
            }
            FieldType::Nested(type_name) => {
                let alignment = schema
                    .get_type_variants(type_name)
                    .map(|t| t.max_alignment)
                    .unwrap_or(DEFAULT_CDR_ALIGNMENT);
                Ok(ElementType::Nested {
                    type_name: type_name.clone(),
                    alignment,
                })
            }
        }
    }

    /// Execute a decode plan.
    fn execute_plan(
        &self,
        plan: &DecodePlan,
        cursor: &mut CdrCursor,
        schema: &MessageSchema,
    ) -> CoreResult<DecodedMessage> {
        let mut result = DecodedMessage::new();
        let mut scope_stack: Vec<String> = Vec::new();
        let mut scope_depth = 0u32;

        for op in &plan.ops {
            match op {
                DecodeOp::Align { alignment } => {
                    cursor.align(*alignment as usize)?;
                }

                DecodeOp::ReadPrimitive {
                    field_path,
                    type_name,
                } => {
                    let value = self.read_primitive(cursor, *type_name)?;
                    self.insert_value(&mut result, &scope_stack, field_path, value)?;
                }

                DecodeOp::ReadString { field_path } => {
                    let value = self.read_string(cursor)?;
                    self.insert_value(&mut result, &scope_stack, field_path, value)?;
                }

                DecodeOp::ReadBytes { field_path } => {
                    let value = self.read_bytes(cursor)?;
                    self.insert_value(&mut result, &scope_stack, field_path, value)?;
                }

                DecodeOp::ReadTime { field_path } => {
                    let value = self.read_time(cursor)?;
                    self.insert_value(&mut result, &scope_stack, field_path, value)?;
                }

                DecodeOp::ReadDuration { field_path } => {
                    let value = self.read_duration(cursor)?;
                    self.insert_value(&mut result, &scope_stack, field_path, value)?;
                }

                DecodeOp::ReadArray {
                    field_path,
                    element_type,
                    count,
                } => {
                    let value = self.read_array(cursor, element_type, *count, schema)?;
                    self.insert_value(&mut result, &scope_stack, field_path, value)?;
                }

                DecodeOp::DecodeNested { field_path, .. } => {
                    // NOTE: In CDR1, alignment is always calculated from the stream origin (byte 4),
                    // NOT from nested struct boundaries. We do NOT push a new origin here.
                    // For CDR2 with XCDR2 encapsulation, origin handling would be different.

                    // Create an empty struct for the nested type
                    if scope_stack.is_empty() {
                        result.insert(
                            field_path.clone(),
                            CodecValue::Struct(std::collections::HashMap::new()),
                        );
                    } else if let Some(parent_path) = scope_stack.last() {
                        if let Some(CodecValue::Struct(parent)) = result.get_mut(parent_path) {
                            let field_name = field_path
                                .strip_prefix(&format!("{parent_path}."))
                                .unwrap_or(field_path);
                            parent.insert(
                                field_name.to_string(),
                                CodecValue::Struct(std::collections::HashMap::new()),
                            );
                        }
                    }
                    scope_stack.push(field_path.clone());
                    scope_depth += 1;
                }

                DecodeOp::EndScope => {
                    if scope_depth == 0 {
                        return Err(CodecError::Other(
                            "Decode plan has mismatched scopes (more EndScope than DecodeNested)"
                                .to_string(),
                        ));
                    }
                    scope_stack.pop();
                    scope_depth -= 1;
                    // NOTE: No origin pop needed for CDR1 - alignment origin stays at stream start
                }
            }
        }

        Ok(result)
    }

    /// Insert a value into the result map at the correct path.
    fn insert_value(
        &self,
        result: &mut DecodedMessage,
        scope_stack: &[String],
        field_path: &str,
        value: CodecValue,
    ) -> CoreResult<()> {
        if scope_stack.is_empty() {
            result.insert(field_path.to_string(), value);
        } else {
            // Find the parent struct
            if let Some(parent_path) = scope_stack.last() {
                if let Some(CodecValue::Struct(parent)) = result.get_mut(parent_path) {
                    // Extract the field name from the full path
                    let field_name = field_path
                        .strip_prefix(&format!("{parent_path}."))
                        .unwrap_or(field_path);
                    parent.insert(field_name.to_string(), value);
                }
            }
        }
        Ok(())
    }

    /// Read a primitive value.
    fn read_primitive(
        &self,
        cursor: &mut CdrCursor,
        type_name: PrimitiveType,
    ) -> CoreResult<CodecValue> {
        match type_name {
            PrimitiveType::Bool => {
                let v = cursor.read_u8()?;
                Ok(CodecValue::Bool(v != 0))
            }
            PrimitiveType::Int8 => {
                let v = cursor.read_i8()?;
                Ok(CodecValue::Int8(v))
            }
            PrimitiveType::Int16 => {
                let v = cursor.read_i16()?;
                Ok(CodecValue::Int16(v))
            }
            PrimitiveType::Int32 => {
                let v = cursor.read_i32()?;
                Ok(CodecValue::Int32(v))
            }
            PrimitiveType::Int64 => {
                let v = cursor.read_i64()?;
                Ok(CodecValue::Int64(v))
            }
            PrimitiveType::UInt8 => {
                let v = cursor.read_u8()?;
                Ok(CodecValue::UInt8(v))
            }
            PrimitiveType::UInt16 => {
                let v = cursor.read_u16()?;
                Ok(CodecValue::UInt16(v))
            }
            PrimitiveType::UInt32 => {
                let v = cursor.read_u32()?;
                Ok(CodecValue::UInt32(v))
            }
            PrimitiveType::UInt64 => {
                let v = cursor.read_u64()?;
                Ok(CodecValue::UInt64(v))
            }
            PrimitiveType::Float32 => {
                let v = cursor.read_f32()?;
                Ok(CodecValue::Float32(v))
            }
            PrimitiveType::Float64 => {
                let v = cursor.read_f64()?;
                Ok(CodecValue::Float64(v))
            }
            PrimitiveType::String => {
                // String is handled separately
                Ok(CodecValue::String(String::new()))
            }
            PrimitiveType::Byte => {
                let v = cursor.read_u8()?;
                Ok(CodecValue::UInt8(v))
            }
        }
    }

    /// Read a string value (matches TS CdrReader.string()).
    fn read_string(&self, cursor: &mut CdrCursor) -> CoreResult<CodecValue> {
        // Read length prefix (4 bytes)
        let len = cursor.read_u32()? as usize;

        if len > MAX_ARRAY_LENGTH {
            return Err(CodecError::Other(format!(
                "String length {len} exceeds maximum allowed {MAX_ARRAY_LENGTH}"
            )));
        }

        if len <= 1 {
            // Empty string (length 0 or 1 for just null terminator)
            cursor.skip(len)?;
            return Ok(CodecValue::String(String::new()));
        }

        // Read string data (len - 1 to exclude null terminator)
        let string_bytes = cursor.read_bytes(len - 1)?;
        let s = std::str::from_utf8(string_bytes)
            .map_err(|e| CodecError::parse("string utf8", format!("{e}")))?
            .to_string();

        // Skip null terminator
        let _ = cursor.read_u8();

        Ok(CodecValue::String(s))
    }

    /// Read a bytes value.
    fn read_bytes(&self, cursor: &mut CdrCursor) -> CoreResult<CodecValue> {
        // Read length prefix (4 bytes)
        let len = cursor.read_u32()? as usize;

        if len > MAX_ARRAY_LENGTH {
            return Err(CodecError::Other(format!(
                "Bytes length {len} exceeds maximum allowed {MAX_ARRAY_LENGTH}"
            )));
        }

        let bytes = cursor.read_bytes(len)?;
        Ok(CodecValue::Bytes(bytes.to_vec()))
    }

    /// Read a ROS time value (sec:int32, nsec:uint32).
    ///
    /// Returns the time as nanoseconds since Unix epoch.
    /// For time fields, we need to align to 4 bytes, read sec as int32,
    /// then read nsec as uint32.
    fn read_time(&self, cursor: &mut CdrCursor) -> CoreResult<CodecValue> {
        // Align to 4 bytes (time fields are 4-byte aligned)
        cursor.align(4)?;

        // Read sec (int32)
        let sec = cursor.read_i32()? as i64;

        // Read nsec (uint32) - already aligned after sec
        let nsec = cursor.read_u32()? as i64;

        // Convert to nanoseconds since Unix epoch
        // Timestamp in nanoseconds = sec * 1e9 + nsec
        let nanos = sec.saturating_mul(NANOS_PER_SEC).saturating_add(nsec);
        Ok(CodecValue::Timestamp(nanos))
    }

    /// Read a ROS duration value (sec:int32, nsec:uint32).
    ///
    /// Returns the duration as nanoseconds (can be negative).
    /// For duration fields, the sec field is signed, so we need to handle
    /// negative durations properly.
    fn read_duration(&self, cursor: &mut CdrCursor) -> CoreResult<CodecValue> {
        // Align to 4 bytes (duration fields are 4-byte aligned)
        cursor.align(4)?;

        // Read sec (int32, can be negative)
        let sec = cursor.read_i32()? as i64;

        // Read nsec (uint32) - always positive
        let nsec = cursor.read_u32()? as i64;

        // Convert to nanoseconds
        // For positive durations: nanos = sec * 1e9 + nsec
        // For negative durations: nanos = sec * 1e9 - nsec (nsec is stored as positive)
        let nanos = if sec < 0 {
            sec.saturating_mul(NANOS_PER_SEC).saturating_sub(nsec)
        } else {
            sec.saturating_mul(NANOS_PER_SEC).saturating_add(nsec)
        };
        Ok(CodecValue::Duration(nanos))
    }

    /// Read a nested type field value recursively.
    ///
    /// This handles arbitrary nesting depth by recursively decoding
    /// all fields of the nested type, including nested types within arrays.
    fn read_nested_type_field(
        &self,
        cursor: &mut CdrCursor,
        type_name: &str,
        schema: &MessageSchema,
    ) -> CoreResult<CodecValue> {
        let nested_type = schema
            .get_type_variants(type_name)
            .ok_or_else(|| CodecError::type_not_found(type_name))?;

        let mut fields = HashMap::new();
        for field in &nested_type.fields {
            let field_value = match &field.type_name {
                FieldType::Primitive(prim) => {
                    if Self::is_string_type(prim) {
                        self.read_string(cursor)?
                    } else {
                        self.read_primitive(cursor, prim.to_core())?
                    }
                }
                FieldType::Array { base_type, size } => {
                    let elem_type = self.element_type(base_type, schema)?;
                    self.read_array(cursor, &elem_type, *size, schema)?
                }
                FieldType::Nested(nested_name) => {
                    // Recursively decode the nested type
                    self.read_nested_type_field(cursor, nested_name, schema)?
                }
            };
            fields.insert(field.name.clone(), field_value);
        }

        Ok(CodecValue::Struct(fields))
    }

    /// Read an array value.
    fn read_array(
        &self,
        cursor: &mut CdrCursor,
        element_type: &ElementType,
        fixed_count: Option<usize>,
        schema: &MessageSchema,
    ) -> CoreResult<CodecValue> {
        let mut values = Vec::new();

        // For primitive arrays, use a fast path that reads directly without per-element alignment
        if let ElementType::Primitive(prim) = element_type {
            // For primitive arrays, elements are stored contiguously.
            // The sequence length is 4-byte aligned (CDR spec), not element-size aligned.
            // Read the 4-byte length first, then data follows (optionally aligned).

            // Read length prefix (for dynamic arrays)
            let len = match fixed_count {
                Some(n) => n,
                None => {
                    let raw_len = cursor.read_u32()? as usize;
                    if raw_len > MAX_ARRAY_LENGTH {
                        return Err(CodecError::Other(format!(
                            "Array length {raw_len} exceeds maximum allowed {MAX_ARRAY_LENGTH}"
                        )));
                    }
                    raw_len
                }
            };

            values.reserve(len.min(1024));

            // For multi-byte primitives in ROS2, the data needs to be aligned to the element's size
            // ROS1 stores primitive array elements contiguously without this alignment
            let elem_size = prim.size().unwrap_or(1);
            if elem_size > 1 && len > 0 && !cursor.is_ros1() {
                cursor.align(elem_size)?;
            }

            // Read all elements contiguously (no per-element alignment for primitive arrays)
            for _ in 0..len {
                let value = match prim {
                    PrimitiveType::Bool => {
                        let v = cursor.read_u8()?;
                        CodecValue::Bool(v != 0)
                    }
                    PrimitiveType::Int8 => CodecValue::Int8(cursor.read_i8()?),
                    PrimitiveType::Int16 => CodecValue::Int16(cursor.read_i16()?),
                    PrimitiveType::Int32 => CodecValue::Int32(cursor.read_i32()?),
                    PrimitiveType::Int64 => CodecValue::Int64(cursor.read_i64()?),
                    PrimitiveType::UInt8 => CodecValue::UInt8(cursor.read_u8()?),
                    PrimitiveType::UInt16 => CodecValue::UInt16(cursor.read_u16()?),
                    PrimitiveType::UInt32 => CodecValue::UInt32(cursor.read_u32()?),
                    PrimitiveType::UInt64 => CodecValue::UInt64(cursor.read_u64()?),
                    PrimitiveType::Float32 => CodecValue::Float32(cursor.read_f32_unaligned()?),
                    PrimitiveType::Float64 => CodecValue::Float64(cursor.read_f64_unaligned()?),
                    PrimitiveType::String => unreachable!("String arrays use String element type"),
                    PrimitiveType::Byte => CodecValue::UInt8(cursor.read_u8()?),
                };
                values.push(value);
            }
        } else {
            // Read length prefix (for dynamic arrays)
            let len = match fixed_count {
                Some(n) => n,
                None => {
                    let raw_len = cursor.read_u32()? as usize;
                    if raw_len > MAX_ARRAY_LENGTH {
                        return Err(CodecError::Other(format!(
                            "Array length {raw_len} exceeds maximum allowed {MAX_ARRAY_LENGTH}"
                        )));
                    }
                    raw_len
                }
            };

            values.reserve(len.min(1024));

            // For complex types (strings, nested), align before each element
            for _ in 0..len {
                let value = match element_type {
                    ElementType::Primitive(prim) => self.read_primitive(cursor, *prim)?,
                    ElementType::String => self.read_string(cursor)?,
                    ElementType::Bytes => self.read_bytes(cursor)?,
                    ElementType::Nested { type_name, .. } => {
                        // Recursively decode nested type (handles arbitrary depth)
                        self.read_nested_type_field(cursor, type_name, schema)?
                    }
                };
                values.push(value);
            }
        }

        Ok(CodecValue::Array(values))
    }
}

impl Default for CdrDecoder {
    fn default() -> Self {
        Self::new()
    }
}

impl crate::Decoder for CdrDecoder {
    /// Decode CDR data using a schema string.
    ///
    /// This implementation parses the schema string into a MessageSchema
    /// and delegates to the native decode method. For high-frequency use,
    /// consider parsing the schema once with `parse_schema()` and using
    /// `CdrDecoder::decode()` directly.
    fn decode(
        &self,
        data: &[u8],
        schema: &str,
        type_name: Option<&str>,
    ) -> CoreResult<DecodedMessage> {
        // Parse schema string to MessageSchema
        let parsed_schema = crate::schema::parse_schema("dynamic", schema)?;

        // Delegate to native decode method
        self.decode(&parsed_schema, data, type_name)
    }
}

// Helper to convert IDL primitive types
#[allow(dead_code)]
trait ToCorePrimitive {
    fn to_core(self) -> PrimitiveType;
}

impl ToCorePrimitive for IdlPrimitiveType {
    fn to_core(self) -> PrimitiveType {
        match self {
            IdlPrimitiveType::Bool => PrimitiveType::Bool,
            IdlPrimitiveType::Int8 => PrimitiveType::Int8,
            IdlPrimitiveType::Int16 => PrimitiveType::Int16,
            IdlPrimitiveType::Int32 => PrimitiveType::Int32,
            IdlPrimitiveType::Int64 => PrimitiveType::Int64,
            IdlPrimitiveType::UInt8 => PrimitiveType::UInt8,
            IdlPrimitiveType::UInt16 => PrimitiveType::UInt16,
            IdlPrimitiveType::UInt32 => PrimitiveType::UInt32,
            IdlPrimitiveType::UInt64 => PrimitiveType::UInt64,
            IdlPrimitiveType::Float32 => PrimitiveType::Float32,
            IdlPrimitiveType::Float64 => PrimitiveType::Float64,
            IdlPrimitiveType::String | IdlPrimitiveType::WString => PrimitiveType::String,
            IdlPrimitiveType::Byte | IdlPrimitiveType::Char => PrimitiveType::Byte,
            IdlPrimitiveType::Time | IdlPrimitiveType::Duration => PrimitiveType::Int64, // Fallback
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::parse_schema;

    #[test]
    fn test_decode_int32() {
        let schema = parse_schema("TestMsg", "int32 value").unwrap();
        let decoder = CdrDecoder::new();
        // CDR header: [0x00, 0x01, 0x00, 0x00] + data
        let mut data = vec![0x00, 0x01, 0x00, 0x00]; // CDR header (little-endian)
        data.extend_from_slice(&42i32.to_le_bytes()); // value

        let result = decoder.decode(&schema, &data, None).unwrap();
        assert_eq!(result.get("value"), Some(&CodecValue::Int32(42)));
    }

    #[test]
    fn test_decode_multiple_fields() {
        let schema = parse_schema("TestMsg", "int32 x\nint32 y").unwrap();
        let decoder = CdrDecoder::new();
        let mut data = vec![0x00, 0x01, 0x00, 0x00]; // CDR header
        data.extend_from_slice(&10i32.to_le_bytes());
        data.extend_from_slice(&20i32.to_le_bytes());

        let result = decoder.decode(&schema, &data, None).unwrap();
        assert_eq!(result.get("x"), Some(&CodecValue::Int32(10)));
        assert_eq!(result.get("y"), Some(&CodecValue::Int32(20)));
    }

    #[test]
    fn test_decode_string() {
        let schema = parse_schema("TestMsg", "string data").unwrap();
        let decoder = CdrDecoder::new();
        let mut data = vec![0x00, 0x01, 0x00, 0x00]; // CDR header
        data.extend_from_slice(&6i32.to_le_bytes()); // length = 6 ("hello" + null)
        data.extend_from_slice(b"hello");
        data.push(0); // null terminator

        let result = decoder.decode(&schema, &data, None).unwrap();
        assert_eq!(
            result.get("data"),
            Some(&CodecValue::String("hello".to_string()))
        );
    }

    #[test]
    fn test_decode_string_with_null_terminator() {
        let schema = parse_schema("TestMsg", "string data").unwrap();
        let decoder = CdrDecoder::new();
        let mut data = vec![0x00, 0x01, 0x00, 0x00]; // CDR header
        data.extend_from_slice(&6i32.to_le_bytes()); // length = 6 ("hello" + null)
        data.extend_from_slice(b"hello");
        data.push(0); // null terminator
        data.extend_from_slice(&42u32.to_le_bytes()); // padding to 4-byte boundary
        data.extend_from_slice(&0u32.to_le_bytes()); // extra padding

        let result = decoder.decode(&schema, &data, None).unwrap();
        assert_eq!(
            result.get("data"),
            Some(&CodecValue::String("hello".to_string()))
        );
    }

    #[test]
    fn test_decode_dynamic_array() {
        let schema = parse_schema("TestMsg", "int32[] values").unwrap();
        let decoder = CdrDecoder::new();
        let mut data = vec![0x00, 0x01, 0x00, 0x00]; // CDR header
        data.extend_from_slice(&3i32.to_le_bytes()); // length = 3
        data.extend_from_slice(&1i32.to_le_bytes());
        data.extend_from_slice(&2i32.to_le_bytes());
        data.extend_from_slice(&3i32.to_le_bytes());

        let result = decoder.decode(&schema, &data, None).unwrap();
        if let Some(CodecValue::Array(arr)) = result.get("values") {
            assert_eq!(arr.len(), 3);
            assert_eq!(arr[0], CodecValue::Int32(1));
            assert_eq!(arr[1], CodecValue::Int32(2));
            assert_eq!(arr[2], CodecValue::Int32(3));
        } else {
            panic!("Expected array");
        }
    }

    #[test]
    fn test_decode_fixed_array() {
        let schema = parse_schema("TestMsg", "float32[3] position").unwrap();
        let decoder = CdrDecoder::new();
        let mut data = vec![0x00, 0x01, 0x00, 0x00]; // CDR header
        data.extend_from_slice(&1.0f32.to_le_bytes());
        data.extend_from_slice(&2.0f32.to_le_bytes());
        data.extend_from_slice(&3.0f32.to_le_bytes());

        let result = decoder.decode(&schema, &data, None).unwrap();
        if let Some(CodecValue::Array(arr)) = result.get("position") {
            assert_eq!(arr.len(), 3);
        } else {
            panic!("Expected array");
        }
    }

    #[test]
    fn test_decode_nested_message() {
        let schema = parse_schema("OuterMsg", "string frame_id\nuint32 seq").unwrap();
        let decoder = CdrDecoder::new();

        let mut data = vec![0x00, 0x01, 0x00, 0x00]; // CDR header
                                                     // frame_id (string)
        data.extend_from_slice(&10i32.to_le_bytes()); // length = 10 ("base_link" + null)
        data.extend_from_slice(b"base_link");
        data.push(0); // null terminator
                      // Padding to align uint32 to 4-byte boundary
                      // After string: 4 (header) + 4 (length) + 10 (data + null) = 18 bytes
                      // Need 2 bytes padding to reach 20 (4-byte aligned)
        data.push(0);
        data.push(0);
        // seq (uint32 at 20-byte boundary)
        data.extend_from_slice(&42u32.to_le_bytes());

        let result = decoder.decode(&schema, &data, None).unwrap();
        assert_eq!(
            result.get("frame_id"),
            Some(&CodecValue::String("base_link".to_string()))
        );
        assert_eq!(result.get("seq"), Some(&CodecValue::UInt32(42)));
    }

    #[test]
    fn test_plan_caching() {
        let schema = parse_schema("TestMsg", "int32 value").unwrap();
        let decoder = CdrDecoder::new();

        // First call generates plan
        let mut data = vec![0x00, 0x01, 0x00, 0x00]; // CDR header
        data.extend_from_slice(&42i32.to_le_bytes());
        let _ = decoder.decode(&schema, &data, None).unwrap();

        // Second call should use cached plan
        let mut data2 = vec![0x00, 0x01, 0x00, 0x00]; // CDR header
        data2.extend_from_slice(&43i32.to_le_bytes());
        let result = decoder.decode(&schema, &data2, None).unwrap();
        assert_eq!(result.get("value"), Some(&CodecValue::Int32(43)));
    }

    #[test]
    fn test_decode_string_too_long() {
        let schema = parse_schema("TestMsg", "string data").unwrap();
        let decoder = CdrDecoder::new();
        let mut data = vec![0x00, 0x01, 0x00, 0x00]; // CDR header
        data.extend_from_slice(&0xFFu32.to_le_bytes()); // length = max u32
                                                        // Don't add actual data - the bounds check should fail first

        let result = decoder.decode(&schema, &data, None);
        assert!(result.is_err());
    }

    #[test]
    fn test_decode_array_too_long() {
        let schema = parse_schema("TestMsg", "int32[] values").unwrap();
        let decoder = CdrDecoder::new();
        let mut data = vec![0x00, 0x01, 0x00, 0x00]; // CDR header
        data.extend_from_slice(&0xFFu32.to_le_bytes()); // length > MAX_ARRAY_LENGTH (when sign-extended)

        let result = decoder.decode(&schema, &data, None);
        assert!(result.is_err());
    }
}
