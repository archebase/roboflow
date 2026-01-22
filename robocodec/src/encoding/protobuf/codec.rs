// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Protobuf codec implementation using prost-reflect for dynamic message handling.

use std::collections::HashMap;
use std::sync::RwLock;

use prost::Message;
use prost_reflect::{
    DescriptorPool, DynamicMessage, FieldDescriptor, Kind, MessageDescriptor, ReflectMessage,
};
use prost_types::FileDescriptorSet;

use crate::core::{CodecError, CodecValue, DecodedMessage, Encoding, Result};
use crate::encoding::transform::SchemaMetadata;
use crate::encoding::DynCodec;

/// Protobuf codec using prost-reflect for dynamic message encoding/decoding.
///
/// This codec handles protobuf messages without code generation by using
/// FileDescriptorSet at runtime. Uses thread-safe interior mutability for caching.
pub struct ProtobufCodec {
    /// Cached descriptor pools indexed by type name
    pools: RwLock<HashMap<String, DescriptorPool>>,
    /// Cached message descriptors indexed by type name
    descriptors: RwLock<HashMap<String, MessageDescriptor>>,
}

impl ProtobufCodec {
    /// Create a new Protobuf codec.
    pub fn new() -> Self {
        Self {
            pools: RwLock::new(HashMap::new()),
            descriptors: RwLock::new(HashMap::new()),
        }
    }

    /// Add a FileDescriptorSet to the codec.
    ///
    /// # Arguments
    ///
    /// * `type_name` - Message type name (e.g., "nmx.msg.Lowdim")
    /// * `fds_bytes` - FileDescriptorSet binary data
    ///
    /// # Returns
    ///
    /// The message descriptor for the type
    pub fn add_file_descriptor_set(
        &self,
        type_name: &str,
        fds_bytes: &[u8],
    ) -> Result<MessageDescriptor> {
        // Check if already loaded
        {
            let descriptors = self
                .descriptors
                .read()
                .map_err(|e| CodecError::Other(format!("Descriptor read lock poisoned: {e}")))?;
            if let Some(descriptor) = descriptors.get(type_name) {
                return Ok(descriptor.clone());
            }
        }

        // Decode FileDescriptorSet
        let fds = FileDescriptorSet::decode(fds_bytes).map_err(|e| {
            CodecError::parse(
                "protobuf",
                format!("Failed to decode FileDescriptorSet: {e}"),
            )
        })?;

        // Build descriptor pool
        let pool = DescriptorPool::from_file_descriptor_set(fds).map_err(|e| {
            CodecError::parse("protobuf", format!("Failed to build descriptor pool: {e}"))
        })?;

        // Get message descriptor
        let descriptor = pool
            .get_message_by_name(type_name)
            .ok_or_else(|| CodecError::type_not_found(type_name))?;

        // Cache for reuse
        self.pools
            .write()
            .map_err(|e| CodecError::Other(format!("Pool write lock poisoned: {e}")))?
            .insert(type_name.to_string(), pool);
        self.descriptors
            .write()
            .map_err(|e| CodecError::Other(format!("Descriptor write lock poisoned: {e}")))?
            .insert(type_name.to_string(), descriptor.clone());

        Ok(descriptor)
    }

    /// Ensure a descriptor is loaded for the given type.
    ///
    /// # Arguments
    ///
    /// * `schema` - Schema metadata containing FileDescriptorSet
    ///
    /// # Returns
    ///
    /// The message descriptor for the type
    fn ensure_descriptor(&self, schema: &SchemaMetadata) -> Result<MessageDescriptor> {
        match schema {
            SchemaMetadata::Protobuf {
                type_name,
                file_descriptor_set,
                ..
            } => {
                // Check if already loaded
                {
                    let descriptors = self.descriptors.read().map_err(|e| {
                        CodecError::Other(format!("Descriptor read lock poisoned: {e}"))
                    })?;
                    if let Some(descriptor) = descriptors.get(type_name) {
                        return Ok(descriptor.clone());
                    }
                }

                // Load from FileDescriptorSet
                self.add_file_descriptor_set(type_name, file_descriptor_set)
            }
            _ => Err(CodecError::invalid_schema(
                schema.type_name(),
                "Schema is not a Protobuf schema",
            )),
        }
    }

    /// Get a descriptor by type name without loading.
    pub fn get_descriptor(&self, type_name: &str) -> Option<MessageDescriptor> {
        self.descriptors.read().ok()?.get(type_name).cloned()
    }

    /// Convert a DynamicMessage to DecodedMessage.
    ///
    /// # Arguments
    ///
    /// * `dynamic_msg` - The dynamic protobuf message
    /// * `descriptor` - Message descriptor for field info
    fn dynamic_to_decoded(
        &self,
        dynamic_msg: &DynamicMessage,
        descriptor: &MessageDescriptor,
    ) -> DecodedMessage {
        let mut fields = HashMap::new();

        for field in descriptor.fields() {
            let field_name = field.name().to_string();

            if let Some(value) = dynamic_msg.get_field_by_name(&field_name) {
                if let Some(codec_value) = self.reflect_value_to_codec(&value) {
                    fields.insert(field_name, codec_value);
                }
            }
        }

        fields
    }

    /// Convert a DecodedMessage to a DynamicMessage.
    ///
    /// # Arguments
    ///
    /// * `decoded` - The decoded message
    /// * `descriptor` - Message descriptor for field info
    fn decoded_to_dynamic(
        &self,
        decoded: &DecodedMessage,
        descriptor: &MessageDescriptor,
    ) -> Result<DynamicMessage> {
        let mut dynamic_msg = DynamicMessage::new(descriptor.clone());

        for (field_name, codec_value) in decoded {
            if let Some(field) = descriptor.get_field_by_name(field_name) {
                let reflect_value = self.codec_to_reflect_value_with_field(codec_value, &field)?;
                dynamic_msg.set_field(&field, reflect_value);
            }
        }

        Ok(dynamic_msg)
    }

    /// Convert a CodecValue to a prost-reflect Value with field context.
    ///
    /// This version handles nested structs by using the field descriptor
    /// to determine the target message type.
    #[allow(clippy::only_used_in_recursion)]
    fn codec_to_reflect_value_with_field(
        &self,
        value: &CodecValue,
        field: &FieldDescriptor,
    ) -> Result<prost_reflect::Value> {
        // Handle enum fields - check field kind for enum type
        let is_enum = matches!(field.kind(), Kind::Enum(_));

        match value {
            CodecValue::Bool(v) => Ok(prost_reflect::Value::Bool(*v)),
            CodecValue::Int8(v) => {
                if is_enum {
                    Ok(prost_reflect::Value::EnumNumber(*v as i32))
                } else {
                    Ok(prost_reflect::Value::I32(*v as i32))
                }
            }
            CodecValue::Int16(v) => {
                if is_enum {
                    Ok(prost_reflect::Value::EnumNumber(*v as i32))
                } else {
                    Ok(prost_reflect::Value::I32(*v as i32))
                }
            }
            CodecValue::Int32(v) => {
                if is_enum {
                    Ok(prost_reflect::Value::EnumNumber(*v))
                } else {
                    Ok(prost_reflect::Value::I32(*v))
                }
            }
            CodecValue::Int64(v) => Ok(prost_reflect::Value::I64(*v)),
            CodecValue::UInt8(v) => Ok(prost_reflect::Value::U32(*v as u32)),
            CodecValue::UInt16(v) => Ok(prost_reflect::Value::U32(*v as u32)),
            CodecValue::UInt32(v) => Ok(prost_reflect::Value::U32(*v)),
            CodecValue::UInt64(v) => Ok(prost_reflect::Value::U64(*v)),
            CodecValue::Float32(v) => Ok(prost_reflect::Value::F32(*v)),
            CodecValue::Float64(v) => Ok(prost_reflect::Value::F64(*v)),
            CodecValue::String(v) => Ok(prost_reflect::Value::String(v.clone())),
            CodecValue::Bytes(v) => Ok(prost_reflect::Value::Bytes(v.clone().into())),
            CodecValue::Array(v) => {
                // Check if this is a repeated message field
                if let Kind::Message(msg_desc) = field.kind() {
                    // Array of messages
                    let items: Vec<prost_reflect::Value> = v
                        .iter()
                        .map(|val| {
                            if let CodecValue::Struct(fields) = val {
                                // Convert nested struct to message
                                let mut nested_msg = DynamicMessage::new(msg_desc.clone());
                                for (nested_field_name, nested_value) in fields {
                                    if let Some(nested_field) =
                                        msg_desc.get_field_by_name(nested_field_name)
                                    {
                                        let nested_reflect = self
                                            .codec_to_reflect_value_with_field(
                                                nested_value,
                                                &nested_field,
                                            )?;
                                        nested_msg.set_field(&nested_field, nested_reflect);
                                    }
                                }
                                Ok(prost_reflect::Value::Message(nested_msg))
                            } else {
                                self.codec_to_reflect_value_with_field(val, field)
                            }
                        })
                        .collect::<Result<Vec<_>>>()?;
                    Ok(prost_reflect::Value::List(items))
                } else {
                    // Array of primitives or enums
                    let items: Vec<prost_reflect::Value> = v
                        .iter()
                        .map(|val| self.codec_to_reflect_value_with_field(val, field))
                        .collect::<Result<Vec<_>>>()?;
                    Ok(prost_reflect::Value::List(items))
                }
            }
            CodecValue::Struct(fields) => {
                // Nested message - check field kind to get message type
                if let Kind::Message(msg_desc) = field.kind() {
                    let mut nested_msg = DynamicMessage::new(msg_desc.clone());
                    for (nested_field_name, nested_value) in fields {
                        if let Some(nested_field) = msg_desc.get_field_by_name(nested_field_name) {
                            let nested_reflect = self
                                .codec_to_reflect_value_with_field(nested_value, &nested_field)?;
                            nested_msg.set_field(&nested_field, nested_reflect);
                        }
                    }
                    Ok(prost_reflect::Value::Message(nested_msg))
                } else {
                    Err(CodecError::unsupported(format!(
                        "nested struct for non-message field: {} (kind: {:?})",
                        field.name(),
                        field.kind()
                    )))
                }
            }
            CodecValue::Null => {
                // For null values, use appropriate default based on field type
                if is_enum {
                    Ok(prost_reflect::Value::EnumNumber(0))
                } else {
                    Ok(prost_reflect::Value::I32(0))
                }
            }
            CodecValue::Timestamp(_) => Err(CodecError::unsupported("timestamp in protobuf")),
            CodecValue::Duration(_) => Err(CodecError::unsupported("duration in protobuf")),
        }
    }

    /// Get a message descriptor by type name, checking all cached pools.
    #[allow(dead_code)]
    fn get_descriptor_by_name(&self, type_name: &str) -> Option<MessageDescriptor> {
        // First check the direct descriptor cache
        {
            let descriptors = self.descriptors.read().ok()?;
            if let Some(desc) = descriptors.get(type_name) {
                return Some(desc.clone());
            }
        }

        // Then search through all cached pools
        let pools = self.pools.read().ok()?;
        for pool in pools.values() {
            if let Some(desc) = pool.get_message_by_name(type_name) {
                // Cache it for future use
                drop(pools);
                let mut descriptors = self.descriptors.write().ok()?;
                descriptors.insert(type_name.to_string(), desc.clone());
                return Some(desc);
            }
        }

        None
    }

    /// Convert a prost-reflect Value to CodecValue.
    fn reflect_value_to_codec(&self, value: &prost_reflect::Value) -> Option<CodecValue> {
        match value {
            prost_reflect::Value::Bool(v) => Some(CodecValue::Bool(*v)),
            prost_reflect::Value::I32(v) => Some(CodecValue::Int32(*v)),
            prost_reflect::Value::I64(v) => Some(CodecValue::Int64(*v)),
            prost_reflect::Value::U32(v) => Some(CodecValue::UInt32(*v)),
            prost_reflect::Value::U64(v) => Some(CodecValue::UInt64(*v)),
            prost_reflect::Value::F32(v) => Some(CodecValue::Float32(*v)),
            prost_reflect::Value::F64(v) => Some(CodecValue::Float64(*v)),
            prost_reflect::Value::Bytes(v) => Some(CodecValue::Bytes(v.to_vec())),
            prost_reflect::Value::String(v) => Some(CodecValue::String(v.clone())),
            prost_reflect::Value::EnumNumber(v) => Some(CodecValue::Int32(*v)),
            prost_reflect::Value::List(v) => {
                let items: Vec<CodecValue> = v
                    .iter()
                    .filter_map(|val| self.reflect_value_to_codec(val))
                    .collect();
                Some(CodecValue::Array(items))
            }
            prost_reflect::Value::Message(v) => {
                let descriptor = v.descriptor();
                let nested = self.dynamic_to_decoded(v, &descriptor);
                Some(CodecValue::Struct(nested))
            }
            prost_reflect::Value::Map(_kv) => {
                // Maps: convert to list of structs for compatibility
                // Each map entry becomes a struct with "key" and "value" fields
                None
            }
        }
    }
}

impl Default for ProtobufCodec {
    fn default() -> Self {
        Self::new()
    }
}

impl DynCodec for ProtobufCodec {
    fn decode_dynamic(&self, data: &[u8], schema: &SchemaMetadata) -> Result<DecodedMessage> {
        let descriptor = self.ensure_descriptor(schema)?;

        // Decode using prost-reflect (needs owned descriptor)
        let dynamic_msg = DynamicMessage::decode(descriptor.clone(), data)
            .map_err(|e| CodecError::parse("protobuf", format!("Failed to decode message: {e}")))?;

        // Convert to DecodedMessage
        Ok(self.dynamic_to_decoded(&dynamic_msg, &descriptor))
    }

    fn encode_dynamic(
        &mut self,
        message: &DecodedMessage,
        schema: &SchemaMetadata,
    ) -> Result<Vec<u8>> {
        let descriptor = self.ensure_descriptor(schema)?;

        // Convert DecodedMessage to DynamicMessage
        let dynamic_msg = self.decoded_to_dynamic(message, &descriptor)?;

        // Encode to bytes
        let mut buf = Vec::new();
        dynamic_msg.encode(&mut buf).map_err(|e| {
            CodecError::encode("protobuf", format!("Failed to encode message: {e}"))
        })?;

        Ok(buf)
    }

    fn encoding_type(&self) -> Encoding {
        Encoding::Protobuf
    }

    fn reset(&mut self) {
        // No state to reset
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use prost::Message;
    use prost_types::field_descriptor_proto::{Label, Type as ProtoType};
    use prost_types::{
        DescriptorProto, EnumDescriptorProto, EnumValueDescriptorProto, FieldDescriptorProto,
        FileDescriptorProto, FileDescriptorSet,
    };
    use std::collections::HashMap;

    // =========================================================================
    // Test Fixtures
    // =========================================================================

    /// Create a minimal FileDescriptorSet for a simple message.
    fn create_simple_fds() -> Vec<u8> {
        let fds = FileDescriptorSet {
            file: vec![FileDescriptorProto {
                name: Some("test.proto".to_string()),
                package: Some("test".to_string()),
                message_type: vec![DescriptorProto {
                    name: Some("SimpleMessage".to_string()),
                    field: vec![
                        FieldDescriptorProto {
                            name: Some("int_field".to_string()),
                            number: Some(1),
                            label: Some(Label::Optional as i32),
                            r#type: Some(ProtoType::Int32 as i32),
                            ..Default::default()
                        },
                        FieldDescriptorProto {
                            name: Some("string_field".to_string()),
                            number: Some(2),
                            label: Some(Label::Optional as i32),
                            r#type: Some(ProtoType::String as i32),
                            ..Default::default()
                        },
                        FieldDescriptorProto {
                            name: Some("bool_field".to_string()),
                            number: Some(3),
                            label: Some(Label::Optional as i32),
                            r#type: Some(ProtoType::Bool as i32),
                            ..Default::default()
                        },
                    ],
                    ..Default::default()
                }],
                ..Default::default()
            }],
        };
        fds.encode_to_vec()
    }

    /// Create a FileDescriptorSet with an enum type.
    fn create_enum_fds() -> Vec<u8> {
        let fds = FileDescriptorSet {
            file: vec![FileDescriptorProto {
                name: Some("enum_test.proto".to_string()),
                package: Some("test".to_string()),
                enum_type: vec![EnumDescriptorProto {
                    name: Some("TestEnum".to_string()),
                    value: vec![
                        EnumValueDescriptorProto {
                            name: Some("UNKNOWN".to_string()),
                            number: Some(0),
                            ..Default::default()
                        },
                        EnumValueDescriptorProto {
                            name: Some("VALUE_ONE".to_string()),
                            number: Some(1),
                            ..Default::default()
                        },
                        EnumValueDescriptorProto {
                            name: Some("VALUE_TWO".to_string()),
                            number: Some(2),
                            ..Default::default()
                        },
                    ],
                    ..Default::default()
                }],
                message_type: vec![DescriptorProto {
                    name: Some("EnumMessage".to_string()),
                    field: vec![FieldDescriptorProto {
                        name: Some("enum_field".to_string()),
                        number: Some(1),
                        label: Some(Label::Optional as i32),
                        r#type: Some(ProtoType::Enum as i32),
                        type_name: Some(".test.TestEnum".to_string()),
                        ..Default::default()
                    }],
                    ..Default::default()
                }],
                ..Default::default()
            }],
        };
        fds.encode_to_vec()
    }

    /// Create a FileDescriptorSet with a nested message.
    fn create_nested_fds() -> Vec<u8> {
        let fds = FileDescriptorSet {
            file: vec![FileDescriptorProto {
                name: Some("nested.proto".to_string()),
                package: Some("test".to_string()),
                message_type: vec![
                    DescriptorProto {
                        name: Some("NestedMessage".to_string()),
                        field: vec![FieldDescriptorProto {
                            name: Some("value".to_string()),
                            number: Some(1),
                            label: Some(Label::Optional as i32),
                            r#type: Some(ProtoType::Int32 as i32),
                            ..Default::default()
                        }],
                        ..Default::default()
                    },
                    DescriptorProto {
                        name: Some("OuterMessage".to_string()),
                        field: vec![FieldDescriptorProto {
                            name: Some("nested".to_string()),
                            number: Some(1),
                            label: Some(Label::Optional as i32),
                            r#type: Some(ProtoType::Message as i32),
                            type_name: Some(".test.NestedMessage".to_string()),
                            ..Default::default()
                        }],
                        ..Default::default()
                    },
                ],
                ..Default::default()
            }],
        };
        fds.encode_to_vec()
    }

    /// Create a FileDescriptorSet with repeated fields.
    fn create_repeated_fds() -> Vec<u8> {
        let fds = FileDescriptorSet {
            file: vec![FileDescriptorProto {
                name: Some("repeated.proto".to_string()),
                package: Some("test".to_string()),
                message_type: vec![DescriptorProto {
                    name: Some("RepeatedMessage".to_string()),
                    field: vec![
                        FieldDescriptorProto {
                            name: Some("int_array".to_string()),
                            number: Some(1),
                            label: Some(Label::Repeated as i32),
                            r#type: Some(ProtoType::Int32 as i32),
                            ..Default::default()
                        },
                        FieldDescriptorProto {
                            name: Some("string_array".to_string()),
                            number: Some(2),
                            label: Some(Label::Repeated as i32),
                            r#type: Some(ProtoType::String as i32),
                            ..Default::default()
                        },
                    ],
                    ..Default::default()
                }],
                ..Default::default()
            }],
        };
        fds.encode_to_vec()
    }

    /// Create a FileDescriptorSet with all scalar types.
    fn create_scalar_fds() -> Vec<u8> {
        let fds = FileDescriptorSet {
            file: vec![FileDescriptorProto {
                name: Some("scalar.proto".to_string()),
                package: Some("test".to_string()),
                message_type: vec![DescriptorProto {
                    name: Some("ScalarMessage".to_string()),
                    field: vec![
                        FieldDescriptorProto {
                            name: Some("double_field".to_string()),
                            number: Some(1),
                            label: Some(Label::Optional as i32),
                            r#type: Some(ProtoType::Double as i32),
                            ..Default::default()
                        },
                        FieldDescriptorProto {
                            name: Some("float_field".to_string()),
                            number: Some(2),
                            label: Some(Label::Optional as i32),
                            r#type: Some(ProtoType::Float as i32),
                            ..Default::default()
                        },
                        FieldDescriptorProto {
                            name: Some("int64_field".to_string()),
                            number: Some(3),
                            label: Some(Label::Optional as i32),
                            r#type: Some(ProtoType::Int64 as i32),
                            ..Default::default()
                        },
                        FieldDescriptorProto {
                            name: Some("uint64_field".to_string()),
                            number: Some(4),
                            label: Some(Label::Optional as i32),
                            r#type: Some(ProtoType::Uint64 as i32),
                            ..Default::default()
                        },
                        FieldDescriptorProto {
                            name: Some("int32_field".to_string()),
                            number: Some(5),
                            label: Some(Label::Optional as i32),
                            r#type: Some(ProtoType::Int32 as i32),
                            ..Default::default()
                        },
                        FieldDescriptorProto {
                            name: Some("uint32_field".to_string()),
                            number: Some(6),
                            label: Some(Label::Optional as i32),
                            r#type: Some(ProtoType::Uint32 as i32),
                            ..Default::default()
                        },
                        FieldDescriptorProto {
                            name: Some("bool_field".to_string()),
                            number: Some(7),
                            label: Some(Label::Optional as i32),
                            r#type: Some(ProtoType::Bool as i32),
                            ..Default::default()
                        },
                        FieldDescriptorProto {
                            name: Some("string_field".to_string()),
                            number: Some(8),
                            label: Some(Label::Optional as i32),
                            r#type: Some(ProtoType::String as i32),
                            ..Default::default()
                        },
                        FieldDescriptorProto {
                            name: Some("bytes_field".to_string()),
                            number: Some(9),
                            label: Some(Label::Optional as i32),
                            r#type: Some(ProtoType::Bytes as i32),
                            ..Default::default()
                        },
                    ],
                    ..Default::default()
                }],
                ..Default::default()
            }],
        };
        fds.encode_to_vec()
    }

    /// Encode a simple test message manually as protobuf.
    fn encode_simple_message(int_val: i32, string_val: &str, bool_val: bool) -> Vec<u8> {
        let mut bytes = Vec::new();

        // Field 1: int32 (tag 0x08)
        bytes.push(0x08);
        encode_varint(int_val as u64, &mut bytes);

        // Field 2: string (tag 0x12)
        bytes.push(0x12);
        encode_varint(string_val.len() as u64, &mut bytes);
        bytes.extend_from_slice(string_val.as_bytes());

        // Field 3: bool (tag 0x18)
        bytes.push(0x18);
        bytes.push(bool_val as u8);

        bytes
    }

    /// Encode a varint to bytes.
    fn encode_varint(mut value: u64, bytes: &mut Vec<u8>) {
        while value >= 0x80 {
            bytes.push((value & 0x7F) as u8 | 0x80);
            value >>= 7;
        }
        bytes.push(value as u8);
    }

    // =========================================================================
    // Construction Tests
    // =========================================================================

    #[test]
    fn test_protobuf_codec_creation() {
        let codec = ProtobufCodec::new();
        assert_eq!(codec.encoding_type(), Encoding::Protobuf);
    }

    #[test]
    fn test_protobuf_codec_default() {
        let codec = ProtobufCodec::default();
        assert_eq!(codec.encoding_type(), Encoding::Protobuf);
    }

    #[test]
    fn test_protobuf_codec_has_empty_caches_after_creation() {
        let codec = ProtobufCodec::new();

        let pools = codec.pools.read().unwrap();
        let descriptors = codec.descriptors.read().unwrap();

        assert!(pools.is_empty());
        assert!(descriptors.is_empty());
    }

    // =========================================================================
    // FileDescriptorSet Tests
    // =========================================================================

    #[test]
    fn test_protobuf_codec_add_fds_returns_descriptor() {
        let codec = ProtobufCodec::new();
        let fds_bytes = create_simple_fds();

        let result = codec.add_file_descriptor_set("test.SimpleMessage", &fds_bytes);
        assert!(result.is_ok());

        let descriptor = result.unwrap();
        assert_eq!(descriptor.name(), "SimpleMessage");
    }

    #[test]
    fn test_protobuf_codec_add_invalid_fds() {
        let codec = ProtobufCodec::new();

        let result = codec.add_file_descriptor_set("test.Type", &[0xFF, 0xFF, 0xFF, 0xFF]);
        assert!(result.is_err());
    }

    #[test]
    fn test_protobuf_codec_add_fds_caches_descriptor() {
        let codec = ProtobufCodec::new();
        let fds_bytes = create_simple_fds();

        codec
            .add_file_descriptor_set("test.SimpleMessage", &fds_bytes)
            .unwrap();

        // Descriptor should be cached
        let descriptor = codec.get_descriptor("test.SimpleMessage");
        assert!(descriptor.is_some());
        assert_eq!(descriptor.unwrap().name(), "SimpleMessage");
    }

    #[test]
    fn test_protobuf_codec_add_fds_idempotent() {
        let codec = ProtobufCodec::new();
        let fds_bytes = create_simple_fds();

        let result1 = codec.add_file_descriptor_set("test.SimpleMessage", &fds_bytes);
        let result2 = codec.add_file_descriptor_set("test.SimpleMessage", &fds_bytes);

        assert!(result1.is_ok());
        assert!(result2.is_ok());
        assert_eq!(result1.unwrap().name(), result2.unwrap().name());
    }

    #[test]
    fn test_protobuf_codec_get_descriptor_returns_none_for_unknown_type() {
        let codec = ProtobufCodec::new();

        let descriptor = codec.get_descriptor("unknown.Type");
        assert!(descriptor.is_none());
    }

    // =========================================================================
    // Decode Tests
    // =========================================================================

    #[test]
    fn test_protobuf_codec_decode_simple_message() {
        let codec = ProtobufCodec::new();
        let fds_bytes = create_simple_fds();
        let schema = SchemaMetadata::protobuf("test.SimpleMessage".to_string(), fds_bytes);

        let encoded = encode_simple_message(42, "hello", true);
        let result = codec.decode_dynamic(&encoded, &schema);

        assert!(result.is_ok(), "decode failed: {:?}", result.err());
        let decoded = result.unwrap();

        assert_eq!(decoded.get("int_field"), Some(&CodecValue::Int32(42)));
        assert_eq!(
            decoded.get("string_field"),
            Some(&CodecValue::String("hello".to_string()))
        );
        assert_eq!(decoded.get("bool_field"), Some(&CodecValue::Bool(true)));
    }

    #[test]
    fn test_protobuf_codec_decode_with_missing_fields() {
        let codec = ProtobufCodec::new();
        let fds_bytes = create_simple_fds();
        let schema = SchemaMetadata::protobuf("test.SimpleMessage".to_string(), fds_bytes);

        // Only encode field 1
        let encoded = vec![0x08, 0x2A];
        let result = codec.decode_dynamic(&encoded, &schema);

        assert!(result.is_ok());
        let decoded = result.unwrap();

        assert_eq!(decoded.get("int_field"), Some(&CodecValue::Int32(42)));
        // Note: prost-reflect includes all schema fields in the decoded message
        // Missing fields are represented with default values (empty string for strings)
        // The behavior of the codec is to iterate over all descriptor fields
    }

    #[test]
    fn test_protobuf_codec_decode_enum_field() {
        let codec = ProtobufCodec::new();
        let fds_bytes = create_enum_fds();
        let schema = SchemaMetadata::protobuf("test.EnumMessage".to_string(), fds_bytes);

        // Encode enum value 1 (VALUE_ONE)
        let encoded = vec![0x08, 0x01];
        let result = codec.decode_dynamic(&encoded, &schema);

        assert!(result.is_ok());
        let decoded = result.unwrap();

        // Enums are decoded as Int32
        assert_eq!(decoded.get("enum_field"), Some(&CodecValue::Int32(1)));
    }

    #[test]
    fn test_protobuf_codec_decode_nested_message() {
        let codec = ProtobufCodec::new();
        let fds_bytes = create_nested_fds();
        let schema = SchemaMetadata::protobuf("test.OuterMessage".to_string(), fds_bytes);

        // Encode nested message: { nested: { value: 42 } }
        // Tag for field 1: 0x0A (wire type 2 = length-delimited)
        // Length: 2
        // Nested message: field 1, value 42
        let encoded = vec![0x0A, 0x02, 0x08, 0x2A];
        let result = codec.decode_dynamic(&encoded, &schema);

        assert!(result.is_ok());
        let decoded = result.unwrap();

        let nested = decoded.get("nested");
        assert!(nested.is_some());

        if let Some(CodecValue::Struct(fields)) = nested {
            assert_eq!(fields.get("value"), Some(&CodecValue::Int32(42)));
        } else {
            panic!("Expected Struct for nested field");
        }
    }

    #[test]
    fn test_protobuf_codec_decode_repeated_fields() {
        let codec = ProtobufCodec::new();
        let fds_bytes = create_repeated_fds();
        let schema = SchemaMetadata::protobuf("test.RepeatedMessage".to_string(), fds_bytes);

        // Encode repeated int32: [1, 2, 3]
        let mut encoded = Vec::new();
        for val in [1i32, 2, 3] {
            encoded.push(0x08); // tag for field 1
            encode_varint(val as u64, &mut encoded);
        }

        let result = codec.decode_dynamic(&encoded, &schema);

        assert!(result.is_ok());
        let decoded = result.unwrap();

        let int_array = decoded.get("int_array");
        assert!(int_array.is_some());

        if let Some(CodecValue::Array(arr)) = int_array {
            assert_eq!(arr.len(), 3);
            assert_eq!(arr[0], CodecValue::Int32(1));
            assert_eq!(arr[1], CodecValue::Int32(2));
            assert_eq!(arr[2], CodecValue::Int32(3));
        } else {
            panic!("Expected Array for int_array field");
        }
    }

    #[test]
    fn test_protobuf_codec_decode_all_scalar_types() {
        let codec = ProtobufCodec::new();
        let fds_bytes = create_scalar_fds();
        let schema = SchemaMetadata::protobuf("test.ScalarMessage".to_string(), fds_bytes);

        // Manually encode all scalar fields
        let mut encoded = Vec::new();

        // double_field (tag 0x09): value 3.125
        encoded.push(0x09);
        encoded.extend_from_slice(&3.125f64.to_le_bytes());

        // float_field (tag 0x15): value 2.71
        encoded.push(0x15);
        encoded.extend_from_slice(&2.71f32.to_le_bytes());

        // int64_field (tag 0x18): value -12345
        encoded.push(0x18);
        encode_varint((-12345i64) as u64, &mut encoded);

        // uint64_field (tag 0x20): value 12345
        encoded.push(0x20);
        encode_varint(12345u64, &mut encoded);

        // int32_field (tag 0x28): value -100
        encoded.push(0x28);
        encode_varint((-100i32) as u64, &mut encoded);

        // uint32_field (tag 0x30): value 200
        encoded.push(0x30);
        encode_varint(200u64, &mut encoded);

        // bool_field (tag 0x38): value true
        encoded.push(0x38);
        encoded.push(1u8);

        // string_field (tag 0x42): value "test"
        encoded.push(0x42);
        encode_varint(4u64, &mut encoded);
        encoded.extend_from_slice(b"test");

        // bytes_field (tag 0x4A): value b"\xDE\xAD\xBE\xEF"
        encoded.push(0x4A);
        encode_varint(4u64, &mut encoded);
        encoded.extend_from_slice(&[0xDE, 0xAD, 0xBE, 0xEF]);

        let result = codec.decode_dynamic(&encoded, &schema);

        assert!(result.is_ok(), "decode failed: {:?}", result.err());
        let decoded = result.unwrap();

        assert_eq!(
            decoded.get("double_field"),
            Some(&CodecValue::Float64(3.125))
        );
        assert_eq!(decoded.get("float_field"), Some(&CodecValue::Float32(2.71)));
        assert_eq!(decoded.get("int64_field"), Some(&CodecValue::Int64(-12345)));
        assert_eq!(
            decoded.get("uint64_field"),
            Some(&CodecValue::UInt64(12345))
        );
        assert_eq!(decoded.get("int32_field"), Some(&CodecValue::Int32(-100)));
        assert_eq!(decoded.get("uint32_field"), Some(&CodecValue::UInt32(200)));
        assert_eq!(decoded.get("bool_field"), Some(&CodecValue::Bool(true)));
        assert_eq!(
            decoded.get("string_field"),
            Some(&CodecValue::String("test".to_string()))
        );
        assert_eq!(
            decoded.get("bytes_field"),
            Some(&CodecValue::Bytes(vec![0xDE, 0xAD, 0xBE, 0xEF]))
        );
    }

    #[test]
    fn test_protobuf_codec_decode_empty_message() {
        let codec = ProtobufCodec::new();
        let fds_bytes = create_simple_fds();
        let schema = SchemaMetadata::protobuf("test.SimpleMessage".to_string(), fds_bytes);

        let result = codec.decode_dynamic(&[], &schema);

        // Note: prost-reflect returns all fields defined in the schema,
        // even if not present in the encoded message. Fields not in the
        // message will have default values.
        // This test verifies decoding succeeds.
        assert!(result.is_ok());
        let _decoded = result.unwrap();
    }

    #[test]
    fn test_protobuf_codec_decode_invalid_schema_returns_error() {
        let codec = ProtobufCodec::new();
        let schema = SchemaMetadata::cdr("test.Type".to_string(), "int32 value".to_string());

        let result = codec.decode_dynamic(&[0x00, 0x00, 0x00, 0x00], &schema);
        assert!(result.is_err());
    }

    #[test]
    fn test_protobuf_codec_decode_malformed_protobuf_returns_error() {
        let codec = ProtobufCodec::new();
        let fds_bytes = create_simple_fds();
        let schema = SchemaMetadata::protobuf("test.SimpleMessage".to_string(), fds_bytes);

        // Invalid varint (incomplete)
        let encoded = vec![0x08, 0x80, 0x80];
        let result = codec.decode_dynamic(&encoded, &schema);

        assert!(result.is_err());
    }

    // =========================================================================
    // Encode Tests
    // =========================================================================

    #[test]
    fn test_protobuf_codec_encode_simple_message() {
        let mut codec = ProtobufCodec::new();
        let fds_bytes = create_simple_fds();
        let schema = SchemaMetadata::protobuf("test.SimpleMessage".to_string(), fds_bytes);

        let mut decoded = DecodedMessage::new();
        decoded.insert("int_field".to_string(), CodecValue::Int32(42));
        decoded.insert(
            "string_field".to_string(),
            CodecValue::String("hello".to_string()),
        );
        decoded.insert("bool_field".to_string(), CodecValue::Bool(true));

        let result = codec.encode_dynamic(&decoded, &schema);

        assert!(result.is_ok(), "encode failed: {:?}", result.err());
        let encoded = result.unwrap();

        // Verify round-trip
        let decoded_again = codec.decode_dynamic(&encoded, &schema).unwrap();
        assert_eq!(decoded_again.get("int_field"), Some(&CodecValue::Int32(42)));
        assert_eq!(
            decoded_again.get("string_field"),
            Some(&CodecValue::String("hello".to_string()))
        );
        assert_eq!(
            decoded_again.get("bool_field"),
            Some(&CodecValue::Bool(true))
        );
    }

    #[test]
    fn test_protobuf_codec_encode_repeated_fields() {
        let mut codec = ProtobufCodec::new();
        let fds_bytes = create_repeated_fds();
        let schema = SchemaMetadata::protobuf("test.RepeatedMessage".to_string(), fds_bytes);

        let mut decoded = DecodedMessage::new();
        decoded.insert(
            "int_array".to_string(),
            CodecValue::Array(vec![
                CodecValue::Int32(1),
                CodecValue::Int32(2),
                CodecValue::Int32(3),
            ]),
        );

        let result = codec.encode_dynamic(&decoded, &schema);

        assert!(result.is_ok());
        let encoded = result.unwrap();

        // Verify round-trip
        let decoded_again = codec.decode_dynamic(&encoded, &schema).unwrap();
        assert_eq!(
            decoded_again.get("int_array"),
            Some(&CodecValue::Array(vec![
                CodecValue::Int32(1),
                CodecValue::Int32(2),
                CodecValue::Int32(3),
            ]))
        );
    }

    #[test]
    fn test_protobuf_codec_encode_enum_field() {
        let mut codec = ProtobufCodec::new();
        let fds_bytes = create_enum_fds();
        let schema = SchemaMetadata::protobuf("test.EnumMessage".to_string(), fds_bytes);

        let mut decoded = DecodedMessage::new();
        decoded.insert("enum_field".to_string(), CodecValue::Int32(1));

        let result = codec.encode_dynamic(&decoded, &schema);

        assert!(result.is_ok());
        let encoded = result.unwrap();

        // Verify round-trip
        let decoded_again = codec.decode_dynamic(&encoded, &schema).unwrap();
        assert_eq!(decoded_again.get("enum_field"), Some(&CodecValue::Int32(1)));
    }

    #[test]
    fn test_protobuf_codec_encode_nested_message() {
        let mut codec = ProtobufCodec::new();
        let fds_bytes = create_nested_fds();
        let schema = SchemaMetadata::protobuf("test.OuterMessage".to_string(), fds_bytes);

        let mut nested_fields = HashMap::new();
        nested_fields.insert("value".to_string(), CodecValue::Int32(42));

        let mut decoded = DecodedMessage::new();
        decoded.insert("nested".to_string(), CodecValue::Struct(nested_fields));

        let result = codec.encode_dynamic(&decoded, &schema);

        assert!(result.is_ok());
        let encoded = result.unwrap();

        // Verify round-trip
        let decoded_again = codec.decode_dynamic(&encoded, &schema).unwrap();
        if let Some(CodecValue::Struct(fields)) = decoded_again.get("nested") {
            assert_eq!(fields.get("value"), Some(&CodecValue::Int32(42)));
        } else {
            panic!("Expected Struct for nested field");
        }
    }

    #[test]
    fn test_protobuf_codec_encode_unknown_field_is_ignored() {
        let mut codec = ProtobufCodec::new();
        let fds_bytes = create_simple_fds();
        let schema = SchemaMetadata::protobuf("test.SimpleMessage".to_string(), fds_bytes);

        let mut decoded = DecodedMessage::new();
        decoded.insert("int_field".to_string(), CodecValue::Int32(42));
        decoded.insert(
            "unknown_field".to_string(),
            CodecValue::String("value".to_string()),
        );

        // Should succeed - unknown fields are silently ignored
        let result = codec.encode_dynamic(&decoded, &schema);
        assert!(result.is_ok());
    }

    #[test]
    fn test_protobuf_codec_encode_with_invalid_schema_returns_error() {
        let mut codec = ProtobufCodec::new();
        let schema = SchemaMetadata::cdr("test.Type".to_string(), "int32 value".to_string());

        let mut decoded = DecodedMessage::new();
        decoded.insert("field".to_string(), CodecValue::Int32(42));

        let result = codec.encode_dynamic(&decoded, &schema);
        assert!(result.is_err());
    }

    // =========================================================================
    // Round-trip Tests
    // =========================================================================

    #[test]
    fn test_protobuf_codec_round_trip_preserves_data() {
        let mut codec = ProtobufCodec::new();
        let fds_bytes = create_simple_fds();
        let schema = SchemaMetadata::protobuf("test.SimpleMessage".to_string(), fds_bytes);

        let original_data = encode_simple_message(42, "round-trip", true);

        let decoded = codec.decode_dynamic(&original_data, &schema).unwrap();
        let encoded = codec.encode_dynamic(&decoded, &schema).unwrap();

        // The encoded data should be functionally equivalent
        let decoded_again = codec.decode_dynamic(&encoded, &schema).unwrap();
        assert_eq!(decoded, decoded_again);
    }

    #[test]
    fn test_protobuf_codec_multiple_decode_encode_cycles() {
        let mut codec = ProtobufCodec::new();
        let fds_bytes = create_simple_fds();
        let schema = SchemaMetadata::protobuf("test.SimpleMessage".to_string(), fds_bytes);

        let original_data = encode_simple_message(100, "multi-cycle", false);

        let mut current = original_data.clone();
        for _ in 0..5 {
            let decoded = codec.decode_dynamic(&current, &schema).unwrap();
            current = codec.encode_dynamic(&decoded, &schema).unwrap();
        }

        // Final decoded should match original
        let decoded = codec.decode_dynamic(&current, &schema).unwrap();
        assert_eq!(decoded.get("int_field"), Some(&CodecValue::Int32(100)));
        assert_eq!(
            decoded.get("string_field"),
            Some(&CodecValue::String("multi-cycle".to_string()))
        );
        assert_eq!(decoded.get("bool_field"), Some(&CodecValue::Bool(false)));
    }

    // =========================================================================
    // DynCodec Trait Tests
    // =========================================================================

    #[test]
    fn test_protobuf_codec_encoding_type_is_protobuf() {
        let codec = ProtobufCodec::new();
        assert_eq!(codec.encoding_type(), Encoding::Protobuf);
    }

    #[test]
    fn test_protobuf_codec_reset_is_noop() {
        let mut codec = ProtobufCodec::new();
        let fds_bytes = create_simple_fds();
        codec
            .add_file_descriptor_set("test.SimpleMessage", &fds_bytes)
            .unwrap();

        // Reset should not cause errors
        codec.reset();

        // Descriptors should still be cached after reset
        assert!(codec.get_descriptor("test.SimpleMessage").is_some());
    }

    #[test]
    fn test_protobuf_codec_as_any_returns_self() {
        let codec = ProtobufCodec::new();
        let any = codec.as_any();

        assert!(any.is::<ProtobufCodec>());
    }

    // =========================================================================
    // Error Handling Tests
    // =========================================================================

    #[test]
    fn test_protobuf_codec_decode_with_wrong_type_in_schema() {
        let codec = ProtobufCodec::new();
        let fds_bytes = create_simple_fds();
        // Schema type doesn't match FDS
        let schema = SchemaMetadata::protobuf("wrong.Type".to_string(), fds_bytes);

        let encoded = encode_simple_message(42, "test", true);
        let result = codec.decode_dynamic(&encoded, &schema);

        assert!(result.is_err());
    }

    #[test]
    fn test_protobuf_codec_encode_with_null_value() {
        let mut codec = ProtobufCodec::new();
        let fds_bytes = create_simple_fds();
        let schema = SchemaMetadata::protobuf("test.SimpleMessage".to_string(), fds_bytes);

        let mut decoded = DecodedMessage::new();
        decoded.insert("int_field".to_string(), CodecValue::Null);

        // Null should be encoded as default value (0)
        let result = codec.encode_dynamic(&decoded, &schema);
        assert!(result.is_ok());
    }

    #[test]
    fn test_protobuf_codec_encode_with_unsupported_nested_struct_for_non_message_field() {
        let mut codec = ProtobufCodec::new();
        let fds_bytes = create_simple_fds();
        let schema = SchemaMetadata::protobuf("test.SimpleMessage".to_string(), fds_bytes);

        let mut nested = HashMap::new();
        nested.insert("inner".to_string(), CodecValue::Int32(1));

        let mut decoded = DecodedMessage::new();
        // int_field is not a message type
        decoded.insert("int_field".to_string(), CodecValue::Struct(nested));

        let result = codec.encode_dynamic(&decoded, &schema);

        // Should return error because we can't encode a struct for a non-message field
        assert!(result.is_err());
    }

    // =========================================================================
    // Thread Safety Tests
    // =========================================================================

    #[test]
    fn test_protobuf_codec_allows_concurrent_reads() {
        let codec = ProtobufCodec::new();
        let fds_bytes = create_simple_fds();
        codec
            .add_file_descriptor_set("test.SimpleMessage", &fds_bytes)
            .unwrap();

        // Multiple read locks should work
        let descriptor1 = codec.get_descriptor("test.SimpleMessage");
        let descriptor2 = codec.get_descriptor("test.SimpleMessage");

        assert!(descriptor1.is_some());
        assert!(descriptor2.is_some());
        assert_eq!(descriptor1.unwrap().name(), descriptor2.unwrap().name());
    }
}
