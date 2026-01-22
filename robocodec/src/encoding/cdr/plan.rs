// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Decode plan generation for CDR decoding.
//!
//! A decode plan is a sequence of operations that can be executed
//! to decode a CDR-encoded message. Plans are generated once per
//! schema and cached for reuse.

use std::fmt;

/// A single decode operation in the execution plan.
#[derive(Debug, Clone, PartialEq)]
pub enum DecodeOp {
    /// Align the cursor to a specific boundary.
    Align {
        /// Alignment boundary in bytes
        alignment: u64,
    },

    /// Read a primitive value.
    ReadPrimitive {
        /// Field path for the value (e.g., "position.x")
        field_path: String,
        /// Primitive type to read
        type_name: crate::PrimitiveType,
    },

    /// Read a string value (4-byte length prefix + UTF-8 bytes).
    ReadString {
        /// Field path for the value
        field_path: String,
    },

    /// Read bytes value (4-byte length prefix + raw bytes).
    ReadBytes {
        /// Field path for the value
        field_path: String,
    },

    /// Read a time value (ROS time: sec:int32, nsec:uint32).
    ReadTime {
        /// Field path for the value
        field_path: String,
    },

    /// Read a duration value (ROS duration: sec:int32, nsec:uint32).
    ReadDuration {
        /// Field path for the value
        field_path: String,
    },

    /// Begin decoding an array.
    ReadArray {
        /// Field path for the array
        field_path: String,
        /// Element type
        element_type: ElementType,
        /// Number of elements (None = dynamic, Some(N) = fixed)
        count: Option<usize>,
    },

    /// Decode a nested message.
    DecodeNested {
        /// Field path for the nested value
        field_path: String,
        /// Type name of the nested message
        type_name: String,
    },

    /// End of the current scope (array or nested message).
    EndScope,
}

/// Element type for arrays.
#[derive(Debug, Clone, PartialEq)]
pub enum ElementType {
    /// Primitive element
    Primitive(crate::PrimitiveType),
    /// String element (for string arrays)
    String,
    /// Bytes element (for bytes arrays)
    Bytes,
    /// Nested message element
    Nested { type_name: String, alignment: u64 },
}

impl ElementType {
    /// Get the alignment for this element type.
    pub fn alignment(&self) -> u64 {
        match self {
            ElementType::Primitive(p) => p.alignment(),
            ElementType::String | ElementType::Bytes => 4,
            ElementType::Nested { alignment, .. } => *alignment,
        }
    }
}

/// A decode plan is a sequence of decode operations.
#[derive(Debug, Clone, PartialEq)]
pub struct DecodePlan {
    /// Ordered sequence of decode operations
    pub ops: Vec<DecodeOp>,
    /// Name of the message type this plan decodes
    pub type_name: String,
}

impl DecodePlan {
    /// Create a new decode plan.
    pub fn new(type_name: String) -> Self {
        Self {
            ops: Vec::new(),
            type_name,
        }
    }

    /// Add an operation to the plan.
    pub fn add_op(&mut self, op: DecodeOp) {
        self.ops.push(op);
    }

    /// Get the current length (number of operations).
    pub fn len(&self) -> usize {
        self.ops.len()
    }

    /// Check if the plan is empty.
    pub fn is_empty(&self) -> bool {
        self.ops.is_empty()
    }
}

impl fmt::Display for DecodePlan {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        writeln!(f, "DecodePlan for '{}':", self.type_name)?;
        for (idx, op) in self.ops.iter().enumerate() {
            writeln!(f, "  {idx:3}: {op:?}")?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::PrimitiveType;

    #[test]
    fn test_decode_op_align_variant() {
        let op = DecodeOp::Align { alignment: 4 };
        assert_eq!(op, DecodeOp::Align { alignment: 4 });
    }

    #[test]
    fn test_decode_op_read_primitive() {
        let op = DecodeOp::ReadPrimitive {
            field_path: "position.x".to_string(),
            type_name: PrimitiveType::Float32,
        };
        assert!(matches!(op, DecodeOp::ReadPrimitive { .. }));
    }

    #[test]
    fn test_decode_op_read_string() {
        let op = DecodeOp::ReadString {
            field_path: "name".to_string(),
        };
        assert_eq!(
            op,
            DecodeOp::ReadString {
                field_path: "name".to_string()
            }
        );
    }

    #[test]
    fn test_decode_op_read_bytes() {
        let op = DecodeOp::ReadBytes {
            field_path: "data".to_string(),
        };
        assert!(matches!(op, DecodeOp::ReadBytes { .. }));
    }

    #[test]
    fn test_decode_op_read_time() {
        let op = DecodeOp::ReadTime {
            field_path: "header.stamp".to_string(),
        };
        assert_eq!(
            op,
            DecodeOp::ReadTime {
                field_path: "header.stamp".to_string()
            }
        );
    }

    #[test]
    fn test_decode_op_read_duration() {
        let op = DecodeOp::ReadDuration {
            field_path: "duration".to_string(),
        };
        assert!(matches!(op, DecodeOp::ReadDuration { .. }));
    }

    #[test]
    fn test_decode_op_read_array_fixed() {
        let op = DecodeOp::ReadArray {
            field_path: "position".to_string(),
            element_type: ElementType::Primitive(PrimitiveType::Float64),
            count: Some(3),
        };
        assert!(matches!(op, DecodeOp::ReadArray { count: Some(3), .. }));
    }

    #[test]
    fn test_decode_op_read_array_dynamic() {
        let op = DecodeOp::ReadArray {
            field_path: "names".to_string(),
            element_type: ElementType::String,
            count: None,
        };
        assert!(matches!(op, DecodeOp::ReadArray { count: None, .. }));
    }

    #[test]
    fn test_decode_op_decode_nested() {
        let op = DecodeOp::DecodeNested {
            field_path: "nested_msg".to_string(),
            type_name: "geometry_msgs/Pose".to_string(),
        };
        assert_eq!(
            op,
            DecodeOp::DecodeNested {
                field_path: "nested_msg".to_string(),
                type_name: "geometry_msgs/Pose".to_string()
            }
        );
    }

    #[test]
    fn test_decode_op_end_scope() {
        let op = DecodeOp::EndScope;
        assert_eq!(op, DecodeOp::EndScope);
    }

    #[test]
    fn test_element_type_primitive() {
        let elem = ElementType::Primitive(PrimitiveType::Int32);
        assert_eq!(elem.alignment(), 4);
    }

    #[test]
    fn test_element_type_string() {
        let elem = ElementType::String;
        assert_eq!(elem.alignment(), 4);
    }

    #[test]
    fn test_element_type_bytes() {
        let elem = ElementType::Bytes;
        assert_eq!(elem.alignment(), 4);
    }

    #[test]
    fn test_element_type_nested() {
        let elem = ElementType::Nested {
            type_name: "std_msgs/Header".to_string(),
            alignment: 8,
        };
        assert_eq!(elem.alignment(), 8);
    }

    #[test]
    fn test_element_type_alignment_for_float64() {
        let elem = ElementType::Primitive(PrimitiveType::Float64);
        assert_eq!(elem.alignment(), 8);
    }

    #[test]
    fn test_element_type_alignment_for_bool() {
        let elem = ElementType::Primitive(PrimitiveType::Bool);
        assert_eq!(elem.alignment(), 1);
    }

    #[test]
    fn test_decode_plan_new() {
        let plan = DecodePlan::new("test/Message".to_string());
        assert!(plan.is_empty());
        assert_eq!(plan.len(), 0);
        assert_eq!(plan.type_name, "test/Message");
    }

    #[test]
    fn test_decode_plan_add_op() {
        let mut plan = DecodePlan::new("test".to_string());
        plan.add_op(DecodeOp::Align { alignment: 4 });
        assert_eq!(plan.len(), 1);
        assert!(!plan.is_empty());
    }

    #[test]
    fn test_decode_plan_add_multiple_ops() {
        let mut plan = DecodePlan::new("test".to_string());
        plan.add_op(DecodeOp::Align { alignment: 8 });
        plan.add_op(DecodeOp::ReadPrimitive {
            field_path: "value".to_string(),
            type_name: PrimitiveType::Int32,
        });
        plan.add_op(DecodeOp::EndScope);
        assert_eq!(plan.len(), 3);
    }

    #[test]
    fn test_decode_plan_is_empty() {
        let plan = DecodePlan::new("test".to_string());
        assert!(plan.is_empty());
        assert_eq!(plan.len(), 0);
    }

    #[test]
    fn test_decode_plan_len() {
        let mut plan = DecodePlan::new("test".to_string());
        assert_eq!(plan.len(), 0);
        plan.add_op(DecodeOp::Align { alignment: 4 });
        assert_eq!(plan.len(), 1);
        plan.add_op(DecodeOp::Align { alignment: 8 });
        assert_eq!(plan.len(), 2);
    }

    #[test]
    fn test_decode_plan_clone() {
        let mut plan = DecodePlan::new("test".to_string());
        plan.add_op(DecodeOp::ReadPrimitive {
            field_path: "x".to_string(),
            type_name: PrimitiveType::Float32,
        });
        let cloned = plan.clone();
        assert_eq!(cloned.len(), plan.len());
        assert_eq!(cloned.type_name, plan.type_name);
    }

    #[test]
    fn test_decode_plan_equality() {
        let mut plan1 = DecodePlan::new("test".to_string());
        plan1.add_op(DecodeOp::Align { alignment: 4 });

        let mut plan2 = DecodePlan::new("test".to_string());
        plan2.add_op(DecodeOp::Align { alignment: 4 });

        assert_eq!(plan1, plan2);
    }

    #[test]
    fn test_decode_plan_inequality() {
        let mut plan1 = DecodePlan::new("test".to_string());
        plan1.add_op(DecodeOp::Align { alignment: 4 });

        let mut plan2 = DecodePlan::new("test".to_string());
        plan2.add_op(DecodeOp::Align { alignment: 8 });

        assert_ne!(plan1, plan2);
    }

    #[test]
    fn test_decode_plan_display() {
        let mut plan = DecodePlan::new("test/Msg".to_string());
        plan.add_op(DecodeOp::Align { alignment: 4 });
        plan.add_op(DecodeOp::ReadPrimitive {
            field_path: "value".to_string(),
            type_name: PrimitiveType::Int32,
        });

        let display = format!("{plan}");
        assert!(display.contains("test/Msg"));
        assert!(display.contains("Align"));
    }

    #[test]
    fn test_element_type_partial_eq() {
        let elem1 = ElementType::Primitive(PrimitiveType::Int32);
        let elem2 = ElementType::Primitive(PrimitiveType::Int32);
        assert_eq!(elem1, elem2);
    }

    #[test]
    fn test_element_type_not_equal() {
        let elem1 = ElementType::Primitive(PrimitiveType::Int32);
        let elem2 = ElementType::Primitive(PrimitiveType::Float32);
        assert_ne!(elem1, elem2);
    }

    #[test]
    fn test_decode_op_clone() {
        let op = DecodeOp::ReadPrimitive {
            field_path: "test.field".to_string(),
            type_name: PrimitiveType::Float64,
        };
        let cloned = op.clone();
        assert_eq!(op, cloned);
    }

    #[test]
    fn test_decode_op_array_with_nested_element() {
        let op = DecodeOp::ReadArray {
            field_path: "nested_array".to_string(),
            element_type: ElementType::Nested {
                type_name: "geometry_msgs/Point".to_string(),
                alignment: 4,
            },
            count: Some(10),
        };
        assert!(matches!(
            op,
            DecodeOp::ReadArray {
                element_type: ElementType::Nested { .. },
                ..
            }
        ));
    }

    #[test]
    fn test_element_type_nested_with_different_alignment() {
        let elem = ElementType::Nested {
            type_name: "test/Type".to_string(),
            alignment: 16,
        };
        assert_eq!(elem.alignment(), 16);
    }

    #[test]
    fn test_decode_plan_with_various_operations() {
        let mut plan = DecodePlan::new("ComplexMessage".to_string());
        plan.add_op(DecodeOp::Align { alignment: 4 });
        plan.add_op(DecodeOp::ReadPrimitive {
            field_path: "header.seq".to_string(),
            type_name: PrimitiveType::UInt32,
        });
        plan.add_op(DecodeOp::ReadTime {
            field_path: "header.stamp".to_string(),
        });
        plan.add_op(DecodeOp::ReadString {
            field_path: "header.frame_id".to_string(),
        });
        plan.add_op(DecodeOp::ReadArray {
            field_path: "positions".to_string(),
            element_type: ElementType::Primitive(PrimitiveType::Float64),
            count: None,
        });
        plan.add_op(DecodeOp::DecodeNested {
            field_path: "nested".to_string(),
            type_name: "NestedType".to_string(),
        });
        plan.add_op(DecodeOp::EndScope);

        assert_eq!(plan.len(), 7);
    }
}
