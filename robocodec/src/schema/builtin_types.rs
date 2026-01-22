// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Predefined ROS2 builtin message types.
//!
//! This module provides the standard builtin_interfaces and std_msgs types that are
//! commonly referenced in ROS2 message definitions.
//!
//! ## Supported Types
//!
//! - `builtin_interfaces/Time` - Timestamp with seconds and nanoseconds
//! - `builtin_interfaces/Duration` - Time duration with seconds and nanoseconds
//! - `std_msgs/Header` - Standard ROS message header with stamp, frame_id
//!
//! Time and Duration have the same structure:
//! ```text
//! int32 sec
//! uint32 nanosec
//! ```

use crate::schema::ast::{Field, FieldType, MessageType, PrimitiveType};

/// Create the predefined builtin_interfaces/Time type.
fn builtin_time() -> MessageType {
    let mut msg_type = MessageType::new("builtin_interfaces/Time".to_string());

    msg_type.add_field(Field {
        name: "sec".to_string(),
        type_name: FieldType::Primitive(PrimitiveType::Int32),
    });

    msg_type.add_field(Field {
        name: "nanosec".to_string(),
        type_name: FieldType::Primitive(PrimitiveType::UInt32),
    });

    msg_type
}

/// Create the predefined builtin_interfaces/msg/Time type (alternative naming).
fn builtin_time_msg() -> MessageType {
    let mut msg_type = MessageType::new("builtin_interfaces/msg/Time".to_string());

    msg_type.add_field(Field {
        name: "sec".to_string(),
        type_name: FieldType::Primitive(PrimitiveType::Int32),
    });

    msg_type.add_field(Field {
        name: "nanosec".to_string(),
        type_name: FieldType::Primitive(PrimitiveType::UInt32),
    });

    msg_type
}

/// Create the predefined builtin_interfaces/Duration type.
fn builtin_duration() -> MessageType {
    let mut msg_type = MessageType::new("builtin_interfaces/Duration".to_string());

    msg_type.add_field(Field {
        name: "sec".to_string(),
        type_name: FieldType::Primitive(PrimitiveType::Int32),
    });

    msg_type.add_field(Field {
        name: "nanosec".to_string(),
        type_name: FieldType::Primitive(PrimitiveType::UInt32),
    });

    msg_type
}

/// Create the predefined builtin_interfaces/msg/Duration type (alternative naming).
fn builtin_duration_msg() -> MessageType {
    let mut msg_type = MessageType::new("builtin_interfaces/msg/Duration".to_string());

    msg_type.add_field(Field {
        name: "sec".to_string(),
        type_name: FieldType::Primitive(PrimitiveType::Int32),
    });

    msg_type.add_field(Field {
        name: "nanosec".to_string(),
        type_name: FieldType::Primitive(PrimitiveType::UInt32),
    });

    msg_type
}

/// Create the predefined std_msgs/Header type.
///
/// Standard ROS message header with timestamp and frame ID.
/// Note: This does not include the `seq` field which is only used in ROS1.
fn builtin_header() -> MessageType {
    let mut msg_type = MessageType::new("std_msgs/Header".to_string());

    // Use builtin_interfaces/Time for stamp field
    msg_type.add_field(Field {
        name: "stamp".to_string(),
        type_name: FieldType::Nested("builtin_interfaces/Time".to_string()),
    });

    msg_type.add_field(Field {
        name: "frame_id".to_string(),
        type_name: FieldType::Primitive(PrimitiveType::String),
    });

    msg_type
}

/// Create the predefined std_msgs/msg/Header type (alternative naming).
fn builtin_header_msg() -> MessageType {
    let mut msg_type = MessageType::new("std_msgs/msg/Header".to_string());

    // Use builtin_interfaces/msg/Time for stamp field
    msg_type.add_field(Field {
        name: "stamp".to_string(),
        type_name: FieldType::Nested("builtin_interfaces/msg/Time".to_string()),
    });

    msg_type.add_field(Field {
        name: "frame_id".to_string(),
        type_name: FieldType::Primitive(PrimitiveType::String),
    });

    msg_type
}

/// Get all predefined builtin message types.
///
/// Returns a vector of all builtin types that should be automatically
/// included in every schema.
///
/// # Examples
///
/// ```no_run
/// # fn main() {
/// use robocodec::schema::builtin_types;
///
/// for builtin_type in builtin_types::get_all() {
///     // Add builtin type to your schema
///     println!("Adding builtin type: {}", builtin_type.name);
/// }
/// # }
/// ```
pub fn get_all() -> Vec<MessageType> {
    vec![
        builtin_time(),
        builtin_time_msg(),
        builtin_duration(),
        builtin_duration_msg(),
        builtin_header(),
        builtin_header_msg(),
    ]
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_builtin_time_structure() {
        let time = builtin_time();

        assert_eq!(time.name, "builtin_interfaces/Time");
        assert_eq!(time.fields.len(), 2);
        assert_eq!(time.fields[0].name, "sec");
        assert_eq!(time.fields[1].name, "nanosec");

        // Verify field types
        assert!(matches!(
            time.fields[0].type_name,
            FieldType::Primitive(PrimitiveType::Int32)
        ));
        assert!(matches!(
            time.fields[1].type_name,
            FieldType::Primitive(PrimitiveType::UInt32)
        ));
    }

    #[test]
    fn test_builtin_time_msg_structure() {
        let time = builtin_time_msg();

        assert_eq!(time.name, "builtin_interfaces/msg/Time");
        assert_eq!(time.fields.len(), 2);
    }

    #[test]
    fn test_builtin_duration_structure() {
        let duration = builtin_duration();

        assert_eq!(duration.name, "builtin_interfaces/Duration");
        assert_eq!(duration.fields.len(), 2);
    }

    #[test]
    fn test_builtin_duration_msg_structure() {
        let duration = builtin_duration_msg();

        assert_eq!(duration.name, "builtin_interfaces/msg/Duration");
        assert_eq!(duration.fields.len(), 2);
    }

    #[test]
    fn test_get_all() {
        let all = get_all();

        assert_eq!(all.len(), 6);

        // Verify we have both naming variants for Time and Duration
        let names: Vec<&str> = all.iter().map(|t| t.name.as_str()).collect();
        assert!(names.contains(&"builtin_interfaces/Time"));
        assert!(names.contains(&"builtin_interfaces/msg/Time"));
        assert!(names.contains(&"builtin_interfaces/Duration"));
        assert!(names.contains(&"builtin_interfaces/msg/Duration"));
        // Verify Header types
        assert!(names.contains(&"std_msgs/Header"));
        assert!(names.contains(&"std_msgs/msg/Header"));
    }

    #[test]
    fn test_builtin_header_structure() {
        let header = builtin_header();

        assert_eq!(header.name, "std_msgs/Header");
        assert_eq!(header.fields.len(), 2);
        assert_eq!(header.fields[0].name, "stamp");
        assert_eq!(header.fields[1].name, "frame_id");
    }

    #[test]
    fn test_builtin_header_msg_structure() {
        let header = builtin_header_msg();

        assert_eq!(header.name, "std_msgs/msg/Header");
        assert_eq!(header.fields.len(), 2);
    }
}
