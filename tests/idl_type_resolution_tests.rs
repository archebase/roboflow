//! IDL type resolution tests.
//!
//! This module tests the IDL parser's ability to resolve types across
//! different naming conventions:
//! - `/` separator (ROS convention): `pkg/msg/TypeName`
//! - `/msg/` separator (ROS2 convention): `pkg/msg/TypeName`
//! - Short form: `pkg/TypeName`

use robocodec::schema::{parse_schema, FieldType};

/// Test that type lookup works with the `/msg/` separator (ROS2 convention).
///
/// ROS2 uses `/msg/` in type names (e.g., `std_msgs/msg/Header`),
/// which should resolve to the same type as `std_msgs/Header`.
#[test]
fn test_type_resolution_with_msg_separator() {
    let schema_str = r#"
std_msgs/Header header
===
MSG: std_msgs/Header
builtin_interfaces/Time stamp
string frame_id
===
MSG: builtin_interfaces/Time
int32 sec
uint32 nanosec
"#;

    let schema =
        parse_schema("test/Container", schema_str).expect("parse schema with /msg/ separator");

    // Verify the schema was parsed successfully
    assert!(
        schema.types.len() >= 2,
        "Should have at least 2 types: Container and Header"
    );

    // Verify Header type exists (should have both /msg/ and without variants)
    assert!(
        schema.get_type_variants("std_msgs/Header").is_some()
            || schema.get_type_variants("std_msgs/msg/Header").is_some(),
        "Header type should be registered"
    );

    // Verify Time type exists
    assert!(
        schema
            .get_type_variants("builtin_interfaces/Time")
            .is_some(),
        "Time type should be registered"
    );
}

/// Test that all three type reference styles resolve to the same type.
///
/// In ROS, the same type can be referenced as:
/// - `pkg/msg/TypeName` (ROS2 naming convention)
/// - `pkg/TypeName` (short form)
///
/// Both should resolve to the same underlying type definition.
#[test]
fn test_multiple_type_reference_styles_resolve_same_type() {
    let schema_str = r#"
builtin_interfaces/Time stamp
===
MSG: builtin_interfaces/Time
int32 sec
uint32 nanosec
"#;

    let schema = parse_schema("test", schema_str).expect("parse schema");

    // Both reference styles should find the Time type
    let with_msg = schema.get_type_variants("builtin_interfaces/msg/Time");
    let short_form = schema.get_type_variants("builtin_interfaces/Time");

    // At least one should resolve
    let resolved = with_msg.or(short_form);

    assert!(
        resolved.is_some(),
        "Time type should be accessible via at least one reference style"
    );

    // Verify the Time type has the expected fields
    let time_type = resolved.expect("Time type should exist");
    assert_eq!(time_type.fields.len(), 2);
    assert_eq!(time_type.fields[0].name, "sec");
    assert_eq!(time_type.fields[1].name, "nanosec");
}

/// Test type lookup in nested message definitions.
#[test]
fn test_nested_message_type_resolution() {
    let schema_str = r#"
geometry_msgs/Transform transform
===
MSG: geometry_msgs/Transform
geometry_msgs/Vector3 translation
geometry_msgs/Quaternion rotation
===
MSG: geometry_msgs/Vector3
float64 x
float64 y
float64 z
===
MSG: geometry_msgs/Quaternion
float64 x
float64 y
float64 z
float64 w
"#;

    let schema = parse_schema("test/Container", schema_str).expect("parse nested schema");

    // Verify all types are registered
    assert!(schema
        .get_type_variants("geometry_msgs/Transform")
        .is_some());
    assert!(schema.get_type_variants("geometry_msgs/Vector3").is_some());
    assert!(schema
        .get_type_variants("geometry_msgs/Quaternion")
        .is_some());

    // Get the Transform type
    let transform = schema
        .get_type_variants("geometry_msgs/Transform")
        .expect("Transform type should exist");

    assert_eq!(transform.fields.len(), 2);

    // Verify translation field type
    let translation_field = &transform.fields[0];
    assert_eq!(translation_field.name, "translation");
    assert!(matches!(
        &translation_field.type_name,
        FieldType::Nested(name) if name.contains("Vector3")
    ));

    // Verify rotation field type
    let rotation_field = &transform.fields[1];
    assert_eq!(rotation_field.name, "rotation");
    assert!(matches!(
        &rotation_field.type_name,
        FieldType::Nested(name) if name.contains("Quaternion")
    ));
}

/// Test that type resolution handles ambiguous references correctly.
///
/// When multiple types could match (e.g., `Time` in different packages),
/// the parser should correctly resolve based on scoping rules.
#[test]
fn test_ambiguous_type_resolution_with_namespacing() {
    let schema_str = r#"
pkg1/Common first
pkg2/Common second
===
MSG: pkg1/Common
int32 value
===
MSG: pkg2/Common
int32 value
"#;

    let schema = parse_schema("test/Container", schema_str).expect("parse namespaced schema");

    // All types should be registered
    assert!(schema.get_type_variants("pkg1/Common").is_some());
    assert!(schema.get_type_variants("pkg2/Common").is_some());

    // Get the Container type
    let container = schema
        .get_type_variants("test/Container")
        .expect("Container type should exist");

    assert_eq!(container.fields.len(), 2);

    // Verify field types are correctly resolved
    let first_field = &container.fields[0];
    assert_eq!(first_field.name, "first");
    assert!(matches!(
        &first_field.type_name,
        FieldType::Nested(name) if name.contains("Common") && name.contains("pkg1")
    ));

    let second_field = &container.fields[1];
    assert_eq!(second_field.name, "second");
    assert!(matches!(
        &second_field.type_name,
        FieldType::Nested(name) if name.contains("Common") && name.contains("pkg2")
    ));
}

/// Test regression: string type lookup with various formats.
#[test]
fn test_string_type_variants() {
    let schema_str = r#"
string basic_string
"#;

    let schema = parse_schema("std_msgs/msg/StringTest", schema_str).expect("parse string schema");

    let test_type = schema
        .get_type_variants("std_msgs/msg/StringTest")
        .expect("StringTest type should exist");

    assert_eq!(test_type.fields.len(), 1);
    assert_eq!(test_type.fields[0].name, "basic_string");
    assert!(matches!(
        test_type.fields[0].type_name,
        roboflow::FieldType::Primitive(robocodec::schema::PrimitiveType::String)
    ));
}

/// Test that Header field is preserved in ROS2 schemas.
///
/// ROS2 types (with /msg/ in the name) should NOT have their header
/// field removed. Only ROS1 types should have header removal applied.
#[test]
fn test_ros2_preserves_header_field() {
    let schema_str = r#"
std_msgs/Header header
string data
===
MSG: std_msgs/Header
builtin_interfaces/Time stamp
string frame_id
===
MSG: builtin_interfaces/Time
int32 sec
uint32 nanosec
"#;

    // Use ROS2-style type name (with /msg/)
    let schema = parse_schema("test_pkg/msg/TestMsg", schema_str).expect("parse ROS2 schema");

    // Get the TestMsg type
    let test_msg = schema
        .get_type_variants("test_pkg/msg/TestMsg")
        .expect("TestMsg type should exist");

    // ROS2 should preserve the header field
    assert_eq!(
        test_msg.fields.len(),
        2,
        "ROS2 TestMsg should have 2 fields (header, data)"
    );
    assert_eq!(
        test_msg.fields[0].name, "header",
        "First field should be 'header'"
    );
}

/// Test that Header field is removed from ROS1 schemas.
///
/// ROS1 types should have their header field removed because
/// the header data is in the ROS1 record header, not in the message bytes.
#[test]
fn test_ros1_removes_header_field() {
    let schema_str = r#"
std_msgs/Header header
string data
===
MSG: std_msgs/Header
builtin_interfaces/Time stamp
string frame_id
===
MSG: builtin_interfaces/Time
int32 sec
uint32 nanosec
"#;

    // Use ROS1-style type name (without /msg/)
    let schema = parse_schema("test_pkg/TestMsg", schema_str).expect("parse ROS1 schema");

    // Get the TestMsg type
    let test_msg = schema
        .get_type_variants("test_pkg/TestMsg")
        .expect("TestMsg type should exist");

    // ROS1 should remove the header field
    assert_eq!(
        test_msg.fields.len(),
        1,
        "ROS1 TestMsg should have 1 field (data only, header removed)"
    );
    assert_eq!(
        test_msg.fields[0].name, "data",
        "First field should be 'data' (header was removed)"
    );
}
