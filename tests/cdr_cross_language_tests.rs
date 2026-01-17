//! Cross-language CDR compatibility tests.
//!
//! This module contains tests validated against other language implementations
//! of the CDR (Common Data Representation) specification.

use robocodec::encoding::cdr::{CdrCursor, CdrDecoder};
use robocodec::schema::parse_schema;

/// Test TFMessage from TypeScript CDR library.
///
/// This is a known-good test case from the TypeScript CDR implementation,
/// validating that our decoder produces the same results for geometry_msgs/TFMessage.
///
/// Reference: TypeScript rosbag library test fixtures
#[test]
fn test_tf2_message_from_typescript_cdr_library() {
    // Example tf2_msgs/TFMessage from TypeScript tests
    // This validates:
    // - CDR header parsing (little endian)
    // - Sequence deserialization
    // - std_msgs/Header with time and string
    // - geometry_msgs/Transform with Vector3 and Quaternion
    let data: Vec<u8> = vec![
        // CDR header (little endian)
        0x00, 0x01, 0x00, 0x00, // Sequence length = 1
        0x01, 0x00, 0x00, 0x00, // stamp.sec = 1490149580
        0xcc, 0xe0, 0xd1, 0x58, // stamp.nanosec = 117017840
        0xf0, 0x8c, 0xf9, 0x06, // frame_id length = 10
        0x0a, 0x00, 0x00, 0x00, // "base_link\0"
        0x62, 0x61, 0x73, 0x65, 0x5f, 0x6c, 0x69, 0x6e, 0x6b, 0x00,
        // Padding to 4-byte boundary
        0x00, 0x00, // child_frame_id length = 6
        0x06, 0x00, 0x00, 0x00, // "radar\0"
        0x72, 0x61, 0x64, 0x61, 0x72, 0x00, // Padding to 8-byte boundary for float64
        0x00, 0x00, // translation.x = 3.835
        0xae, 0x47, 0xe1, 0x7a, 0x14, 0xae, 0x0e, 0x40, // translation.y = 0
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // translation.z = 0
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // rotation.x = 0
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // rotation.y = 0
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // rotation.z = 0
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // rotation.w = 1.0
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xf0, 0x3f,
    ];

    let mut cursor = CdrCursor::new(&data).expect("create cursor");

    // Verify sequence length
    let seq_len = cursor.read_u32().expect("read sequence length");
    assert_eq!(seq_len, 1, "Expected exactly 1 transform in the sequence");

    // Verify header timestamp
    let sec = cursor.read_u32().expect("read stamp.sec");
    let nsec = cursor.read_u32().expect("read stamp.nanosec");
    assert_eq!(sec, 1490149580, "Timestamp seconds should match reference");
    assert_eq!(
        nsec, 117017840,
        "Timestamp nanoseconds should match reference"
    );

    // Verify frame_id string with alignment handling
    let frame_id_len = cursor.read_u32().expect("read frame_id length") as usize;
    assert_eq!(
        frame_id_len, 10,
        "frame_id length should include null terminator"
    );
    let frame_id_bytes = cursor
        .read_bytes(frame_id_len - 1)
        .expect("read frame_id bytes");
    let _null = cursor.read_u8().expect("read null terminator");
    let frame_id = std::str::from_utf8(frame_id_bytes).expect("frame_id should be valid UTF-8");
    assert_eq!(frame_id, "base_link", "frame_id should match reference");

    // Verify child_frame_id with alignment
    cursor
        .align(4)
        .expect("align to 4 bytes for child_frame_id");
    let child_frame_id_len = cursor.read_u32().expect("read child_frame_id length") as usize;
    assert_eq!(
        child_frame_id_len, 6,
        "child_frame_id length should include null terminator"
    );
    let child_frame_id_bytes = cursor
        .read_bytes(child_frame_id_len - 1)
        .expect("read child_frame_id bytes");
    let _null = cursor.read_u8().expect("read null terminator");
    let child_frame_id =
        std::str::from_utf8(child_frame_id_bytes).expect("child_frame_id should be valid UTF-8");
    assert_eq!(
        child_frame_id, "radar",
        "child_frame_id should match reference"
    );

    // Verify translation (Vector3) with 8-byte alignment
    cursor.align(8).expect("align to 8 bytes for float64");
    let tx = cursor.read_f64().expect("read translation.x");
    let ty = cursor.read_f64().expect("read translation.y");
    let tz = cursor.read_f64().expect("read translation.z");
    assert!(
        (tx - 3.835).abs() < 0.001,
        "translation.x should be approximately 3.835"
    );
    assert_eq!(ty, 0.0, "translation.y should be exactly 0");
    assert_eq!(tz, 0.0, "translation.z should be exactly 0");

    // Verify rotation (Quaternion)
    let rx = cursor.read_f64().expect("read rotation.x");
    let ry = cursor.read_f64().expect("read rotation.y");
    let rz = cursor.read_f64().expect("read rotation.z");
    let rw = cursor.read_f64().expect("read rotation.w");
    assert_eq!(rx, 0.0, "rotation.x should be exactly 0");
    assert_eq!(ry, 0.0, "rotation.y should be exactly 0");
    assert_eq!(rz, 0.0, "rotation.z should be exactly 0");
    assert_eq!(
        rw, 1.0,
        "rotation.w should be exactly 1.0 (unit quaternion)"
    );

    // Verify we consumed the entire message
    assert_eq!(
        cursor.position(),
        data.len(),
        "Should have consumed all bytes in the message"
    );
}

/// Test decoding TFMessage using the high-level decoder.
///
/// This test validates that the schema-based decoder correctly handles
/// the nested structure of TFMessage with proper type resolution.
#[test]
fn test_tf2_message_schema_parsing() {
    let schema_str = r#"
geometry_msgs/TransformStamped[] transforms
===
MSG: geometry_msgs/TransformStamped
std_msgs/Header header
string child_frame_id
geometry_msgs/Transform transform
===
MSG: std_msgs/Header
builtin_interfaces/Time stamp
string frame_id
===
MSG: builtin_interfaces/Time
int32 sec
uint32 nanosec
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

    // Use ROS2-style type name with /msg/ to avoid ROS1 detection
    let schema = parse_schema("tf2_msgs/msg/TFMessage", schema_str).expect("parse schema");

    // Verify schema was parsed correctly
    assert_eq!(schema.name, "tf2_msgs/msg/TFMessage");

    // Check top-level TFMessage type
    let tf_message = schema
        .get_type("tf2_msgs/msg/TFMessage")
        .expect("TFMessage type should exist");
    assert_eq!(
        tf_message.fields.len(),
        1,
        "TFMessage should have 1 field (transforms array)"
    );

    // Check transforms field is an array of TransformStamped
    let transforms_field = &tf_message.fields[0];
    assert_eq!(transforms_field.name, "transforms");
    match &transforms_field.type_name {
        robocodec::schema::FieldType::Array { base_type, size } => {
            assert!(size.is_none(), "transforms should be a dynamic array");
            match base_type.as_ref() {
                robocodec::schema::FieldType::Nested(name) => {
                    assert!(
                        name.contains("TransformStamped"),
                        "Array element type should be TransformStamped"
                    );
                }
                _ => panic!("Array base type should be Nested (TransformStamped)"),
            }
        }
        _ => panic!("transforms field should be an Array type"),
    }

    // Check TransformStamped type
    let transform_stamped = schema
        .get_type_variants("geometry_msgs/TransformStamped")
        .expect("TransformStamped type should exist");
    assert_eq!(
        transform_stamped.fields.len(),
        3,
        "TransformStamped should have 3 fields"
    );

    // Verify field names
    assert_eq!(transform_stamped.fields[0].name, "header");
    assert_eq!(transform_stamped.fields[1].name, "child_frame_id");
    assert_eq!(transform_stamped.fields[2].name, "transform");

    // Check that Header field is preserved (not removed by ROS1 processing)
    match &transform_stamped.fields[0].type_name {
        robocodec::schema::FieldType::Nested(name) => {
            assert!(name.contains("Header"), "First field should be Header type");
        }
        _ => panic!("header field should be a Nested type"),
    }
}

/// Test string sequence encoding/decoding with proper alignment.
///
/// This test validates that sequences of strings are correctly aligned,
/// which is a common source of CDR parsing bugs.
#[test]
fn test_string_sequence_alignment() {
    let schema_str = r#"
string[] names
===
MSG: std_msgs/Header
builtin_interfaces/Time stamp
string frame_id
===
MSG: builtin_interfaces/Time
int32 sec
uint32 nanosec
"#;

    let schema = parse_schema("test/StringArray", schema_str).expect("parse schema");

    // Build test data: ["left_arm_joint1", "left_arm_joint2"]
    let mut data = vec![0x00, 0x01, 0x00, 0x00]; // CDR header

    // Sequence length = 2
    data.extend_from_slice(&2u32.to_le_bytes());

    // First string: "left_arm_joint1" (15 chars + null = 16)
    data.extend_from_slice(&16u32.to_le_bytes());
    data.extend_from_slice(b"left_arm_joint1");
    data.push(0);

    // Second string: "left_arm_joint2" (15 chars + null = 16)
    // After first string: 4 + 4 + 16 + 1 = 25 bytes, need 3 bytes padding to align to 4
    while data.len() % 4 != 0 {
        data.push(0);
    }
    data.extend_from_slice(&16u32.to_le_bytes());
    data.extend_from_slice(b"left_arm_joint2");
    data.push(0);

    let decoder = CdrDecoder::new();
    let result = decoder
        .decode(&schema, &data, None)
        .expect("decode string sequence");

    assert!(result.contains_key("names"));

    if let Some(robocodec::CodecValue::Array(arr)) = result.get("names") {
        assert_eq!(arr.len(), 2);
        if let robocodec::CodecValue::String(s1) = &arr[0] {
            assert_eq!(s1, "left_arm_joint1");
        } else {
            panic!("First element should be a string");
        }
        if let robocodec::CodecValue::String(s2) = &arr[1] {
            assert_eq!(s2, "left_arm_joint2");
        } else {
            panic!("Second element should be a string");
        }
    } else {
        panic!("'names' should be an array");
    }
}
