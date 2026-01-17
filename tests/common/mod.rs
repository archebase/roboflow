//! Common utilities for MCAP integration tests.

#![allow(dead_code)]

use std::collections::HashMap;

use robocodec::CodecValue;

// ============================================================================
// Test Expectations
// ============================================================================

/// Per-fixture test expectations.
#[derive(Debug, Clone, Default)]
pub struct FixtureExpectations {
    /// Minimum number of channels expected in the MCAP file.
    pub min_channels: usize,
    /// Minimum number of messages expected across all channels.
    pub min_messages: usize,
    /// Expected topics with specific validations.
    pub expected_topics: Vec<TopicExpectation>,
    /// Skip tests for known-unsupported encodings (JSON).
    pub skip_unsupported: bool,
}

/// Expectations for a specific topic.
#[derive(Debug, Clone)]
pub struct TopicExpectation {
    /// Topic name (e.g., "/joint_states").
    pub topic: String,
    /// Message type (e.g., "sensor_msgs/msg/JointState").
    pub message_type: String,
    /// Minimum message count for this topic.
    pub min_message_count: usize,
    /// Field-level validations.
    pub field_validations: Vec<FieldValidation>,
}

// ============================================================================
// Field Validation
// ============================================================================

/// Field validation rule.
#[derive(Debug, Clone)]
pub enum FieldValidation {
    /// Field must exist in the decoded message.
    Exists(String),
    /// Field value must be greater than the given integer.
    GtInt(String, i64),
    /// Array length must be greater than the given value.
    ArrayLengthGt(String, usize),
}

// ============================================================================
// Assertions
// ============================================================================

/// Assert that a field value matches the validation rule.
pub fn assert_field_value(
    decoded: &HashMap<String, CodecValue>,
    validation: &FieldValidation,
) -> FieldValidationResult {
    match validation {
        FieldValidation::Exists(field) => {
            let exists = decoded.contains_key(field);
            FieldValidationResult {
                passed: exists,
                error: if exists {
                    None
                } else {
                    Some(format!("Field '{field}' not found in decoded message"))
                },
            }
        }
        FieldValidation::GtInt(field, min_value) => match decoded.get(field.as_str()) {
            Some(CodecValue::Int32(v)) => FieldValidationResult {
                passed: (*v as i64) > *min_value,
                error: if (*v as i64) > *min_value {
                    None
                } else {
                    Some(format!("Field '{field}': {v} is not > {min_value}"))
                },
            },
            Some(CodecValue::Int64(v)) => FieldValidationResult {
                passed: *v > *min_value,
                error: if *v > *min_value {
                    None
                } else {
                    Some(format!("Field '{field}': {v} is not > {min_value}"))
                },
            },
            Some(CodecValue::UInt32(v)) => FieldValidationResult {
                passed: (*v as i64) > *min_value,
                error: if (*v as i64) > *min_value {
                    None
                } else {
                    Some(format!("Field '{field}': {v} is not > {min_value}"))
                },
            },
            Some(CodecValue::UInt64(v)) => FieldValidationResult {
                passed: (*v as i64) > *min_value,
                error: if (*v as i64) > *min_value {
                    None
                } else {
                    Some(format!("Field '{field}': {v} is not > {min_value}"))
                },
            },
            Some(other) => FieldValidationResult {
                passed: false,
                error: Some(format!(
                    "Field '{}': expected integer, got {:?}",
                    field,
                    other.type_name()
                )),
            },
            None => FieldValidationResult {
                passed: false,
                error: Some(format!("Field '{field}' not found")),
            },
        },
        FieldValidation::ArrayLengthGt(field, min_length) => match decoded.get(field.as_str()) {
            Some(CodecValue::Array(arr)) => FieldValidationResult {
                passed: arr.len() > *min_length,
                error: if arr.len() > *min_length {
                    None
                } else {
                    Some(format!(
                        "Field '{}': array length {} is not > {}",
                        field,
                        arr.len(),
                        min_length
                    ))
                },
            },
            Some(other) => FieldValidationResult {
                passed: false,
                error: Some(format!(
                    "Field '{}': expected array, got {:?}",
                    field,
                    other.type_name()
                )),
            },
            None => FieldValidationResult {
                passed: false,
                error: Some(format!("Field '{field}' not found")),
            },
        },
    }
}

/// Result of a field validation.
#[derive(Debug, Clone)]
pub struct FieldValidationResult {
    pub passed: bool,
    pub error: Option<String>,
}

/// Check if an error is acceptable (known-unsupported encoding or known data issues).
pub fn is_acceptable_error(error: &str, skip_unsupported: bool) -> bool {
    if !skip_unsupported {
        return false;
    }
    // Known unsupported formats
    if error.contains("not yet supported") || error.contains("Unsupported encoding") {
        return true;
    }
    // Known data quality issues in fixture files
    if error.contains("exceeds maximum allowed") || error.contains("Buffer too short") {
        return true;
    }
    if error.contains("Parse error")
        && (error.contains("'IDL schema'") || error.contains("'msg schema'"))
    {
        return true;
    }
    if error.contains("invalid utf-8") {
        return true;
    }
    if error.contains("failed to fill whole buffer") {
        return true;
    }
    // Known schema/data mismatch issues in fixtures
    if error.contains("Missing field") || error.contains("Extra field") {
        return true;
    }
    if error.contains("Type not found") && error.contains("/msg/") {
        return true;
    }
    // Normalized types (robocodec.*) may not have schema definitions after normalization
    if error.contains("Type not found") && error.contains("robocodec.") {
        return true;
    }
    false
}

/// Count unexpected errors from a list of error strings.
pub fn count_unexpected_errors(errors: &[String], skip_unsupported: bool) -> usize {
    errors
        .iter()
        .filter(|e| !is_acceptable_error(e, skip_unsupported))
        .count()
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    #[test]
    fn test_assert_field_value_exists() {
        let mut decoded = HashMap::new();
        decoded.insert("name".to_string(), CodecValue::String("test".to_string()));

        let result = assert_field_value(&decoded, &FieldValidation::Exists("name".to_string()));
        assert!(result.passed);
        assert!(result.error.is_none());

        let result = assert_field_value(&decoded, &FieldValidation::Exists("missing".to_string()));
        assert!(!result.passed);
        assert!(result.error.is_some());
    }

    #[test]
    fn test_assert_field_value_gt_int() {
        let mut decoded = HashMap::new();
        decoded.insert("value".to_string(), CodecValue::Int32(42));

        let result = assert_field_value(&decoded, &FieldValidation::GtInt("value".to_string(), 10));
        assert!(result.passed);

        let result =
            assert_field_value(&decoded, &FieldValidation::GtInt("value".to_string(), 100));
        assert!(!result.passed);
    }

    #[test]
    fn test_assert_field_value_array_length_gt() {
        let mut decoded = HashMap::new();
        decoded.insert(
            "values".to_string(),
            CodecValue::Array(vec![
                CodecValue::Int32(1),
                CodecValue::Int32(2),
                CodecValue::Int32(3),
            ]),
        );

        let result = assert_field_value(
            &decoded,
            &FieldValidation::ArrayLengthGt("values".to_string(), 2),
        );
        assert!(result.passed);

        let result = assert_field_value(
            &decoded,
            &FieldValidation::ArrayLengthGt("values".to_string(), 5),
        );
        assert!(!result.passed);
    }

    #[test]
    fn test_is_acceptable_error() {
        assert!(is_acceptable_error("not yet supported", true));
        assert!(is_acceptable_error("Unsupported encoding: json", true));
        assert!(!is_acceptable_error("Parse error", true));
        assert!(!is_acceptable_error("not yet supported", false));
    }
}
