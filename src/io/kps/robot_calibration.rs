//! Robot calibration JSON generation from URDF files.
//!
//! Parses URDF files to extract joint information and generates
//! `robot_calibration.json` as required by Kps dataset format.

use serde::Serialize;
use std::collections::HashMap;
use std::fs;
use std::path::Path;

/// Robot calibration data for a single joint.
#[derive(Debug, Clone, Serialize)]
pub struct JointCalibration {
    /// Joint index/ID
    pub id: usize,

    /// Drive mode (0 = position control, etc.)
    pub drive_mode: u32,

    /// Homing offset in radians
    pub homing_offset: f64,

    /// Minimum joint limit in radians
    pub range_min: f64,

    /// Maximum joint limit in radians
    pub range_max: f64,
}

/// Robot calibration JSON structure.
#[derive(Debug, Clone, Serialize)]
pub struct RobotCalibration {
    /// Map of joint name to calibration data
    #[serde(flatten)]
    pub joints: HashMap<String, JointCalibration>,
}

/// URDF joint element.
#[derive(Debug, Clone)]
struct UrdfJoint {
    name: String,
    #[allow(dead_code)]
    joint_type: String,
    limit: Option<JointLimit>,
}

/// URDF joint limit element.
#[derive(Debug, Clone)]
struct JointLimit {
    lower: f64,
    upper: f64,
}

/// Robot calibration generator from URDF files.
pub struct RobotCalibrationGenerator;

impl RobotCalibrationGenerator {
    /// Generate robot calibration from a URDF file.
    pub fn from_urdf(urdf_path: &Path) -> Result<RobotCalibration, Box<dyn std::error::Error>> {
        let content = fs::read_to_string(urdf_path)?;
        Self::from_urdf_str(&content)
    }

    /// Generate robot calibration from URDF XML string.
    pub fn from_urdf_str(xml: &str) -> Result<RobotCalibration, Box<dyn std::error::Error>> {
        let mut joints = HashMap::new();

        // Simple XML parsing for joint elements
        for joint_elem in Self::parse_urdf_joints(xml) {
            let id = joints.len();

            // Get limits, defaulting to +/- pi if not specified
            let (min, max) = if let Some(ref limit) = joint_elem.limit {
                (limit.lower, limit.upper)
            } else {
                (-std::f64::consts::PI, std::f64::consts::PI)
            };

            let calibration = JointCalibration {
                id,
                drive_mode: 0,      // Default to position control
                homing_offset: 0.0, // Default no offset
                range_min: min,
                range_max: max,
            };

            joints.insert(joint_elem.name.clone(), calibration);
        }

        Ok(RobotCalibration { joints })
    }

    /// Generate robot calibration from joint names (minimal).
    ///
    /// Use this when no URDF is available - creates default calibration
    /// with standard joint limits.
    pub fn from_joint_names(joint_names: &[String]) -> RobotCalibration {
        let mut joints = HashMap::new();

        for (i, name) in joint_names.iter().enumerate() {
            joints.insert(
                name.clone(),
                JointCalibration {
                    id: i,
                    drive_mode: 0,
                    homing_offset: 0.0,
                    range_min: -std::f64::consts::PI,
                    range_max: std::f64::consts::PI,
                },
            );
        }

        RobotCalibration { joints }
    }

    /// Write robot calibration JSON to file.
    pub fn write_calibration(
        output_dir: &Path,
        calibration: &RobotCalibration,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let json = serde_json::to_string_pretty(calibration)?;
        let path = output_dir.join("robot_calibration.json");
        fs::write(&path, json)?;
        println!("Created: {}", path.display());
        Ok(())
    }

    /// Parse joint elements from URDF XML.
    fn parse_urdf_joints(xml: &str) -> Vec<UrdfJoint> {
        let mut joints = Vec::new();

        // Find all <joint> elements
        let mut remaining = xml;
        while let Some(start) = remaining.find("<joint") {
            remaining = &remaining[start..];

            // Find the closing '>'
            let end = match remaining.find('>') {
                Some(e) => e,
                None => break,
            };
            let joint_tag = &remaining[..=end];

            // Extract joint name
            let name = Self::extract_xml_attr(joint_tag, "name")
                .unwrap_or_else(|| format!("joint_{}", joints.len()));

            // Extract joint type
            let joint_type =
                Self::extract_xml_attr(joint_tag, "type").unwrap_or("revolute".to_string());

            // Extract limits from <limit> child element
            let limit = Self::parse_joint_limit(&remaining[end..]);

            joints.push(UrdfJoint {
                name,
                joint_type,
                limit,
            });

            // Move past this joint element
            if let Some(close) = remaining.find("</joint>") {
                remaining = &remaining[close + 8..];
            } else {
                break;
            }
        }

        joints
    }

    /// Parse <limit> element from joint content.
    fn parse_joint_limit(content: &str) -> Option<JointLimit> {
        let start = content.find("<limit")?;
        let content_from_limit = &content[start..];

        // Find the closing '>' or '/>'
        let tag_end = content_from_limit.find('>')?;
        let tag_content = &content_from_limit[..tag_end];

        // Find all attribute pairs using simple string search
        let mut lower = None;
        let mut upper = None;

        // Find lower="..."
        if let Some(lower_pos) = tag_content.find("lower=\"") {
            let value_start = lower_pos + 7; // len("lower=\"")
            let search_area = &tag_content[value_start..];
            if let Some(value_end) = search_area.find('"') {
                let value_str = &tag_content[value_start..value_start + value_end];
                lower = value_str.parse().ok();
            }
        }

        // Find upper="..."
        if let Some(upper_pos) = tag_content.find("upper=\"") {
            let value_start = upper_pos + 7; // len("upper=\"")
            let search_area = &tag_content[value_start..];
            if let Some(value_end) = search_area.find('"') {
                let value_str = &tag_content[value_start..value_start + value_end];
                upper = value_str.parse().ok();
            }
        }

        Some(JointLimit {
            lower: lower.unwrap_or(-std::f64::consts::PI),
            upper: upper.unwrap_or(std::f64::consts::PI),
        })
    }

    /// Extract an XML attribute value.
    fn extract_xml_attr(tag: &str, attr_name: &str) -> Option<String> {
        let pattern = &format!(r#"{}=""#, attr_name);
        let start = tag.find(pattern)?;
        let value_start = start + pattern.len();
        let value_end = tag[value_start..].find('"')?;
        Some(tag[value_start..value_start + value_end].to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const SAMPLE_URDF: &str = r#"
<?xml version="1.0"?>
<robot name="test_robot">
  <joint name="joint1" type="revolute">
    <limit lower="-3.14" upper="3.14"/>
  </joint>
  <joint name="joint2" type="continuous">
    <limit lower="-6.28" upper="6.28"/>
  </joint>
  <joint name="gripper" type="prismatic">
    <limit lower="0.0" upper="0.1"/>
  </joint>
</robot>
"#;

    #[test]
    fn test_parse_urdf_joints() {
        let joints = RobotCalibrationGenerator::parse_urdf_joints(SAMPLE_URDF);
        println!("Parsed joints: {:?}", joints);
        assert_eq!(joints.len(), 3);
        assert_eq!(joints[0].name, "joint1");
        assert_eq!(joints[1].name, "joint2");
        assert_eq!(joints[2].name, "gripper");
    }

    #[test]
    fn test_from_urdf_str() {
        let calibration = RobotCalibrationGenerator::from_urdf_str(SAMPLE_URDF).unwrap();
        assert_eq!(calibration.joints.len(), 3);

        let joint1 = calibration.joints.get("joint1").unwrap();
        assert_eq!(joint1.id, 0);
        assert_eq!(joint1.range_min, -3.14);
        assert_eq!(joint1.range_max, 3.14);
    }

    #[test]
    fn test_from_joint_names() {
        let names = vec![
            "joint_a".to_string(),
            "joint_b".to_string(),
            "joint_c".to_string(),
        ];

        let calibration = RobotCalibrationGenerator::from_joint_names(&names);
        assert_eq!(calibration.joints.len(), 3);

        let joint_a = calibration.joints.get("joint_a").unwrap();
        assert_eq!(joint_a.id, 0);
    }

    #[test]
    fn test_serialize_calibration() {
        let calibration = RobotCalibrationGenerator::from_urdf_str(SAMPLE_URDF).unwrap();
        let json = serde_json::to_string_pretty(&calibration).unwrap();

        assert!(json.contains("joint1"));
        assert!(json.contains("range_min"));
        assert!(json.contains("drive_mode"));
    }
}
