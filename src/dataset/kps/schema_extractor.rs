// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Schema-aware message extraction for Kps datasets.
//!
//! This module provides field-aware extraction from ROS/ROS2 messages,
//! organizing data into the HDF5 structure required by Kps.

use std::collections::HashMap;

use robocodec::CodecValue;

/// Extracted data organized for HDF5 storage.
#[derive(Debug, Clone, Default)]
pub struct ExtractedData {
    /// Position arrays organized by joint group
    pub joint_positions: HashMap<String, Vec<f32>>,
    /// Velocity arrays organized by joint group
    pub joint_velocities: HashMap<String, Vec<f32>>,
    /// Joint name arrays
    pub joint_names: HashMap<String, Vec<String>>,
    /// Image data
    pub images: HashMap<String, ImageData>,
    /// Other state data
    pub state_data: HashMap<String, Vec<f32>>,
    /// Action data
    pub action_data: HashMap<String, Vec<f32>>,
}

/// Image data with metadata.
#[derive(Debug, Clone)]
pub struct ImageData {
    pub width: u32,
    pub height: u32,
    pub data: Vec<u8>,
    pub is_depth: bool,
}

/// Schema-aware message extractor.
pub struct SchemaAwareExtractor;

impl SchemaAwareExtractor {
    /// Extract data from a decoded message based on its message type.
    pub fn extract_message(
        message_type: &str,
        topic: &str,
        data: &[(String, CodecValue)],
    ) -> ExtractedData {
        match message_type {
            "sensor_msgs/JointState" | "sensor_msgs/msg/JointState" => {
                Self::extract_joint_state(data)
            }
            "sensor_msgs/Image" | "sensor_msgs/msg/Image" => {
                Self::extract_image(topic, data, false)
            }
            "sensor_msgs/CompressedImage" | "sensor_msgs/msg/CompressedImage" => {
                Self::extract_image(topic, data, false)
            }
            "stereo_msgs/DisparityImage" | "stereo_msgs/msg/DisparityImage" => {
                Self::extract_disparity(topic, data)
            }
            _ => Self::extract_generic(data),
        }
    }

    /// Extract JointState message into organized joint data.
    fn extract_joint_state(data: &[(String, CodecValue)]) -> ExtractedData {
        let mut result = ExtractedData::default();
        let mut names = Vec::new();
        let mut positions = Vec::new();
        let mut velocities = Vec::new();

        for (key, value) in data.iter() {
            match key.as_str() {
                "name" => {
                    if let CodecValue::Array(arr) = value {
                        for v in arr.iter() {
                            if let CodecValue::String(s) = v {
                                names.push(s.clone());
                            }
                        }
                    }
                }
                "position" => {
                    if let CodecValue::Array(arr) = value {
                        for v in arr.iter() {
                            if let CodecValue::Float64(f) = v {
                                positions.push(*f as f32);
                            } else if let CodecValue::Float32(f) = v {
                                positions.push(*f);
                            }
                        }
                    }
                }
                "velocity" => {
                    if let CodecValue::Array(arr) = value {
                        for v in arr.iter() {
                            if let CodecValue::Float64(f) = v {
                                velocities.push(*f as f32);
                            } else if let CodecValue::Float32(f) = v {
                                velocities.push(*f);
                            }
                        }
                    }
                }
                _ => {}
            }
        }

        let joint_groups = Self::organize_joints_by_group(&names);

        for (group, indices) in &joint_groups {
            let group_positions: Vec<f32> = indices
                .iter()
                .filter_map(|&i| positions.get(i).copied())
                .collect();
            let group_velocities: Vec<f32> = indices
                .iter()
                .filter_map(|&i| velocities.get(i).copied())
                .collect();
            let group_names: Vec<String> = indices
                .iter()
                .filter_map(|&i| names.get(i).cloned())
                .collect();

            if !group_positions.is_empty() {
                result
                    .joint_positions
                    .insert(group.clone(), group_positions);
            }
            if !group_velocities.is_empty() {
                result
                    .joint_velocities
                    .insert(group.clone(), group_velocities);
            }
            if !group_names.is_empty() {
                result.joint_names.insert(group.clone(), group_names);
            }
        }

        if !positions.is_empty() && result.joint_positions.is_empty() {
            result
                .joint_positions
                .insert("joint".to_string(), positions);
        }
        if !velocities.is_empty() && result.joint_velocities.is_empty() {
            result
                .joint_velocities
                .insert("joint".to_string(), velocities);
        }
        if !names.is_empty() && result.joint_names.is_empty() {
            result.joint_names.insert("joint".to_string(), names);
        }

        result
    }

    /// Extract image data from an Image message.
    fn extract_image(topic: &str, data: &[(String, CodecValue)], is_depth: bool) -> ExtractedData {
        let mut result = ExtractedData::default();
        let mut width = 0u32;
        let mut height = 0u32;
        let mut image_data: Option<Vec<u8>> = None;

        for (key, value) in data.iter() {
            match key.as_str() {
                "width" => {
                    if let CodecValue::UInt32(w) = value {
                        width = *w;
                    }
                }
                "height" => {
                    if let CodecValue::UInt32(h) = value {
                        height = *h;
                    }
                }
                "data" => {
                    if let CodecValue::Bytes(b) = value {
                        image_data = Some(b.clone());
                    }
                }
                _ => {}
            }
        }

        if let Some(data) = image_data {
            let camera_name = Self::topic_to_camera_name(topic);
            result.images.insert(
                camera_name,
                ImageData {
                    width,
                    height,
                    data,
                    is_depth,
                },
            );
        }

        result
    }

    /// Extract disparity image (16-bit depth).
    fn extract_disparity(topic: &str, data: &[(String, CodecValue)]) -> ExtractedData {
        Self::extract_image(topic, data, true)
    }

    /// Generic extraction for unknown message types.
    fn extract_generic(data: &[(String, CodecValue)]) -> ExtractedData {
        let mut result = ExtractedData::default();
        let mut numeric_values = Vec::new();

        for (_key, value) in data.iter() {
            match value {
                CodecValue::Float32(n) => numeric_values.push(*n),
                CodecValue::Float64(n) => numeric_values.push(*n as f32),
                _ => {}
            }
        }

        if !numeric_values.is_empty() {
            result
                .state_data
                .insert("generic".to_string(), numeric_values);
        }

        result
    }

    /// Organize joint names into groups based on naming patterns.
    fn organize_joints_by_group(names: &[String]) -> HashMap<String, Vec<usize>> {
        let mut groups: HashMap<String, Vec<usize>> = HashMap::new();

        let patterns: [(&str, &[&str]); 6] = [
            ("effector", &["gripper", "effector", "finger"]),
            ("end", &["end_effector", "tool"]),
            ("head", &["head", "neck", "camera"]),
            ("arm", &["arm", "elbow", "shoulder", "wrist"]),
            ("leg", &["leg", "knee", "ankle", "hip", "foot"]),
            ("waist", &["waist", "torso", "spine"]),
        ];

        for (i, name) in names.iter().enumerate() {
            let name_lower = name.to_lowercase();
            let mut assigned = false;

            for (group, keywords) in &patterns {
                for keyword in *keywords {
                    if name_lower.contains(keyword) {
                        groups.entry(group.to_string()).or_default().push(i);
                        assigned = true;
                        break;
                    }
                }
                if assigned {
                    break;
                }
            }

            if !assigned {
                groups.entry("joint".to_string()).or_default().push(i);
            }
        }

        groups
    }

    /// Convert topic name to camera name.
    fn topic_to_camera_name(topic: &str) -> String {
        topic.trim_start_matches('/').replace('/', "_")
    }
}

/// Helper for detecting depth image topics.
pub fn is_depth_topic(topic: &str) -> bool {
    let topic_lower = topic.to_lowercase();
    topic_lower.contains("depth")
        || topic_lower.contains("disparity")
        || topic_lower.contains("range")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_topic_to_camera_name() {
        assert_eq!(
            SchemaAwareExtractor::topic_to_camera_name("/camera/high"),
            "camera_high"
        );
    }

    #[test]
    fn test_is_depth_topic() {
        assert!(is_depth_topic("/depth/image"));
        assert!(is_depth_topic("/camera/depth"));
        assert!(!is_depth_topic("/camera/rgb"));
    }

    #[test]
    fn test_organize_joints() {
        let names = vec![
            "gripper_joint".into(),
            "head_pan".into(),
            "left_knee".into(),
        ];

        let groups = SchemaAwareExtractor::organize_joints_by_group(&names);

        assert!(groups.contains_key("effector"));
        assert!(groups.contains_key("head"));
        assert!(groups.contains_key("leg"));
    }
}
