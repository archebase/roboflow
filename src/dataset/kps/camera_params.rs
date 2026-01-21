// Copyright (c) 2026 ArcheBase
// Roboflow is licensed under Mulan PSL v2.
// You can use this software according to the terms and conditions of the Mulan PSL v2.
// You may obtain a copy of Mulan PSL v2 at:
//     http://license.coscl.org.cn/MulanPSL2
// THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
// EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
// MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.

//! Camera parameter extraction and JSON writing for Kps datasets.
//!
//! Extracts camera intrinsic and extrinsic parameters from ROS/ROS2 messages
//! and writes them to JSON files as per the Kps v1.2 specification.
//!
//! ## Output Files
//!
//! For each camera:
//! - `<camera_name>_intrinsic_params.json`: fx, fy, cx, cy, width, height, distortion
//! - `<camera_name>_extrinsic_params.json`: frame_id, child_frame_id, position, orientation

use serde::Serialize;
use std::collections::HashMap;
use std::fs;
use std::path::Path;

use robocodec::CodecValue;

/// Camera intrinsic parameters.
#[derive(Debug, Clone, Serialize)]
pub struct IntrinsicParams {
    /// Focal length x (pixels)
    pub fx: f64,
    /// Focal length y (pixels)
    pub fy: f64,
    /// Principal point x (pixels)
    pub cx: f64,
    /// Principal point y (pixels)
    pub cy: f64,
    /// Image width (pixels)
    pub width: u32,
    /// Image height (pixels)
    pub height: u32,
    /// Distortion coefficients [k1, k2, k3, p1, p2]
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub distortion: Vec<f64>,
}

impl IntrinsicParams {
    /// Create intrinsic parameters from individual values.
    pub fn new(fx: f64, fy: f64, cx: f64, cy: f64, width: u32, height: u32) -> Self {
        Self {
            fx,
            fy,
            cx,
            cy,
            width,
            height,
            distortion: Vec::new(),
        }
    }

    /// Set distortion coefficients.
    pub fn with_distortion(mut self, distortion: Vec<f64>) -> Self {
        self.distortion = distortion;
        self
    }

    /// Create from ROS CameraInfo message fields.
    ///
    /// CameraInfo has:
    /// - K: [fx, 0, cx, 0, fy, cy, 0, 0, 1] (3x3 matrix as flat array)
    /// - D: [k1, k2, t1, t2, k3] or [k1, k2, k3, k4, k5, k6, ...]
    /// - width, height
    pub fn from_ros_camera_info(k: &[f64], d: &[f64], width: u32, height: u32) -> Option<Self> {
        if k.len() >= 9 {
            Some(Self {
                fx: k[0],
                fy: k[4],
                cx: k[2],
                cy: k[5],
                width,
                height,
                distortion: d.to_vec(),
            })
        } else {
            None
        }
    }
}

/// Camera extrinsic parameters (pose).
#[derive(Debug, Clone, Serialize)]
pub struct ExtrinsicParams {
    /// Parent frame ID
    pub frame_id: String,
    /// Child frame ID (camera frame)
    pub child_frame_id: String,
    /// Position [x, y, z] in meters
    pub position: Position,
    /// Orientation [x, y, z, w] as quaternion
    pub orientation: Orientation,
}

/// 3D position.
#[derive(Debug, Clone, Serialize)]
pub struct Position {
    pub x: f64,
    pub y: f64,
    pub z: f64,
}

impl Position {
    fn new(x: f64, y: f64, z: f64) -> Self {
        Self { x, y, z }
    }
}

/// Quaternion orientation.
#[derive(Debug, Clone, Serialize)]
pub struct Orientation {
    pub x: f64,
    pub y: f64,
    pub z: f64,
    pub w: f64,
}

impl Orientation {
    fn new(x: f64, y: f64, z: f64, w: f64) -> Self {
        Self { x, y, z, w }
    }
}

impl ExtrinsicParams {
    /// Create extrinsic parameters from a TF transform.
    pub fn from_tf_transform(
        frame_id: String,
        child_frame_id: String,
        translation: (f64, f64, f64),
        rotation: (f64, f64, f64, f64),
    ) -> Self {
        Self {
            frame_id,
            child_frame_id,
            position: Position::new(translation.0, translation.1, translation.2),
            orientation: Orientation::new(rotation.0, rotation.1, rotation.2, rotation.3),
        }
    }
}

/// Collected camera parameters.
#[derive(Debug, Clone, Default)]
pub struct CameraParams {
    /// Intrinsic parameters (if available)
    pub intrinsics: Option<IntrinsicParams>,
    /// Extrinsic parameters (if available)
    pub extrinsics: Option<ExtrinsicParams>,
}

/// Manager for collecting and writing camera parameters.
pub struct CameraParamCollector {
    /// Collected parameters by camera name
    cameras: HashMap<String, CameraParams>,
}

impl CameraParamCollector {
    /// Create a new collector.
    pub fn new() -> Self {
        Self {
            cameras: HashMap::new(),
        }
    }

    /// Add or update camera parameters.
    pub fn add_camera(&mut self, name: String, params: CameraParams) {
        self.cameras.insert(name, params);
    }

    /// Update intrinsics for a camera.
    pub fn update_intrinsics(&mut self, name: &str, intrinsics: IntrinsicParams) {
        self.cameras.entry(name.to_string()).or_default().intrinsics = Some(intrinsics);
    }

    /// Update extrinsics for a camera.
    pub fn update_extrinsics(&mut self, name: &str, extrinsics: ExtrinsicParams) {
        self.cameras.entry(name.to_string()).or_default().extrinsics = Some(extrinsics);
    }

    /// Get all camera names.
    pub fn camera_names(&self) -> Vec<String> {
        self.cameras.keys().cloned().collect()
    }

    /// Write all camera parameter JSON files.
    ///
    /// Creates `<camera>_intrinsic_params.json` and `<camera>_extrinsic_params.json`
    /// for each camera in the output directory.
    pub fn write_all(&self, output_dir: &Path) -> Result<(), Box<dyn std::error::Error>> {
        for (name, params) in &self.cameras {
            // Write intrinsics if available
            if let Some(intrinsics) = &params.intrinsics {
                self.write_intrinsics(output_dir, name, intrinsics)?;
            }

            // Write extrinsics if available
            if let Some(extrinsics) = &params.extrinsics {
                self.write_extrinsics(output_dir, name, extrinsics)?;
            }
        }
        Ok(())
    }

    /// Write intrinsic parameters JSON file.
    fn write_intrinsics(
        &self,
        output_dir: &Path,
        camera_name: &str,
        params: &IntrinsicParams,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let filename = format!("{}_intrinsic_params.json", camera_name);
        let filepath = output_dir.join(&filename);

        let json = serde_json::to_string_pretty(params)?;
        fs::write(&filepath, json)?;

        println!("  Wrote camera intrinsics: {}", filename);
        Ok(())
    }

    /// Write extrinsic parameters JSON file.
    fn write_extrinsics(
        &self,
        output_dir: &Path,
        camera_name: &str,
        params: &ExtrinsicParams,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let filename = format!("{}_extrinsic_params.json", camera_name);
        let filepath = output_dir.join(&filename);

        let json = serde_json::to_string_pretty(params)?;
        fs::write(&filepath, json)?;

        println!("  Wrote camera extrinsics: {}", filename);
        Ok(())
    }

    /// Extract camera parameters from decoded messages.
    ///
    /// This method processes MCAP messages and extracts camera intrinsic/extrinsic
    /// parameters from ROS CameraInfo and TF messages.
    ///
    /// # Arguments
    /// * `reader` - MCAP reader to get messages from
    /// * `camera_topics` - Map of camera name to topic prefix (e.g., "hand_right" -> "/camera/hand/right")
    /// * `parent_frame` - Parent frame for extrinsics (e.g., "base_link")
    pub fn extract_from_mcap(
        &mut self,
        reader: &robocodec::mcap::McapReader,
        camera_topics: HashMap<String, String>,
        parent_frame: &str,
    ) -> Result<(), Box<dyn std::error::Error>> {
        println!("  Extracting camera parameters...");

        let iter = reader.decode_messages()?;

        // Track camera frames for TF lookup
        let mut camera_frames: HashMap<String, String> = HashMap::new();
        // Store all transforms for later lookup: child_frame_id -> (frame_id, transform)
        let mut transforms: HashMap<String, Vec<(String, ExtrinsicParams)>> = HashMap::new();

        for result in iter {
            let (msg, channel_info) = result?;

            // Check if this is a camera_info topic
            if let Some(camera_name) =
                self.find_camera_for_topic(&channel_info.topic, &camera_topics)
            {
                if let Some(intrinsics) = self.extract_camera_info(&msg, &camera_name) {
                    self.update_intrinsics(&camera_name, intrinsics);

                    // Try to extract the frame_id from camera_info header
                    if let Some(frame_id) = self.get_nested_string(&msg, &["header", "frame_id"]) {
                        camera_frames.insert(camera_name.clone(), frame_id);
                    }
                }
            }

            // Check if this is a TF topic
            if channel_info.topic == "/tf" || channel_info.topic == "/tf_static" {
                self.collect_tf_transforms(&msg, &mut transforms);
            }
        }

        // Now match up camera frames with transforms
        for (camera_name, camera_frame) in &camera_frames {
            if let Some(tf_list) = transforms.get(camera_frame) {
                // Find transform from parent_frame
                for (frame_id, extrinsics) in tf_list {
                    if frame_id == parent_frame {
                        self.update_extrinsics(camera_name, extrinsics.clone());
                        break;
                    }
                }
            }
        }

        Ok(())
    }

    /// Find camera name for a given topic.
    fn find_camera_for_topic(
        &self,
        topic: &str,
        camera_topics: &HashMap<String, String>,
    ) -> Option<String> {
        for (name, prefix) in camera_topics {
            if topic.starts_with(prefix) || topic.starts_with(&format!("{}/", prefix)) {
                return Some(name.clone());
            }
        }
        None
    }

    /// Extract intrinsic parameters from a CameraInfo message.
    fn extract_camera_info(
        &self,
        msg: &robocodec::DecodedMessage,
        _camera_name: &str,
    ) -> Option<IntrinsicParams> {
        // Extract K matrix (camera intrinsic matrix)
        let k = self.get_numeric_array(msg, &["K"])?;

        // Extract D array (distortion coefficients)
        let d = self.get_numeric_array(msg, &["D"]).unwrap_or_default();

        // Extract image dimensions
        let width = self.get_u32(msg, &["width"]).unwrap_or(0);
        let height = self.get_u32(msg, &["height"]).unwrap_or(0);

        IntrinsicParams::from_ros_camera_info(&k, &d, width, height)
    }

    /// Collect TF transforms from a TF message.
    fn collect_tf_transforms(
        &self,
        msg: &robocodec::DecodedMessage,
        transforms: &mut HashMap<String, Vec<(String, ExtrinsicParams)>>,
    ) {
        // TF messages contain a "transforms" array
        if let Some(CodecValue::Array(transforms_array)) = msg.get("transforms") {
            for transform in transforms_array.iter() {
                if let CodecValue::Struct(tf_obj) = transform {
                    // Extract child_frame_id
                    let child_frame_id = self
                        .get_nested_string(tf_obj, &["child_frame_id"])
                        .unwrap_or("".to_string());

                    // Extract frame_id from header
                    let frame_id = self
                        .get_nested_string(tf_obj, &["header", "frame_id"])
                        .unwrap_or("".to_string());

                    // Extract transform data
                    if let Some(transform_data) = self.get_nested_struct(tf_obj, &["transform"]) {
                        // Extract translation
                        let translation_data =
                            self.get_nested_struct(transform_data, &["translation"]);
                        let translation = if let Some(t) = translation_data {
                            (
                                self.get_f64(t, &["x"]).unwrap_or(0.0),
                                self.get_f64(t, &["y"]).unwrap_or(0.0),
                                self.get_f64(t, &["z"]).unwrap_or(0.0),
                            )
                        } else {
                            (0.0, 0.0, 0.0)
                        };

                        // Extract rotation (quaternion)
                        let rotation_data = self.get_nested_struct(transform_data, &["rotation"]);
                        let rotation = if let Some(r) = rotation_data {
                            (
                                self.get_f64(r, &["x"]).unwrap_or(0.0),
                                self.get_f64(r, &["y"]).unwrap_or(0.0),
                                self.get_f64(r, &["z"]).unwrap_or(0.0),
                                self.get_f64(r, &["w"]).unwrap_or(1.0),
                            )
                        } else {
                            (0.0, 0.0, 0.0, 1.0)
                        };

                        let extrinsics = ExtrinsicParams::from_tf_transform(
                            frame_id.clone(),
                            child_frame_id.clone(),
                            translation,
                            rotation,
                        );

                        transforms
                            .entry(child_frame_id)
                            .or_default()
                            .push((frame_id.clone(), extrinsics));
                    }
                }
            }
        }
    }

    /// Get nested string value from a message.
    fn get_nested_string(&self, msg: &robocodec::DecodedMessage, path: &[&str]) -> Option<String> {
        let mut current = msg;

        for (i, &key) in path.iter().enumerate() {
            if i == path.len() - 1 {
                // Last element - get the string value
                if let Some(CodecValue::String(s)) = current.get(key) {
                    return Some(s.clone());
                }
                return None;
            }

            // Navigate deeper
            if let Some(CodecValue::Struct(nested)) = current.get(key) {
                current = nested;
            } else {
                return None;
            }
        }
        None
    }

    /// Get nested struct from a message.
    fn get_nested_struct<'a>(
        &self,
        msg: &'a robocodec::DecodedMessage,
        path: &[&str],
    ) -> Option<&'a robocodec::DecodedMessage> {
        let mut current = msg;

        for &key in path.iter() {
            if let Some(CodecValue::Struct(nested)) = current.get(key) {
                current = nested;
            } else {
                return None;
            }
        }
        Some(current)
    }

    /// Get numeric array from a message at the given path.
    fn get_numeric_array(
        &self,
        msg: &robocodec::DecodedMessage,
        path: &[&str],
    ) -> Option<Vec<f64>> {
        let mut current = msg;

        for (i, &key) in path.iter().enumerate() {
            if i == path.len() - 1 {
                // Last element - get the array
                if let Some(CodecValue::Array(arr)) = current.get(key) {
                    let mut values = Vec::new();
                    for item in arr.iter() {
                        match item {
                            CodecValue::Float64(n) => values.push(*n),
                            CodecValue::Float32(n) => values.push(*n as f64),
                            CodecValue::Int32(n) => values.push(*n as f64),
                            CodecValue::Int64(n) => values.push(*n as f64),
                            CodecValue::UInt32(n) => values.push(*n as f64),
                            CodecValue::UInt64(n) => values.push(*n as f64),
                            _ => {}
                        }
                    }
                    return Some(values);
                }
                return None;
            }

            // Navigate deeper
            if let Some(CodecValue::Struct(nested)) = current.get(key) {
                current = nested;
            } else {
                return None;
            }
        }
        None
    }

    /// Get f64 value at a nested path.
    fn get_f64(&self, msg: &robocodec::DecodedMessage, path: &[&str]) -> Option<f64> {
        let mut current = msg;

        for (i, &key) in path.iter().enumerate() {
            if i == path.len() - 1 {
                // Last element
                if let Some(val) = current.get(key) {
                    return match val {
                        CodecValue::Float64(n) => Some(*n),
                        CodecValue::Float32(n) => Some(*n as f64),
                        CodecValue::Int32(n) => Some(*n as f64),
                        CodecValue::Int64(n) => Some(*n as f64),
                        CodecValue::UInt32(n) => Some(*n as f64),
                        _ => None,
                    };
                }
                return None;
            }

            if let Some(CodecValue::Struct(nested)) = current.get(key) {
                current = nested;
            } else {
                return None;
            }
        }
        None
    }

    /// Get u32 value at a nested path.
    fn get_u32(&self, msg: &robocodec::DecodedMessage, path: &[&str]) -> Option<u32> {
        let mut current = msg;

        for (i, &key) in path.iter().enumerate() {
            if i == path.len() - 1 {
                if let Some(val) = current.get(key) {
                    return match val {
                        CodecValue::UInt32(n) => Some(*n),
                        CodecValue::UInt16(n) => Some(*n as u32),
                        CodecValue::UInt8(n) => Some(*n as u32),
                        CodecValue::Int32(n) => Some(*n as u32),
                        _ => None,
                    };
                }
                return None;
            }

            if let Some(CodecValue::Struct(nested)) = current.get(key) {
                current = nested;
            } else {
                return None;
            }
        }
        None
    }
}

impl Default for CameraParamCollector {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_intrinsic_params_new() {
        let params = IntrinsicParams::new(500.0, 500.0, 320.0, 240.0, 640, 480);
        assert_eq!(params.fx, 500.0);
        assert_eq!(params.fy, 500.0);
        assert_eq!(params.cx, 320.0);
        assert_eq!(params.cy, 240.0);
        assert_eq!(params.width, 640);
        assert_eq!(params.height, 480);
        assert!(params.distortion.is_empty());
    }

    #[test]
    fn test_intrinsic_params_with_distortion() {
        let params = IntrinsicParams::new(500.0, 500.0, 320.0, 240.0, 640, 480)
            .with_distortion(vec![0.1, 0.01, -0.001, 0.0, 0.0]);
        assert_eq!(params.distortion.len(), 5);
    }

    #[test]
    fn test_intrinsic_params_from_ros_camera_info() {
        // K matrix: [fx, 0, cx, 0, fy, cy, 0, 0, 1]
        let k = vec![500.0, 0.0, 320.0, 0.0, 500.0, 240.0, 0.0, 0.0, 1.0];
        let d = vec![0.1, 0.01, -0.001];

        let params = IntrinsicParams::from_ros_camera_info(&k, &d, 640, 480).unwrap();
        assert_eq!(params.fx, 500.0);
        assert_eq!(params.fy, 500.0);
        assert_eq!(params.cx, 320.0);
        assert_eq!(params.cy, 240.0);
        assert_eq!(params.distortion, d);
    }

    #[test]
    fn test_extrinsic_params_from_tf() {
        let params = ExtrinsicParams::from_tf_transform(
            "base_link".to_string(),
            "camera_link".to_string(),
            (0.1, 0.2, 0.3),
            (0.0, 0.0, 0.0, 1.0),
        );
        assert_eq!(params.frame_id, "base_link");
        assert_eq!(params.child_frame_id, "camera_link");
        assert_eq!(params.position.x, 0.1);
        assert_eq!(params.position.y, 0.2);
        assert_eq!(params.position.z, 0.3);
        assert_eq!(params.orientation.x, 0.0);
        assert_eq!(params.orientation.y, 0.0);
        assert_eq!(params.orientation.z, 0.0);
        assert_eq!(params.orientation.w, 1.0);
    }

    #[test]
    fn test_camera_param_collector() {
        let mut collector = CameraParamCollector::new();

        collector.update_intrinsics(
            "hand_right",
            IntrinsicParams::new(500.0, 500.0, 320.0, 240.0, 640, 480),
        );

        let names = collector.camera_names();
        assert_eq!(names.len(), 1);
        assert_eq!(names[0], "hand_right");
    }
}
