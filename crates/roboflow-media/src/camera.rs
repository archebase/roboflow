// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Camera calibration types and LeRobot parameter writers.

use std::collections::HashMap;
use std::fs;
use std::path::Path;

use roboflow_core::{Result, RoboflowError};
use serde::{Deserialize, Serialize};

/// Camera intrinsic parameters in LeRobot format.
///
/// Intrinsic parameters describe the internal characteristics of a camera,
/// including focal length, principal point, and lens distortion.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CameraIntrinsic {
    /// Focal length x (pixels)
    pub fx: f64,
    /// Focal length y (pixels)
    pub fy: f64,
    /// Principal point x (pixels)
    pub ppx: f64,
    /// Principal point y (pixels)
    pub ppy: f64,
    /// Distortion model name
    pub distortion_model: String,
    /// k1 distortion coefficient
    pub k1: f64,
    /// k2 distortion coefficient
    pub k2: f64,
    /// k3 distortion coefficient
    pub k3: f64,
    /// p1 distortion coefficient
    pub p1: f64,
    /// p2 distortion coefficient
    pub p2: f64,
}

/// Camera extrinsic parameters in LeRobot format.
///
/// Extrinsic parameters describe the camera's position and orientation
/// relative to a reference coordinate system (typically robot base).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CameraExtrinsic {
    /// Extrinsic data wrapper (matches LeRobot format)
    pub extrinsic: ExtrinsicData,
}

/// The actual extrinsic data.
///
/// Contains rotation matrix and translation vector representing
/// the camera-to-world transformation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExtrinsicData {
    /// 3x3 rotation matrix (row-major)
    pub rotation_matrix: Vec<Vec<f64>>,
    /// Translation vector [x, y, z]
    pub translation_vector: Vec<f64>,
}

impl CameraExtrinsic {
    /// Create extrinsic from rotation matrix and translation.
    ///
    /// # Arguments
    ///
    /// * `rotation_matrix` - 3x3 rotation matrix as nested arrays
    /// * `translation` - Translation vector [x, y, z]
    pub fn new(rotation_matrix: [[f64; 3]; 3], translation: [f64; 3]) -> Self {
        Self {
            extrinsic: ExtrinsicData {
                rotation_matrix: vec![
                    rotation_matrix[0].to_vec(),
                    rotation_matrix[1].to_vec(),
                    rotation_matrix[2].to_vec(),
                ],
                translation_vector: translation.to_vec(),
            },
        }
    }

    /// Create extrinsic from flat arrays.
    ///
    /// # Arguments
    ///
    /// * `rotation_matrix` - 3x3 rotation matrix as flat array (row-major)
    /// * `translation` - Translation vector [x, y, z]
    pub fn from_arrays(rotation_matrix: [f64; 9], translation: [f64; 3]) -> Self {
        Self {
            extrinsic: ExtrinsicData {
                rotation_matrix: vec![
                    vec![rotation_matrix[0], rotation_matrix[1], rotation_matrix[2]],
                    vec![rotation_matrix[3], rotation_matrix[4], rotation_matrix[5]],
                    vec![rotation_matrix[6], rotation_matrix[7], rotation_matrix[8]],
                ],
                translation_vector: translation.to_vec(),
            },
        }
    }
}

/// Writer for camera parameters.
///
/// Handles writing intrinsic and extrinsic camera parameters to JSON files
/// in the LeRobot `parameters/` directory structure.
pub struct CameraParamsWriter<'a> {
    intrinsics: &'a HashMap<String, CameraIntrinsic>,
    extrinsics: &'a HashMap<String, CameraExtrinsic>,
}

impl<'a> CameraParamsWriter<'a> {
    /// Create a new camera params writer.
    pub fn new(
        intrinsics: &'a HashMap<String, CameraIntrinsic>,
        extrinsics: &'a HashMap<String, CameraExtrinsic>,
    ) -> Self {
        Self {
            intrinsics,
            extrinsics,
        }
    }

    /// Write camera parameters to the specified output directory.
    ///
    /// Creates JSON files in `{output_dir}/parameters/`:
    /// - `{camera}_intrinsic.json` for intrinsic parameters
    /// - `{camera}_extrinsic.json` for extrinsic parameters
    pub fn write(&self, output_dir: &Path) -> Result<()> {
        if self.intrinsics.is_empty() && self.extrinsics.is_empty() {
            return Ok(());
        }

        let params_dir = output_dir.join("parameters");
        fs::create_dir_all(&params_dir).map_err(|e| {
            RoboflowError::encode(
                "CameraParameters",
                format!("Failed to create parameters directory: {}", e),
            )
        })?;

        self.write_intrinsics(&params_dir)?;
        self.write_extrinsics(&params_dir)?;
        Ok(())
    }

    fn write_intrinsics(&self, params_dir: &Path) -> Result<()> {
        for (camera, intrinsic) in self.intrinsics {
            let filename = format!("{}_intrinsic.json", camera);
            let filepath = params_dir.join(&filename);

            let json = serde_json::to_string_pretty(intrinsic).map_err(|e| {
                RoboflowError::encode(
                    "CameraParameters",
                    format!("Failed to serialize intrinsic params for {}: {}", camera, e),
                )
            })?;

            fs::write(&filepath, json).map_err(|e| {
                RoboflowError::encode(
                    "CameraParameters",
                    format!("Failed to write intrinsic params for {}: {}", filename, e),
                )
            })?;

            tracing::debug!(
                camera = %camera,
                file = %filename,
                "Wrote camera intrinsics"
            );
        }
        Ok(())
    }

    fn write_extrinsics(&self, params_dir: &Path) -> Result<()> {
        for (camera, extrinsic) in self.extrinsics {
            let filename = format!("{}_extrinsic.json", camera);
            let filepath = params_dir.join(&filename);

            let json = serde_json::to_string_pretty(extrinsic).map_err(|e| {
                RoboflowError::encode(
                    "CameraParameters",
                    format!("Failed to serialize extrinsic params for {}: {}", camera, e),
                )
            })?;

            fs::write(&filepath, json).map_err(|e| {
                RoboflowError::encode(
                    "CameraParameters",
                    format!("Failed to write extrinsic params for {}: {}", filename, e),
                )
            })?;

            tracing::debug!(
                camera = %camera,
                file = %filename,
                "Wrote camera extrinsics"
            );
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn test_camera_intrinsic_creation() {
        let intrinsic = CameraIntrinsic {
            fx: 500.0,
            fy: 500.0,
            ppx: 320.0,
            ppy: 240.0,
            distortion_model: "plumb_bob".to_string(),
            k1: 0.1,
            k2: 0.01,
            k3: 0.001,
            p1: 0.0,
            p2: 0.0,
        };

        assert_eq!(intrinsic.fx, 500.0);
        assert_eq!(intrinsic.fy, 500.0);
        assert_eq!(intrinsic.ppx, 320.0);
        assert_eq!(intrinsic.ppy, 240.0);
        assert_eq!(intrinsic.distortion_model, "plumb_bob");
    }

    #[test]
    fn test_camera_intrinsic_serialization() {
        let intrinsic = CameraIntrinsic {
            fx: 500.0,
            fy: 500.0,
            ppx: 320.0,
            ppy: 240.0,
            distortion_model: "plumb_bob".to_string(),
            k1: 0.1,
            k2: 0.01,
            k3: 0.001,
            p1: 0.0,
            p2: 0.0,
        };

        let json = serde_json::to_string(&intrinsic).unwrap();
        let deserialized: CameraIntrinsic = serde_json::from_str(&json).unwrap();

        assert_eq!(deserialized.fx, intrinsic.fx);
        assert_eq!(deserialized.fy, intrinsic.fy);
        assert_eq!(deserialized.distortion_model, intrinsic.distortion_model);
    }

    #[test]
    fn test_camera_extrinsic_new() {
        let rotation = [[1.0, 0.0, 0.0], [0.0, 1.0, 0.0], [0.0, 0.0, 1.0]];
        let translation = [0.0, 0.0, 0.0];

        let extrinsic = CameraExtrinsic::new(rotation, translation);

        assert_eq!(extrinsic.extrinsic.rotation_matrix.len(), 3);
        assert_eq!(extrinsic.extrinsic.rotation_matrix[0], vec![1.0, 0.0, 0.0]);
        assert_eq!(extrinsic.extrinsic.translation_vector, vec![0.0, 0.0, 0.0]);
    }

    #[test]
    fn test_camera_extrinsic_from_arrays() {
        let rotation_flat = [1.0, 0.0, 0.0, 0.0, 1.0, 0.0, 0.0, 0.0, 1.0];
        let translation = [1.0, 2.0, 3.0];

        let extrinsic = CameraExtrinsic::from_arrays(rotation_flat, translation);

        assert_eq!(extrinsic.extrinsic.rotation_matrix.len(), 3);
        assert_eq!(extrinsic.extrinsic.rotation_matrix[0], vec![1.0, 0.0, 0.0]);
        assert_eq!(extrinsic.extrinsic.rotation_matrix[1], vec![0.0, 1.0, 0.0]);
        assert_eq!(extrinsic.extrinsic.rotation_matrix[2], vec![0.0, 0.0, 1.0]);
        assert_eq!(extrinsic.extrinsic.translation_vector, vec![1.0, 2.0, 3.0]);
    }

    #[test]
    fn test_camera_extrinsic_serialization() {
        let extrinsic = CameraExtrinsic::new(
            [[1.0, 0.0, 0.0], [0.0, 1.0, 0.0], [0.0, 0.0, 1.0]],
            [0.0, 0.0, 0.0],
        );

        let json = serde_json::to_string(&extrinsic).unwrap();
        let deserialized: CameraExtrinsic = serde_json::from_str(&json).unwrap();

        assert_eq!(
            deserialized.extrinsic.rotation_matrix,
            extrinsic.extrinsic.rotation_matrix
        );
        assert_eq!(
            deserialized.extrinsic.translation_vector,
            extrinsic.extrinsic.translation_vector
        );
    }

    #[test]
    fn test_empty_params() {
        let intrinsics = HashMap::new();
        let extrinsics = HashMap::new();
        let writer = CameraParamsWriter::new(&intrinsics, &extrinsics);

        let dir = tempdir().unwrap();
        let result = writer.write(dir.path());
        assert!(result.is_ok());
    }

    #[test]
    fn test_write_intrinsics() {
        let mut intrinsics = HashMap::new();
        intrinsics.insert(
            "cam_0".to_string(),
            CameraIntrinsic {
                fx: 500.0,
                fy: 500.0,
                ppx: 320.0,
                ppy: 240.0,
                distortion_model: "brown_conrady".to_string(),
                k1: 0.0,
                k2: 0.0,
                k3: 0.0,
                p1: 0.0,
                p2: 0.0,
            },
        );

        let extrinsics = HashMap::new();
        let writer = CameraParamsWriter::new(&intrinsics, &extrinsics);

        let dir = tempdir().unwrap();
        let result = writer.write(dir.path());
        assert!(result.is_ok());

        let filepath = dir.path().join("parameters/cam_0_intrinsic.json");
        assert!(filepath.exists());
    }
}
