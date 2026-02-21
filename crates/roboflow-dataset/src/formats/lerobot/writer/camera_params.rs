// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Camera parameters writing utilities.
//!
//! This module provides utilities for writing camera intrinsic and extrinsic
//! parameters to JSON files in the LeRobot format.

use std::collections::HashMap;
use std::fs;
use std::path::Path;

use roboflow_core::{Result, RoboflowError};

use super::camera::{CameraExtrinsic, CameraIntrinsic};

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

        // Check file was created
        let filepath = dir.path().join("parameters/cam_0_intrinsic.json");
        assert!(filepath.exists());
    }
}
