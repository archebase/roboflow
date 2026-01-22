// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Kps delivery disk structure generation.
//!
//! Creates the full directory structure required for Kps dataset delivery.
//!
//! ## Structure
//!
//! ```text
//! F盘/  (or configured root)
//! └── <Robot>-<EndEffector>-<Scene>/
//!     ├── episode_0/
//!     │   ├── props/
//!     │   ├── reward_0.parquet
//!     │   └── ...
//!     ├── meta/
//!     │   ├── info.json
//!     │   └── episodes/
//!     ├── videos/
//!     │   ├── camera_0.mp4
//!     │   └── depth_camera_0.mkv
//!     ├── URDF/
//!     │   └── <Robot>-<EndEffector>-v1.0/
//!     │       └── robot_calibration.json
//!     └── README.md
//! ```

use std::fs;
use std::path::{Path, PathBuf};

use crate::dataset::kps::{KpsConfig, RobotCalibration};

/// Configuration for delivery structure generation.
#[derive(Debug, Clone)]
pub struct DeliveryConfig {
    /// Root directory (e.g., "F盘" for Chinese systems)
    pub root: PathBuf,

    /// Robot name
    pub robot_name: String,

    /// End effector name
    pub end_effector: String,

    /// Scene name
    pub scene_name: String,

    /// Version string
    pub version: String,
}

impl Default for DeliveryConfig {
    fn default() -> Self {
        Self {
            root: PathBuf::from("F盘"),
            robot_name: "Robot".to_string(),
            end_effector: "Gripper".to_string(),
            scene_name: "Scene1".to_string(),
            version: "v1.0".to_string(),
        }
    }
}

impl DeliveryConfig {
    pub fn new(
        root: impl AsRef<Path>,
        robot_name: String,
        end_effector: String,
        scene_name: String,
    ) -> Self {
        Self {
            root: root.as_ref().to_path_buf(),
            robot_name,
            end_effector,
            scene_name,
            version: "v1.0".to_string(),
        }
    }
}

/// Delivery disk structure generator.
pub struct DeliveryBuilder;

impl DeliveryBuilder {
    /// Create the full delivery structure from a converted dataset.
    ///
    /// # Arguments
    /// * `source_dir` - Directory containing the converted dataset
    /// * `config` - Delivery configuration
    /// * `dataset_config` - Kps dataset configuration
    /// * `calibration` - Optional robot calibration data
    ///
    /// # Returns
    /// Path to the delivery root directory
    pub fn create_delivery_structure(
        source_dir: &Path,
        config: &DeliveryConfig,
        dataset_config: &KpsConfig,
        calibration: Option<&RobotCalibration>,
        urdf_path: Option<&Path>,
    ) -> Result<PathBuf, Box<dyn std::error::Error>> {
        let delivery_root = config.root.join(format!(
            "{}-{}-{}",
            config.robot_name, config.end_effector, config.scene_name
        ));

        fs::create_dir_all(&delivery_root)?;

        // 1. Copy episode data
        Self::copy_episode_data(source_dir, &delivery_root)?;

        // 2. Create URDF directory structure
        Self::create_urdf_structure(
            &delivery_root,
            &config.robot_name,
            &config.end_effector,
            &config.version,
            calibration,
            urdf_path,
        )?;

        // 3. Create README
        Self::create_readme(&delivery_root, config, dataset_config)?;

        println!("Delivery structure created: {}", delivery_root.display());

        Ok(delivery_root)
    }

    /// Copy episode data from source to delivery directory.
    fn copy_episode_data(
        source_dir: &Path,
        delivery_root: &Path,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let episode_target = delivery_root.join("episode_0");

        // Copy meta directory
        let meta_source = source_dir.join("meta");
        if meta_source.exists() {
            let meta_target = episode_target.join("meta");
            Self::copy_dir_recursive(&meta_source, &meta_target)?;
        }

        // Copy videos directory
        let videos_source = source_dir.join("videos");
        if videos_source.exists() {
            let videos_target = episode_target.join("videos");
            Self::copy_dir_recursive(&videos_source, &videos_target)?;
        }

        // Copy parquet files if any
        for entry in fs::read_dir(source_dir)? {
            let entry = entry?;
            let path = entry.path();

            if path.extension().and_then(|s| s.to_str()) == Some("parquet") {
                let target = episode_target.join(path.file_name().unwrap());
                fs::copy(&path, &target)?;
            }
        }

        Ok(())
    }

    /// Create URDF directory structure with calibration file.
    fn create_urdf_structure(
        delivery_root: &Path,
        robot_name: &str,
        end_effector: &str,
        version: &str,
        calibration: Option<&RobotCalibration>,
        urdf_path: Option<&Path>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let urdf_dir = delivery_root
            .join("URDF")
            .join(format!("{}-{}-{}", robot_name, end_effector, version));

        fs::create_dir_all(&urdf_dir)?;

        // Write robot_calibration.json
        if let Some(cal) = calibration {
            let json = serde_json::to_string_pretty(cal)?;
            let cal_path = urdf_dir.join("robot_calibration.json");
            fs::write(&cal_path, json)?;
            println!("Created: {}", cal_path.display());
        }

        // Copy URDF file if provided
        if let Some(urdf) = urdf_path {
            let file_name = urdf
                .file_name()
                .and_then(|n| n.to_str())
                .unwrap_or("robot.urdf");
            let urdf_target = urdf_dir.join(file_name);
            fs::copy(urdf, &urdf_target)?;
            println!("Copied URDF: {}", urdf_target.display());
        }

        Ok(())
    }

    /// Create README.md file for the delivery.
    fn create_readme(
        delivery_root: &Path,
        config: &DeliveryConfig,
        dataset_config: &KpsConfig,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let readme_path = delivery_root.join("README.md");

        let content = format!(
            r#"# Kps Dataset: {} {} {}

## Dataset Information

- **Robot**: {} {}
- **End Effector**: {}
- **Scene**: {}
- **FPS**: {}
- **Episodes**: 1

## Structure

```
episode_0/
├── meta/           # Dataset metadata
├── videos/         # Video recordings
└── *.parquet       # Episode data
```

## URDF

Robot URDF and calibration are located in `URDF/{}-{}/`.

## Usage

```python
import kps
env = kps.make("{}")
```

---
Generated by roboflow
"#,
            dataset_config.dataset.name,
            config.robot_name,
            config.end_effector,
            config.robot_name,
            config.end_effector,
            config.scene_name,
            dataset_config.dataset.fps,
            config.robot_name,
            config.end_effector,
            config.version,
            delivery_root.display()
        );

        fs::write(&readme_path, content)?;
        println!("Created: {}", readme_path.display());

        Ok(())
    }

    /// Recursively copy a directory.
    fn copy_dir_recursive(source: &Path, target: &Path) -> Result<(), Box<dyn std::error::Error>> {
        fs::create_dir_all(target)?;

        for entry in fs::read_dir(source)? {
            let entry = entry?;
            let source_path = entry.path();
            let target_path = target.join(entry.file_name());

            if source_path.is_dir() {
                Self::copy_dir_recursive(&source_path, &target_path)?;
            } else {
                fs::copy(&source_path, &target_path)?;
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_delivery_config_default() {
        let config = DeliveryConfig::default();
        assert_eq!(config.scene_name, "Scene1");
        assert_eq!(config.version, "v1.0");
    }

    #[test]
    fn test_delivery_config_new() {
        let config = DeliveryConfig::new(
            "/tmp",
            "MyRobot".to_string(),
            "Gripper".to_string(),
            "Kitchen".to_string(),
        );
        assert_eq!(config.root, PathBuf::from("/tmp"));
        assert_eq!(config.robot_name, "MyRobot");
        assert_eq!(config.end_effector, "Gripper");
        assert_eq!(config.scene_name, "Kitchen");
    }
}
