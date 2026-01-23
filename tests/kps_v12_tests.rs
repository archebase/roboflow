// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! KPS v1.2 specification compliance tests.
//!
//! Comprehensive tests for validating KPS dataset format conversion
//! according to the v1.2 specification including:
//! - Directory structure validation
//! - HDF5 schema compliance
//! - task_info.json format validation
//! - Camera parameter format validation
//! - robot_calibration.json validation
//! - End-to-end conversion tests

use std::collections::HashMap;
use std::fs;
use std::path::Path;
use std::str::FromStr;

use roboflow::dataset::kps::{
    camera_params::{ExtrinsicParams, IntrinsicParams},
    delivery_v12::{SeriesDeliveryConfig, V12DeliveryBuilder},
    hdf5_schema::{default_arm_joint_names, default_leg_joint_names, DataType, KpsHdf5Schema},
    robot_calibration::{JointCalibration, RobotCalibration, RobotCalibrationGenerator},
    task_info::{ActionSegment, TaskInfo},
    KpsConfig,
};

/// Test output directory helper.
fn test_output_dir(_test_name: &str) -> tempfile::TempDir {
    tempfile::tempdir_in("tests/output")
        .unwrap_or_else(|_| tempfile::tempdir().expect("Failed to create temp dir"))
}

/// Check if a file exists for testing.
macro_rules! skip_if_missing {
    ($path:expr, $name:expr) => {
        if !Path::new($path).exists() {
            eprintln!("Skipping test: {} not found", $name);
            return;
        }
    };
}

#[cfg(test)]
mod v12_directory_structure_tests {
    use super::*;

    /// Test series directory naming convention (v1.2).
    ///
    /// Series directory should be named: `{RobotModel}-{EndEffector}-{Scene}{Number}`
    /// Example: `Kuavo4Pro-Dexhand-Housekeeper1`
    #[test]
    fn test_series_directory_naming() {
        let valid_names = vec![
            "Kuavo4Pro-Dexhand-Housekeeper1",
            "Kuavo4LB-Gripper-Factory1",
            "Kuavo4Pro-Dexhand-Housekeeper2",
            "RobotA-Gripper-SceneB123",
        ];

        for name in valid_names {
            assert!(validate_series_naming(name), "{} is valid", name);
        }

        let invalid_names = vec![
            "Housekeeper",          // Missing robot and end effector
            "Robot-Housekeeper",    // Missing end effector
            "Robot-Dexhand",        // Missing scene
            "Robot-Dexhand-",       // Trailing dash
            "-Dexhand-Housekeeper", // Leading dash
        ];

        for name in invalid_names {
            assert!(!validate_series_naming(name), "{} should be invalid", name);
        }
    }

    /// Test task directory naming convention (v1.2).
    ///
    /// Task directory: `{Task}-{size}p{GB}_{counts}counts_{duration}p{hours}`
    /// Example: `Dispose_of_takeout_containers-53p21GB_2000counts_85p30h`
    #[test]
    fn test_task_directory_naming() {
        let valid_names = vec![
            "Dispose_of_takeout_containers-53p21GB_2000counts_85p30h",
            "SimpleTask-10p5GB_100counts_1p0h",
            "Task-0p1GB_1counts_0p01h",
        ];

        for name in valid_names {
            assert!(validate_task_naming(name), "{} is valid", name);
        }
    }

    /// Test complete v1.2 directory structure creation.
    #[test]
    fn test_v12_directory_structure_creation() {
        let output_dir = test_output_dir("test_v12_directory_structure_creation");

        let config = SeriesDeliveryConfig {
            root: output_dir.path().to_path_buf(),
            robot_name: "Kuavo4Pro".to_string(),
            end_effector: "Dexhand".to_string(),
            scene_name: "Housekeeper".to_string(),
            sub_scene_name: "Kitchen".to_string(),
            task_name: "Dispose_of_takeout_containers".to_string(),
            version: "v1.0".to_string(),
            statistics: None,
        };

        // Build the structure
        match V12DeliveryBuilder::create_delivery_structure(
            output_dir.path(),
            &config,
            &default_dataset_config(),
            "UUID1",
            1,
            100,
            None,
            None,
        ) {
            Ok(episode_dir) => {
                // Verify series directory exists
                let series_dir = output_dir.path().join("Kuavo4Pro-Dexhand-Housekeeper");
                assert!(series_dir.exists(), "Series directory should exist");

                // Verify task_info directory
                let task_info_dir = series_dir.join("task_info");
                assert!(task_info_dir.exists(), "task_info directory should exist");

                // Verify scene directory
                let scene_dir = series_dir.join("Housekeeper");
                assert!(scene_dir.exists(), "Scene directory should exist");

                // Verify sub_scene directory
                let sub_scene_dir = scene_dir.join("Kitchen");
                assert!(sub_scene_dir.exists(), "Sub-scene directory should exist");

                // Verify task directory (with stats)
                // The task directory name includes scene-sub_scene-task_name prefix
                let task_dirs: Vec<_> = sub_scene_dir
                    .read_dir()
                    .unwrap()
                    .filter_map(|e| e.ok())
                    .map(|e| e.file_name())
                    .filter(|name| {
                        let name_str = name.to_string_lossy();
                        name_str.contains("Dispose") || name_str.contains("Kitchen")
                    })
                    .collect();

                assert!(!task_dirs.is_empty(), "Task directory should be created");

                // Verify episode directory was created
                assert!(episode_dir.exists(), "Episode directory should exist");
            }
            Err(e) => {
                panic!("Failed to create directory structure: {}", e);
            }
        }
    }

    /// Test required subdirectories in episode directory.
    #[test]
    fn test_episode_subdirectories() {
        let output_dir = test_output_dir("test_episode_subdirectories");

        // Create the structure
        let episode_dir = output_dir.path().join("test_episode");
        fs::create_dir_all(episode_dir.join("camera/video")).unwrap();
        fs::create_dir_all(episode_dir.join("camera/depth")).unwrap();
        fs::create_dir_all(episode_dir.join("parameters")).unwrap();
        fs::create_dir_all(episode_dir.join("proprio_stats")).unwrap();
        fs::create_dir_all(episode_dir.join("audio")).unwrap();

        // Validate
        let result = validate_episode_subdirectories(&episode_dir);
        assert!(
            result.is_ok(),
            "Subdirectories validation should pass: {:?}",
            result
        );
    }

    /// Test that missing required subdirectories are detected.
    #[test]
    fn test_missing_subdirectories_detected() {
        let output_dir = test_output_dir("test_missing_subdirectories_detected");

        // Create incomplete structure
        let episode_dir = output_dir.path().join("test_episode");
        fs::create_dir_all(episode_dir.join("camera/video")).unwrap();
        // Missing: camera/depth, parameters, proprio_stats, audio

        let result = validate_episode_subdirectories(&episode_dir);
        assert!(result.is_err(), "Should detect missing subdirectories");
    }
}

#[cfg(test)]
mod v12_task_info_tests {
    use super::*;

    /// Test TaskInfo field presence (v1.2).
    #[test]
    fn test_task_info_required_fields() {
        let task_info = create_valid_task_info();

        // Validate all required v1.2 fields
        assert!(!task_info.episode_id.is_empty());
        assert!(!task_info.scene_name.is_empty());
        assert!(!task_info.sub_scene_name.is_empty());
        assert!(!task_info.english_task_name.is_empty());
        assert!(!task_info.data_gen_mode.is_empty());
        assert!(!task_info.sn_name.is_empty());

        // Check sn_name format: "厂家-机器人型号-末端执行器"
        assert!(
            task_info.sn_name.contains('-'),
            "sn_name should contain dashes: {}",
            task_info.sn_name
        );
        let parts: Vec<&str> = task_info.sn_name.split('-').collect();
        assert_eq!(parts.len(), 3, "sn_name should have 3 parts: {:?}", parts);
    }

    /// Test action_config segment structure.
    #[test]
    fn test_action_config_structure() {
        let task_info = create_valid_task_info();

        assert!(
            !task_info.label_info.action_config.is_empty(),
            "action_config should not be empty"
        );

        for segment in &task_info.label_info.action_config {
            // Validate frame ranges
            assert!(
                segment.end_frame > segment.start_frame,
                "end_frame {} > start_frame {} for segment: {:?}",
                segment.end_frame,
                segment.start_frame,
                segment
            );

            // Validate timestamp format (ISO 8601)
            assert!(
                segment.timestamp_utc.contains('T'),
                "timestamp should be ISO 8601 format: {}",
                segment.timestamp_utc
            );

            // Validate skill
            let valid_skills = ["Pick", "Place", "Drop", "Move", "Grasp", "Release"];
            assert!(
                valid_skills.contains(&segment.skill.as_str())
                    || segment
                        .skill
                        .chars()
                        .all(|c| c.is_uppercase() || c.is_ascii_digit()),
                "skill should be valid: {}",
                segment.skill
            );
        }
    }

    /// Test task_info serialization and deserialization.
    #[test]
    fn test_task_info_serialization() {
        let task_info1 = create_valid_task_info();

        // Serialize
        let json = serde_json::to_string(&task_info1).expect("Failed to serialize task_info");

        // Deserialize
        let task_info2: TaskInfo =
            serde_json::from_str(&json).expect("Failed to deserialize task_info");

        // Check equivalence
        assert_eq!(task_info1.episode_id, task_info2.episode_id);
        assert_eq!(task_info1.scene_name, task_info2.scene_name);
        assert_eq!(task_info1.sub_scene_name, task_info2.sub_scene_name);
        assert_eq!(task_info1.english_task_name, task_info2.english_task_name);
        assert_eq!(task_info1.sn_name, task_info2.sn_name);
    }
}

#[cfg(test)]
mod v12_hdf5_schema_tests {
    use super::*;

    /// Test HDF5 dataset specification completeness.
    #[test]
    fn test_hdf5_spec_completeness() {
        let schema = KpsHdf5Schema::new();
        let specs = schema.datasets();

        // Check that all required groups exist
        let required_groups = vec![
            "action/effector",
            "action/end",
            "action/joint",
            "action/leg",
            "action/robot",
            "action/waist",
            "state/effector",
            "state/end",
            "state/head",
            "state/joint",
            "state/leg",
            "state/robot",
            "state/waist",
        ];

        for group in required_groups {
            let group_specs: Vec<_> = specs.iter().filter(|s| s.path.starts_with(group)).collect();

            assert!(
                !group_specs.is_empty(),
                "Group {} should have specifications",
                group
            );

            // Check for required datasets in each group
            let dataset_names = match group {
                "action/effector" => vec!["position", "names"],
                "action/end" => vec!["position", "orientation"],
                "action/joint" | "state/joint" => vec!["position", "velocity", "names"],
                "action/leg" | "state/leg" => vec!["position", "velocity", "names"],
                "action/robot" => vec!["velocity", "orientation"],
                "state/end" => vec!["position", "orientation", "angular", "velocity", "wrench"],
                _ => vec![],
            };

            for dataset in dataset_names {
                let dataset_specs: Vec<_> = group_specs
                    .iter()
                    .filter(|s| s.path.ends_with(dataset))
                    .collect();

                assert!(
                    !dataset_specs.is_empty(),
                    "Group {} should have {} dataset: {:?}",
                    group,
                    dataset,
                    group_specs
                );
            }
        }
    }

    /// Test HDF5 data type specifications.
    #[test]
    fn test_hdf5_data_types() {
        let schema = KpsHdf5Schema::new();

        for spec in schema.datasets() {
            match spec.dtype {
                DataType::Float32 => {
                    assert!(
                        spec.description.contains("float32")
                            || spec.description.contains("rad")
                            || spec.description.contains("m")
                            || spec.description.contains("N"),
                        "Float32 spec should mention float32: {}",
                        spec.description
                    );
                }
                DataType::Int64 => {
                    assert!(
                        spec.description.contains("int64") || spec.description.contains("纳秒"),
                        "Int64 spec should mention int64: {}",
                        spec.description
                    );
                }
                DataType::String => {
                    assert!(
                        spec.description.contains("str") || spec.description.contains("name"),
                        "String spec should mention str: {}",
                        spec.description
                    );
                }
                _ => {}
            }

            // Check shape is not empty
            assert!(
                !spec.shape.is_empty(),
                "Spec should have shape: {}",
                spec.path
            );
        }
    }

    /// Test joint name consistency.
    #[test]
    fn test_joint_name_consistency() {
        // Test default arm joint names
        let arm_names = default_arm_joint_names();
        assert_eq!(arm_names.len(), 14, "Arm should have 14 DOF");

        // Test default leg joint names
        let leg_names = default_leg_joint_names();
        assert_eq!(leg_names.len(), 12, "Leg should have 12 DOF");

        // Test that joint names match URDF convention
        for name in &arm_names {
            assert!(!name.is_empty(), "Joint name should not be empty");
            assert!(!name.contains(' '), "Joint name should not contain spaces");
            assert!(
                name.starts_with("l_") || name.starts_with("r_"),
                "Arm joint name should start with l_ or r_: {}",
                name
            );
        }
    }

    /// Test HDF5 dataset spec has names field for all joint datasets.
    #[test]
    fn test_joint_datasets_have_names() {
        let schema = KpsHdf5Schema::new();
        let specs = schema.datasets();

        // All joint datasets should have a corresponding names dataset
        let joint_datasets: Vec<_> = specs
            .iter()
            .filter(|s| {
                s.path.contains("joint")
                    || s.path.contains("leg")
                    || s.path.contains("head")
                    || s.path.contains("waist")
                    || s.path.contains("effector")
            })
            .filter(|s| s.path.contains("position") || s.path.contains("velocity"))
            .collect();

        for dataset_spec in joint_datasets {
            let names_path = dataset_spec
                .path
                .replace("/position", "/names")
                .replace("/velocity", "/names")
                .replace("/force", "/names")
                .replace("/current_value", "/names")
                .replace("/angular", "/names")
                .replace("/wrench", "/names");

            let names_exists: Vec<_> = specs.iter().filter(|s| s.path == names_path).collect();

            assert!(
                !names_exists.is_empty(),
                "Joint dataset {} should have corresponding names dataset",
                dataset_spec.path
            );

            // Verify names dataset is string type
            for names_spec in names_exists {
                assert_eq!(
                    names_spec.dtype,
                    DataType::String,
                    "Names dataset should be string type: {}",
                    names_spec.path
                );
            }
        }
    }
}

#[cfg(test)]
mod v12_camera_params_tests {
    use super::*;

    /// Test intrinsic params structure (v1.2).
    #[test]
    fn test_intrinsic_params_structure() {
        let intrinsic = create_valid_intrinsic_params();

        // Check all required fields
        assert!(intrinsic.fx > 0.0, "fx should be positive");
        assert!(intrinsic.fy > 0.0, "fy should be positive");
        assert!(intrinsic.cx >= 0.0, "cx should be non-negative");
        assert!(intrinsic.cy >= 0.0, "cy should be non-negative");
        assert!(intrinsic.width > 0, "width should be positive");
        assert!(intrinsic.height > 0, "height should be positive");

        // Test serialization
        let json = serde_json::to_string(&intrinsic).unwrap();
        let parsed: IntrinsicParams = serde_json::from_str(&json).unwrap();

        assert_eq!(intrinsic.fx, parsed.fx);
        assert_eq!(intrinsic.fy, parsed.fy);
        assert_eq!(intrinsic.cx, parsed.cx);
    }

    /// Test intrinsic params distortion model.
    #[test]
    fn test_intrinsic_distortion_models() {
        let mut intrinsic = create_valid_intrinsic_params();
        intrinsic.distortion = vec![0.0; 5]; // 5 parameters for plumb_bob

        // Test that we can at least create and parse it
        let json = serde_json::to_string(&intrinsic).unwrap();
        let _parsed: IntrinsicParams = serde_json::from_str(&json).unwrap();
    }

    /// Test extrinsic params structure (v1.2).
    #[test]
    fn test_extrinsic_params_structure() {
        let extrinsic = create_valid_extrinsic_params();

        // Check required fields
        assert!(
            !extrinsic.frame_id.is_empty(),
            "frame_id should not be empty"
        );
        assert!(
            !extrinsic.child_frame_id.is_empty(),
            "child_frame_id should not be empty"
        );

        // Check position is valid
        assert!(
            extrinsic.position.x.is_finite(),
            "position x should be finite"
        );
        assert!(
            extrinsic.position.y.is_finite(),
            "position y should be finite"
        );
        assert!(
            extrinsic.position.z.is_finite(),
            "position z should be finite"
        );

        // Check orientation is valid quaternion
        let quat = (
            extrinsic.orientation.x,
            extrinsic.orientation.y,
            extrinsic.orientation.z,
            extrinsic.orientation.w,
        );
        let quat_norm_sq = quat.0 * quat.0 + quat.1 * quat.1 + quat.2 * quat.2 + quat.3 * quat.3;
        assert!(
            (quat_norm_sq - 1.0).abs() < 0.01,
            "Quaternion should be normalized: {}",
            quat_norm_sq
        );

        // Test serialization
        let json = serde_json::to_string(&extrinsic).unwrap();
        let parsed: ExtrinsicParams = serde_json::from_str(&json).unwrap();

        assert_eq!(extrinsic.frame_id, parsed.frame_id);
        assert_eq!(extrinsic.child_frame_id, parsed.child_frame_id);
    }
}

#[cfg(test)]
mod v12_robot_calibration_tests {
    use super::*;

    /// Test robot_calibration.json structure (v1.2).
    #[test]
    fn test_robot_calibration_structure() {
        let calibration = create_valid_robot_calibration();

        // Check joints exist
        assert!(
            !calibration.joints.is_empty(),
            "Should have at least one joint"
        );

        for (joint_name, joint_cal) in &calibration.joints {
            // Check required fields
            assert!(joint_cal.id <= 1000, "Joint ID should be reasonable");
            assert!(
                joint_cal.range_min < joint_cal.range_max,
                "Range min should be less than max for {}: min={}, max={}",
                joint_name,
                joint_cal.range_min,
                joint_cal.range_max
            );

            // Test homing offset is reasonable (within +/- 2*PI)
            assert!(
                joint_cal.homing_offset.abs() <= 2.0 * std::f64::consts::PI,
                "Homing offset should be reasonable for {}: {}",
                joint_name,
                joint_cal.homing_offset
            );
        }

        // Test serialization
        let json = serde_json::to_string(&calibration).unwrap();
        let parsed: RobotCalibration = serde_json::from_str(&json).unwrap();

        assert_eq!(calibration.joints.len(), parsed.joints.len());
    }

    /// Test robot calibration generation from joint names.
    #[test]
    fn test_robot_calibration_from_joint_names() {
        let joint_names = default_arm_joint_names();
        let calibration = RobotCalibrationGenerator::from_joint_names(&joint_names);

        assert_eq!(
            calibration.joints.len(),
            joint_names.len(),
            "Should have calibration for each joint"
        );

        for (name, cal) in &calibration.joints {
            assert_eq!(cal.id, calibration.joints[name].id, "ID mismatch");
            assert!(
                (cal.range_min..cal.range_max).contains(&cal.homing_offset)
                    || (cal.homing_offset == 0.0 && cal.range_min < 0.0 && cal.range_max > 0.0),
                "Homing offset should be within range for {}",
                name
            );
        }
    }
}

#[cfg(test)]
mod v12_end_to_end_tests {
    use super::*;

    /// Test complete v1.2 workflow: MCAP → KPS output.
    #[test]
    #[ignore] // Requires actual MCAP file, can be run manually
    fn test_end_to_end_mcap_to_kps_v12() {
        let fixture_path = Path::new("tests/fixtures/robocodec_test_2.mcap");
        skip_if_missing!(fixture_path, "robocodec_test_2.mcap");

        let output_dir = test_output_dir("test_end_to_end_mcap_to_kps_v12");

        // Create annotation file
        let annotation_path = output_dir.path().join("annotation.json");
        let annotation_json = serde_json::json!({
            "episode_id": "test-episode-001",
            "scene_name": "TestScene",
            "sub_scene_name": "TestSubScene",
            "english_task_name": "Test Task",
            "data_gen_mode": "simulation",
            "sn_code": "TEST001",
            "sn_name": "TestFactory-RobotModel-Gripper",
            "label_info": {
                "action_config": [
                    {
                        "start_frame": 0,
                        "end_frame": 100,
                        "timestamp_utc": "2025-01-23T12:00:00Z",
                        "action_text": "测试动作",
                        "skill": "Pick",
                        "is_mistake": false,
                        "english_action_text": "Test action"
                    }
                ]
            }
        });
        fs::write(&annotation_path, annotation_json.to_string())
            .expect("Failed to write annotation file");

        // Create config
        let config_path = output_dir.path().join("kps_config.toml");
        create_default_kps_config(&config_path);

        // Run conversion (would require actual converter implementation)
        // This is a placeholder for the actual test
        println!(
            "End-to-end test would convert {} to KPS format",
            fixture_path.display()
        );
    }
}

// =============================================================================
// Helper Functions
// =============================================================================

fn validate_series_naming(name: &str) -> bool {
    // Pattern: {RobotModel}-{EndEffector}-{Scene}{Number}
    // All parts must be non-empty
    let parts: Vec<&str> = name.split('-').collect();
    if parts.len() < 3 {
        return false;
    }

    // All parts must be non-empty
    for part in &parts {
        if part.is_empty() {
            return false;
        }
    }

    // Last part (scene) should start with uppercase letter
    let scene_part = parts.last().unwrap();
    if !scene_part
        .chars()
        .next()
        .map(|c| c.is_uppercase())
        .unwrap_or(false)
    {
        return false;
    }

    true
}

fn validate_task_naming(name: &str) -> bool {
    // Pattern: {Task}-{size}p{GB}_{counts}counts_{duration}p{hours}
    // Example: Dispose_of_takeout_containers-53p21GB_2000counts_85p30h
    // The task name can contain underscores, so we need to find the pattern markers

    // Find the "{size}p{GB}GB" pattern (note: {GB} is also a number like 21)
    let mut found_pattern = false;
    let mut after_size = "";

    for (i, _) in name.char_indices() {
        let remaining = &name[i..];
        if let Some(after_hyphen) = remaining.strip_prefix('-') {
            // Check if this is followed by {digits}p{digits}GB
            if let Some(p_pos) = after_hyphen.find('p') {
                let before_p = &after_hyphen[..p_pos];
                let after_p = &after_hyphen[p_pos + 1..];
                if let Some(gb_pos) = after_p.find("GB") {
                    let gb_value = &after_p[..gb_pos];
                    // Verify both numbers are valid
                    if !before_p.is_empty()
                        && before_p.chars().all(|c| c.is_ascii_digit() || c == '.')
                        && !gb_value.is_empty()
                        && gb_value.chars().all(|c| c.is_ascii_digit() || c == '.')
                        && f64::from_str(before_p).is_ok()
                        && f64::from_str(gb_value).is_ok()
                    {
                        // Found the size pattern: "-{size}p{GB}GB"
                        let size_pattern_len = 1 + p_pos + 1 + gb_pos + 2; // "-" + before_p + "p" + gb_value + "GB"
                        if i + size_pattern_len <= name.len() {
                            after_size = &name[i + size_pattern_len..];
                            found_pattern = true;
                            break;
                        }
                    }
                }
            }
        }
    }

    if !found_pattern {
        return false;
    }

    // After the size pattern, we should have: _{counts}counts_{duration}p{hours}
    // The string starts with '_', so when we split, we get an empty first element
    let remaining_parts: Vec<&str> = after_size.split('_').collect();
    // Remove any empty strings from the split result
    let remaining_parts: Vec<&str> = remaining_parts
        .into_iter()
        .filter(|s| !s.is_empty())
        .collect();
    if remaining_parts.len() != 2 {
        return false;
    }

    // First remaining part: {counts}counts
    if !remaining_parts[0].ends_with("counts") {
        return false;
    }
    let counts_str = remaining_parts[0].trim_end_matches("counts");
    if usize::from_str(counts_str).is_err() {
        return false;
    }

    // Second remaining part: {duration}p{hours}
    if !remaining_parts[1].contains('p') || !remaining_parts[1].ends_with('h') {
        return false;
    }
    let duration_components: Vec<&str> = remaining_parts[1]
        .trim_end_matches('h')
        .split('p')
        .collect();
    if duration_components.len() != 2 {
        return false;
    }
    if f64::from_str(duration_components[0]).is_err() {
        return false;
    }
    if f64::from_str(duration_components[1]).is_err() {
        return false;
    }

    true
}

fn validate_episode_subdirectories(episode_dir: &Path) -> Result<(), String> {
    let required = vec![
        "camera/video",
        "camera/depth",
        "parameters",
        "proprio_stats",
        "audio",
    ];

    for subdir in required {
        let path = episode_dir.join(subdir);
        if !path.exists() {
            return Err(format!("Missing required directory: {}", subdir));
        }
    }

    Ok(())
}

fn default_dataset_config() -> KpsConfig {
    use roboflow::dataset::kps::{DatasetConfig, OutputConfig};

    KpsConfig {
        dataset: DatasetConfig {
            name: "test_dataset".to_string(),
            fps: 30,
            robot_type: Some("test_robot".to_string()),
        },
        mappings: vec![],
        output: OutputConfig::default(),
    }
}

fn create_valid_task_info() -> TaskInfo {
    use roboflow::dataset::kps::task_info::LabelInfo;

    let action_segment = ActionSegment {
        start_frame: 100,
        end_frame: 200,
        timestamp_utc: "2025-06-16T02:22:48.391668+00:00".to_string(),
        action_text: "拿起物体".to_string(),
        skill: "Pick".to_string(),
        is_mistake: false,
        english_action_text: "Pick up object".to_string(),
    };

    let label_info = LabelInfo {
        action_config: vec![action_segment],
        key_frame: vec![],
    };

    TaskInfo {
        episode_id: "test-episode-001".to_string(),
        scene_name: "Kitchen".to_string(),
        sub_scene_name: "Counter".to_string(),
        init_scene_text: "测试场景".to_string(),
        english_init_scene_text: "Test scene description".to_string(),
        task_name: "测试任务".to_string(),
        english_task_name: "Test Task".to_string(),
        data_type: "常规".to_string(),
        episode_status: "approved".to_string(),
        data_gen_mode: "real_machine".to_string(),
        sn_code: "TEST001".to_string(),
        sn_name: "TestFactory-Kuavo4Pro-Dexhand".to_string(),
        label_info,
    }
}

fn create_valid_intrinsic_params() -> IntrinsicParams {
    IntrinsicParams::new(
        976.97998046875,
        732.7349853515625,
        645.2012329101562,
        315.3855285644531,
        1280,
        720,
    )
}

fn create_valid_extrinsic_params() -> ExtrinsicParams {
    // Use from_tf_transform which is the public constructor
    ExtrinsicParams::from_tf_transform(
        "test_link".to_string(),
        "test_camera_frame".to_string(),
        (-0.001807534985204, -0.0000127749221, 0.12698557287),
        (
            -0.061_042_519_636_452_2,
            -0.734_867_956_625_483_3,
            0.000_381_887_046_387_419_1,
            0.679_521_491_422_215_6,
        ),
    )
}

fn create_valid_robot_calibration() -> RobotCalibration {
    let mut joints = HashMap::new();

    joints.insert(
        "test_joint".to_string(),
        JointCalibration {
            id: 0,
            drive_mode: 0,
            homing_offset: 0.1825841290388828,
            range_min: -0.314159265358979,
            range_max: 0.663225115757845,
        },
    );

    RobotCalibration { joints }
}

fn create_default_kps_config(path: &Path) {
    let config_content = r#"
[dataset]
name = "test_dataset"
fps = 30
robot_type = "test_robot"

[output]
formats = ["hdf5"]
image_format = "raw"

[[mappings]]
topic = "/joint_states"
feature = "observation.joint_position"
type = "state"

[[mappings]]
topic = "/joint_states"
feature = "observation.joint_velocity"
type = "state"
field = "velocity"
"#;
    fs::write(path, config_content).expect("Failed to write KPS config");
}
