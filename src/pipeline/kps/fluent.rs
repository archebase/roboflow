//! Fluent API for KPS conversion pipeline.
//!
//! This module provides a type-safe, fluent API for converting MCAP/BAG files
//! to KPS dataset format with full integration of pipeline, writer, and delivery.
//!
//! # Examples
//!
//! ```no_run
//! use robocodec::pipeline::kps::KpsConverter;
//!
//! fn main() -> Result<(), Box<dyn std::error::Error>> {
//!     // Simple conversion
//!     let _report = KpsConverter::new("input.mcap", "output_dir")
//!         .config("config.toml")
//!         .run()?;
//!
//!     // Advanced conversion with v1.2 delivery structure
//!     let _report = KpsConverter::new("input.mcap", "output_dir")
//!         .config("config.toml")
//!         .v12_delivery()
//!         .robot("Kuavo4Pro")
//!         .end_effector("Dexhand")
//!         .scene("Housekeeper")
//!         .sub_scene("Kitchen")
//!         .task("Dispose_of_takeout_containers")
//!         .run()?;
//!     Ok(())
//! }
//! ```

use std::path::{Path, PathBuf};

use crate::core::{CodecError, Result};
use crate::io::kps::{
    delivery_v12::{SeriesDeliveryConfig, StatisticsCollector, V12DeliveryBuilder},
    KpsConfig, RobotCalibration,
};

#[cfg(test)]
use crate::io::kps::delivery_v12::TaskStatistics;
use crate::pipeline::kps::{config::TimeAlignerConfig, KpsPipeline, KpsPipelineConfig, KpsReport};

/// Fluent API builder for KPS conversion.
pub struct KpsConverter {
    /// Input file path
    input: PathBuf,

    /// Output directory path
    output: PathBuf,

    /// KPS config (loaded from file or inline)
    kps_config: Option<KpsConfig>,

    /// Pipeline config
    pipeline_config: Option<KpsPipelineConfig>,

    /// V1.2 delivery configuration
    v12_config: Option<SeriesDeliveryConfig>,

    /// Robot calibration data
    calibration: Option<RobotCalibration>,

    /// URDF file path
    urdf_path: Option<PathBuf>,

    /// Statistics collector
    statistics: Option<StatisticsCollector>,

    /// Whether statistics tracking is enabled
    track_stats: bool,
}

impl KpsConverter {
    /// Create a new KPS converter with input and output paths.
    ///
    /// # Arguments
    /// * `input` - Input MCAP file path
    /// * `output` - Output directory path
    pub fn new(input: impl AsRef<Path>, output: impl AsRef<Path>) -> Self {
        Self {
            input: input.as_ref().to_path_buf(),
            output: output.as_ref().to_path_buf(),
            kps_config: None,
            pipeline_config: None,
            v12_config: None,
            calibration: None,
            urdf_path: None,
            statistics: None,
            track_stats: false,
        }
    }

    /// Load KPS configuration from a TOML file.
    ///
    /// # Arguments
    /// * `config_path` - Path to the TOML configuration file
    pub fn config(mut self, config_path: impl AsRef<Path>) -> Self {
        let path = config_path.as_ref();

        // Try to load config from file
        let kps_config = if let Ok(content) = std::fs::read_to_string(path) {
            toml::from_str(&content).ok()
        } else {
            None
        };

        self.kps_config = kps_config;
        self
    }

    /// Set KPS configuration directly.
    ///
    /// # Arguments
    /// * `config` - KPS configuration
    pub fn with_config(mut self, config: KpsConfig) -> Self {
        self.kps_config = Some(config);
        self
    }

    /// Set the channel capacity for the pipeline.
    ///
    /// Default is 16.
    pub fn channel_capacity(mut self, capacity: usize) -> Self {
        if let Some(ref mut config) = self.pipeline_config {
            config.channel_capacity = capacity;
        } else {
            self.pipeline_config = Some(KpsPipelineConfig {
                channel_capacity: capacity,
                ..Default::default()
            });
        }
        self
    }

    /// Set the target FPS for time alignment.
    ///
    /// Default is from the KPS config.
    pub fn target_fps(mut self, fps: u32) -> Self {
        if let Some(ref mut config) = self.pipeline_config {
            config.time_aligner.target_fps = fps;
        } else {
            self.pipeline_config = Some(KpsPipelineConfig {
                time_aligner: TimeAlignerConfig {
                    target_fps: fps,
                    ..Default::default()
                },
                ..Default::default()
            });
        }
        self
    }

    /// Set the robot calibration data.
    pub fn calibration(mut self, calibration: RobotCalibration) -> Self {
        self.calibration = Some(calibration);
        self
    }

    /// Set the URDF file path for v1.2 delivery.
    pub fn urdf(mut self, path: impl AsRef<Path>) -> Self {
        self.urdf_path = Some(path.as_ref().to_path_buf());
        self
    }

    // ========================================================================
    // V1.2 Delivery Methods (require config first)
    // ========================================================================

    /// Enable v1.2 delivery structure with default configuration.
    ///
    /// Chain additional methods to configure the v1.2 delivery structure.
    pub fn v12_delivery(mut self) -> Self {
        self.v12_config = Some(SeriesDeliveryConfig::new(
            PathBuf::from("F盘"),
            "Robot".to_string(),
            "Gripper".to_string(),
            "Scene1".to_string(),
            "SubScene1".to_string(),
            "Task1".to_string(),
        ));
        self
    }

    /// Enable v1.2 delivery structure with custom configuration.
    ///
    /// # Arguments
    /// * `root` - Root directory (e.g., "F盘")
    /// * `robot` - Robot name
    /// * `end_effector` - End effector name (Dexhand/Gripper)
    /// * `scene` - Scene name
    /// * `sub_scene` - Sub-scene name
    /// * `task` - Task name
    pub fn v12_delivery_with(
        mut self,
        root: impl AsRef<Path>,
        robot: &str,
        end_effector: &str,
        scene: &str,
        sub_scene: &str,
        task: &str,
    ) -> Self {
        self.v12_config = Some(SeriesDeliveryConfig::new(
            root.as_ref(),
            robot.to_string(),
            end_effector.to_string(),
            scene.to_string(),
            sub_scene.to_string(),
            task.to_string(),
        ));
        self
    }

    /// Set the robot name for v1.2 delivery.
    pub fn robot(mut self, robot: &str) -> Self {
        if let Some(ref mut config) = self.v12_config {
            config.robot_name = robot.to_string();
        }
        self
    }

    /// Set the end effector for v1.2 delivery.
    pub fn end_effector(mut self, end_effector: &str) -> Self {
        if let Some(ref mut config) = self.v12_config {
            config.end_effector = end_effector.to_string();
        }
        self
    }

    /// Set the scene name for v1.2 delivery.
    pub fn scene(mut self, scene: &str) -> Self {
        if let Some(ref mut config) = self.v12_config {
            config.scene_name = scene.to_string();
        }
        self
    }

    /// Set the sub-scene name for v1.2 delivery.
    pub fn sub_scene(mut self, sub_scene: &str) -> Self {
        if let Some(ref mut config) = self.v12_config {
            config.sub_scene_name = sub_scene.to_string();
        }
        self
    }

    /// Set the task name for v1.2 delivery.
    pub fn task(mut self, task: &str) -> Self {
        if let Some(ref mut config) = self.v12_config {
            config.task_name = task.to_string();
        }
        self
    }

    /// Set the version for v1.2 delivery.
    pub fn version(mut self, version: &str) -> Self {
        if let Some(ref mut config) = self.v12_config {
            config.version = version.to_string();
        }
        self
    }

    /// Set the root directory for v1.2 delivery.
    pub fn root(mut self, root: impl AsRef<Path>) -> Self {
        if let Some(ref mut config) = self.v12_config {
            config.root = root.as_ref().to_path_buf();
        }
        self
    }

    /// Enable statistics tracking for final directory naming.
    ///
    /// When enabled, the task directory will be renamed after conversion
    /// with actual statistics (size, episode count, duration).
    pub fn with_statistics(mut self) -> Self {
        self.track_stats = true;
        // Get FPS from config for statistics
        let fps = self
            .kps_config
            .as_ref()
            .map(|c| c.dataset.fps)
            .unwrap_or(30);
        self.statistics = Some(StatisticsCollector::new(fps));
        self
    }

    /// Run the conversion.
    pub fn run(self) -> Result<KpsReport> {
        self.run_impl()
    }

    /// Internal implementation of the conversion.
    fn run_impl(self) -> Result<KpsReport> {
        // Get or create default KPS config
        let kps_config = self.kps_config.unwrap_or_else(|| KpsConfig {
            dataset: crate::io::kps::DatasetConfig {
                name: "dataset".to_string(),
                fps: 30,
                robot_type: None,
            },
            mappings: vec![],
            output: crate::io::kps::OutputConfig::default(),
        });

        // Build pipeline config from KPS config and any custom settings
        let mut pipeline_config = if let Some(pc) = self.pipeline_config {
            pc
        } else {
            KpsPipelineConfig::from_kps_config(kps_config.clone())
        };

        // Override kps_config to ensure it's set
        pipeline_config.kps_config = kps_config.clone();

        // For v1.2 delivery, create placeholder structure first
        let episode_uuid = V12DeliveryBuilder::generate_episode_uuid();
        let (actual_output, temp_task_dir) = if let Some(ref v12) = self.v12_config {
            // Create v1.2 delivery structure with placeholder name
            let temp_dir = V12DeliveryBuilder::create_delivery_structure_placeholder(
                &v12.root,
                v12,
                &kps_config,
                self.calibration.as_ref(),
                self.urdf_path.as_deref(),
            )
            .map_err(|e| CodecError::encode("KpsConverter", e.to_string()))?;

            // The episode directory is inside the temp task directory
            let episode_dir = temp_dir.join(&episode_uuid);
            std::fs::create_dir_all(&episode_dir)
                .map_err(|e| CodecError::encode("KpsConverter", e.to_string()))?;

            (episode_dir, Some(temp_dir))
        } else {
            // Regular output directory
            (self.output.clone(), None)
        };

        // Create and run pipeline
        let pipeline = KpsPipeline::new(&self.input, &actual_output, pipeline_config.clone())
            .map_err(|e| CodecError::encode("KpsConverter", e.to_string()))?;

        let report = pipeline
            .run()
            .map_err(|e| CodecError::encode("KpsConverter", e.to_string()))?;

        // Finalize v1.2 delivery with statistics if tracking
        if let (Some(v12), Some(temp_dir)) = (self.v12_config, temp_task_dir) {
            let final_dir = if self.track_stats {
                // Calculate statistics and rename directory
                V12DeliveryBuilder::finalize_with_statistics(
                    &temp_dir,
                    &v12,
                    &kps_config,
                    &[episode_uuid],
                )
                .map_err(|e| CodecError::encode("KpsConverter", e.to_string()))?
            } else {
                // Just rename to proper format without stats
                let scene_dir = temp_dir.parent().and_then(|p| p.parent()).ok_or_else(|| {
                    CodecError::encode("KpsConverter", "Invalid directory structure")
                })?;
                let final_task_name = format!(
                    "{}-{}-{}",
                    v12.scene_name, v12.sub_scene_name, v12.task_name
                );
                let final_task_dir = scene_dir.join(&final_task_name);
                std::fs::rename(&temp_dir, &final_task_dir)
                    .map_err(|e| CodecError::encode("KpsConverter", e.to_string()))?;
                final_task_dir
            };

            // Update report with actual output location
            return Ok(KpsReport {
                output_dir: final_dir.display().to_string(),
                ..report
            });
        }

        Ok(report)
    }
}

/// Convenience function for simple KPS conversion.
///
/// # Arguments
/// * `input` - Input MCAP file path
/// * `output` - Output directory path
/// * `config` - KPS configuration
pub fn convert_to_kps(
    input: impl AsRef<Path>,
    output: impl AsRef<Path>,
    config: &KpsConfig,
) -> Result<KpsReport> {
    let pipeline_config = KpsPipelineConfig::from_kps_config(config.clone());

    let pipeline = KpsPipeline::new(input, output, pipeline_config)?;
    pipeline.run()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::io::kps::config::ImageFormat;
    use crate::io::kps::{DatasetConfig, Mapping, MappingType, OutputConfig};

    /// Create a test KPS config
    fn test_config() -> KpsConfig {
        KpsConfig {
            dataset: DatasetConfig {
                name: "test_dataset".to_string(),
                fps: 30,
                robot_type: Some("TestRobot".to_string()),
            },
            mappings: vec![
                Mapping {
                    topic: "/camera/high".to_string(),
                    feature: "observation.camera_high".to_string(),
                    mapping_type: MappingType::Image,
                },
                Mapping {
                    topic: "/joint_states".to_string(),
                    feature: "observation.joint_position".to_string(),
                    mapping_type: MappingType::State,
                },
            ],
            output: OutputConfig {
                formats: vec![],
                image_format: ImageFormat::Mp4,
                max_frames: None,
            },
        }
    }

    // =========================================================================
    // Basic Creation Tests
    // =========================================================================

    #[test]
    fn test_converter_creation() {
        let converter = KpsConverter::new("input.mcap", "output");

        assert_eq!(converter.input, Path::new("input.mcap"));
        assert_eq!(converter.output, Path::new("output"));
        assert!(converter.kps_config.is_none());
        assert!(converter.v12_config.is_none());
    }

    // =========================================================================
    // Config Tests
    // =========================================================================

    #[test]
    fn test_with_config() {
        let config = test_config();
        let converter = KpsConverter::new("input.mcap", "output").with_config(config.clone());

        assert!(converter.kps_config.is_some());
        let stored_config = converter.kps_config.unwrap();
        assert_eq!(stored_config.dataset.name, config.dataset.name);
        assert_eq!(stored_config.mappings.len(), config.mappings.len());
    }

    // =========================================================================
    // Pipeline Config Tests
    // =========================================================================

    #[test]
    fn test_channel_capacity() {
        let converter = KpsConverter::new("input.mcap", "output").channel_capacity(64);

        assert_eq!(converter.pipeline_config.unwrap().channel_capacity, 64);
    }

    #[test]
    fn test_target_fps() {
        let converter = KpsConverter::new("input.mcap", "output").target_fps(60);

        assert_eq!(
            converter.pipeline_config.unwrap().time_aligner.target_fps,
            60
        );
    }

    // =========================================================================
    // V1.2 Delivery Tests
    // =========================================================================

    #[test]
    fn test_v12_delivery_defaults() {
        let converter = KpsConverter::new("input.mcap", "output").v12_delivery();

        let v12 = converter.v12_config.as_ref().unwrap();
        assert_eq!(v12.robot_name, "Robot");
        assert_eq!(v12.end_effector, "Gripper");
        assert_eq!(v12.scene_name, "Scene1");
        assert_eq!(v12.sub_scene_name, "SubScene1");
        assert_eq!(v12.task_name, "Task1");
        assert_eq!(v12.version, "v1.0");
        assert_eq!(v12.root, Path::new("F盘"));
    }

    #[test]
    fn test_v12_delivery_with_custom() {
        let converter = KpsConverter::new("input.mcap", "output").v12_delivery_with(
            "/tmp",
            "TestRobot",
            "Gripper",
            "Kitchen",
            "Counter",
            "PickObject",
        );

        let v12 = converter.v12_config.as_ref().unwrap();
        assert_eq!(v12.root, Path::new("/tmp"));
        assert_eq!(v12.robot_name, "TestRobot");
        assert_eq!(v12.end_effector, "Gripper");
        assert_eq!(v12.scene_name, "Kitchen");
        assert_eq!(v12.sub_scene_name, "Counter");
        assert_eq!(v12.task_name, "PickObject");
    }

    #[test]
    fn test_v12_robot() {
        let converter = KpsConverter::new("input.mcap", "output")
            .v12_delivery()
            .robot("MyRobot");

        assert_eq!(converter.v12_config.as_ref().unwrap().robot_name, "MyRobot");
    }

    #[test]
    fn test_v12_end_effector() {
        let converter = KpsConverter::new("input.mcap", "output")
            .v12_delivery()
            .end_effector("Dexhand");

        assert_eq!(
            converter.v12_config.as_ref().unwrap().end_effector,
            "Dexhand"
        );
    }

    #[test]
    fn test_v12_scene() {
        let converter = KpsConverter::new("input.mcap", "output")
            .v12_delivery()
            .scene("LivingRoom");

        assert_eq!(
            converter.v12_config.as_ref().unwrap().scene_name,
            "LivingRoom"
        );
    }

    #[test]
    fn test_v12_sub_scene() {
        let converter = KpsConverter::new("input.mcap", "output")
            .v12_delivery()
            .sub_scene("Table");

        assert_eq!(
            converter.v12_config.as_ref().unwrap().sub_scene_name,
            "Table"
        );
    }

    #[test]
    fn test_v12_task() {
        let converter = KpsConverter::new("input.mcap", "output")
            .v12_delivery()
            .task("PourWater");

        assert_eq!(
            converter.v12_config.as_ref().unwrap().task_name,
            "PourWater"
        );
    }

    #[test]
    fn test_v12_version() {
        let converter = KpsConverter::new("input.mcap", "output")
            .v12_delivery()
            .version("v2.0");

        assert_eq!(converter.v12_config.as_ref().unwrap().version, "v2.0");
    }

    #[test]
    fn test_v12_root() {
        let converter = KpsConverter::new("input.mcap", "output")
            .v12_delivery()
            .root("/custom/root");

        assert_eq!(
            converter.v12_config.as_ref().unwrap().root,
            Path::new("/custom/root")
        );
    }

    #[test]
    fn test_v12_full_chain() {
        let converter = KpsConverter::new("input.mcap", "output")
            .v12_delivery()
            .robot("Kuavo4Pro")
            .end_effector("Dexhand")
            .scene("Housekeeper")
            .sub_scene("Kitchen")
            .task("Dispose_of_takeout_containers")
            .version("v1.0")
            .root("F盘");

        let v12 = converter.v12_config.as_ref().unwrap();
        assert_eq!(v12.robot_name, "Kuavo4Pro");
        assert_eq!(v12.end_effector, "Dexhand");
        assert_eq!(v12.scene_name, "Housekeeper");
        assert_eq!(v12.sub_scene_name, "Kitchen");
        assert_eq!(v12.task_name, "Dispose_of_takeout_containers");
        assert_eq!(v12.version, "v1.0");
        assert_eq!(v12.root, Path::new("F盘"));
    }

    #[test]
    fn test_series_dir_name() {
        let converter = KpsConverter::new("input.mcap", "output")
            .v12_delivery()
            .robot("Kuavo4Pro")
            .end_effector("Dexhand")
            .scene("Housekeeper");

        assert_eq!(
            converter.v12_config.as_ref().unwrap().series_dir_name(),
            "Kuavo4Pro-Dexhand-Housekeeper"
        );
    }

    #[test]
    fn test_urdf_dir_name() {
        let converter = KpsConverter::new("input.mcap", "output")
            .v12_delivery()
            .robot("Kuavo4Pro")
            .end_effector("Dexhand")
            .version("v2.1");

        assert_eq!(
            converter.v12_config.as_ref().unwrap().urdf_dir_name(),
            "Kuavo4Pro-Dexhand-v2.1"
        );
    }

    // =========================================================================
    // Statistics Tests
    // =========================================================================

    #[test]
    fn test_with_statistics() {
        let converter = KpsConverter::new("input.mcap", "output")
            .with_config(test_config())
            .v12_delivery()
            .with_statistics();

        assert!(converter.statistics.is_some());
        assert_eq!(converter.statistics.as_ref().unwrap().fps, 30);
        assert!(converter.track_stats);
    }

    #[test]
    fn test_statistics_uses_config_fps() {
        let config = KpsConfig {
            dataset: DatasetConfig {
                name: "test".to_string(),
                fps: 60,
                robot_type: None,
            },
            mappings: vec![],
            output: OutputConfig::default(),
        };

        let converter = KpsConverter::new("input.mcap", "output")
            .with_config(config)
            .v12_delivery()
            .with_statistics();

        assert_eq!(converter.statistics.as_ref().unwrap().fps, 60);
    }

    // =========================================================================
    // Chaining Tests
    // =========================================================================

    #[test]
    fn test_full_chaining() {
        let converter = KpsConverter::new("test.mcap", "out")
            .with_config(test_config())
            .channel_capacity(128)
            .target_fps(60)
            .v12_delivery()
            .robot("R1")
            .end_effector("E1")
            .scene("S1")
            .sub_scene("SS1")
            .task("T1");

        // Verify all settings were applied
        assert_eq!(
            converter.pipeline_config.as_ref().unwrap().channel_capacity,
            128
        );
        assert_eq!(
            converter
                .pipeline_config
                .as_ref()
                .unwrap()
                .time_aligner
                .target_fps,
            60
        );
        assert_eq!(converter.v12_config.as_ref().unwrap().robot_name, "R1");
        assert_eq!(converter.v12_config.as_ref().unwrap().task_name, "T1");
    }

    // =========================================================================
    // Path Component Tests
    // =========================================================================

    #[test]
    fn test_full_v12_path_components() {
        let temp_dir = tempfile::TempDir::new().unwrap();
        let root = temp_dir.path();

        let converter = KpsConverter::new("input.mcap", root.join("output"))
            .v12_delivery_with(
                root,
                "Robot1",
                "EndEffector1",
                "Scene1",
                "SubScene1",
                "Task1",
            )
            .version("v3.0");

        let v12 = converter.v12_config.as_ref().unwrap();

        // Test all path components
        assert_eq!(v12.series_dir_name(), "Robot1-EndEffector1-Scene1");
        assert_eq!(v12.urdf_dir_name(), "Robot1-EndEffector1-v3.0");
        assert!(v12.task_dir_name().contains("Scene1-SubScene1-Task1"));
    }

    // =========================================================================
    // Mapping Type Tests
    // =========================================================================

    #[test]
    fn test_multiple_mapping_types() {
        let config = KpsConfig {
            dataset: DatasetConfig {
                name: "test".to_string(),
                fps: 30,
                robot_type: None,
            },
            mappings: vec![
                Mapping {
                    topic: "/camera/high".to_string(),
                    feature: "observation.camera_high".to_string(),
                    mapping_type: MappingType::Image,
                },
                Mapping {
                    topic: "/joint_states".to_string(),
                    feature: "observation.joint_position".to_string(),
                    mapping_type: MappingType::State,
                },
                Mapping {
                    topic: "/action".to_string(),
                    feature: "action.joint_position".to_string(),
                    mapping_type: MappingType::Action,
                },
                Mapping {
                    topic: "/timestamp".to_string(),
                    feature: "observation.timestamp".to_string(),
                    mapping_type: MappingType::Timestamp,
                },
                Mapping {
                    topic: "/audio".to_string(),
                    feature: "observation.audio".to_string(),
                    mapping_type: MappingType::Audio,
                },
                Mapping {
                    topic: "/imu".to_string(),
                    feature: "other_sensors.imu.angular_velocity".to_string(),
                    mapping_type: MappingType::OtherSensor,
                },
            ],
            output: OutputConfig::default(),
        };

        let converter = KpsConverter::new("input.mcap", "output").with_config(config);

        assert_eq!(converter.kps_config.as_ref().unwrap().mappings.len(), 6);
    }

    // =========================================================================
    // Convenience Function Tests
    // =========================================================================

    #[test]
    fn test_convert_to_kps_function() {
        let config = test_config();

        // Should compile (will fail at runtime if input doesn't exist)
        let result = convert_to_kps("nonexistent.mcap", "output", &config);
        assert!(result.is_err());
    }

    // =========================================================================
    // Statistics Formatting Tests
    // =========================================================================

    #[test]
    fn test_task_statistics_format() {
        let stats = TaskStatistics::new(53.21, 2000, 85.30);

        assert_eq!(stats.format_size(), "53p21GB");
        assert_eq!(stats.format_duration(), "85p30h");
        assert_eq!(stats.task_dir_suffix(), "53p21GB_2000counts_85p30h");
    }

    #[test]
    fn test_statistics_collector() {
        let mut collector = StatisticsCollector::new(30);
        collector.add_episode(900);
        collector.add_file(1024 * 1024 * 100);

        assert_eq!(collector.episode_count, 1);
        assert_eq!(collector.total_frames, 900);
        assert_eq!(collector.total_bytes, 100 * 1024 * 1024);

        let stats = collector.to_statistics();
        assert!(stats.size_gb > 0.09 && stats.size_gb < 0.10); // ~100MB
        assert_eq!(stats.episode_count, 1);
    }
}
