// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Kps v1.2 specification compliant delivery disk structure generation.
//!
//! Creates the full directory structure required for Kps v1.2 dataset delivery.
//!
//! ## v1.2 Structure
//!
//! ```text
//! F盘/  (or configured root)
//! └── <Robot>-<EndEffector>-<Scene>/           # Series directory
//!     ├── task_info/                           # At series level
//!     │   └── <Scene>-<SubScene>-<Task>-<info>.json
//!     ├── <Scene>/                             # Scene directory
//!     │   └── <SubScene>/                      # SubScene directory
//!     │       └── <Task>-<info>/               # Task directory (with stats)
//!     │           ├── <UUID1>/                 # Episode UUID
//!     │           │   ├── camera/
//!     │           │   │   ├── video/           # Color videos
//!     │           │   │   └── depth/           # Depth videos
//!     │           │   ├── parameters/          # Camera params
//!     │           │   ├── proprio_stats/       # HDF5 files
//!     │           │   │   ├── proprio_stats.hdf5
//!     │           │   │   └── proprio_stats_original.hdf5
//!     │           │   └── audio/               # Audio files
//!     │           └── <UUID2>/
//!     ├── URDF/
//!     │   └── <Robot>-<EndEffector>-v1.0/
//!     │       ├── robot_calibration.json
//!     │       └── robot.urdf
//!     └── README.md
//! ```
//!
//! ## Task Directory Naming
//!
//! The task directory name includes actual statistics:
//! `{Task}-{size}GB_{counts}counts_{duration}h`
//!
//! Example: `Dispose_of_takeout_containers-53p21GB_2000counts_85p30h`
//! - Size: 53.21 GB (using "p" as decimal separator)
//! - Count: 2000 episodes
//! - Duration: 85.30 hours (using "p" as decimal separator)

use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::dataset::kps::{KpsConfig, RobotCalibration};

/// Statistics calculated from episodes for task directory naming.
#[derive(Debug, Clone)]
pub struct TaskStatistics {
    /// Total size in GB
    pub size_gb: f64,

    /// Total number of episodes
    pub episode_count: usize,

    /// Total duration in hours
    pub duration_hours: f64,
}

/// Collector for tracking statistics incrementally during data writing.
#[derive(Debug, Clone, Default)]
pub struct StatisticsCollector {
    /// Total bytes written
    pub total_bytes: u64,

    /// Number of episodes written
    pub episode_count: usize,

    /// Total frames written
    pub total_frames: usize,

    /// FPS for duration calculation
    pub fps: u32,
}

impl StatisticsCollector {
    /// Create a new collector with the specified FPS.
    pub fn new(fps: u32) -> Self {
        Self {
            fps,
            ..Default::default()
        }
    }

    /// Record a file write operation.
    pub fn add_file(&mut self, bytes: u64) {
        self.total_bytes += bytes;
    }

    /// Record an episode completion.
    pub fn add_episode(&mut self, frames: usize) {
        self.episode_count += 1;
        self.total_frames += frames;
    }

    /// Get the current duration in hours.
    pub fn duration_hours(&self) -> f64 {
        if self.fps > 0 && self.total_frames > 0 {
            (self.total_frames as f64) / (self.fps as f64) / 3600.0
        } else {
            0.0
        }
    }

    /// Get the current size in GB.
    pub fn size_gb(&self) -> f64 {
        self.total_bytes as f64 / (1024.0 * 1024.0 * 1024.0)
    }

    /// Convert to `TaskStatistics`.
    pub fn to_statistics(&self) -> TaskStatistics {
        TaskStatistics::new(self.size_gb(), self.episode_count, self.duration_hours())
    }
}

impl TaskStatistics {
    /// Create new statistics.
    pub fn new(size_gb: f64, episode_count: usize, duration_hours: f64) -> Self {
        Self {
            size_gb,
            episode_count,
            duration_hours,
        }
    }

    /// Calculate statistics from a directory containing episode data.
    ///
    /// Scans the directory and calculates:
    /// - Total size in GB
    /// - Episode count (number of subdirectories)
    /// - Total duration (from HDF5 metadata if available)
    pub fn calculate_from_dir(dir: &Path, fps: u32) -> Result<Self, Box<dyn std::error::Error>> {
        let mut total_size = 0u64;
        let mut episode_count = 0usize;
        let mut total_frames = 0usize;

        // Walk through directory
        for entry in fs::read_dir(dir)? {
            let entry = entry?;
            let path = entry.path();

            // Count subdirectories as episodes
            if path.is_dir() {
                episode_count += 1;

                // Add directory size
                if let Ok(size) = Self::dir_size(&path) {
                    total_size += size;
                }

                // Try to extract frame count from HDF5 files
                for sub_entry in fs::read_dir(&path)? {
                    let sub_entry = sub_entry?;
                    let sub_path = sub_entry.path();

                    // Check for HDF5 files in proprio_stats
                    if sub_path.extension().and_then(|s| s.to_str()) == Some("hdf5") {
                        if let Ok(frames) = Self::extract_frame_count_from_hdf5(&sub_path) {
                            total_frames = total_frames.max(frames);
                        }
                    }
                }
            } else if path.is_file() {
                // Add file size
                if let Ok(metadata) = fs::metadata(&path) {
                    total_size += metadata.len();
                }
            }
        }

        // Calculate duration from frames and FPS
        let duration_hours = if total_frames > 0 && fps > 0 {
            (total_frames as f64) / (fps as f64) / 3600.0
        } else {
            0.0
        };

        // Convert bytes to GB
        let size_gb = total_size as f64 / (1024.0 * 1024.0 * 1024.0);

        Ok(Self {
            size_gb,
            episode_count,
            duration_hours,
        })
    }

    /// Calculate total size of a directory recursively.
    fn dir_size(dir: &Path) -> Result<u64, Box<dyn std::error::Error>> {
        let mut total = 0u64;

        for entry in fs::read_dir(dir)? {
            let entry = entry?;
            let path = entry.path();

            if path.is_dir() {
                total += Self::dir_size(&path)?;
            } else if let Ok(metadata) = fs::metadata(&path) {
                total += metadata.len();
            }
        }

        Ok(total)
    }

    /// Extract frame count from an HDF5 file.
    ///
    /// Reads the frame_count attribute or infers from dataset shapes.
    fn extract_frame_count_from_hdf5(path: &Path) -> Result<usize, Box<dyn std::error::Error>> {
        #[cfg(feature = "kps-hdf5")]
        {
            use hdf5::File;

            let file = File::open(path)?;

            // Try to read frame_count attribute
            if let Ok(frame_count) = file.attr("frame_count") {
                if let Ok(count) = frame_count.read_scalar::<usize>() {
                    return Ok(count);
                }
            }

            // Try to infer from dataset shapes
            // Look for common KPS dataset paths
            let common_paths = [
                "state/joint/position",
                "state/joint/velocity",
                "action/joint/position",
                "action/joint/velocity",
                "state/effector/position",
                "action/effector/position",
                "observations",
                "actions",
            ];

            for dataset_path in &common_paths {
                if let Ok(dataset) = file.dataset(dataset_path) {
                    if let Ok(dspace) = dataset.space() {
                        let shape = dspace.shape();
                        if !shape.is_empty() {
                            return Ok(shape[0]);
                        }
                    }
                }
            }
        }

        #[cfg(not(feature = "kps-hdf5"))]
        {
            let _ = path;
        }

        // Default fallback
        Ok(0)
    }

    /// Format size with "p" as decimal separator (e.g., 53.21 -> "53p21").
    pub fn format_size(&self) -> String {
        Self::format_with_p_decimal(self.size_gb, "GB")
    }

    /// Format duration with "p" as decimal separator (e.g., 85.30 -> "85p30").
    pub fn format_duration(&self) -> String {
        Self::format_with_p_decimal(self.duration_hours, "h")
    }

    /// Format a number with "p" as decimal separator.
    fn format_with_p_decimal(value: f64, suffix: &str) -> String {
        format!("{:.2}", value).replace('.', "p") + suffix
    }

    /// Generate the task directory suffix: {size}GB_{counts}counts_{duration}h
    pub fn task_dir_suffix(&self) -> String {
        format!(
            "{}_{}counts_{}",
            self.format_size(),
            self.episode_count,
            self.format_duration()
        )
    }
}

/// Extended configuration for v1.2 delivery structure generation.
#[derive(Debug, Clone)]
pub struct SeriesDeliveryConfig {
    /// Root directory (e.g., "F盘" for Chinese systems)
    pub root: PathBuf,

    /// Robot name
    pub robot_name: String,

    /// End effector name (Dexhand/Gripper)
    pub end_effector: String,

    /// Scene name
    pub scene_name: String,

    /// Sub-scene name
    pub sub_scene_name: String,

    /// Task name
    pub task_name: String,

    /// Version string
    pub version: String,

    /// Optional calculated statistics for task directory naming
    pub statistics: Option<TaskStatistics>,
}

impl Default for SeriesDeliveryConfig {
    fn default() -> Self {
        Self {
            root: PathBuf::from("F盘"),
            robot_name: "Robot".to_string(),
            end_effector: "Gripper".to_string(),
            scene_name: "Scene1".to_string(),
            sub_scene_name: "SubScene1".to_string(),
            task_name: "Task1".to_string(),
            version: "v1.0".to_string(),
            statistics: None,
        }
    }
}

impl SeriesDeliveryConfig {
    pub fn new(
        root: impl AsRef<Path>,
        robot_name: String,
        end_effector: String,
        scene_name: String,
        sub_scene_name: String,
        task_name: String,
    ) -> Self {
        Self {
            root: root.as_ref().to_path_buf(),
            robot_name,
            end_effector,
            scene_name,
            sub_scene_name,
            task_name,
            version: "v1.0".to_string(),
            statistics: None,
        }
    }

    /// Set calculated statistics for task directory naming.
    pub fn with_statistics(mut self, statistics: TaskStatistics) -> Self {
        self.statistics = Some(statistics);
        self
    }

    /// Calculate and set statistics from a directory.
    pub fn with_calculated_statistics(
        mut self,
        dir: &Path,
        fps: u32,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        self.statistics = Some(TaskStatistics::calculate_from_dir(dir, fps)?);
        Ok(self)
    }

    /// Generate the series directory name: {Robot}-{EndEffector}-{Scene}
    pub fn series_dir_name(&self) -> String {
        format!(
            "{}-{}-{}",
            self.robot_name, self.end_effector, self.scene_name
        )
    }

    /// Generate the task directory name: {Scene}-{SubScene}-{Task}-{stats}
    ///
    /// Example: `Housekeeper-Kitchen-Dispose_of_takeout_containers-53p21GB_2000counts_85p30h`
    pub fn task_dir_name(&self) -> String {
        let base = format!(
            "{}-{}-{}",
            self.scene_name, self.sub_scene_name, self.task_name
        );

        if let Some(stats) = &self.statistics {
            format!("{}-{}", base, stats.task_dir_suffix())
        } else {
            base
        }
    }

    /// Generate the URDF directory name: {Robot}-{EndEffector}-{version}
    pub fn urdf_dir_name(&self) -> String {
        format!("{}-{}-{}", self.robot_name, self.end_effector, self.version)
    }
}

/// Task information metadata for v1.2 specification.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskInfo {
    /// Task name
    pub task: String,

    /// Scene name
    pub scene: String,

    /// Sub-scene name
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sub_scene: Option<String>,

    /// Robot type
    pub robot: String,

    /// End effector type
    pub end_effector: String,

    /// Description of the task
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,

    /// Number of episodes
    pub num_episodes: usize,

    /// Total frames across all episodes
    pub total_frames: usize,

    /// FPS of the dataset
    pub fps: u32,

    /// Additional metadata
    #[serde(skip_serializing_if = "HashMap::is_empty")]
    #[serde(flatten)]
    pub extra: HashMap<String, serde_json::Value>,
}

impl TaskInfo {
    /// Create a new task info from config and stats.
    pub fn from_config(
        config: &SeriesDeliveryConfig,
        dataset_config: &KpsConfig,
        num_episodes: usize,
        total_frames: usize,
    ) -> Self {
        let mut extra = HashMap::new();

        // Add timestamp
        if let Ok(now) = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH) {
            extra.insert("created_at".to_string(), serde_json::json!(now.as_secs()));
        }

        Self {
            task: config.task_name.clone(),
            scene: config.scene_name.clone(),
            sub_scene: Some(config.sub_scene_name.clone()),
            robot: config.robot_name.clone(),
            end_effector: config.end_effector.clone(),
            description: None,
            num_episodes,
            total_frames,
            fps: dataset_config.dataset.fps,
            extra,
        }
    }

    /// Set a description.
    pub fn with_description(mut self, description: String) -> Self {
        self.description = Some(description);
        self
    }

    /// Add extra metadata.
    pub fn with_extra(mut self, key: String, value: serde_json::Value) -> Self {
        self.extra.insert(key, value);
        self
    }
}

/// v1.2 compliant delivery disk structure generator.
pub struct V12DeliveryBuilder;

impl V12DeliveryBuilder {
    /// Create a delivery structure with a temporary name (without statistics).
    ///
    /// The task directory is created with a temporary name that can be renamed later
    /// using `finalize_with_statistics()` after writing is complete.
    ///
    /// # Returns
    /// Path to the task directory (for later renaming)
    pub fn create_delivery_structure_placeholder(
        root: &Path,
        config: &SeriesDeliveryConfig,
        dataset_config: &KpsConfig,
        calibration: Option<&RobotCalibration>,
        urdf_path: Option<&Path>,
    ) -> Result<PathBuf, Box<dyn std::error::Error>> {
        // Create series directory (use provided root or config root)
        let series_root = root.join(config.series_dir_name());
        fs::create_dir_all(&series_root)?;

        // Create task_info directory
        let task_info_dir = series_root.join("task_info");
        fs::create_dir_all(&task_info_dir)?;

        // Create scene/sub_scene directories with temporary name
        let scene_dir = series_root.join(&config.scene_name);
        let sub_scene_dir = scene_dir.join(&config.sub_scene_name);

        // Use a temporary task directory name (will be renamed later)
        let temp_task_name = format!("{}_temp", config.task_name);
        let task_dir = sub_scene_dir.join(&temp_task_name);
        fs::create_dir_all(&task_dir)?;

        // Create URDF directory structure
        Self::create_urdf_structure_v12(
            &series_root,
            &config.robot_name,
            &config.end_effector,
            &config.version,
            calibration,
            urdf_path,
        )?;

        // Create README
        Self::create_readme_v12(&series_root, config, dataset_config)?;

        println!(
            "Created v1.2 delivery structure (placeholder): {}",
            task_dir.display()
        );

        Ok(task_dir)
    }

    /// Finalize the delivery by renaming the task directory with actual statistics.
    ///
    /// # Arguments
    /// * `temp_task_dir` - The temporary task directory path from `create_delivery_structure_placeholder`
    /// * `config` - The delivery configuration (will be updated with statistics)
    /// * `dataset_config` - The dataset configuration
    /// * `episode_uuids` - List of episode UUIDs written
    ///
    /// # Returns
    /// Path to the finalized task directory
    pub fn finalize_with_statistics(
        temp_task_dir: &Path,
        config: &SeriesDeliveryConfig,
        dataset_config: &KpsConfig,
        episode_uuids: &[String],
    ) -> Result<PathBuf, Box<dyn std::error::Error>> {
        // Calculate statistics from the temporary directory
        let statistics =
            TaskStatistics::calculate_from_dir(temp_task_dir, dataset_config.dataset.fps)?;

        // Create final task directory name with statistics
        let scene_dir = temp_task_dir
            .parent()
            .and_then(|p| p.parent())
            .ok_or("Invalid temporary directory structure")?;
        let final_task_name = format!(
            "{}-{}-{}-{}",
            config.scene_name,
            config.sub_scene_name,
            config.task_name,
            statistics.task_dir_suffix()
        );
        let final_task_dir = scene_dir.join(&final_task_name);

        // Rename the temporary directory to the final name
        fs::rename(temp_task_dir, &final_task_dir)?;
        println!(
            "Renamed: {} -> {}",
            temp_task_dir.display(),
            final_task_dir.display()
        );

        // Update and write task info JSON
        let series_root = scene_dir
            .parent()
            .and_then(|p| p.parent())
            .ok_or("Invalid series directory structure")?;
        let task_info_dir = series_root.join("task_info");

        let task_info = TaskInfo::from_config(
            config,
            dataset_config,
            episode_uuids.len(),
            statistics.episode_count,
        );
        let task_info_json = serde_json::to_string_pretty(&task_info)?;
        let task_info_path = task_info_dir.join(format!("{}.json", final_task_name));

        // Remove old task info if it exists
        if task_info_path.exists() {
            fs::remove_file(&task_info_path)?;
        }
        fs::write(&task_info_path, task_info_json)?;
        println!("Updated: {}", task_info_path.display());

        Ok(final_task_dir)
    }

    /// Create the full v1.2 compliant delivery structure.
    ///
    /// # Arguments
    /// * `source_dir` - Directory containing the converted dataset
    /// * `config` - v1.2 delivery configuration
    /// * `dataset_config` - Kps dataset configuration
    /// * `episode_uuid` - UUID for this episode
    /// * `num_episodes` - Total number of episodes
    /// * `total_frames` - Total frames across all episodes
    /// * `calibration` - Optional robot calibration data
    /// * `urdf_path` - Optional path to URDF file
    ///
    /// # Returns
    /// Path to the episode directory (UUID directory)
    #[allow(clippy::too_many_arguments)]
    pub fn create_delivery_structure(
        source_dir: &Path,
        config: &SeriesDeliveryConfig,
        dataset_config: &KpsConfig,
        episode_uuid: &str,
        num_episodes: usize,
        total_frames: usize,
        calibration: Option<&RobotCalibration>,
        urdf_path: Option<&Path>,
    ) -> Result<PathBuf, Box<dyn std::error::Error>> {
        // Create series directory
        let series_root = config.root.join(config.series_dir_name());
        fs::create_dir_all(&series_root)?;

        // Create task_info directory and write task info JSON
        let task_info_dir = series_root.join("task_info");
        fs::create_dir_all(&task_info_dir)?;

        let task_info = TaskInfo::from_config(config, dataset_config, num_episodes, total_frames);
        let task_info_json = serde_json::to_string_pretty(&task_info)?;
        let task_info_path = task_info_dir.join(format!("{}.json", config.task_dir_name()));
        fs::write(&task_info_path, task_info_json)?;
        println!("Created: {}", task_info_path.display());

        // Create scene/sub_scene directories
        let scene_dir = series_root.join(&config.scene_name);
        let sub_scene_dir = scene_dir.join(&config.sub_scene_name);
        let task_dir = sub_scene_dir.join(config.task_dir_name());
        fs::create_dir_all(&task_dir)?;

        // Create episode UUID directory
        let episode_dir = task_dir.join(episode_uuid);
        fs::create_dir_all(&episode_dir)?;

        // Create v1.2 subdirectories
        let camera_video_dir = episode_dir.join("camera").join("video");
        let camera_depth_dir = episode_dir.join("camera").join("depth");
        let parameters_dir = episode_dir.join("parameters");
        let proprio_stats_dir = episode_dir.join("proprio_stats");
        let audio_dir = episode_dir.join("audio");

        fs::create_dir_all(&camera_video_dir)?;
        fs::create_dir_all(&camera_depth_dir)?;
        fs::create_dir_all(&parameters_dir)?;
        fs::create_dir_all(&proprio_stats_dir)?;
        fs::create_dir_all(&audio_dir)?;

        // Copy episode data
        Self::copy_episode_data_v12(source_dir, &episode_dir)?;

        // Create URDF directory structure
        Self::create_urdf_structure_v12(
            &series_root,
            &config.robot_name,
            &config.end_effector,
            &config.version,
            calibration,
            urdf_path,
        )?;

        // Create README
        Self::create_readme_v12(&series_root, config, dataset_config)?;

        println!("v1.2 Delivery structure created: {}", episode_dir.display());

        Ok(episode_dir)
    }

    /// Copy episode data from source to v1.2 episode directory.
    fn copy_episode_data_v12(
        source_dir: &Path,
        episode_dir: &Path,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let camera_video_dir = episode_dir.join("camera").join("video");
        let camera_depth_dir = episode_dir.join("camera").join("depth");
        let parameters_dir = episode_dir.join("parameters");
        let proprio_stats_dir = episode_dir.join("proprio_stats");
        let audio_dir = episode_dir.join("audio");

        // Check for various source directories and files
        let source_videos = source_dir.join("videos");
        let source_meta = source_dir.join("meta");

        // Copy color videos to camera/video/
        if source_videos.exists() {
            for entry in fs::read_dir(&source_videos)? {
                let entry = entry?;
                let path = entry.path();

                // Determine if this is a color or depth video
                let file_name = path
                    .file_name()
                    .and_then(|n| n.to_str())
                    .unwrap_or("unknown");

                let is_depth = file_name.to_lowercase().contains("depth");

                let target_dir = if is_depth {
                    &camera_depth_dir
                } else {
                    &camera_video_dir
                };

                if path.is_file() {
                    let target = target_dir.join(file_name);
                    fs::copy(&path, &target)?;
                    println!("Copied: {} -> {}", path.display(), target.display());
                }
            }
        }

        // Copy HDF5 files to proprio_stats/
        for entry in fs::read_dir(source_dir)? {
            let entry = entry?;
            let path = entry.path();

            if path.extension().and_then(|s| s.to_str()) == Some("hdf5") {
                let target = proprio_stats_dir.join(path.file_name().unwrap());
                fs::copy(&path, &target)?;
                println!("Copied: {} -> {}", path.display(), target.display());
            }
        }

        // Copy camera parameters to parameters/
        if source_meta.exists() {
            // Look for camera parameter files
            for entry in fs::read_dir(&source_meta)? {
                let entry = entry?;
                let path = entry.path();

                let file_name = path
                    .file_name()
                    .and_then(|n| n.to_str())
                    .unwrap_or("unknown");

                // Copy files that look like camera parameters
                if file_name.contains("camera")
                    || file_name.contains("intrinsics")
                    || file_name.contains("extrinsics")
                    || file_name.contains("calibration")
                {
                    let target = parameters_dir.join(file_name);
                    fs::copy(&path, &target)?;
                    println!("Copied: {} -> {}", path.display(), target.display());
                }
            }
        }

        // Copy audio files to audio/
        for entry in fs::read_dir(source_dir)? {
            let entry = entry?;
            let path = entry.path();

            if let Some(ext) = path.extension() {
                if matches!(
                    ext.to_str(),
                    Some("wav") | Some("mp3") | Some("ogg") | Some("flac")
                ) {
                    let target = audio_dir.join(path.file_name().unwrap());
                    fs::copy(&path, &target)?;
                    println!("Copied: {} -> {}", path.display(), target.display());
                }
            }
        }

        Ok(())
    }

    /// Create URDF directory structure at series level.
    fn create_urdf_structure_v12(
        series_root: &Path,
        robot_name: &str,
        end_effector: &str,
        version: &str,
        calibration: Option<&RobotCalibration>,
        urdf_path: Option<&Path>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let urdf_top_dir = series_root.join("URDF");
        let urdf_dir = urdf_top_dir.join(format!("{}-{}-{}", robot_name, end_effector, version));

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

    /// Create README.md file for the v1.2 delivery.
    fn create_readme_v12(
        series_root: &Path,
        config: &SeriesDeliveryConfig,
        dataset_config: &KpsConfig,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let readme_path = series_root.join("README.md");

        let series_name = config.series_dir_name();
        let urdf_dir_name = config.urdf_dir_name();

        // Build content using string concatenation to avoid format string issues
        let mut content = String::new();
        content.push_str(&format!(
            "# Kps v1.2 Dataset: {}\n\n",
            dataset_config.dataset.name
        ));
        content.push_str("## Dataset Information (v1.2 Specification)\n\n");
        content.push_str(&format!(
            "- **Robot**: {} {}\n",
            config.robot_name, config.end_effector
        ));
        content.push_str(&format!("- **Scene**: {}\n", config.scene_name));
        content.push_str(&format!("- **Sub-Scene**: {}\n", config.sub_scene_name));
        content.push_str(&format!("- **Task**: {}\n", config.task_name));
        content.push_str(&format!("- **FPS**: {}\n\n", dataset_config.dataset.fps));
        content.push_str("## v1.2 Directory Structure\n\n");
        content.push_str(&format!("```\n{}/\n", series_name));
        content.push_str("├── task_info/           # Task metadata at series level\n");
        content.push_str("├── <Scene>/             # Scene directory\n");
        content.push_str("│   └── <SubScene>/      # SubScene directory\n");
        content.push_str("│       └── <Task>-<info>/\n");
        content.push_str("│           └── <UUID>/  # Episode UUID\n");
        content.push_str("│               ├── camera/\n");
        content.push_str("│               │   ├── video/       # Color videos\n");
        content.push_str("│               │   └── depth/       # Depth videos\n");
        content.push_str("│               ├── parameters/      # Camera parameters\n");
        content.push_str("│               ├── proprio_stats/   # HDF5 state files\n");
        content.push_str("│               └── audio/           # Audio recordings\n");
        content.push_str("└── URDF/                # Robot URDF at series level\n");
        content.push_str(&format!("    └── {}/\n", urdf_dir_name));
        content.push_str("```\n\n");
        content.push_str("## Task Info\n\n");
        content.push_str(&format!(
            "Task information is located in `task_info/{}.json`.\n\n",
            config.task_name
        ));
        content.push_str("## URDF\n\n");
        content.push_str(&format!(
            "Robot URDF and calibration are located in `URDF/{}`.\n\n",
            urdf_dir_name
        ));
        content.push_str("## Usage\n\n");
        content.push_str("```python\nimport kps\n# Load episode by UUID\n```\n\n");
        content.push_str("---\nGenerated by roboflow - Kps v1.2 compliant\n");

        fs::write(&readme_path, content)?;
        println!("Created: {}", readme_path.display());

        Ok(())
    }

    /// Generate a new UUID for an episode.
    pub fn generate_episode_uuid() -> String {
        Uuid::new_v4().to_string()
    }

    /// Recursively copy a directory.
    #[allow(dead_code)]
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

/// Helper for building v1.2 delivery config with a fluent API.
pub struct SeriesDeliveryConfigBuilder {
    config: SeriesDeliveryConfig,
}

impl SeriesDeliveryConfigBuilder {
    /// Create a new builder.
    pub fn new() -> Self {
        Self {
            config: SeriesDeliveryConfig::default(),
        }
    }

    /// Set the root directory.
    pub fn root(mut self, root: impl AsRef<Path>) -> Self {
        self.config.root = root.as_ref().to_path_buf();
        self
    }

    /// Set the robot name.
    pub fn robot(mut self, robot: String) -> Self {
        self.config.robot_name = robot;
        self
    }

    /// Set the end effector.
    pub fn end_effector(mut self, end_effector: String) -> Self {
        self.config.end_effector = end_effector;
        self
    }

    /// Set the scene name.
    pub fn scene(mut self, scene: String) -> Self {
        self.config.scene_name = scene;
        self
    }

    /// Set the sub-scene name.
    pub fn sub_scene(mut self, sub_scene: String) -> Self {
        self.config.sub_scene_name = sub_scene;
        self
    }

    /// Set the task name.
    pub fn task(mut self, task: String) -> Self {
        self.config.task_name = task;
        self
    }

    /// Set the version.
    pub fn version(mut self, version: String) -> Self {
        self.config.version = version;
        self
    }

    /// Set statistics for task directory naming.
    pub fn statistics(mut self, statistics: TaskStatistics) -> Self {
        self.config.statistics = Some(statistics);
        self
    }

    /// Build the config.
    pub fn build(self) -> SeriesDeliveryConfig {
        self.config
    }
}

impl Default for SeriesDeliveryConfigBuilder {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_series_delivery_config_default() {
        let config = SeriesDeliveryConfig::default();
        assert_eq!(config.scene_name, "Scene1");
        assert_eq!(config.sub_scene_name, "SubScene1");
        assert_eq!(config.task_name, "Task1");
        assert_eq!(config.version, "v1.0");
    }

    #[test]
    fn test_series_dir_name() {
        let config = SeriesDeliveryConfig {
            robot_name: "Kuavo4Pro".to_string(),
            end_effector: "Dexhand".to_string(),
            scene_name: "Housekeeper".to_string(),
            ..Default::default()
        };
        assert_eq!(config.series_dir_name(), "Kuavo4Pro-Dexhand-Housekeeper");
    }

    #[test]
    fn test_task_dir_name() {
        // 53.21 GB, 2000 episodes, 85.30 hours
        let stats = TaskStatistics::new(53.21, 2000, 85.30);
        let config = SeriesDeliveryConfig {
            scene_name: "Housekeeper".to_string(),
            sub_scene_name: "Kitchen".to_string(),
            task_name: "Dispose_of_takeout_containers".to_string(),
            statistics: Some(stats),
            ..Default::default()
        };
        assert_eq!(
            config.task_dir_name(),
            "Housekeeper-Kitchen-Dispose_of_takeout_containers-53p21GB_2000counts_85p30h"
        );
    }

    #[test]
    fn test_task_statistics_format() {
        let stats = TaskStatistics::new(53.21, 2000, 85.30);
        assert_eq!(stats.format_size(), "53p21GB");
        assert_eq!(stats.format_duration(), "85p30h");
        assert_eq!(stats.task_dir_suffix(), "53p21GB_2000counts_85p30h");
    }

    #[test]
    fn test_task_statistics_rounding() {
        // Test rounding behavior for edge cases
        // Note: Rust uses banker's rounding (round half to even)
        let stats = TaskStatistics::new(1.00, 100, 0.50);
        assert_eq!(stats.format_size(), "1p00GB");
        assert_eq!(stats.format_duration(), "0p50h");

        // Test values that round up
        let stats2 = TaskStatistics::new(1.006, 100, 0.506);
        assert_eq!(stats2.format_size(), "1p01GB");
        assert_eq!(stats2.format_duration(), "0p51h");
    }

    #[test]
    fn test_urdf_dir_name() {
        let config = SeriesDeliveryConfig {
            robot_name: "Kuavo4Pro".to_string(),
            end_effector: "Dexhand".to_string(),
            version: "v1.0".to_string(),
            ..Default::default()
        };
        assert_eq!(config.urdf_dir_name(), "Kuavo4Pro-Dexhand-v1.0");
    }

    #[test]
    fn test_task_info_from_config() {
        let config = SeriesDeliveryConfig {
            robot_name: "Robot".to_string(),
            end_effector: "Gripper".to_string(),
            scene_name: "Scene1".to_string(),
            sub_scene_name: "SubScene1".to_string(),
            task_name: "Pick".to_string(),
            ..Default::default()
        };

        let dataset_config = KpsConfig {
            dataset: crate::dataset::kps::DatasetConfig {
                name: "test".to_string(),
                fps: 30,
                robot_type: None,
            },
            mappings: vec![],
            output: crate::dataset::kps::OutputConfig::default(),
        };

        let task_info = TaskInfo::from_config(&config, &dataset_config, 1, 1000);
        assert_eq!(task_info.task, "Pick");
        assert_eq!(task_info.scene, "Scene1");
        assert_eq!(task_info.sub_scene, Some("SubScene1".to_string()));
        assert_eq!(task_info.robot, "Robot");
        assert_eq!(task_info.end_effector, "Gripper");
        assert_eq!(task_info.num_episodes, 1);
        assert_eq!(task_info.total_frames, 1000);
        assert_eq!(task_info.fps, 30);
    }

    #[test]
    fn test_series_delivery_config_builder() {
        let config = SeriesDeliveryConfigBuilder::new()
            .robot("MyRobot".to_string())
            .end_effector("Gripper".to_string())
            .scene("Kitchen".to_string())
            .sub_scene("Counter".to_string())
            .task("Pick".to_string())
            .version("v2.0".to_string())
            .build();

        assert_eq!(config.robot_name, "MyRobot");
        assert_eq!(config.end_effector, "Gripper");
        assert_eq!(config.scene_name, "Kitchen");
        assert_eq!(config.sub_scene_name, "Counter");
        assert_eq!(config.task_name, "Pick");
        assert_eq!(config.version, "v2.0");
    }

    #[test]
    fn test_generate_episode_uuid() {
        let uuid1 = V12DeliveryBuilder::generate_episode_uuid();
        let uuid2 = V12DeliveryBuilder::generate_episode_uuid();

        assert_ne!(uuid1, uuid2);
        assert_eq!(uuid1.len(), 36); // Standard UUID format
    }

    #[test]
    fn test_statistics_collector() {
        let mut collector = StatisticsCollector::new(30);

        // Simulate writing episodes
        collector.add_episode(900); // 30 seconds at 30 fps
        collector.add_file(1024 * 1024 * 100); // 100 MB

        collector.add_episode(1800); // 60 seconds at 30 fps
        collector.add_file(1024 * 1024 * 200); // 200 MB

        assert_eq!(collector.episode_count, 2);
        assert_eq!(collector.total_frames, 2700);
        assert_eq!(collector.total_bytes, 300 * 1024 * 1024);

        // Duration: 2700 frames / 30 fps / 3600 = 0.025 hours
        assert!((collector.duration_hours() - 0.025).abs() < 0.001);

        // Size: 300 MB / (1024^3) ≈ 0.29 GB
        assert!((collector.size_gb() - 0.29).abs() < 0.01);
    }

    #[test]
    fn test_statistics_collector_to_statistics() {
        let mut collector = StatisticsCollector::new(30);

        // 2000 episodes, 90000 frames (50 hours at 30fps), 53.21 GB
        collector.add_episode(45); // Small episode
        collector.add_file(1024 * 1024 * 1024 * 53 + 1024 * 1024 * 215); // ~53.21 GB

        let stats = collector.to_statistics();
        assert_eq!(stats.episode_count, 1);
        assert!(stats.size_gb > 53.0 && stats.size_gb < 53.3);
    }
}
