// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Kps v1.2 specification compliant HDF5 writer.
//!
//! This writer creates HDF5 files that comply with the Kps v1.2 specification,
//! with the following structure:
//!
//! ```text
//! proprio_stats.hdf5:
//! ├── action/                    # Action data
//! │   ├── effector/             # End effector actions
//! │   │   ├── position
//! │   │   └── velocity
//! │   ├── joint/                # Joint actions
//! │   │   ├── position
//! │   │   └── velocity
//! │   └── end/                  # End effector actions
//! │       ├── position
//! │       └── velocity
//! ├── state/                    # State/observation data
//! │   ├── effector/
//! │   │   ├── position
//! │   │   └── velocity
//! │   ├── joint/
//! │   │   ├── position
//! │   │   └── velocity
//! │   └── end/
//! │       ├── position
//! │       └── velocity
//! ├── other_sensors/            # Other sensor data
//! │   ├── imu/
//! │   │   ├── angular_velocity
//! │   │   └── linear_acceleration
//! │   └── ...
//! └── metadata/                 # File metadata
//!     ├── names
//!     ├── timestamps
//!     └── ...
//! ```

use std::collections::HashMap;
use std::path::Path;

use crate::core::Result;
use crate::dataset::common::{AlignedFrame, WriterStats};
use crate::dataset::kps::config::KpsConfig;
use crate::dataset::kps::writers::base::{KpsWriter, KpsWriterError};
use robocodec::io::metadata::ChannelInfo;

/// Dataset specification for v1.2 HDF5 structure.
#[derive(Debug, Clone)]
pub struct DatasetSpec {
    /// HDF5 path for the dataset
    pub path: String,

    /// Shape of the dataset (0 for unlimited dimension)
    pub shape: Vec<usize>,

    /// Data type
    pub dtype: DataType,

    /// Description of the dataset
    pub description: String,
}

/// HDF5 data type.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DataType {
    Float32,
    Float64,
    Int32,
    Int64,
    UInt8,
    String,
}

impl DataType {
    /// Get the byte size for this type.
    pub fn size(&self) -> usize {
        match self {
            DataType::Float32 | DataType::Int32 => 4,
            DataType::Float64 | DataType::Int64 => 8,
            DataType::UInt8 => 1,
            DataType::String => 0, // Variable length
        }
    }
}

/// HDF5 schema for v1.2 specification.
#[derive(Debug, Clone)]
pub struct V12Hdf5Schema {
    /// All dataset specifications
    pub datasets: Vec<DatasetSpec>,
}

impl V12Hdf5Schema {
    /// Create a new v1.2 schema from config and channels.
    pub fn from_config(config: &KpsConfig, _channels: &HashMap<u16, ChannelInfo>) -> Self {
        let mut datasets = Vec::new();

        // Build standard action and state datasets based on mappings
        for mapping in &config.mappings {
            let feature_parts: Vec<&str> = mapping.feature.split('.').collect();

            if feature_parts.len() < 2 {
                continue;
            }

            let category = feature_parts[0]; // "observation" or "action"
            let subfeature = if feature_parts.len() > 2 {
                &feature_parts[1..].join(".")
            } else {
                feature_parts[1]
            };

            // Map to v1.2 paths
            let (group_base, data_type) = match category {
                "observation" => ("state", DataType::Float32),
                "action" => ("action", DataType::Float32),
                _ => continue,
            };

            // Parse the subfeature to determine joint group and data type
            // Format: observation.joint.position -> state/joint/position
            let parts: Vec<&str> = subfeature.split('.').collect();
            let (joint_group, data_name) = if parts.len() >= 2 {
                (parts[0], parts[1])
            } else {
                // Default to "joint" group if not specified
                ("joint", *parts.first().unwrap_or(&"position"))
            };

            let path = format!("{}/{}/{}", group_base, joint_group, data_name);

            datasets.push(DatasetSpec {
                path,
                shape: vec![0, 7], // Will be updated based on actual data
                dtype: data_type,
                description: format!("{} {} data", joint_group, data_name),
            });
        }

        // Add standard metadata datasets
        datasets.extend(Self::metadata_datasets());

        // Add other_sensors datasets
        datasets.extend(Self::other_sensors_datasets());

        Self { datasets }
    }

    /// Get standard metadata dataset specifications.
    fn metadata_datasets() -> Vec<DatasetSpec> {
        vec![
            DatasetSpec {
                path: "metadata/timestamps".to_string(),
                shape: vec![0],
                dtype: DataType::Int64,
                description: "Timestamp for each frame (nanoseconds)".to_string(),
            },
            DatasetSpec {
                path: "metadata/episode_index".to_string(),
                shape: vec![0],
                dtype: DataType::Int32,
                description: "Episode index for each frame".to_string(),
            },
            DatasetSpec {
                path: "metadata/frame_index".to_string(),
                shape: vec![0],
                dtype: DataType::Int32,
                description: "Frame index within episode".to_string(),
            },
            DatasetSpec {
                path: "metadata/reward".to_string(),
                shape: vec![0],
                dtype: DataType::Float32,
                description: "Reward value (if available)".to_string(),
            },
        ]
    }

    /// Get other_sensors dataset specifications.
    fn other_sensors_datasets() -> Vec<DatasetSpec> {
        vec![
            DatasetSpec {
                path: "other_sensors/imu/angular_velocity".to_string(),
                shape: vec![0, 3],
                dtype: DataType::Float32,
                description: "IMU angular velocity [wx, wy, wz] in rad/s".to_string(),
            },
            DatasetSpec {
                path: "other_sensors/imu/linear_acceleration".to_string(),
                shape: vec![0, 3],
                dtype: DataType::Float32,
                description: "IMU linear acceleration [ax, ay, az] in m/s²".to_string(),
            },
            DatasetSpec {
                path: "other_sensors/imu/orientation".to_string(),
                shape: vec![0, 4],
                dtype: DataType::Float32,
                description: "IMU orientation as quaternion [x, y, z, w]".to_string(),
            },
        ]
    }

    /// Get a dataset spec by path.
    pub fn get_dataset(&self, path: &str) -> Option<&DatasetSpec> {
        self.datasets.iter().find(|d| d.path == path)
    }

    /// Get all dataset paths for a group.
    pub fn get_group_datasets(&self, group: &str) -> Vec<&DatasetSpec> {
        self.datasets
            .iter()
            .filter(|d| d.path.starts_with(group))
            .collect()
    }
}

/// Kps v1.2 compliant HDF5 writer.
///
/// This writer creates HDF5 files with the v1.2 specification structure,
/// including action/, state/, other_sensors/, and metadata/ groups.
#[allow(dead_code)] // Suppress warnings about unused fields during API migration
pub struct V12Hdf5Writer {
    /// Episode ID for this writer.
    episode_id: String,

    /// Output directory path.
    output_dir: std::path::PathBuf,

    /// Number of frames written.
    frame_count: usize,

    /// Number of images encoded.
    images_encoded: usize,

    /// Number of state records written.
    state_records: usize,

    /// HDF5 file handle.
    #[cfg(feature = "dataset-hdf5")]
    hdf5_file: Option<hdf5::File>,

    /// HDF5 datasets by full path.
    #[cfg(feature = "dataset-hdf5")]
    datasets: HashMap<String, hdf5::Dataset>,

    /// HDF5 groups for quick access.
    #[cfg(feature = "dataset-hdf5")]
    groups: HashMap<String, hdf5::Group>,

    /// Schema for this file.
    schema: V12Hdf5Schema,

    /// Whether initialized.
    initialized: bool,

    /// Frame dimension tracking (for knowing when to resize).
    frame_dimensions: HashMap<String, usize>,

    /// Channel info by topic.
    channels: HashMap<String, ChannelInfo>,

    /// Start time for duration calculation.
    start_time: Option<std::time::Instant>,

    /// Buffered state data by dataset path.
    #[cfg(feature = "dataset-hdf5")]
    state_buffers: HashMap<String, Vec<Vec<f32>>>,

    /// Buffered timestamp data.
    #[cfg(feature = "dataset-hdf5")]
    timestamp_buffer: Vec<i64>,

    /// Buffered frame index data.
    #[cfg(feature = "dataset-hdf5")]
    frame_index_buffer: Vec<i32>,
}

impl V12Hdf5Writer {
    /// Create a new v1.2 HDF5 writer.
    pub fn create(
        output_dir: impl AsRef<Path>,
        episode_id: &str,
        config: &KpsConfig,
        channels: &HashMap<u16, ChannelInfo>,
    ) -> Result<Self> {
        let output_dir = output_dir.as_ref();
        std::fs::create_dir_all(output_dir)?;

        // Build schema from config
        let schema = V12Hdf5Schema::from_config(config, channels);

        // Store channel info
        let mut channel_map = HashMap::new();
        for ch in channels.values() {
            channel_map.insert(ch.topic.clone(), ch.clone());
        }

        Ok(Self {
            episode_id: episode_id.to_string(),
            output_dir: output_dir.to_path_buf(),
            frame_count: 0,
            images_encoded: 0,
            state_records: 0,
            #[cfg(feature = "dataset-hdf5")]
            hdf5_file: None,
            #[cfg(feature = "dataset-hdf5")]
            datasets: HashMap::new(),
            #[cfg(feature = "dataset-hdf5")]
            groups: HashMap::new(),
            schema,
            initialized: false,
            frame_dimensions: HashMap::new(),
            channels: channel_map,
            start_time: None,
            #[cfg(feature = "dataset-hdf5")]
            state_buffers: HashMap::new(),
            #[cfg(feature = "dataset-hdf5")]
            timestamp_buffer: Vec::new(),
            #[cfg(feature = "dataset-hdf5")]
            frame_index_buffer: Vec::new(),
        })
    }

    /// Create the v1.2 HDF5 group structure.
    #[cfg(feature = "dataset-hdf5")]
    fn create_v12_structure(&mut self, file: &hdf5::File) -> Result<()> {
        // Create top-level groups
        let action_group = file
            .create_group("action")
            .map_err(|e| KpsWriterError::Hdf5(e.to_string()))?;
        let state_group = file
            .create_group("state")
            .map_err(|e| KpsWriterError::Hdf5(e.to_string()))?;
        let other_sensors_group = file
            .create_group("other_sensors")
            .map_err(|e| KpsWriterError::Hdf5(e.to_string()))?;
        let metadata_group = file
            .create_group("metadata")
            .map_err(|e| KpsWriterError::Hdf5(e.to_string()))?;

        // Store groups for quick access
        self.groups.insert("action".to_string(), action_group);
        self.groups.insert("state".to_string(), state_group);
        self.groups
            .insert("other_sensors".to_string(), other_sensors_group);
        self.groups.insert("metadata".to_string(), metadata_group);

        // Create subgroups and datasets based on schema
        // Clone the dataset specs to avoid borrow checker issues
        let dataset_specs = self.schema.datasets.clone();
        for dataset_spec in &dataset_specs {
            self.create_dataset_from_spec(file, dataset_spec)?;
        }

        Ok(())
    }

    /// Create a dataset from a specification.
    #[cfg(feature = "dataset-hdf5")]
    fn create_dataset_from_spec(&mut self, file: &hdf5::File, spec: &DatasetSpec) -> Result<()> {
        // Ensure parent groups exist
        let path_parts: Vec<&str> = spec.path.split('/').collect();
        if path_parts.len() < 2 {
            return Err(KpsWriterError::InvalidData(format!(
                "Invalid dataset path: {}",
                spec.path
            ))
            .into());
        }

        // Create parent groups
        let mut current_path = String::new();
        for (i, part) in path_parts.iter().enumerate() {
            if i == path_parts.len() - 1 {
                break; // Last part is the dataset name
            }
            if i > 0 {
                current_path.push('/');
            }
            current_path.push_str(part);

            if !self.groups.contains_key(&current_path) {
                let group = file
                    .create_group(&current_path)
                    .map_err(|e| KpsWriterError::Hdf5(e.to_string()))?;
                self.groups.insert(current_path.clone(), group);
            }
        }

        // Create the dataset with proper shape and type based on dtype
        let dataset = match spec.dtype {
            DataType::Float32 => {
                // For extendable datasets, use 0 for unlimited dimension
                let shape: Vec<usize> = spec
                    .shape
                    .iter()
                    .map(|&s| if s == 0 { 0 } else { s })
                    .collect();
                file.new_dataset::<f32>()
                    .shape(shape.as_slice())
                    .create(&*spec.path)
                    .map_err(|e| KpsWriterError::Hdf5(e.to_string()))?
            }
            DataType::Float64 => {
                let shape: Vec<usize> = spec
                    .shape
                    .iter()
                    .map(|&s| if s == 0 { 0 } else { s })
                    .collect();
                file.new_dataset::<f64>()
                    .shape(shape.as_slice())
                    .create(&*spec.path)
                    .map_err(|e| KpsWriterError::Hdf5(e.to_string()))?
            }
            DataType::Int32 => {
                let shape: Vec<usize> = spec
                    .shape
                    .iter()
                    .map(|&s| if s == 0 { 0 } else { s })
                    .collect();
                file.new_dataset::<i32>()
                    .shape(shape.as_slice())
                    .create(&*spec.path)
                    .map_err(|e| KpsWriterError::Hdf5(e.to_string()))?
            }
            DataType::Int64 => {
                let shape: Vec<usize> = spec
                    .shape
                    .iter()
                    .map(|&s| if s == 0 { 0 } else { s })
                    .collect();
                file.new_dataset::<i64>()
                    .shape(shape.as_slice())
                    .create(&*spec.path)
                    .map_err(|e| KpsWriterError::Hdf5(e.to_string()))?
            }
            DataType::UInt8 => {
                let shape: Vec<usize> = spec
                    .shape
                    .iter()
                    .map(|&s| if s == 0 { 0 } else { s })
                    .collect();
                file.new_dataset::<u8>()
                    .shape(shape.as_slice())
                    .create(&*spec.path)
                    .map_err(|e| KpsWriterError::Hdf5(e.to_string()))?
            }
            DataType::String => {
                // For string data, use fixed-size array for now
                let shape: Vec<usize> = spec
                    .shape
                    .iter()
                    .map(|&s| if s == 0 { 1 } else { s })
                    .collect();
                file.new_dataset::<hdf5::types::FixedAscii<256>>()
                    .shape(shape.as_slice())
                    .create(&*spec.path)
                    .map_err(|e| KpsWriterError::Hdf5(e.to_string()))?
            }
        };

        self.datasets.insert(spec.path.clone(), dataset);
        Ok(())
    }

    /// Map a feature path to v1.2 HDF5 path.
    ///
    /// observation.joint.position -> state/joint/position
    /// action.effector.position -> action/effector/position
    fn map_feature_to_v12_path(&self, feature: &str) -> Option<String> {
        let parts: Vec<&str> = feature.split('.').collect();
        if parts.len() < 2 {
            return None;
        }

        let category = match parts[0] {
            "observation" => "state",
            "action" => "action",
            _ => return None,
        };

        if parts.len() >= 3 {
            // observation.joint.position -> state/joint/position
            Some(format!("{}/{}", category, parts[1..].join("/")))
        } else {
            // observation.state -> state/state (fallback)
            Some(format!("{}/{}", category, parts[1]))
        }
    }

    /// Write data to a v1.2 dataset.
    ///
    /// Data is buffered and written at finalization.
    #[cfg(feature = "dataset-hdf5")]
    fn write_to_dataset(&mut self, path: &str, data: &[f32]) -> Result<()> {
        self.state_buffers
            .entry(path.to_string())
            .or_default()
            .push(data.to_vec());
        Ok(())
    }

    /// Write all buffered data to HDF5 datasets.
    #[cfg(feature = "dataset-hdf5")]
    fn write_buffered_data(&mut self) -> Result<()> {
        // Write state buffers
        for (path, states) in &self.state_buffers {
            if states.is_empty() {
                continue;
            }

            let dim = states.first().map(|s| s.len()).unwrap_or(0);
            if dim == 0 {
                continue;
            }

            // Get or create dataset
            if let Some(dataset) = self.datasets.get(path) {
                // Resize to final size
                dataset
                    .resize([states.len(), dim])
                    .map_err(|e| KpsWriterError::Hdf5(e.to_string()))?;

                // Flatten and write
                let flat_data: Vec<f32> = states.iter().flatten().copied().collect();
                dataset
                    .write(&flat_data)
                    .map_err(|e| KpsWriterError::Hdf5(e.to_string()))?;
            }
        }

        // Write timestamp buffer
        if !self.timestamp_buffer.is_empty()
            && let Some(dataset) = self.datasets.get("metadata/timestamps")
        {
            dataset
                .resize([self.timestamp_buffer.len()])
                .map_err(|e| KpsWriterError::Hdf5(e.to_string()))?;
            dataset
                .write(&self.timestamp_buffer)
                .map_err(|e| KpsWriterError::Hdf5(e.to_string()))?;
        }

        // Write frame index buffer
        if !self.frame_index_buffer.is_empty()
            && let Some(dataset) = self.datasets.get("metadata/frame_index")
        {
            dataset
                .resize([self.frame_index_buffer.len()])
                .map_err(|e| KpsWriterError::Hdf5(e.to_string()))?;
            dataset
                .write(&self.frame_index_buffer)
                .map_err(|e| KpsWriterError::Hdf5(e.to_string()))?;
        }

        Ok(())
    }
}

impl KpsWriter for V12Hdf5Writer {
    fn initialize(
        &mut self,
        config: &KpsConfig,
        channels: &HashMap<u16, ChannelInfo>,
    ) -> Result<()> {
        #[cfg(feature = "dataset-hdf5")]
        {
            use hdf5::File;

            // Create HDF5 file
            let hdf5_path = self.output_dir.join("proprio_stats.hdf5");

            let file = File::create(&hdf5_path).map_err(|e| KpsWriterError::Hdf5(e.to_string()))?;
            self.hdf5_file = Some(file);

            // Clone the file to avoid borrow checker issues
            // hdf5::File uses reference counting internally
            let hdf5_file = self.hdf5_file.as_ref().unwrap().clone();

            // Create v1.2 structure
            self.create_v12_structure(&hdf5_file)?;

            // Update channels mapping
            self.channels.clear();
            for ch in channels.values() {
                self.channels.insert(ch.topic.clone(), ch.clone());
            }

            // Rebuild schema with updated channel info
            self.schema = V12Hdf5Schema::from_config(config, channels);

            self.initialized = true;
            self.start_time = Some(std::time::Instant::now());

            tracing::info!(
                path = %hdf5_path.display(),
                "Initialized v1.2 HDF5 writer"
            );

            Ok(())
        }

        #[cfg(not(feature = "dataset-hdf5"))]
        {
            let _ = (config, channels);
            Err(crate::core::RoboflowError::ParseError {
                context: "HDF5 writer".to_string(),
                message: "HDF5 support not enabled".to_string(),
            })
        }
    }

    fn write_frame(&mut self, frame: &AlignedFrame) -> Result<()> {
        #[cfg(feature = "dataset-hdf5")]
        {
            if !self.initialized {
                return Err(
                    KpsWriterError::InvalidData("Writer not initialized".to_string()).into(),
                );
            }

            // Buffer metadata
            let timestamp_ns = frame.timestamp as i64;
            self.timestamp_buffer.push(timestamp_ns);

            let frame_idx = self.frame_count as i32;
            self.frame_index_buffer.push(frame_idx);

            // Buffer states (observations)
            for (feature, values) in &frame.states {
                if let Some(v12_path) = self.map_feature_to_v12_path(feature) {
                    self.write_to_dataset(&v12_path, values)?;
                    self.state_records += 1;
                }
            }

            // Buffer actions
            for (feature, values) in &frame.actions {
                if let Some(v12_path) = self.map_feature_to_v12_path(feature) {
                    self.write_to_dataset(&v12_path, values)?;
                    self.state_records += 1;
                }
            }

            // Track images for encoding count (actual video encoding is separate)
            self.images_encoded += frame.images.len();

            self.frame_count += 1;

            Ok(())
        }

        #[cfg(not(feature = "dataset-hdf5"))]
        {
            let _ = frame;
            Err(crate::core::RoboflowError::ParseError {
                context: "HDF5 writer".to_string(),
                message: "HDF5 support not enabled".to_string(),
            })
        }
    }

    fn finalize(
        &mut self,
        _config: &KpsConfig,
        _camera_params: Option<&crate::dataset::kps::camera_params::CameraParamCollector>,
    ) -> Result<WriterStats> {
        #[cfg(feature = "dataset-hdf5")]
        {
            // Write all buffered data to HDF5
            self.write_buffered_data()?;

            // Close HDF5 file
            self.hdf5_file = None;

            let duration = self
                .start_time
                .map(|t| t.elapsed().as_secs_f64())
                .unwrap_or(0.0);

            tracing::info!(
                frames = self.frame_count,
                state_records = self.state_records,
                "Finalized v1.2 HDF5 writer"
            );

            Ok(WriterStats {
                frames_written: self.frame_count,
                images_encoded: self.images_encoded,
                state_records: self.state_records,
                output_bytes: 0,
                duration_sec: duration,
            })
        }

        #[cfg(not(feature = "dataset-hdf5"))]
        {
            let _ = (_config, _camera_params);
            Err(crate::core::RoboflowError::ParseError {
                context: "HDF5 writer".to_string(),
                message: "HDF5 support not enabled".to_string(),
            })
        }
    }

    fn frame_count(&self) -> usize {
        self.frame_count
    }

    fn is_initialized(&self) -> bool {
        self.initialized
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    #[test]
    fn test_map_feature_to_v12_path() {
        // Create a dummy writer for testing
        let config = KpsConfig {
            dataset: crate::dataset::kps::DatasetConfig {
                name: "test".to_string(),
                fps: 30,
                robot_type: None,
            },
            mappings: vec![],
            output: crate::dataset::kps::OutputConfig::default(),
        };

        let temp_dir = std::env::temp_dir();
        let writer =
            V12Hdf5Writer::create(&temp_dir, "test_episode", &config, &HashMap::new()).unwrap();

        // Test observation mapping
        assert_eq!(
            writer.map_feature_to_v12_path("observation.joint.position"),
            Some("state/joint/position".to_string())
        );
        assert_eq!(
            writer.map_feature_to_v12_path("observation.effector.position"),
            Some("state/effector/position".to_string())
        );

        // Test action mapping
        assert_eq!(
            writer.map_feature_to_v12_path("action.joint.position"),
            Some("action/joint/position".to_string())
        );
        assert_eq!(
            writer.map_feature_to_v12_path("action.effector.velocity"),
            Some("action/effector/velocity".to_string())
        );

        // Test invalid paths
        assert_eq!(writer.map_feature_to_v12_path("invalid.path"), None);
        assert_eq!(writer.map_feature_to_v12_path("observation"), None);
    }

    #[test]
    fn test_v12_schema_other_sensors() {
        let datasets = V12Hdf5Schema::other_sensors_datasets();
        assert_eq!(datasets.len(), 3);
        assert_eq!(datasets[0].path, "other_sensors/imu/angular_velocity");
        assert_eq!(datasets[1].path, "other_sensors/imu/linear_acceleration");
        assert_eq!(datasets[2].path, "other_sensors/imu/orientation");
    }

    #[test]
    fn test_v12_schema_metadata() {
        let datasets = V12Hdf5Schema::metadata_datasets();
        assert!(datasets.iter().any(|d| d.path == "metadata/timestamps"));
        assert!(datasets.iter().any(|d| d.path == "metadata/frame_index"));
        assert!(datasets.iter().any(|d| d.path == "metadata/episode_index"));
        assert!(datasets.iter().any(|d| d.path == "metadata/reward"));
    }

    #[test]
    fn test_data_type_size() {
        assert_eq!(DataType::Float32.size(), 4);
        assert_eq!(DataType::Float64.size(), 8);
        assert_eq!(DataType::Int32.size(), 4);
        assert_eq!(DataType::Int64.size(), 8);
        assert_eq!(DataType::UInt8.size(), 1);
        assert_eq!(DataType::String.size(), 0);
    }
}
