// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Streaming HDF5 writer for Kps datasets.
//!
//! This writer implements the [`KpsWriter`] trait for HDF5 format,
//! supporting frame-by-frame writing for pipeline integration.

use std::collections::HashMap;
use std::path::Path;

use crate::core::Result;
use crate::dataset::common::{AlignedFrame, DatasetWriter, ImageData, WriterStats};
use crate::dataset::kps::config::KpsConfig;
use crate::dataset::kps::writers::base::{KpsWriter, KpsWriterError};
use robocodec::io::metadata::ChannelInfo;

/// Buffered image data for HDF5 writing.
#[derive(Debug, Clone)]
struct BufferedImageData {
    data: Vec<u8>,
    #[allow(dead_code)]
    width: usize,
    #[allow(dead_code)]
    height: usize,
}

/// Streaming HDF5 writer for Kps datasets.
///
/// This writer supports frame-by-frame writing for pipeline integration.
/// Data is buffered and written to HDF5 at finalization.
#[allow(dead_code)] // Suppress warnings about unused HDF5 dataset fields during API migration
pub struct StreamingHdf5Writer {
    /// Episode ID for this writer.
    episode_id: usize,

    /// Output directory path.
    output_dir: std::path::PathBuf,

    /// Number of frames written.
    frame_count: usize,

    /// Number of images encoded.
    images_encoded: usize,

    /// Number of state records written.
    state_records: usize,

    /// HDF5 file handle (when feature is enabled).
    #[cfg(feature = "kps-hdf5")]
    hdf5_file: Option<hdf5::File>,

    /// HDF5 datasets for image data.
    #[cfg(feature = "kps-hdf5")]
    image_datasets: HashMap<String, hdf5::Dataset>,

    /// HDF5 datasets for state data.
    #[cfg(feature = "kps-hdf5")]
    state_datasets: HashMap<String, hdf5::Dataset>,

    /// HDF5 datasets for action data.
    #[cfg(feature = "kps-hdf5")]
    action_datasets: HashMap<String, hdf5::Dataset>,

    /// HDF5 group for observations.
    #[cfg(feature = "kps-hdf5")]
    obs_group: Option<hdf5::Group>,

    /// HDF5 group for actions.
    #[cfg(feature = "kps-hdf5")]
    action_group: Option<hdf5::Group>,

    /// HDF5 group for metadata.
    #[cfg(feature = "kps-hdf5")]
    metadata_group: Option<hdf5::Group>,

    /// Whether initialized.
    initialized: bool,

    /// Image shapes tracking.
    image_shapes: HashMap<String, (usize, usize)>,

    /// State dimensions tracking.
    state_dims: HashMap<String, usize>,

    /// Channel info by topic.
    channels: HashMap<String, ChannelInfo>,

    /// Kps config.
    config: Option<KpsConfig>,

    /// Start time for duration calculation.
    start_time: Option<std::time::Instant>,

    /// Buffered image data for delayed writing.
    image_buffers: HashMap<String, Vec<BufferedImageData>>,

    /// Buffered state data for delayed writing.
    state_buffers: HashMap<String, Vec<Vec<f32>>>,

    /// Buffered action data for delayed writing.
    action_buffers: HashMap<String, Vec<Vec<f32>>>,
}

impl StreamingHdf5Writer {
    /// Create a new HDF5 writer for the specified output directory.
    pub fn create(
        output_dir: impl AsRef<Path>,
        episode_id: usize,
        config: &KpsConfig,
    ) -> std::result::Result<Self, KpsWriterError> {
        let output_dir = output_dir.as_ref();
        std::fs::create_dir_all(output_dir)?;

        Ok(Self {
            episode_id,
            output_dir: output_dir.to_path_buf(),
            frame_count: 0,
            images_encoded: 0,
            state_records: 0,
            #[cfg(feature = "kps-hdf5")]
            hdf5_file: None,
            #[cfg(feature = "kps-hdf5")]
            image_datasets: HashMap::new(),
            #[cfg(feature = "kps-hdf5")]
            state_datasets: HashMap::new(),
            #[cfg(feature = "kps-hdf5")]
            action_datasets: HashMap::new(),
            #[cfg(feature = "kps-hdf5")]
            obs_group: None,
            #[cfg(feature = "kps-hdf5")]
            action_group: None,
            #[cfg(feature = "kps-hdf5")]
            metadata_group: None,
            initialized: false,
            image_shapes: HashMap::new(),
            state_dims: HashMap::new(),
            channels: HashMap::new(),
            config: Some(config.clone()),
            start_time: None,
            image_buffers: HashMap::new(),
            state_buffers: HashMap::new(),
            action_buffers: HashMap::new(),
        })
    }

    /// Create HDF5 datasets for a feature based on shape information.
    #[cfg(feature = "kps-hdf5")]
    fn create_dataset_for_feature(
        &mut self,
        feature: &str,
        group: &hdf5::Group,
        is_image: bool,
        _shape: Option<&[usize]>,
    ) -> std::result::Result<(), KpsWriterError> {
        use hdf5::types::VarLenArray;

        if is_image {
            // For images, we use variable-length byte arrays
            let images = self.image_buffers.get(feature);
            let count = images.map(|v| v.len()).unwrap_or(0);

            if count > 0 {
                let dataset = group
                    .new_dataset::<VarLenArray<u8>>()
                    .shape(count)
                    .create(feature)
                    .map_err(|e| KpsWriterError::Hdf5(e.to_string()))?;
                self.image_datasets.insert(feature.to_string(), dataset);
            }
        } else {
            // For state/action data, we use fixed-size 2D arrays
            let states = self
                .state_buffers
                .get(feature)
                .or_else(|| self.action_buffers.get(feature));
            if let Some(states) = states {
                let count = states.len();
                let dim = states.first().map(|v| v.len()).unwrap_or(0);

                if count > 0 && dim > 0 {
                    let dataset = group
                        .new_dataset::<f32>()
                        .shape([count, dim])
                        .create(feature)
                        .map_err(|e| KpsWriterError::Hdf5(e.to_string()))?;

                    if self.state_buffers.contains_key(feature) {
                        self.state_datasets.insert(feature.to_string(), dataset);
                    } else {
                        self.action_datasets.insert(feature.to_string(), dataset);
                    }
                }
            }
        }

        Ok(())
    }

    /// Write image data to an HDF5 dataset.
    #[cfg(feature = "kps-hdf5")]
    fn write_image_to_dataset(
        &mut self,
        feature: &str,
        data: &ImageData,
    ) -> std::result::Result<(), KpsWriterError> {
        // Buffer the image data for writing at finalization
        self.image_buffers
            .entry(feature.to_string())
            .or_default()
            .push(BufferedImageData {
                data: data.data.clone(),
                width: data.width as usize,
                height: data.height as usize,
            });
        Ok(())
    }

    /// Write state data to an HDF5 dataset.
    #[cfg(feature = "kps-hdf5")]
    fn write_state_to_dataset(
        &mut self,
        feature: &str,
        values: &[f32],
    ) -> std::result::Result<(), KpsWriterError> {
        // Buffer the state data for writing at finalization
        self.state_buffers
            .entry(feature.to_string())
            .or_default()
            .push(values.to_vec());
        Ok(())
    }

    /// Write buffered data to HDF5 datasets.
    #[cfg(feature = "kps-hdf5")]
    fn write_buffered_data(&mut self) -> Result<()> {
        use hdf5::types::VarLenArray;

        let obs_group =
            self.obs_group
                .as_ref()
                .ok_or_else(|| crate::core::RoboflowError::ParseError {
                    context: "HDF5 writer".to_string(),
                    message: "Observations group not initialized".to_string(),
                })?;
        let action_group =
            self.action_group
                .as_ref()
                .ok_or_else(|| crate::core::RoboflowError::ParseError {
                    context: "HDF5 writer".to_string(),
                    message: "Actions group not initialized".to_string(),
                })?;

        // Write image data
        for (feature, images) in &self.image_buffers {
            if images.is_empty() {
                continue;
            }

            let dataset = obs_group
                .new_dataset::<VarLenArray<u8>>()
                .shape(images.len())
                .create(&**feature)
                .map_err(|e| KpsWriterError::Hdf5(e.to_string()))?;

            let varlen_images: Vec<VarLenArray<u8>> = images
                .iter()
                .map(|img| VarLenArray::from_slice(&img.data))
                .collect();

            dataset
                .write(&varlen_images)
                .map_err(|e| KpsWriterError::Hdf5(e.to_string()))?;
            self.image_datasets.insert(feature.clone(), dataset);
        }

        // Write state data
        for (feature, states) in &self.state_buffers {
            if states.is_empty() {
                continue;
            }

            let dim = states.first().map(|s| s.len()).unwrap_or(0);
            if dim == 0 {
                continue;
            }

            let dataset = obs_group
                .new_dataset::<f32>()
                .shape([states.len(), dim])
                .create(&**feature)
                .map_err(|e| KpsWriterError::Hdf5(e.to_string()))?;

            let flat_data: Vec<f32> = states.iter().flatten().copied().collect();
            dataset
                .write(&flat_data)
                .map_err(|e| KpsWriterError::Hdf5(e.to_string()))?;
            self.state_datasets.insert(feature.clone(), dataset);
        }

        // Write action data
        for (feature, actions) in &self.action_buffers {
            if actions.is_empty() {
                continue;
            }

            let dim = actions.first().map(|a| a.len()).unwrap_or(0);
            if dim == 0 {
                continue;
            }

            let dataset = action_group
                .new_dataset::<f32>()
                .shape([actions.len(), dim])
                .create(&**feature)
                .map_err(|e| KpsWriterError::Hdf5(e.to_string()))?;

            let flat_data: Vec<f32> = actions.iter().flatten().copied().collect();
            dataset
                .write(&flat_data)
                .map_err(|e| KpsWriterError::Hdf5(e.to_string()))?;
            self.action_datasets.insert(feature.clone(), dataset);
        }

        Ok(())
    }

    /// Write metadata files (info.json, episode.jsonl).
    fn write_metadata_files(&self, config: &KpsConfig) -> std::result::Result<(), KpsWriterError> {
        use crate::dataset::kps::info;

        // Write info.json
        info::write_info_json(
            &self.output_dir,
            config,
            self.frame_count as u64,
            &self.image_shapes,
            &self.state_dims,
        )
        .map_err(|e| KpsWriterError::Encoding(e.to_string()))?;

        // Write episode.jsonl
        info::write_episode_json(
            &self.output_dir,
            self.episode_id,
            0,
            self.frame_count as u64 * 1_000_000_000 / config.dataset.fps as u64,
            self.frame_count,
        )
        .map_err(|e| KpsWriterError::Encoding(e.to_string()))?;

        Ok(())
    }
}

impl KpsWriter for StreamingHdf5Writer {
    fn initialize(
        &mut self,
        config: &KpsConfig,
        channels: &HashMap<u16, ChannelInfo>,
    ) -> Result<()> {
        #[cfg(feature = "kps-hdf5")]
        {
            // Store config and channels
            self.config = Some(config.clone());
            for ch in channels.values() {
                self.channels.insert(ch.topic.clone(), ch.clone());
            }

            // Create HDF5 file
            let hdf5_path = self
                .output_dir
                .join(format!("episode_{:06}.hdf5", self.episode_id));

            let file =
                hdf5::File::create(&hdf5_path).map_err(|e| KpsWriterError::Hdf5(e.to_string()))?;
            self.hdf5_file = Some(file);

            let hdf5_file = self.hdf5_file.as_ref().unwrap();

            // Create groups
            let obs_group = hdf5_file
                .create_group("observations")
                .map_err(|e| KpsWriterError::Hdf5(e.to_string()))?;
            self.obs_group = Some(obs_group);

            let action_group = hdf5_file
                .create_group("actions")
                .map_err(|e| KpsWriterError::Hdf5(e.to_string()))?;
            self.action_group = Some(action_group);

            let metadata_group = hdf5_file
                .create_group("metadata")
                .map_err(|e| KpsWriterError::Hdf5(e.to_string()))?;
            self.metadata_group = Some(metadata_group);

            // Create datasets based on mappings
            for mapping in &config.mappings {
                let feature_name = mapping
                    .feature
                    .strip_prefix("observation.")
                    .or_else(|| mapping.feature.strip_prefix("action."))
                    .unwrap_or(&mapping.feature);

                let is_observation = mapping.feature.starts_with("observation.");
                let is_image = matches!(
                    mapping.mapping_type,
                    crate::dataset::kps::MappingType::Image
                );

                // Clone the appropriate group to avoid borrow checker issues
                // hdf5::Group uses reference counting internally
                let group = if is_observation {
                    self.obs_group.as_ref().unwrap().clone()
                } else {
                    self.action_group.as_ref().unwrap().clone()
                };

                self.create_dataset_for_feature(feature_name, &group, is_image, None)?;
            }

            self.initialized = true;
            self.start_time = Some(std::time::Instant::now());

            Ok(())
        }

        #[cfg(not(feature = "kps-hdf5"))]
        {
            let _ = (config, channels);
            Err(crate::core::RoboflowError::ParseError {
                context: "HDF5 writer".to_string(),
                message: "HDF5 support not enabled".to_string(),
            })
        }
    }

    fn write_frame(&mut self, frame: &AlignedFrame) -> Result<()> {
        #[cfg(feature = "kps-hdf5")]
        {
            if !self.initialized {
                return Err(
                    KpsWriterError::InvalidData("Writer not initialized".to_string()).into(),
                );
            }

            // Write images
            for (feature, data) in &frame.images {
                let feature_name = feature.strip_prefix("observation.").unwrap_or(feature);

                // Update shape tracking
                if data.width > 0 && data.height > 0 {
                    self.image_shapes.insert(
                        feature_name.to_string(),
                        (data.width as usize, data.height as usize),
                    );
                }

                self.write_image_to_dataset(feature_name, data)?;
                self.images_encoded += 1;
            }

            // Write states
            for (feature, values) in &frame.states {
                let feature_name = feature.strip_prefix("observation.").unwrap_or(feature);

                // Update dimension tracking
                self.state_dims
                    .insert(feature_name.to_string(), values.len());

                self.write_state_to_dataset(feature_name, values)?;
                self.state_records += 1;
            }

            // Write actions
            for (feature, values) in &frame.actions {
                let feature_name = feature.strip_prefix("action.").unwrap_or(feature);

                // Update dimension tracking
                self.state_dims
                    .insert(feature_name.to_string(), values.len());

                self.write_state_to_dataset(feature_name, values)?;
                self.state_records += 1;
            }

            self.frame_count += 1;

            Ok(())
        }

        #[cfg(not(feature = "kps-hdf5"))]
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
        config: &KpsConfig,
        _camera_params: Option<&crate::dataset::kps::camera_params::CameraParamCollector>,
    ) -> Result<WriterStats> {
        #[cfg(feature = "kps-hdf5")]
        {
            // Write all buffered data to HDF5 datasets
            self.write_buffered_data()?;

            // Write metadata files
            self.write_metadata_files(config)?;

            let duration = self
                .start_time
                .map(|t| t.elapsed().as_secs_f64())
                .unwrap_or(0.0);

            Ok(WriterStats {
                frames_written: self.frame_count,
                images_encoded: self.images_encoded,
                state_records: self.state_records,
                output_bytes: 0, // HDF5 doesn't easily report size
                duration_sec: duration,
            })
        }

        #[cfg(not(feature = "kps-hdf5"))]
        {
            let _ = (config, _camera_params);
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

/// Implement DatasetWriter for StreamingHdf5Writer to enable generic trait usage.
impl DatasetWriter for StreamingHdf5Writer {
    fn initialize(&mut self, config: &dyn std::any::Any) -> Result<()> {
        let kps_config = config.downcast_ref::<KpsConfig>().ok_or_else(|| {
            crate::RoboflowError::parse("DatasetWriter", "Expected KpsConfig for HDF5 writer")
        })?;

        // Initialize with empty channels map since DatasetWriter doesn't provide channel info
        KpsWriter::initialize(self, kps_config, &std::collections::HashMap::new())
    }

    fn write_frame(&mut self, frame: &AlignedFrame) -> Result<()> {
        KpsWriter::write_frame(self, frame)
    }

    fn finalize(&mut self, config: &dyn std::any::Any) -> Result<WriterStats> {
        let kps_config = config.downcast_ref::<KpsConfig>().ok_or_else(|| {
            crate::RoboflowError::parse("DatasetWriter", "Expected KpsConfig for HDF5 writer")
        })?;

        KpsWriter::finalize(self, kps_config, None)
    }

    fn frame_count(&self) -> usize {
        KpsWriter::frame_count(self)
    }

    fn is_initialized(&self) -> bool {
        KpsWriter::is_initialized(self)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_create_writer() {
        let temp_dir = std::env::temp_dir();
        let config = KpsConfig {
            dataset: crate::dataset::kps::DatasetConfig {
                name: "test".to_string(),
                fps: 30,
                robot_type: None,
            },
            mappings: vec![],
            output: crate::dataset::kps::OutputConfig::default(),
        };

        let result = StreamingHdf5Writer::create(&temp_dir, 0, &config);
        #[cfg(feature = "kps-hdf5")]
        assert!(result.is_ok());
        #[cfg(not(feature = "kps-hdf5"))]
        assert!(result.is_err());
    }
}
