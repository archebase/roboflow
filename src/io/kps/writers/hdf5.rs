//! Streaming HDF5 writer for Kps datasets.
//!
//! This writer implements the [`KpsWriter`] trait for HDF5 format,
//! supporting frame-by-frame writing for pipeline integration.

use std::collections::HashMap;
use std::path::Path;

use crate::io::kps::config::KpsConfig;
use crate::io::kps::writers::base::{
    AlignedFrame, ImageData, KpsWriter, KpsWriterError, WriterStats,
};
use crate::io::metadata::ChannelInfo;

/// Streaming HDF5 writer for Kps datasets.
///
/// This writer supports frame-by-frame writing for pipeline integration.
/// It maintains HDF5 dataset handles and writes data incrementally.
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
}

impl StreamingHdf5Writer {
    /// Create a new HDF5 writer for the specified output directory.
    pub fn create(
        output_dir: impl AsRef<Path>,
        episode_id: usize,
        config: &KpsConfig,
    ) -> Result<Self, KpsWriterError> {
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
        })
    }

    /// Create HDF5 datasets for a feature based on shape information.
    #[cfg(feature = "kps-hdf5")]
    fn create_dataset_for_feature(
        &mut self,
        feature: &str,
        group: &hdf5::Group,
        is_image: bool,
        shape: Option<&[usize]>,
    ) -> Result<(), KpsWriterError> {
        use hdf5::types::{FixedOpaque, Float32, VarLenArray};

        if is_image {
            // For images, create an opaque dataset
            let dataspace = hdf5::dataspace::Dataspace::resizeable();
            let dataset = group
                .create_dataset(feature, dataspace, &hdf5::datatype::DataType::Opaque(0))
                .map_err(|e| KpsWriterError::Hdf5(e.to_string()))?;
            self.image_datasets.insert(feature.to_string(), dataset);
        } else {
            // For state/action, create a float32 array dataset
            let dataspace = hdf5::dataspace::Dataspace::rows(0);
            let dtype = if let Some(dim) = shape {
                // Fixed-size array
                VarLenArray::new(*dim)
            } else {
                // Variable size
                Float32
            };
            let dataset = group
                .create_dataset(feature, dataspace, &dtype)
                .map_err(|e| KpsWriterError::Hdf5(e.to_string()))?;
            self.state_datasets.insert(feature.to_string(), dataset);
        }

        Ok(())
    }

    /// Write image data to an HDF5 dataset.
    #[cfg(feature = "kps-hdf5")]
    fn write_image_to_dataset(
        &mut self,
        feature: &str,
        data: &ImageData,
    ) -> Result<(), KpsWriterError> {
        use hdf5::types::FixedOpaque;

        if let Some(dataset) = self.image_datasets.get(feature) {
            let opaque_data = FixedOpaque::new(&data.data);
            dataset
                .write(&opaque_data)
                .map_err(|e| KpsWriterError::Hdf5(e.to_string()))?;
            Ok(())
        } else {
            Err(KpsWriterError::FeatureNotMapped(feature.to_string()))
        }
    }

    /// Write state data to an HDF5 dataset.
    #[cfg(feature = "kps-hdf5")]
    fn write_state_to_dataset(
        &mut self,
        feature: &str,
        values: &[f32],
    ) -> Result<(), KpsWriterError> {
        if let Some(dataset) = self.state_datasets.get(feature) {
            dataset
                .write(values)
                .map_err(|e| KpsWriterError::Hdf5(e.to_string()))?;
            Ok(())
        } else if let Some(dataset) = self.action_datasets.get(feature) {
            dataset
                .write(values)
                .map_err(|e| KpsWriterError::Hdf5(e.to_string()))?;
            Ok(())
        } else {
            Err(KpsWriterError::FeatureNotMapped(feature.to_string()))
        }
    }

    /// Write metadata files (info.json, episode.jsonl).
    fn write_metadata_files(&self, config: &KpsConfig) -> Result<(), KpsWriterError> {
        use crate::io::kps::info;

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
    ) -> Result<(), KpsWriterError> {
        #[cfg(feature = "kps-hdf5")]
        {
            use hdf5::Group;

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
            let obs_group = self.obs_group.as_ref().unwrap();
            let action_group = self.action_group.as_ref().unwrap();

            for mapping in &config.mappings {
                let feature_name = mapping
                    .feature
                    .strip_prefix("observation.")
                    .or_else(|| mapping.feature.strip_prefix("action."))
                    .unwrap_or(&mapping.feature);

                let is_observation = mapping.feature.starts_with("observation.");
                let group = if is_observation {
                    obs_group
                } else {
                    action_group
                };
                let is_image = matches!(mapping.mapping_type, crate::io::kps::MappingType::Image);

                self.create_dataset_for_feature(feature_name, group, is_image, None)?;
            }

            self.initialized = true;
            self.start_time = Some(std::time::Instant::now());

            Ok(())
        }

        #[cfg(not(feature = "kps-hdf5"))]
        {
            let _ = (config, channels);
            Err(KpsWriterError::Encoding(
                "HDF5 support not enabled".to_string(),
            ))
        }
    }

    fn write_frame(&mut self, frame: &AlignedFrame) -> Result<(), KpsWriterError> {
        #[cfg(feature = "kps-hdf5")]
        {
            if !self.initialized {
                return Err(KpsWriterError::InvalidData(
                    "Writer not initialized".to_string(),
                ));
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
            Err(KpsWriterError::Encoding(
                "HDF5 support not enabled".to_string(),
            ))
        }
    }

    fn finalize(
        &mut self,
        config: &KpsConfig,
        _camera_params: Option<&crate::io::kps::camera_params::CameraParamCollector>,
    ) -> Result<WriterStats, KpsWriterError> {
        #[cfg(feature = "kps-hdf5")]
        {
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
            Err(KpsWriterError::Encoding(
                "HDF5 support not enabled".to_string(),
            ))
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

    #[test]
    fn test_create_writer() {
        let temp_dir = std::env::temp_dir();
        let config = KpsConfig {
            dataset: crate::io::kps::DatasetConfig {
                name: "test".to_string(),
                fps: 30,
                robot_type: None,
            },
            mappings: vec![],
            output: crate::io::kps::OutputConfig::default(),
        };

        let result = StreamingHdf5Writer::create(&temp_dir, 0, &config);
        #[cfg(feature = "kps-hdf5")]
        assert!(result.is_ok());
        #[cfg(not(feature = "kps-hdf5"))]
        assert!(result.is_err());
    }
}
