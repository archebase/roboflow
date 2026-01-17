//! LeRobot HDF5 format writer.
//!
//! Writes LeRobot datasets in the legacy HDF5 format, where each episode
//! is a single HDF5 file containing all observations and actions.

use std::collections::HashMap;
use std::path::Path;

use crate::format::lerobot::config::LeRobotConfig;

/// HDF5 LeRobot dataset writer.
///
/// Creates HDF5 files compatible with LeRobot's legacy format.
pub struct Hdf5LeRobotWriter {
    episode_id: usize,
    output_dir: std::path::PathBuf,
    frame_count: usize,
    image_shapes: HashMap<String, (usize, usize)>,
    state_shapes: HashMap<String, usize>,
}

impl Hdf5LeRobotWriter {
    /// Create a new HDF5 writer for an episode.
    ///
    /// Creates the output directory and initializes the HDF5 file.
    pub fn create(
        output_dir: impl AsRef<Path>,
        episode_id: usize,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        let output_dir = output_dir.as_ref();
        std::fs::create_dir_all(output_dir)?;

        Ok(Self {
            episode_id,
            output_dir: output_dir.to_path_buf(),
            frame_count: 0,
            image_shapes: HashMap::new(),
            state_shapes: HashMap::new(),
        })
    }

    /// Record the shape of an image topic.
    pub fn record_image_shape(&mut self, topic: String, width: usize, height: usize) {
        self.image_shapes.insert(topic, (width, height));
    }

    /// Record the dimension of a state topic.
    pub fn record_state_dimension(&mut self, topic: String, dim: usize) {
        self.state_shapes.insert(topic, dim);
    }

    /// Write the complete HDF5 file from MCAP data.
    ///
    /// This method processes all messages and writes them to the HDF5 file.
    /// Returns the number of frames written.
    pub fn write_from_mcap(
        &mut self,
        mcap_path: impl AsRef<Path>,
        config: &LeRobotConfig,
    ) -> Result<usize, Box<dyn std::error::Error>> {
        #[cfg(feature = "lerobot-hdf5")]
        {
            self.write_from_mcap_impl(mcap_path, config)
        }
        #[cfg(not(feature = "lerobot-hdf5"))]
        {
            // Note: parameters are unused when feature is disabled
            let _ = (mcap_path, config);
            Err("HDF5 support not enabled. Add feature 'lerobot-hdf5' to Cargo.toml".into())
        }
    }

    #[cfg(feature = "lerobot-hdf5")]
    fn write_from_mcap_impl(
        &mut self,
        mcap_path: impl AsRef<Path>,
        config: &LeRobotConfig,
    ) -> Result<usize, Box<dyn std::error::Error>> {
        use crate::format::lerobot::config::{Mapping, MappingType};
        use hdf5::File as Hdf5File;
        use hdf5::Group;

        let mcap_path_ref = mcap_path.as_ref();

        println!("Converting MCAP to LeRobot HDF5 format");
        println!("  Input: {}", mcap_path_ref.display());
        println!("  Output: {}", self.output_dir.display());

        // Open MCAP file
        let reader = crate::RoboReader::open(mcap_path_ref)?;

        // Create HDF5 file
        let hdf5_path = self
            .output_dir
            .join(format!("episode_{:06}.hdf5", self.episode_id));
        let hdf5_file = Hdf5File::create(&hdf5_path)?;

        println!("  Created HDF5 file: {}", hdf5_path.display());

        // Create groups
        let obs_group = hdf5_file.create_group("observations")?;
        let action_group = hdf5_file.create_group("actions")?;
        let metadata_group = hdf5_file.create_group("metadata")?;

        // Track datasets and buffers
        let mut image_datasets: HashMap<String, hdf5::Dataset> = HashMap::new();
        let mut state_datasets: HashMap<String, hdf5::Dataset> = HashMap::new();
        let mut action_datasets: HashMap<String, hdf5::Dataset> = HashMap::new();

        let mut max_frames = 0usize;
        let mut frame_index = 0usize;

        // Process messages
        let iter = reader.decode_messages()?;
        for result in iter {
            let (msg, channel_info) = result?;

            // Find matching mapping
            let mapping = config
                .mappings
                .iter()
                .find(|m| channel_info.topic == m.topic || channel_info.topic.contains(&m.topic));

            let Some(mapping) = mapping else {
                continue;
            };

            // Write based on type
            match &mapping.mapping_type {
                MappingType::Image => {
                    self.write_image_to_hdf5(
                        &hdf5_file,
                        &obs_group,
                        mapping,
                        &msg,
                        &mut image_datasets,
                    )?;
                }
                MappingType::State | MappingType::Action => {
                    self.write_state_to_hdf5(
                        &hdf5_file,
                        if matches!(mapping.mapping_type, MappingType::Action) {
                            &action_group
                        } else {
                            &obs_group
                        },
                        mapping,
                        &msg,
                        if matches!(mapping.mapping_type, MappingType::Action) {
                            &mut action_datasets
                        } else {
                            &mut state_datasets
                        },
                    )?;
                }
                _ => {}
            }

            frame_index += 1;
            if frame_index % 100 == 0 {
                println!("  Processed {} frames...", frame_index);
            }
        }

        self.frame_count = frame_index;
        max_frames = frame_index;

        // Write metadata
        self.write_metadata(&metadata_group, config, max_frames)?;

        println!("  Wrote {} frames", self.frame_count);

        Ok(self.frame_count)
    }

    #[cfg(feature = "lerobot-hdf5")]
    fn write_image_to_hdf5(
        &mut self,
        _file: &hdf5::File,
        obs_group: &hdf5::Group,
        mapping: &Mapping,
        msg: &crate::DecodedMessage,
        datasets: &mut HashMap<String, hdf5::Dataset>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        use crate::CodecValue;
        use hdf5::types::{FixedOpaque, VarLenOpaque};

        // Extract image data
        let mut width = 0u32;
        let mut height = 0u32;
        let mut data: Option<&[u8]> = None;

        for (key, value) in msg.iter() {
            match key.as_str() {
                "width" => {
                    if let CodecValue::UInt32(w) = value {
                        width = *w;
                        self.record_image_shape(
                            mapping.topic.clone(),
                            width as usize,
                            height as usize,
                        );
                    }
                }
                "height" => {
                    if let CodecValue::UInt32(h) = value {
                        height = *h;
                        self.record_image_shape(
                            mapping.topic.clone(),
                            width as usize,
                            height as usize,
                        );
                    }
                }
                "data" => {
                    if let CodecValue::Bytes(b) = value {
                        data = Some(b);
                    }
                }
                _ => {}
            }
        }

        let Some(image_data) = data else {
            return Ok(()); // Skip if no image data
        };

        // Parse feature path (e.g., "observation.camera_0" -> "images/camera_0")
        let feature_name = mapping
            .feature
            .strip_prefix("observation.")
            .unwrap_or(&mapping.feature);
        let dataset_name = format!("images/{}", feature_name);

        // Create or get dataset
        let dataset = if !datasets.contains_key(&dataset_name) {
            let dataspace = hdf5::dataspace::Dataspace::resizeable();
            let dataset = obs_group.create_dataset(
                &dataset_name,
                dataspace,
                &hdf5::datatype::DataType::Opaque(0),
            )?;
            datasets.insert(dataset_name.clone(), dataset);
            dataset
        } else {
            datasets.get(&dataset_name).unwrap()
        };

        // Write image data as opaque
        let opaque_data = FixedOpaque::new(image_data);
        dataset.write(&opaque_data)?;

        Ok(())
    }

    #[cfg(feature = "lerobot-hdf5")]
    fn write_state_to_hdf5(
        &mut self,
        _file: &hdf5::File,
        group: &hdf5::Group,
        mapping: &Mapping,
        msg: &crate::DecodedMessage,
        datasets: &mut HashMap<String, hdf5::Dataset>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        use crate::CodecValue;

        // Extract numeric array data
        let mut values: Vec<f32> = Vec::new();

        for (_key, value) in msg.iter() {
            match value {
                CodecValue::UInt8(n) => values.push(*n as f32),
                CodecValue::UInt16(n) => values.push(*n as f32),
                CodecValue::UInt32(n) => values.push(*n as f32),
                CodecValue::UInt64(n) => values.push(*n as f32),
                CodecValue::Int8(n) => values.push(*n as f32),
                CodecValue::Int16(n) => values.push(*n as f32),
                CodecValue::Int32(n) => values.push(*n as f32),
                CodecValue::Int64(n) => values.push(*n as f32),
                CodecValue::Float32(n) => values.push(*n),
                CodecValue::Float64(n) => values.push(*n as f32),
                CodecValue::Array(_) => {
                    // Arrays might need special handling
                    // For now, skip complex nested types
                }
                _ => {}
            }
        }

        if values.is_empty() {
            return Ok(());
        }

        self.record_state_dimension(mapping.topic.clone(), values.len());

        // Parse feature path
        let feature_name = mapping
            .feature
            .strip_prefix("observation.")
            .or_else(|| mapping.feature.strip_prefix("action."))
            .unwrap_or(&mapping.feature);

        // Create or get dataset
        let dataset = if !datasets.contains_key(feature_name) {
            let dataspace = hdf5::dataspace::Dataspace::resizeable();
            let dataset = group.create_dataset(
                feature_name,
                dataspace,
                &hdf5::datatype::DataType::Float32,
            )?;
            datasets.insert(feature_name.to_string(), dataset);
            dataset
        } else {
            datasets.get(feature_name).unwrap()
        };

        // Write the array
        dataset.write(&values)?;

        Ok(())
    }

    #[cfg(feature = "lerobot-hdf5")]
    fn write_metadata(
        &self,
        group: &hdf5::Group,
        config: &LeRobotConfig,
        frame_count: usize,
    ) -> Result<(), Box<dyn std::error::Error>> {
        // Write episode metadata
        use hdf5::types::VarLenUnicode;

        let metadata = vec![
            ("dataset_name", config.dataset.name.as_str()),
            ("fps", config.fps.to_string().as_str()),
            ("total_frames", frame_count.to_string().as_str()),
        ];

        for (key, value) in metadata {
            let dataspace = hdf5::dataspace::Dataspace::scalar();
            let dataset =
                group.create_dataset(key, dataspace, &hdf5::datatype::DataType::VarLenUnicode)?;
            dataset.write(&VarLenUnicode::new(value))?;
        }

        Ok(())
    }

    /// Get the output directory path.
    pub fn output_dir(&self) -> &Path {
        &self.output_dir
    }

    /// Get the number of frames written.
    pub fn frame_count(&self) -> usize {
        self.frame_count
    }

    /// Get recorded image shapes.
    pub fn image_shapes(&self) -> &HashMap<String, (usize, usize)> {
        &self.image_shapes
    }

    /// Get recorded state shapes.
    pub fn state_shapes(&self) -> &HashMap<String, usize> {
        &self.state_shapes
    }

    /// Finalize and close the writer.
    ///
    /// Writes metadata files (info.json, episode.jsonl).
    pub fn finish(self, config: &LeRobotConfig) -> Result<(), Box<dyn std::error::Error>> {
        use crate::format::lerobot::info;

        // Write info.json
        info::write_info_json(
            &self.output_dir,
            config,
            self.frame_count as u64,
            &self.image_shapes,
            &self.state_shapes,
        )?;

        // Write episode.jsonl
        info::write_episode_json(
            &self.output_dir,
            self.episode_id,
            0,
            self.frame_count as u64 * 1_000_000_000 / config.dataset.fps as u64,
            self.frame_count,
        )?;

        println!();
        println!(
            "LeRobot HDF5 dataset created: {}",
            self.output_dir.display()
        );

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_create_writer() {
        let temp_dir = std::env::temp_dir();
        let writer = Hdf5LeRobotWriter::create(&temp_dir, 0);
        assert!(writer.is_ok());
    }
}
