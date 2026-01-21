//! Kps HDF5 format writer.
//!
//! Writes Kps datasets in the legacy HDF5 format, where each episode
//! is a single HDF5 file containing all observations and actions.

use std::collections::HashMap;
use std::path::Path;

use super::config::KpsConfig;

/// Buffered image data for HDF5 writing.
#[derive(Debug, Clone)]
#[cfg(feature = "kps-hdf5")]
struct BufferedImageData {
    data: Vec<u8>,
}

/// HDF5 Kps dataset writer.
///
/// Creates HDF5 files compatible with Kps's legacy format.
pub struct Hdf5KpsWriter {
    episode_id: usize,
    output_dir: std::path::PathBuf,
    frame_count: usize,
    image_shapes: HashMap<String, (usize, usize)>,
    state_shapes: HashMap<String, usize>,
    /// Buffers for data collection (only used with kps-hdf5 feature)
    #[cfg(feature = "kps-hdf5")]
    image_buffers: HashMap<String, Vec<BufferedImageData>>,
    #[cfg(feature = "kps-hdf5")]
    state_buffers: HashMap<String, Vec<Vec<f32>>>,
    #[cfg(feature = "kps-hdf5")]
    action_buffers: HashMap<String, Vec<Vec<f32>>>,
}

impl Hdf5KpsWriter {
    /// Create a new HDF5 writer for an episode.
    ///
    /// Creates the output directory and initializes the HDF5 file.
    pub fn create(
        output_dir: impl AsRef<Path>,
        episode_id: usize,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        let output_dir = output_dir.as_ref();
        std::fs::create_dir_all(output_dir)?;

        #[cfg(feature = "kps-hdf5")]
        let (image_buffers, state_buffers, action_buffers) =
            (HashMap::new(), HashMap::new(), HashMap::new());

        Ok(Self {
            episode_id,
            output_dir: output_dir.to_path_buf(),
            frame_count: 0,
            image_shapes: HashMap::new(),
            state_shapes: HashMap::new(),
            #[cfg(feature = "kps-hdf5")]
            image_buffers,
            #[cfg(feature = "kps-hdf5")]
            state_buffers,
            #[cfg(feature = "kps-hdf5")]
            action_buffers,
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
        config: &KpsConfig,
    ) -> Result<usize, Box<dyn std::error::Error>> {
        #[cfg(feature = "kps-hdf5")]
        {
            self.write_from_mcap_impl(mcap_path, config)
        }
        #[cfg(not(feature = "kps-hdf5"))]
        {
            // Note: parameters are unused when feature is disabled
            let _ = (mcap_path, config);
            Err("HDF5 support not enabled. Add feature 'kps-hdf5' to Cargo.toml".into())
        }
    }

    #[cfg(feature = "kps-hdf5")]
    fn write_from_mcap_impl(
        &mut self,
        mcap_path: impl AsRef<Path>,
        config: &KpsConfig,
    ) -> Result<usize, Box<dyn std::error::Error>> {
        use hdf5::File as Hdf5File;

        let mcap_path_ref = mcap_path.as_ref();

        println!("Converting MCAP to Kps HDF5 format");
        println!("  Input: {}", mcap_path_ref.display());
        println!("  Output: {}", self.output_dir.display());

        // Open MCAP file
        let reader = crate::RoboReader::open(mcap_path_ref)?;

        // First pass: collect all data into buffers
        let mut frame_index = 0usize;

        for result in reader.decode_messages()? {
            let (msg, channel_info) = result?;

            // Find matching mapping
            let mapping = config
                .mappings
                .iter()
                .find(|m| channel_info.topic == m.topic || channel_info.topic.contains(&m.topic));

            let Some(mapping) = mapping else {
                continue;
            };

            // Buffer data based on type
            match &mapping.mapping_type {
                crate::format::kps::config::MappingType::Image => {
                    self.buffer_image_data(mapping, &msg)?;
                }
                crate::format::kps::config::MappingType::State
                | crate::format::kps::config::MappingType::Action => {
                    self.buffer_state_data(
                        mapping,
                        &msg,
                        matches!(
                            mapping.mapping_type,
                            crate::format::kps::config::MappingType::Action
                        ),
                    )?;
                }
                _ => {}
            }

            frame_index += 1;
            if frame_index.is_multiple_of(100) {
                println!("  Processed {} frames...", frame_index);
            }
        }

        self.frame_count = frame_index;

        // Create HDF5 file and write all buffered data
        let hdf5_path = self
            .output_dir
            .join(format!("episode_{:06}.hdf5", self.episode_id));
        let hdf5_file = Hdf5File::create(&hdf5_path)?;

        println!("  Created HDF5 file: {}", hdf5_path.display());

        // Create groups
        let obs_group = hdf5_file.create_group("observations")?;
        let action_group = hdf5_file.create_group("actions")?;
        let metadata_group = hdf5_file.create_group("metadata")?;

        // Write all buffered data to HDF5
        self.write_buffered_images(&obs_group)?;
        self.write_buffered_states(&obs_group, false)?;
        self.write_buffered_states(&action_group, true)?;

        // Write metadata
        self.write_metadata(&metadata_group, config, frame_index)?;

        println!("  Wrote {} frames", self.frame_count);

        Ok(self.frame_count)
    }

    /// Buffer image data for later writing.
    #[cfg(feature = "kps-hdf5")]
    fn buffer_image_data(
        &mut self,
        mapping: &crate::format::kps::config::Mapping,
        msg: &crate::DecodedMessage,
    ) -> Result<(), Box<dyn std::error::Error>> {
        use crate::CodecValue;

        let mut width = 0u32;
        let mut height = 0u32;
        let mut image_data: Option<Vec<u8>> = None;

        for (key, value) in msg.iter() {
            match key.as_str() {
                "width" => {
                    if let CodecValue::UInt32(w) = value {
                        width = *w;
                    }
                }
                "height" => {
                    if let CodecValue::UInt32(h) = value {
                        height = *h;
                    }
                }
                "data" => {
                    if let CodecValue::Bytes(bytes) = value {
                        image_data = Some(bytes.clone());
                    }
                }
                _ => {}
            }
        }

        let Some(data) = image_data else {
            return Ok(());
        };

        // Record image shape
        if width > 0 && height > 0 {
            self.record_image_shape(mapping.topic.clone(), width as usize, height as usize);
        }

        // Buffer the image data
        let sanitized_name = mapping.topic.replace('/', "_");
        self.image_buffers
            .entry(sanitized_name)
            .or_default()
            .push(BufferedImageData { data });

        Ok(())
    }

    /// Buffer state data for later writing.
    #[cfg(feature = "kps-hdf5")]
    fn buffer_state_data(
        &mut self,
        mapping: &crate::format::kps::config::Mapping,
        msg: &crate::DecodedMessage,
        is_action: bool,
    ) -> Result<(), Box<dyn std::error::Error>> {
        use crate::CodecValue;

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
                    // Skip complex nested types
                }
                _ => {}
            }
        }

        if values.is_empty() {
            return Ok(());
        }

        self.record_state_dimension(mapping.topic.clone(), values.len());

        // Buffer the state data
        let sanitized_name = mapping.topic.replace('/', "_");
        if is_action {
            self.action_buffers
                .entry(sanitized_name)
                .or_default()
                .push(values);
        } else {
            self.state_buffers
                .entry(sanitized_name)
                .or_default()
                .push(values);
        }

        Ok(())
    }

    /// Write buffered image data to HDF5.
    #[cfg(feature = "kps-hdf5")]
    fn write_buffered_images(
        &mut self,
        group: &hdf5::Group,
    ) -> Result<(), Box<dyn std::error::Error>> {
        use hdf5::types::VarLenArray;

        for (name, images) in &self.image_buffers {
            if images.is_empty() {
                continue;
            }

            // Create dataset with variable-length arrays
            let dataset = group
                .new_dataset::<VarLenArray<u8>>()
                .shape(images.len())
                .create(&**name)
                .map_err(|e| crate::io::kps::writers::base::KpsWriterError::Hdf5(e.to_string()))?;

            // Convert images to VarLenArray and write
            let varlen_images: Vec<VarLenArray<u8>> = images
                .iter()
                .map(|img| VarLenArray::from_slice(&img.data))
                .collect();

            dataset.write(&varlen_images)?;
        }

        Ok(())
    }

    /// Write buffered state data to HDF5.
    #[cfg(feature = "kps-hdf5")]
    fn write_buffered_states(
        &mut self,
        group: &hdf5::Group,
        is_action: bool,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let buffers = if is_action {
            &self.action_buffers
        } else {
            &self.state_buffers
        };

        for (name, states) in buffers {
            if states.is_empty() {
                continue;
            }

            // Determine the dimension (all states should have the same dimension)
            let dim = states.first().map(|s| s.len()).unwrap_or(0);
            if dim == 0 {
                continue;
            }

            // Create 2D dataset [num_frames, dim]
            let dataset = group
                .new_dataset::<f32>()
                .shape([states.len(), dim])
                .create(&**name)
                .map_err(|e| crate::io::kps::writers::base::KpsWriterError::Hdf5(e.to_string()))?;

            // Flatten data into 1D array for HDF5
            let flat_data: Vec<f32> = states.iter().flatten().copied().collect();
            dataset.write(&flat_data)?;
        }

        Ok(())
    }

    #[cfg(feature = "kps-hdf5")]
    fn write_metadata(
        &self,
        group: &hdf5::Group,
        config: &KpsConfig,
        frame_count: usize,
    ) -> Result<(), Box<dyn std::error::Error>> {
        // Write episode metadata as attributes on the group
        // This avoids issues with the HDF5 dataset builder API
        let metadata: Vec<(&str, String)> = vec![
            ("dataset_name", config.dataset.name.clone()),
            ("fps", config.dataset.fps.to_string()),
            ("total_frames", frame_count.to_string()),
        ];

        for (key, value) in metadata {
            // Use the low-level attribute API
            group
                .attr(key)?
                .write(&value)
                .map_err(|e| crate::io::kps::writers::base::KpsWriterError::Hdf5(e.to_string()))?;
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
    pub fn finish(self, config: &KpsConfig) -> Result<(), Box<dyn std::error::Error>> {
        use super::info;

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
        println!("Kps HDF5 dataset created: {}", self.output_dir.display());

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_create_writer() {
        let temp_dir = std::env::temp_dir();
        let writer = Hdf5KpsWriter::create(&temp_dir, 0);
        assert!(writer.is_ok());
    }
}
