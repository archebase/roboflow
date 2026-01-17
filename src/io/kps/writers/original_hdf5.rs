//! Original (unaligned) HDF5 writer for proprio_stats_original.hdf5.
//!
//! This writer stores original unaligned message data at original frequencies,
//! preserving the original timestamps from the MCAP file. This is required by
//! the Kps v1.2 specification for data provenance and analysis.
//!
//! ## Structure
//!
//! ```text
//! proprio_stats_original.hdf5:
//! ├── action/
//! │   ├── effector/
//! │   │   ├── position (with original timestamps)
//! │   │   └── velocity
//! │   └── joint/
//! │       ├── position
//! │       └── velocity
//! ├── state/
//! │   ├── effector/
//! │   ├── joint/
//! │   └── end/
//! ├── other_sensors/
//! │   └── imu/
//! ├── metadata/
//! │   ├── original_timestamps (ns)
//! │   └── topics (string array)
//! ```

use std::collections::HashMap;
use std::path::Path;

use crate::io::kps::config::KpsConfig;
use crate::io::kps::writers::base::KpsWriterError;
use crate::io::metadata::ChannelInfo;

/// Original decoded message with timestamp.
///
/// This represents a message before time alignment,
/// preserving the original timestamp from the MCAP file.
#[derive(Debug, Clone)]
pub struct OriginalMessage {
    /// Original timestamp in nanoseconds.
    pub timestamp: u64,

    /// Topic name.
    pub topic: String,

    /// Message data as key-value pairs.
    pub data: Vec<(String, crate::CodecValue)>,
}

impl OriginalMessage {
    /// Create a new original message.
    pub fn new(timestamp: u64, topic: String, data: Vec<(String, crate::CodecValue)>) -> Self {
        Self {
            timestamp,
            topic,
            data,
        }
    }

    /// Extract numeric values as f32 array.
    pub fn extract_float_array(&self) -> Option<Vec<f32>> {
        let mut values = Vec::new();

        for (_key, value) in &self.data {
            match value {
                crate::CodecValue::Float32(n) => values.push(*n),
                crate::CodecValue::Float64(n) => values.push(*n as f32),
                crate::CodecValue::UInt8(n) => values.push(*n as f32),
                crate::CodecValue::UInt16(n) => values.push(*n as f32),
                crate::CodecValue::UInt32(n) => values.push(*n as f32),
                crate::CodecValue::UInt64(n) => values.push(*n as f32),
                crate::CodecValue::Int8(n) => values.push(*n as f32),
                crate::CodecValue::Int16(n) => values.push(*n as f32),
                crate::CodecValue::Int32(n) => values.push(*n as f32),
                crate::CodecValue::Int64(n) => values.push(*n as f32),
                crate::CodecValue::Array(arr) => {
                    for v in arr.iter() {
                        match v {
                            crate::CodecValue::Float32(n) => values.push(*n),
                            crate::CodecValue::Float64(n) => values.push(*n as f32),
                            _ => {}
                        }
                    }
                }
                _ => {}
            }
        }

        if values.is_empty() {
            None
        } else {
            Some(values)
        }
    }
}

/// Original HDF5 writer for proprio_stats_original.hdf5.
///
/// This writer stores data at original frequencies without interpolation or resampling.
pub struct OriginalHdf5Writer {
    /// Episode ID for this writer.
    episode_id: String,

    /// Output directory path.
    output_dir: std::path::PathBuf,

    /// HDF5 file handle.
    #[cfg(feature = "kps-hdf5")]
    hdf5_file: Option<hdf5::File>,

    /// HDF5 datasets by path.
    #[cfg(feature = "kps-hdf5")]
    datasets: HashMap<String, hdf5::Dataset>,

    /// HDF5 groups for quick access.
    #[cfg(feature = "kps-hdf5")]
    groups: HashMap<String, hdf5::Group>,

    /// Per-topic buffers for accumulating data.
    /// Maps topic -> Vec<(timestamp, values)>
    topic_buffers: HashMap<String, Vec<(u64, Vec<f32>)>>,

    /// Per-topic dimension tracking.
    topic_dimensions: HashMap<String, usize>,

    /// Whether initialized.
    initialized: bool,

    /// Number of messages stored.
    message_count: usize,
}

impl OriginalHdf5Writer {
    /// Create a new original HDF5 writer.
    pub fn create(output_dir: impl AsRef<Path>, episode_id: &str) -> Result<Self, KpsWriterError> {
        let output_dir = output_dir.as_ref();
        std::fs::create_dir_all(output_dir)?;

        Ok(Self {
            episode_id: episode_id.to_string(),
            output_dir: output_dir.to_path_buf(),
            #[cfg(feature = "kps-hdf5")]
            hdf5_file: None,
            #[cfg(feature = "kps-hdf5")]
            datasets: HashMap::new(),
            #[cfg(feature = "kps-hdf5")]
            groups: HashMap::new(),
            topic_buffers: HashMap::new(),
            topic_dimensions: HashMap::new(),
            initialized: false,
            message_count: 0,
        })
    }

    /// Initialize the writer with channel info.
    pub fn initialize_with_channels(
        &mut self,
        channels: &HashMap<u16, ChannelInfo>,
    ) -> Result<(), KpsWriterError> {
        #[cfg(feature = "kps-hdf5")]
        {
            use hdf5::Group;

            // Create HDF5 file
            let hdf5_path = self.output_dir.join("proprio_stats_original.hdf5");

            let file =
                hdf5::File::create(&hdf5_path).map_err(|e| KpsWriterError::Hdf5(e.to_string()))?;
            self.hdf5_file = Some(file);

            let hdf5_file = self.hdf5_file.as_ref().unwrap();

            // Create top-level groups
            let action_group = hdf5_file
                .create_group("action")
                .map_err(|e| KpsWriterError::Hdf5(e.to_string()))?;
            let state_group = hdf5_file
                .create_group("state")
                .map_err(|e| KpsWriterError::Hdf5(e.to_string()))?;
            let other_sensors_group = hdf5_file
                .create_group("other_sensors")
                .map_err(|e| KpsWriterError::Hdf5(e.to_string()))?;
            let metadata_group = hdf5_file
                .create_group("metadata")
                .map_err(|e| KpsWriterError::Hdf5(e.to_string()))?;

            self.groups.insert("action".to_string(), action_group);
            self.groups.insert("state".to_string(), state_group);
            self.groups
                .insert("other_sensors".to_string(), other_sensors_group);
            self.groups.insert("metadata".to_string(), metadata_group);

            // Create metadata datasets
            self.create_metadata_datasets(hdf5_file)?;

            // Initialize buffers for all topics
            for channel in channels.values() {
                self.topic_buffers.insert(channel.topic.clone(), Vec::new());
            }

            self.initialized = true;

            tracing::info!(
                path = %hdf5_path.display(),
                "Initialized original HDF5 writer"
            );

            Ok(())
        }

        #[cfg(not(feature = "kps-hdf5"))]
        {
            let _ = channels;
            Err(KpsWriterError::Encoding(
                "HDF5 support not enabled".to_string(),
            ))
        }
    }

    /// Create metadata datasets.
    #[cfg(feature = "kps-hdf5")]
    fn create_metadata_datasets(&mut self, file: &hdf5::File) -> Result<(), KpsWriterError> {
        // Original timestamps dataset
        let ts_dataspace = hdf5::dataspace::Dataspace::rows(0);
        let ts_dataset = file
            .create_dataset(
                "metadata/original_timestamps",
                ts_dataspace,
                &hdf5::dtype::Int64,
            )
            .map_err(|e| KpsWriterError::Hdf5(e.to_string()))?;
        self.datasets
            .insert("metadata/original_timestamps".to_string(), ts_dataset);

        // Topics string array (for reference)
        let topic_dataspace = hdf5::dataspace::Dataspace::rows(0);
        let fixed_string = hdf5::datatype::FixedAscii::new(64);
        let topic_dataset = file
            .create_dataset("metadata/topics", topic_dataspace, &fixed_string)
            .map_err(|e| KpsWriterError::Hdf5(e.to_string()))?;
        self.datasets
            .insert("metadata/topics".to_string(), topic_dataset);

        Ok(())
    }

    /// Add an original message to the buffer.
    ///
    /// Messages are buffered and written to HDF5 on finalize.
    pub fn add_message(&mut self, msg: OriginalMessage) -> Result<(), KpsWriterError> {
        if let Some(values) = msg.extract_float_array() {
            let buffer = self.topic_buffers.entry(msg.topic.clone()).or_default();
            buffer.push((msg.timestamp, values));

            // Update dimension tracking
            let dim = values.len();
            self.topic_dimensions
                .entry(msg.topic.clone())
                .and_modify(|d| *d = (*d).max(dim))
                .or_insert(dim);

            self.message_count += 1;
        }

        Ok(())
    }

    /// Map a topic to a v1.2 HDF5 path.
    ///
    /// /joint_states -> state/joint/position
    /// /effector/command -> action/effector/position
    fn map_topic_to_v12_path(&self, topic: &str) -> String {
        let topic_lower = topic.to_lowercase();

        // Determine if this is action or state
        let (base, data_name) = if topic_lower.contains("command")
            || topic_lower.contains("action")
            || topic_lower.contains("goal")
        {
            ("action", "position")
        } else if topic_lower.contains("velocity") {
            ("state", "velocity")
        } else {
            ("state", "position")
        };

        // Determine joint group
        let joint_group = if topic_lower.contains("effector")
            || topic_lower.contains("gripper")
            || topic_lower.contains("hand")
        {
            "effector"
        } else if topic_lower.contains("end") {
            "end"
        } else {
            "joint"
        };

        format!("{}/{}/{}", base, joint_group, data_name)
    }

    /// Write all buffered data to HDF5.
    #[cfg(feature = "kps-hdf5")]
    fn write_buffered_data(&mut self) -> Result<(), KpsWriterError> {
        use hdf5::types::VarLenArray;

        let file = self.hdf5_file.as_ref().unwrap();

        // Create datasets for each topic with buffered data
        for (topic, buffer) in &self.topic_buffers {
            if buffer.is_empty() {
                continue;
            }

            let v12_path = self.map_topic_to_v12_path(topic);
            let dim = *self
                .topic_dimensions
                .get(topic)
                .unwrap_or(&buffer[0].1.len());

            // Create parent groups if needed
            let path_parts: Vec<&str> = v12_path.split('/').collect();
            if path_parts.len() >= 2 {
                let mut current_path = String::new();
                for (i, part) in path_parts.iter().enumerate() {
                    if i == path_parts.len() - 1 {
                        break;
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
            }

            // Create dataset with proper dimension
            let dataspace = hdf5::dataspace::Dataspace::rows(buffer.len());
            let dtype = VarLenArray::<f32>::new(dim);
            let dataset = file
                .create_dataset(&v12_path, dataspace, &dtype)
                .map_err(|e| KpsWriterError::Hdf5(e.to_string()))?;

            // Write all values
            let values: Vec<f32> = buffer.iter().flat_map(|(_, v)| v.iter().copied()).collect();
            dataset
                .write(&values)
                .map_err(|e| KpsWriterError::Hdf5(e.to_string()))?;

            self.datasets.insert(v12_path.clone(), dataset);
        }

        // Write original timestamps
        let all_timestamps: Vec<i64> = self
            .topic_buffers
            .values()
            .flat_map(|buf| buf.iter().map(|(ts, _)| *ts as i64))
            .collect();

        if let Some(dataset) = self.datasets.get("metadata/original_timestamps") {
            dataset
                .write(&all_timestamps)
                .map_err(|e| KpsWriterError::Hdf5(e.to_string()))?;
        }

        Ok(())
    }

    /// Finalize and write all data.
    pub fn finalize(&mut self) -> Result<usize, KpsWriterError> {
        #[cfg(feature = "kps-hdf5")]
        {
            if !self.initialized {
                return Err(KpsWriterError::InvalidData(
                    "Writer not initialized".to_string(),
                ));
            }

            // Write all buffered data
            self.write_buffered_data()?;

            // Close HDF5 file
            self.hdf5_file = None;

            tracing::info!(
                messages = self.message_count,
                topics = self.topic_buffers.len(),
                "Finalized original HDF5 writer"
            );

            Ok(self.message_count)
        }

        #[cfg(not(feature = "kps-hdf5"))]
        {
            Err(KpsWriterError::Encoding(
                "HDF5 support not enabled".to_string(),
            ))
        }
    }

    /// Get the number of messages buffered.
    pub fn message_count(&self) -> usize {
        self.message_count
    }

    /// Check if initialized.
    pub fn is_initialized(&self) -> bool {
        self.initialized
    }
}

/// Factory for creating original HDF5 writers.
pub struct OriginalHdf5WriterFactory;

impl OriginalHdf5WriterFactory {
    /// Create a new original HDF5 writer.
    pub fn create(
        output_dir: impl AsRef<Path>,
        episode_id: &str,
    ) -> Result<OriginalHdf5Writer, KpsWriterError> {
        OriginalHdf5Writer::create(output_dir, episode_id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::CodecValue;

    #[test]
    fn test_original_message_extract_float_array() {
        let msg = OriginalMessage {
            timestamp: 1000,
            topic: "/joint_states".to_string(),
            data: vec![(
                "position".to_string(),
                CodecValue::Array(vec![
                    CodecValue::Float32(1.0),
                    CodecValue::Float32(2.0),
                    CodecValue::Float32(3.0),
                ]),
            )],
        };

        let values = msg.extract_float_array().unwrap();
        assert_eq!(values, vec![1.0, 2.0, 3.0]);
    }

    #[test]
    fn test_map_topic_to_v12_path() {
        let writer = OriginalHdf5Writer {
            episode_id: "test".to_string(),
            output_dir: std::path::PathBuf::from("/tmp"),
            #[cfg(feature = "kps-hdf5")]
            hdf5_file: None,
            #[cfg(feature = "kps-hdf5")]
            datasets: HashMap::new(),
            #[cfg(feature = "kps-hdf5")]
            groups: HashMap::new(),
            topic_buffers: HashMap::new(),
            topic_dimensions: HashMap::new(),
            initialized: false,
            message_count: 0,
        };

        assert_eq!(
            writer.map_topic_to_v12_path("/joint_states"),
            "state/joint/position"
        );
        assert_eq!(
            writer.map_topic_to_v12_path("/effector/command"),
            "action/effector/position"
        );
        assert_eq!(
            writer.map_topic_to_v12_path("/joint_velocities"),
            "state/joint/velocity"
        );
        assert_eq!(
            writer.map_topic_to_v12_path("/gripper/action"),
            "action/effector/position"
        );
    }

    #[test]
    fn test_add_message() {
        let mut writer = OriginalHdf5Writer {
            episode_id: "test".to_string(),
            output_dir: std::path::PathBuf::from("/tmp"),
            #[cfg(feature = "kps-hdf5")]
            hdf5_file: None,
            #[cfg(feature = "kps-hdf5")]
            datasets: HashMap::new(),
            #[cfg(feature = "kps-hdf5")]
            groups: HashMap::new(),
            topic_buffers: HashMap::new(),
            topic_dimensions: HashMap::new(),
            initialized: false,
            message_count: 0,
        };

        let msg = OriginalMessage {
            timestamp: 1000,
            topic: "/joint_states".to_string(),
            data: vec![(
                "position".to_string(),
                CodecValue::Array(vec![CodecValue::Float32(1.0), CodecValue::Float32(2.0)]),
            )],
        };

        writer.add_message(msg).unwrap();
        assert_eq!(writer.message_count(), 1);
        assert!(writer.topic_buffers.contains_key("/joint_states"));
    }
}
