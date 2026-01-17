//! Format writer abstraction and implementations.
//!
//! This module provides a unified interface for writing different robotics data formats
//! (MCAP, ROS1 bag, etc.) through the `FormatWriter` trait.

use std::collections::{BTreeMap, HashMap};
use std::fs::File;
use std::io::BufWriter;
use std::path::Path;

use crate::core::{CodecError, Result};
use crate::format::bag::writer::BagWriter;

/// Trait for writing robotics data to different file formats.
///
/// This trait abstracts over format-specific writers (MCAP, ROS1 bag, etc.)
/// to provide a unified API.
pub trait FormatWriter: Send {
    /// Add a channel/topic to the file.
    ///
    /// # Arguments
    ///
    /// * `topic` - Topic name (e.g., "/chatter")
    /// * `msg_type` - Message type name (e.g., "std_msgs/String")
    /// * `schema` - Schema definition text
    fn add_channel(&mut self, topic: &str, msg_type: &str, schema: &str) -> Result<()>;

    /// Write a message to a channel.
    ///
    /// # Arguments
    ///
    /// * `topic` - Topic name (must have been added via `add_channel`)
    /// * `data` - Encoded message data
    /// * `time_ns` - Timestamp in nanoseconds
    fn write_message(&mut self, topic: &str, data: &[u8], time_ns: u64) -> Result<()>;

    /// Finalize and close the file.
    fn finish(&mut self) -> Result<()>;

    /// Get as `Any` for downcasting to concrete types.
    fn as_any(&self) -> &dyn std::any::Any;

    /// Get as `Any` mutable for downcasting to concrete types.
    fn as_any_mut(&mut self) -> &mut dyn std::any::Any;
}

// =============================================================================
// MCAP Format Writer
// =============================================================================

/// MCAP format writer implementation.
pub struct McapFormatWriter {
    /// MCAP writer
    writer: mcap::Writer<BufWriter<File>>,
    /// Schema ID indexed by message type name
    schema_ids: HashMap<String, u16>,
    /// Channel ID indexed by topic name
    channel_ids: HashMap<String, u16>,
    /// Sequence number indexed by channel ID
    sequences: HashMap<u16, u32>,
}

impl McapFormatWriter {
    /// Create a new MCAP writer.
    pub fn create<P: AsRef<Path>>(path: P) -> Result<Self> {
        let file = File::create(&path).map_err(|e| {
            CodecError::encode("McapFormatWriter", format!("Failed to create file: {e}"))
        })?;

        let writer = mcap::Writer::new(BufWriter::new(file)).map_err(|e| {
            CodecError::encode(
                "McapFormatWriter",
                format!("Failed to create MCAP writer: {e}"),
            )
        })?;

        Ok(Self {
            writer,
            schema_ids: HashMap::new(),
            channel_ids: HashMap::new(),
            sequences: HashMap::new(),
        })
    }
}

impl FormatWriter for McapFormatWriter {
    fn add_channel(&mut self, topic: &str, msg_type: &str, schema: &str) -> Result<()> {
        // Return early if channel already exists
        if self.channel_ids.contains_key(topic) {
            return Ok(());
        }

        // Add schema if not exists
        let schema_id = *self
            .schema_ids
            .entry(msg_type.to_string())
            .or_insert_with(|| {
                self.writer
                    .add_schema(msg_type, "ros2msg", schema.as_bytes())
                    .unwrap_or(0) // Use schema_id 0 if add_schema fails
            });

        // Add channel
        let channel_id = self
            .writer
            .add_channel(schema_id, topic, "cdr", &BTreeMap::new())
            .map_err(|e| {
                CodecError::encode("McapFormatWriter", format!("Failed to add channel: {e}"))
            })?;

        self.channel_ids.insert(topic.to_string(), channel_id);
        Ok(())
    }

    fn write_message(&mut self, topic: &str, data: &[u8], time_ns: u64) -> Result<()> {
        let channel_id = *self.channel_ids.get(topic).ok_or_else(|| {
            CodecError::encode(
                "McapFormatWriter",
                format!("Channel not found: {topic}. Call add_channel first."),
            )
        })?;

        let sequence = self.sequences.entry(channel_id).or_insert(0);

        self.writer
            .write_to_known_channel(
                &mcap::records::MessageHeader {
                    channel_id,
                    sequence: *sequence,
                    log_time: time_ns,
                    publish_time: time_ns,
                },
                data,
            )
            .map_err(|e| {
                CodecError::encode("McapFormatWriter", format!("Failed to write message: {e}"))
            })?;

        *sequence += 1;
        Ok(())
    }

    fn finish(&mut self) -> Result<()> {
        self.writer.finish().map_err(|e| {
            CodecError::encode("McapFormatWriter", format!("Failed to finish: {e}"))
        })?;
        Ok(())
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn std::any::Any {
        self
    }
}

// =============================================================================
// BAG Format Writer
// =============================================================================

/// BAG format writer implementation.
pub struct BagFormatWriter {
    /// BAG writer (Option to allow taking ownership for finish)
    writer: Option<BagWriter>,
    /// Connection ID indexed by topic name
    conn_ids: HashMap<String, u16>,
    /// Next connection ID to assign
    next_conn_id: u16,
}

impl BagFormatWriter {
    /// Create a new BAG writer.
    pub fn create<P: AsRef<Path>>(path: P) -> Result<Self> {
        let writer = BagWriter::create(&path)?;

        Ok(Self {
            writer: Some(writer),
            conn_ids: HashMap::new(),
            next_conn_id: 0,
        })
    }
}

impl FormatWriter for BagFormatWriter {
    fn add_channel(&mut self, topic: &str, msg_type: &str, schema: &str) -> Result<()> {
        // Return early if channel already exists
        if self.conn_ids.contains_key(topic) {
            return Ok(());
        }

        // Add connection
        if let Some(writer) = &mut self.writer {
            writer.add_connection(self.next_conn_id, topic, msg_type, schema)?;
        }

        self.conn_ids.insert(topic.to_string(), self.next_conn_id);
        self.next_conn_id += 1;
        Ok(())
    }

    fn write_message(&mut self, topic: &str, data: &[u8], time_ns: u64) -> Result<()> {
        let conn_id = *self.conn_ids.get(topic).ok_or_else(|| {
            CodecError::encode(
                "BagFormatWriter",
                format!("Channel not found: {topic}. Call add_channel first."),
            )
        })?;

        if let Some(writer) = &mut self.writer {
            let msg = crate::format::bag::BagMessage::new(conn_id, time_ns, data.to_vec());
            writer.write_message(&msg)?;
        }

        Ok(())
    }

    fn finish(&mut self) -> Result<()> {
        if let Some(writer) = self.writer.take() {
            writer.finish().map_err(|e| {
                CodecError::encode("BagFormatWriter", format!("Failed to finish: {e}"))
            })?;
        }
        Ok(())
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn std::any::Any {
        self
    }
}

// =============================================================================
// RoboWriter Facade
// =============================================================================

/// Unified robotics data writer.
///
/// `RoboWriter` provides a single API for writing robotics data to different
/// file formats (MCAP, ROS1 bag, etc.). Format is detected from file extension.
pub struct RoboWriter {
    inner: Box<dyn FormatWriter>,
    path: String,
}

impl RoboWriter {
    /// Create a robotics data file, detecting format from extension.
    ///
    /// Supported formats:
    /// - `.mcap` - MCAP files
    /// - `.bag` - ROS1 bag files
    pub fn create<P: AsRef<Path>>(path: P) -> Result<Self> {
        let path_ref = path.as_ref();
        let path_str = path_ref.to_string_lossy().to_string();
        let extension = path_ref
            .extension()
            .and_then(|e| e.to_str())
            .unwrap_or("")
            .to_lowercase();

        let inner: Box<dyn FormatWriter> = match extension.as_str() {
            "mcap" => Box::new(McapFormatWriter::create(path_ref)?),
            "bag" => Box::new(BagFormatWriter::create(path_ref)?),
            _ => {
                return Err(CodecError::encode(
                    "RoboWriter",
                    format!("Unknown file format: '.{extension}'. Supported: .mcap, .bag",),
                ))
            }
        };

        Ok(Self {
            inner,
            path: path_str,
        })
    }

    /// Add a channel/topic to the file.
    ///
    /// # Arguments
    ///
    /// * `topic` - Topic name (e.g., "/chatter")
    /// * `msg_type` - Message type name (e.g., "std_msgs/String")
    /// * `schema` - Schema definition text
    ///
    /// # Example
    ///
    /// ```ignore
    /// let mut writer = RoboWriter::create("output.mcap")?;
    /// writer.add_channel("/chatter", "std_msgs/String", "string data")?;
    /// ```
    pub fn add_channel(&mut self, topic: &str, msg_type: &str, schema: &str) -> Result<()> {
        self.inner.add_channel(topic, msg_type, schema)
    }

    /// Write a message to a channel.
    ///
    /// # Arguments
    ///
    /// * `topic` - Topic name (must have been added via `add_channel`)
    /// * `data` - Encoded message data
    /// * `time_ns` - Timestamp in nanoseconds
    ///
    /// # Example
    ///
    /// ```ignore
    /// writer.write_message("/chatter", &encoded_data, 1234567890)?;
    /// ```
    pub fn write_message(&mut self, topic: &str, data: &[u8], time_ns: u64) -> Result<()> {
        self.inner.write_message(topic, data, time_ns)
    }

    /// Finalize and close the file.
    ///
    /// This must be called to ensure all data is flushed to disk.
    pub fn finish(&mut self) -> Result<()> {
        self.inner.finish()
    }

    /// Get the file path.
    pub fn path(&self) -> &str {
        &self.path
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_robo_writer_create_mcap() {
        let temp_file = std::env::temp_dir().join("test_writer.mcap");
        let result = RoboWriter::create(&temp_file);
        assert!(
            result.is_ok(),
            "Failed to create MCAP writer: {:?}",
            result.err()
        );
        // Cleanup
        let _ = std::fs::remove_file(&temp_file);
    }

    #[test]
    fn test_robo_writer_create_bag() {
        let temp_file = std::env::temp_dir().join("test_writer.bag");
        let result = RoboWriter::create(&temp_file);
        assert!(
            result.is_ok(),
            "Failed to create BAG writer: {:?}",
            result.err()
        );
        // Cleanup
        let _ = std::fs::remove_file(&temp_file);
    }

    #[test]
    fn test_robo_writer_create_unknown_format() {
        let temp_file = std::env::temp_dir().join("test_writer.unknown");
        let result = RoboWriter::create(&temp_file);
        assert!(result.is_err());
        // Check error message contains expected text
        match result {
            Err(e) => assert!(e.to_string().contains("Unknown file format")),
            Ok(_) => panic!("Expected error for unknown format"),
        }
    }

    #[test]
    fn test_mcap_writer_add_channel_and_write() {
        let temp_file = std::env::temp_dir().join("test_mcap_write.mcap");
        let mut writer = McapFormatWriter::create(&temp_file).unwrap();

        // Add channel
        let result = writer.add_channel("/test", "std_msgs/String", "string data");
        assert!(result.is_ok());

        // Write message
        let result = writer.write_message("/test", b"test data", 1234567890);
        assert!(result.is_ok());

        // Finish
        let result = writer.finish();
        assert!(result.is_ok());

        // Verify file exists
        assert!(temp_file.exists());

        // Cleanup
        let _ = std::fs::remove_file(&temp_file);
    }

    #[test]
    fn test_bag_writer_add_channel_and_write() {
        let temp_file = std::env::temp_dir().join("test_bag_write.bag");
        let mut writer = BagFormatWriter::create(&temp_file).unwrap();

        // Add channel
        let result = writer.add_channel("/test", "std_msgs/String", "string data");
        assert!(result.is_ok());

        // Write message
        let result = writer.write_message("/test", b"test data", 1234567890);
        assert!(result.is_ok());

        // Finish
        let result = writer.finish();
        assert!(result.is_ok());

        // Verify file exists
        assert!(temp_file.exists());

        // Cleanup
        let _ = std::fs::remove_file(&temp_file);
    }
}
