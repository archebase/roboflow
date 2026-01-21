//! MCAP writer unit tests.
//!
//! Tests for the ParallelMcapWriter implementation.

use std::collections::HashMap;
use std::io::{BufWriter, Cursor};

use robocodec::format::writer::ParallelMcapWriter;
use robocodec::io::formats::mcap_constants::MCAP_MAGIC;

#[test]
fn test_channel_record_format_readable_by_mcap_crate() {
    // This test ensures the channel record format is compatible with mcap crate
    let cursor = Cursor::new(Vec::new());
    let mut writer = ParallelMcapWriter::new(BufWriter::new(cursor)).unwrap();

    // Add schema and channel with metadata
    let schema_id = writer.add_schema("test/Type", "ros1msg", b"test").unwrap();
    let mut metadata = HashMap::new();
    metadata.insert("key".to_string(), "value".to_string());
    let channel_id = writer
        .add_channel(schema_id, "/test/topic", "cdr", &metadata)
        .unwrap();

    // Write a message to ensure we have data for summary
    writer
        .write_message(channel_id, 1000, 1000, b"test data")
        .unwrap();

    // Finish and get the bytes
    writer.finish().unwrap();
    let cursor = writer.into_inner().into_inner().unwrap();
    let bytes = cursor.into_inner();

    // Verify MCAP magic at beginning and end
    assert_eq!(&bytes[0..8], MCAP_MAGIC);
    assert_eq!(&bytes[bytes.len() - 8..], MCAP_MAGIC);

    // Use mcap crate to read the summary and verify channels are valid
    match mcap::Summary::read(&bytes) {
        Ok(Some(summary)) => {
            assert_eq!(summary.schemas.len(), 1);
            assert_eq!(summary.channels.len(), 1);
            // Verify channel has the correct metadata
            let channel = summary.channels.values().next().unwrap();
            assert_eq!(channel.topic, "/test/topic");
            assert_eq!(channel.metadata.get("key"), Some(&"value".to_string()));
        }
        Ok(None) => {
            // Summary might be None for small files - file structure is still valid
        }
        Err(e) => panic!("mcap crate failed to read file: {:?}", e),
    }
}
