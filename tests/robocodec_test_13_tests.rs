//! MCAP integration tests for test fixture 13.
//!
//! This test validates that robocodec can parse schemas from robocodec_test_13.mcap
//! file and decode messages correctly.

use std::path::Path;

use mcap::MessageStream;

use robocodec::encoding::CdrDecoder;
use robocodec::schema::parse_schema;
use robocodec::encoding::ProtobufDecoder;

// Import common test utilities
mod common;

/// Path to the fixtures directory.
const FIXTURES_DIR: &str = "tests/fixtures";

/// Test the robocodec_test_13.mcap fixture file.
#[test]
fn test_robocodec_test_13_fixture() {
    let fixture_path = Path::new(FIXTURES_DIR).join("robocodec_test_13.mcap");

    assert!(
        fixture_path.exists(),
        "Fixture file not found: {}",
        fixture_path.display()
    );

    println!("\n=== Testing MCAP fixture: robocodec_test_13 ===");

    // Open the MCAP file
    let file = std::fs::File::open(&fixture_path)
        .unwrap_or_else(|e| panic!("Failed to open MCAP file: {e}"));

    let mmap = unsafe { memmap2::Mmap::map(&file) }
        .unwrap_or_else(|e| panic!("Failed to mmap MCAP file: {e}"));

    // Read summary
    let summary = mcap::Summary::read(&mmap)
        .unwrap_or_else(|e| panic!("Failed to read MCAP summary: {e}"))
        .unwrap_or_else(|| panic!("MCAP file has no summary"));

    println!("MCAP has {} channels", summary.channels.len());

    let mut channels_tested = 0;
    let mut total_messages = 0;
    let mut decode_errors: Vec<String> = Vec::new();

    // Test each channel
    for (channel_id, channel) in &summary.channels {
        let schema_entry = channel
            .schema
            .as_ref()
            .and_then(|s| summary.schemas.get(&s.id));

        let schema_name = schema_entry
            .map(|s| s.name.clone())
            .unwrap_or_else(|| format!("channel_{channel_id}"));

        let encoding = channel.message_encoding.to_lowercase();

        println!(
            "  Channel [{}]: {}, encoding: {}, schema: {}",
            channel_id,
            channel.topic,
            channel.message_encoding,
            schema_entry
                .map(|s| &s.name)
                .unwrap_or(&"(none)".to_string())
        );

        // Handle different encodings with appropriate decoders
        if encoding.contains("protobuf") {
            println!("    Handling as protobuf");
            let decoder = ProtobufDecoder::new();
            let stream = MessageStream::new(&mmap).unwrap();
            let mut messages_tested = 0;

            for msg_result in stream {
                if let Ok(msg) = &msg_result {
                    if msg.channel.id == *channel_id {
                        match decoder.decode(&msg.data) {
                            Ok(decoded) => {
                                if !decoded.is_empty() {
                                    messages_tested += 1;
                                }
                            }
                            Err(e) => {
                                let err_msg =
                                    format!("Decode error on channel [{channel_id}]: {e}");
                                eprintln!("    {err_msg}");
                                decode_errors.push(err_msg);
                            }
                        }

                        if messages_tested >= 1 {
                            break;
                        }
                    }
                }
            }
            total_messages += messages_tested;
        } else if encoding.contains("cdr") {
            println!("    Handling as CDR encoding");
            if let Some(entry) = schema_entry {
                let schema_bytes = &entry.data;
                let schema_str = String::from_utf8(schema_bytes.to_vec());

                if let Ok(definition) = schema_str {
                    match parse_schema(&schema_name, &definition) {
                        Ok(schema) => {
                            println!("    Schema parsed: {} types", schema.types.len());

                            // Read and decode the first message
                            let stream = MessageStream::new(&mmap).unwrap();
                            let decoder = CdrDecoder::new();
                            let mut messages_tested = 0;

                            for msg_result in stream {
                                if let Ok(msg) = &msg_result {
                                    if msg.channel.id == *channel_id {
                                        match decoder.decode(&schema, &msg.data, None) {
                                            Ok(_) => {
                                                messages_tested += 1;
                                            }
                                            Err(e) => {
                                                let err_msg = format!(
                                                    "Decode error on channel [{channel_id}]: {e}"
                                                );
                                                eprintln!("    {err_msg}");
                                                decode_errors.push(err_msg);
                                            }
                                        }

                                        if messages_tested >= 1 {
                                            break;
                                        }
                                    }
                                }
                            }
                            total_messages += messages_tested;
                        }
                        Err(e) => {
                            let err_msg =
                                format!("Schema parse error on channel [{channel_id}]: {e}");
                            eprintln!("    {err_msg}");
                            decode_errors.push(err_msg);
                        }
                    }
                }
            }
        }

        channels_tested += 1;
    }

    // Assert no decode errors occurred
    assert!(
        decode_errors.is_empty(),
        "MCAP decoding had {} errors:\n{}",
        decode_errors.len(),
        decode_errors.join("\n")
    );

    // Assert expectations met
    assert!(
        channels_tested >= 1,
        "Expected at least 1 channel, got {channels_tested}"
    );

    println!(
        "  ✓ All expectations met for robocodec_test_13 ({channels_tested} channels, {total_messages} messages)"
    );
}
