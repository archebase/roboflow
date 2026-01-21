//! MCAP integration tests for test fixture 13.
//!
//! This test validates that roboflow can parse schemas from robocodec_test_13.mcap
//! file and decode messages correctly.

use std::path::Path;

use robocodec::encoding::CdrDecoder;
use robocodec::encoding::ProtobufDecoder;
use robocodec::mcap::McapReader;
use robocodec::schema::parse_schema;

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

    // Open the MCAP file using McapReader
    let reader =
        McapReader::open(fixture_path).unwrap_or_else(|e| panic!("Failed to open MCAP file: {e}"));

    let channels = reader.channels();
    println!("MCAP has {} channels", channels.len());

    let mut channels_tested = 0;
    let mut total_messages = 0;
    let mut decode_errors: Vec<String> = Vec::new();

    // Test each channel
    for (&channel_id, channel) in channels {
        let schema_name = channel.message_type.clone();
        let encoding = channel.encoding.to_lowercase();

        println!(
            "  Channel [{}]: {}, encoding: {}, schema: {}",
            channel_id, channel.topic, channel.encoding, channel.message_type
        );

        // Handle different encodings with appropriate decoders
        if encoding.contains("protobuf") {
            println!("    Handling as protobuf");
            let decoder = ProtobufDecoder::new();
            let mut messages_tested = 0;

            if let Ok(raw_iter) = reader.iter_raw() {
                if let Ok(stream) = raw_iter.stream() {
                    for (msg, _ch) in stream.flatten() {
                        if msg.channel_id == channel_id {
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
            }
            total_messages += messages_tested;
        } else if encoding.contains("cdr") {
            println!("    Handling as CDR encoding");
            if let Some(schema_text) = &channel.schema {
                let definition = schema_text;

                match parse_schema(&schema_name, definition) {
                    Ok(schema) => {
                        println!("    Schema parsed: {} types", schema.types.len());

                        // Read and decode the first message
                        let decoder = CdrDecoder::new();
                        let mut messages_tested = 0;

                        if let Ok(raw_iter) = reader.iter_raw() {
                            if let Ok(stream) = raw_iter.stream() {
                                for (msg, _ch) in stream.flatten() {
                                    if msg.channel_id == channel_id {
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
                        }
                        total_messages += messages_tested;
                    }
                    Err(e) => {
                        let err_msg = format!("Schema parse error on channel [{channel_id}]: {e}");
                        eprintln!("    {err_msg}");
                        decode_errors.push(err_msg);
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
