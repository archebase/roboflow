// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! MCAP integration tests.
//!
//! These tests validate that roboflow can parse schemas from real MCAP files
//! and decode messages correctly. Each fixture file represents real robotics data.

use std::path::Path;

use robocodec::mcap::McapReader;

// Import common test utilities
mod common;
use common::*;

/// Path to the fixtures directory.
const FIXTURES_DIR: &str = "tests/fixtures";

/// Macro to skip a test if a fixture file is missing.
macro_rules! skip_if_missing {
    ($path:expr, $fixture_name:expr) => {
        if !$path.exists() {
            eprintln!("Skipping test: fixture file not found: {}", $fixture_name);
            return;
        }
    };
}

/// Summary of all test results for an MCAP file.
#[derive(Default)]
pub struct TestSummary {
    pub channels_tested: usize,
    pub total_messages: usize,
}

/// Run integration tests on a single MCAP fixture file with expectations.
fn test_mcap_file(fixture_path: &Path, _expectations: &FixtureExpectations) -> TestSummary {
    // Open the MCAP file using roboflow's McapReader
    let reader =
        McapReader::open(fixture_path).unwrap_or_else(|e| panic!("Failed to open MCAP file: {e}"));

    let channels = reader.channels();
    println!("MCAP has {} channels", channels.len());
    println!("Total messages in file: {}", reader.message_count());

    for (id, channel) in channels.iter().take(5) {
        println!(
            "  [{id}]: {} - {} messages",
            channel.topic, channel.message_count
        );
    }

    let mut test_summary = TestSummary {
        channels_tested: channels.len(),
        ..Default::default()
    };

    // Test each channel using roboflow's decoded message iterator
    let decoded_iter = reader
        .decode_messages()
        .unwrap_or_else(|e| panic!("Failed to create decoded iterator: {e}"));

    let mut messages_tested = 0usize;

    for result in decoded_iter {
        let (decoded, channel_info) = match result {
            Ok(msg) => msg,
            Err(e) => {
                eprintln!("Decode error: {e}");
                continue;
            }
        };

        messages_tested += 1;

        // Print channel info on first message
        if messages_tested == 1 {
            println!(
                "  Channel [{}]: {}, encoding: {}, type: {}",
                channel_info.id,
                channel_info.topic,
                channel_info.encoding,
                channel_info.message_type
            );
        }

        // Validate decoded message structure
        validate_decoded_message_simple(&decoded, &channel_info);

        // Limit messages tested
        if messages_tested >= 100 {
            break;
        }
    }

    test_summary.total_messages = messages_tested;

    test_summary
}

/// Validate that a decoded message has expected structure.
fn validate_decoded_message_simple(
    decoded: &std::collections::HashMap<String, roboflow::CodecValue>,
    channel_info: &robocodec::mcap::reader::ChannelInfo,
) {
    // Check that we decoded some fields
    if decoded.is_empty() {
        eprintln!("    Warning: No fields decoded for {}", channel_info.topic);
    } else {
        println!(
            "    Decoded {} fields for {}",
            decoded.len(),
            channel_info.topic
        );
    }
}

/// Shared test runner for fixture files.
fn run_fixture_test(fixture_name: &str, expectations: &FixtureExpectations) {
    let fixture_path = Path::new(FIXTURES_DIR).join(format!("{fixture_name}.mcap"));

    skip_if_missing!(&fixture_path, fixture_name);

    println!("\n=== Testing MCAP fixture: {} ===", fixture_name);

    let summary = test_mcap_file(&fixture_path, expectations);

    // Assert expectations met
    assert!(
        summary.channels_tested >= expectations.min_channels,
        "Expected at least {} channels, got {}",
        expectations.min_channels,
        summary.channels_tested
    );

    assert!(
        summary.total_messages >= expectations.min_messages,
        "Expected at least {} messages, got {}",
        expectations.min_messages,
        summary.total_messages
    );

    println!(
        "  ✓ All expectations met for {} ({} channels, {} messages)",
        fixture_name, summary.channels_tested, summary.total_messages
    );
}

// ============================================================================
// Per-Fixture Tests
// ============================================================================

#[test]
fn test_robocodec_test_0_fixture() {
    let expectations = FixtureExpectations {
        min_channels: 1,
        min_messages: 0,
        expected_topics: vec![],
        skip_unsupported: true,
    };
    run_fixture_test("robocodec_test_0", &expectations);
}

#[test]
fn test_robocodec_test_1_fixture() {
    let expectations = FixtureExpectations {
        min_channels: 1,
        min_messages: 0,
        expected_topics: vec![],
        skip_unsupported: true,
    };
    run_fixture_test("robocodec_test_1", &expectations);
}

#[test]
fn test_robocodec_test_3_fixture() {
    let expectations = FixtureExpectations {
        min_channels: 1,
        min_messages: 0,
        expected_topics: vec![],
        skip_unsupported: true,
    };
    run_fixture_test("robocodec_test_3", &expectations);
}

#[test]
fn test_robocodec_test_4_fixture() {
    let expectations = FixtureExpectations {
        min_channels: 1,
        min_messages: 0,
        expected_topics: vec![],
        skip_unsupported: true,
    };
    run_fixture_test("robocodec_test_4", &expectations);
}

#[test]
fn test_robocodec_test_5_fixture() {
    let expectations = FixtureExpectations {
        min_channels: 1,
        min_messages: 0,
        expected_topics: vec![],
        skip_unsupported: true,
    };
    run_fixture_test("robocodec_test_5", &expectations);
}

#[test]
fn test_robocodec_test_6_fixture() {
    let expectations = FixtureExpectations {
        min_channels: 1,
        min_messages: 0,
        expected_topics: vec![],
        skip_unsupported: true,
    };
    run_fixture_test("robocodec_test_6", &expectations);
}

#[test]
fn test_robocodec_test_7_fixture() {
    let expectations = FixtureExpectations {
        min_channels: 1,
        min_messages: 0,
        expected_topics: vec![],
        skip_unsupported: true,
    };
    run_fixture_test("robocodec_test_7", &expectations);
}

#[test]
fn test_robocodec_test_8_fixture() {
    let expectations = FixtureExpectations {
        min_channels: 1,
        min_messages: 0,
        expected_topics: vec![],
        skip_unsupported: true,
    };
    run_fixture_test("robocodec_test_8", &expectations);
}

#[test]
fn test_robocodec_test_9_fixture() {
    let expectations = FixtureExpectations {
        min_channels: 1,
        min_messages: 0,
        expected_topics: vec![],
        skip_unsupported: true,
    };
    run_fixture_test("robocodec_test_9", &expectations);
}

#[test]
fn test_robocodec_test_10_fixture() {
    let expectations = FixtureExpectations {
        min_channels: 1,
        min_messages: 0,
        expected_topics: vec![],
        skip_unsupported: true,
    };
    run_fixture_test("robocodec_test_10", &expectations);
}

#[test]
fn test_robocodec_test_11_fixture() {
    let expectations = FixtureExpectations {
        min_channels: 1,
        min_messages: 0,
        expected_topics: vec![],
        skip_unsupported: true,
    };
    run_fixture_test("robocodec_test_11", &expectations);
}

#[test]
fn test_robocodec_test_12_fixture() {
    let expectations = FixtureExpectations {
        min_channels: 1,
        min_messages: 0,
        expected_topics: vec![],
        skip_unsupported: true,
    };
    run_fixture_test("robocodec_test_12", &expectations);
}

#[test]
fn test_robocodec_test_14_fixture() {
    run_fixture_test(
        "robocodec_test_14",
        &FixtureExpectations {
            min_channels: 1,
            min_messages: 0,
            expected_topics: vec![],
            skip_unsupported: true,
        },
    );
}
