// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Full pipeline round-trip tests for correctness verification.
//!
//! These tests verify that the complete AsyncPipeline (parallel reader → compression → writer)
//! produces correct output that matches the input when read back.
//!
//! Usage:
//!   cargo test -p roboflow --test pipeline_round_trip_tests -- --nocapture

use std::collections::HashMap;
use std::path::Path;

use robocodec::io::traits::FormatReader;
use robocodec::{bag::BagFormat, mcap::McapFormat};

/// Per-channel message data for verification.
#[derive(Debug, Clone, PartialEq)]
struct ChannelMessage {
    channel_id: u16,
    log_time: u64,
    publish_time: u64,
    data: Vec<u8>,
}

/// Collect all messages from an MCAP file, grouped by channel.
fn collect_mcap_messages_by_channel(
    path: &str,
) -> Result<HashMap<u16, Vec<ChannelMessage>>, Box<dyn std::error::Error>> {
    use robocodec::io::traits::ParallelReader;

    let reader = McapFormat::open(path)?;
    let (sender, receiver) = crossbeam_channel::unbounded();

    // Spawn parallel reader
    std::thread::spawn(move || {
        let _ = reader.read_parallel(
            robocodec::io::traits::ParallelReaderConfig::default(),
            sender,
        );
    });

    let mut messages: HashMap<u16, Vec<ChannelMessage>> = HashMap::new();

    for chunk in receiver {
        for msg in &chunk.messages {
            messages
                .entry(msg.channel_id)
                .or_default()
                .push(ChannelMessage {
                    channel_id: msg.channel_id,
                    log_time: msg.log_time,
                    publish_time: msg.publish_time,
                    data: msg.data.clone(),
                });
        }
    }

    Ok(messages)
}

/// Collect all messages from a BAG file, grouped by channel.
///
/// Uses BagFormat (same as pipeline) to ensure consistent channel ID assignment.
fn collect_bag_messages_by_channel(
    path: &str,
) -> Result<HashMap<u16, Vec<ChannelMessage>>, Box<dyn std::error::Error>> {
    use robocodec::io::traits::ParallelReader;

    let reader = BagFormat::open(path)?;
    let (sender, receiver) = crossbeam_channel::unbounded();

    // Spawn parallel reader (same as pipeline)
    std::thread::spawn(move || {
        let _ = reader.read_parallel(
            robocodec::io::traits::ParallelReaderConfig::default(),
            sender,
        );
    });

    let mut messages: HashMap<u16, Vec<ChannelMessage>> = HashMap::new();

    for chunk in receiver {
        for msg in &chunk.messages {
            messages
                .entry(msg.channel_id)
                .or_default()
                .push(ChannelMessage {
                    channel_id: msg.channel_id,
                    log_time: msg.log_time,
                    publish_time: msg.publish_time,
                    data: msg.data.clone(),
                });
        }
    }

    Ok(messages)
}

/// Verify that messages match between input and output.
///
/// This function matches messages by their content (log_time, publish_time, data)
/// regardless of channel ID, since channel IDs may differ between input formats
/// (BAG uses 0-based, MCAP may use arbitrary IDs).
fn verify_messages_match(
    input_messages: &HashMap<u16, Vec<ChannelMessage>>,
    output_messages: &HashMap<u16, Vec<ChannelMessage>>,
) -> Result<(), String> {
    // Collect all input messages
    let mut all_input_msgs: Vec<&ChannelMessage> = input_messages.values().flatten().collect();
    all_input_msgs.sort_by(|a, b| {
        a.log_time
            .cmp(&b.log_time)
            .then_with(|| a.publish_time.cmp(&b.publish_time))
            .then_with(|| a.data.len().cmp(&b.data.len()))
            .then_with(|| a.data.cmp(&b.data))
    });

    // Collect all output messages
    let mut all_output_msgs: Vec<&ChannelMessage> = output_messages.values().flatten().collect();
    all_output_msgs.sort_by(|a, b| {
        a.log_time
            .cmp(&b.log_time)
            .then_with(|| a.publish_time.cmp(&b.publish_time))
            .then_with(|| a.data.len().cmp(&b.data.len()))
            .then_with(|| a.data.cmp(&b.data))
    });

    // Check total message counts match
    if all_input_msgs.len() != all_output_msgs.len() {
        return Err(format!(
            "Total message count mismatch. input={}, output={}",
            all_input_msgs.len(),
            all_output_msgs.len()
        ));
    }

    // Check each message matches
    for (i, (input_msg, output_msg)) in all_input_msgs
        .iter()
        .zip(all_output_msgs.iter())
        .enumerate()
    {
        if input_msg.log_time != output_msg.log_time {
            return Err(format!(
                "Message {}: log_time mismatch. input={}, output={}",
                i, input_msg.log_time, output_msg.log_time
            ));
        }

        if input_msg.publish_time != output_msg.publish_time {
            return Err(format!(
                "Message {}: publish_time mismatch. input={}, output={}",
                i, input_msg.publish_time, output_msg.publish_time
            ));
        }

        if input_msg.data != output_msg.data {
            return Err(format!(
                "Message {}: data mismatch. input_len={}, output_len={}",
                i,
                input_msg.data.len(),
                output_msg.data.len()
            ));
        }
    }

    // Verify channel counts match
    if input_messages.len() != output_messages.len() {
        return Err(format!(
            "Channel count mismatch. input={}, output={}",
            input_messages.len(),
            output_messages.len()
        ));
    }

    Ok(())
}

#[test]
fn test_bag_to_mcap_round_trip() {
    let input_bag = "tests/fixtures/robocodec_test_15.bag";
    let output_mcap = "/tmp/claude/roboflow_round_trip_test.mcap";

    // Clean up existing output file
    let _ = std::fs::remove_file(output_mcap);

    if !Path::new(input_bag).exists() {
        eprintln!("Skipping test: fixture not found at {}", input_bag);
        return;
    }

    println!("=== BAG → MCAP Round-Trip Test ===");
    println!("Input: {}", input_bag);

    // Step 1: Collect messages from input BAG
    let input_messages = match collect_bag_messages_by_channel(input_bag) {
        Ok(msgs) => msgs,
        Err(e) => {
            eprintln!("Failed to read input BAG: {}", e);
            return;
        }
    };

    let total_input_msgs: usize = input_messages.values().map(|v| v.len()).sum();
    println!(
        "Input: {} channels, {} messages",
        input_messages.len(),
        total_input_msgs
    );

    // Step 2: Run the full AsyncPipeline (BAG → MCAP)
    let result = roboflow::Robocodec::open(vec![input_bag])
        .and_then(|builder| builder.write_to(output_mcap).run());

    match &result {
        Ok(_) => println!("Pipeline completed successfully"),
        Err(e) => {
            eprintln!("Pipeline failed: {}", e);
            panic!("Pipeline should succeed");
        }
    }

    // Step 3: Collect messages from output MCAP
    let output_messages = match collect_mcap_messages_by_channel(output_mcap) {
        Ok(msgs) => msgs,
        Err(e) => {
            eprintln!("Failed to read output MCAP: {}", e);
            panic!("Output MCAP should be readable");
        }
    };

    let total_output_msgs: usize = output_messages.values().map(|v| v.len()).sum();
    println!(
        "Output: {} channels, {} messages",
        output_messages.len(),
        total_output_msgs
    );

    // Step 4: Verify messages match
    if let Err(e) = verify_messages_match(&input_messages, &output_messages) {
        panic!("Message verification failed: {}", e);
    }

    println!(
        "✓ All {} messages match (data, timestamps, order)",
        total_input_msgs
    );
}

#[test]
fn test_mcap_to_mcap_round_trip() {
    let input_mcap = "tests/fixtures/robocodec_test_0.mcap";
    let output_mcap = "/tmp/claude/roboflow_mcap_round_trip_test.mcap";

    // Clean up existing output file
    let _ = std::fs::remove_file(output_mcap);

    if !Path::new(input_mcap).exists() {
        eprintln!("Skipping test: fixture not found at {}", input_mcap);
        return;
    }

    println!("=== MCAP → MCAP Round-Trip Test ===");
    println!("Input: {}", input_mcap);

    // Step 1: Collect messages from input MCAP
    let input_messages = match collect_mcap_messages_by_channel(input_mcap) {
        Ok(msgs) => msgs,
        Err(e) => {
            eprintln!("Failed to read input MCAP: {}", e);
            return;
        }
    };

    let total_input_msgs: usize = input_messages.values().map(|v| v.len()).sum();
    println!(
        "Input: {} channels, {} messages",
        input_messages.len(),
        total_input_msgs
    );

    // Step 2: Run the full AsyncPipeline (MCAP → MCAP)
    let result = roboflow::Robocodec::open(vec![input_mcap])
        .and_then(|builder| builder.write_to(output_mcap).run());

    match &result {
        Ok(_) => println!("Pipeline completed successfully"),
        Err(e) => {
            eprintln!("Pipeline failed: {}", e);
            panic!("Pipeline should succeed");
        }
    }

    // Step 3: Collect messages from output MCAP
    let output_messages = match collect_mcap_messages_by_channel(output_mcap) {
        Ok(msgs) => msgs,
        Err(e) => {
            eprintln!("Failed to read output MCAP: {}", e);
            panic!("Output MCAP should be readable");
        }
    };

    let total_output_msgs: usize = output_messages.values().map(|v| v.len()).sum();
    println!(
        "Output: {} channels, {} messages",
        output_messages.len(),
        total_output_msgs
    );

    // Step 4: Verify messages match
    if let Err(e) = verify_messages_match(&input_messages, &output_messages) {
        panic!("Message verification failed: {}", e);
    }

    println!(
        "✓ All {} messages match (data, timestamps, order)",
        total_input_msgs
    );
}

#[test]
fn test_bag_to_mcap_with_different_presets() {
    let input_bag = "tests/fixtures/robocodec_test_15.bag";

    // Clean up existing output files
    for name in ["fast", "balanced", "slow"] {
        let _ = std::fs::remove_file(format!("/tmp/claude/roboflow_round_trip_{}.mcap", name));
    }

    if !Path::new(input_bag).exists() {
        eprintln!("Skipping test: fixture not found at {}", input_bag);
        return;
    }

    println!("=== BAG → MCAP with Different Presets ===");

    // Collect input messages once
    let input_messages = match collect_bag_messages_by_channel(input_bag) {
        Ok(msgs) => msgs,
        Err(e) => {
            eprintln!("Failed to read input BAG: {}", e);
            return;
        }
    };

    let presets = [
        ("fast", roboflow::pipeline::fluent::CompressionPreset::Fast),
        (
            "balanced",
            roboflow::pipeline::fluent::CompressionPreset::Balanced,
        ),
        ("slow", roboflow::pipeline::fluent::CompressionPreset::Slow),
    ];

    for (name, preset) in presets {
        let output = format!("/tmp/claude/roboflow_round_trip_{}.mcap", name);

        println!("\nTesting preset: {}", name);

        // Run with preset
        let result = roboflow::Robocodec::open(vec![input_bag])
            .and_then(|builder| builder.write_to(&output).with_compression(preset).run());

        if let Err(e) = &result {
            eprintln!("Pipeline failed with preset {}: {}", name, e);
            panic!("Pipeline should succeed with preset {}", name);
        }

        // Verify output
        let output_messages = match collect_mcap_messages_by_channel(&output) {
            Ok(msgs) => msgs,
            Err(e) => {
                eprintln!("Failed to read output MCAP: {}", e);
                panic!("Output MCAP should be readable with preset {}", name);
            }
        };

        if let Err(e) = verify_messages_match(&input_messages, &output_messages) {
            panic!("Message verification failed with preset {}: {}", name, e);
        }

        println!("✓ Preset '{}' passed verification", name);
    }
}

#[test]
fn test_channel_info_preservation() {
    let input_bag = "tests/fixtures/robocodec_test_15.bag";
    let output_mcap = "/tmp/claude/roboflow_channel_info_test.mcap";

    // Clean up existing output file
    let _ = std::fs::remove_file(output_mcap);

    if !Path::new(input_bag).exists() {
        eprintln!("Skipping test: fixture not found at {}", input_bag);
        return;
    }

    println!("=== Channel Info Preservation Test ===");

    // Read input channels
    let input_reader = BagFormat::open(input_bag).unwrap();
    let input_channels = input_reader.channels().clone();

    // Run pipeline
    roboflow::Robocodec::open(vec![input_bag])
        .and_then(|builder| builder.write_to(output_mcap).run())
        .expect("Pipeline should succeed");

    // Read output channels
    let output_reader = McapFormat::open(output_mcap).unwrap();
    let output_channels = output_reader.channels().clone();

    println!("Input channels: {}", input_channels.len());
    println!("Output channels: {}", output_channels.len());

    // Verify channel count matches
    assert_eq!(
        input_channels.len(),
        output_channels.len(),
        "Channel count should be preserved"
    );

    // Verify each channel's topic and message type
    for in_ch in input_channels.values() {
        let found = output_channels
            .values()
            .any(|out_ch| out_ch.topic == in_ch.topic && out_ch.message_type == in_ch.message_type);

        assert!(
            found,
            "Channel {} ({}) not found in output",
            in_ch.topic, in_ch.message_type
        );
    }

    println!("✓ All channel information preserved");
}
