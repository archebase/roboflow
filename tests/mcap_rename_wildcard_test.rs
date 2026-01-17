//! Test MCAP rewriting with wildcard type renaming and round-trip verification.
//!
//! Usage:
//!   cargo test -p robocodec --test mcap_rename_wildcard -- --nocapture

use robocodec::format::mcap::McapRewriter;
use robocodec::format::mcap::transform::TransformBuilder;
use robocodec::{RewriteOptions, RoboReader};
use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;

#[test]
fn test_wildcard_rename_sensor_msgs() {
    // Use nissan fixture from strata-core
    let input_path = "../strata-core/tests/fixtures/nissan_zala_50_zeg_4_0.mcap";
    let output_path = "/tmp/nissan_renamed.mcap";

    // Skip test if fixture doesn't exist
    if !Path::new(input_path).exists() {
        eprintln!("Skipping test: fixture not found at {input_path}");
        return;
    }

    // The nissan MCAP contains these types:
    // - sensor_msgs/msg/Imu
    // - sensor_msgs/msg/MagneticField
    // - std_msgs/msg/String
    // - std_msgs/msg/Float32
    // - geometry_msgs/msg/PoseStamped

    // Test renaming sensor_msgs to my_sensor_msgs and geometry_msgs to my_geometry_msgs
    let options = RewriteOptions::default().with_transforms(
        TransformBuilder::new()
            .with_type_rename_wildcard("sensor_msgs/*", "my_sensor_msgs/*")
            .with_type_rename_wildcard("geometry_msgs/*", "my_geometry_msgs/*")
            .build(),
    );

    let mut rewriter = McapRewriter::with_options(options);

    let result = rewriter.rewrite(input_path, output_path);
    assert!(result.is_ok(), "Rewrite should succeed: {:?}", result.err());

    let stats = result.unwrap();
    println!("Rewrite complete!");
    println!("  Channels: {}", stats.channel_count);
    println!("  Messages processed: {}", stats.message_count);
    println!("  Types renamed: {}", stats.types_renamed);
    println!("  Re-encoded: {}", stats.reencoded_count);

    // Verify output file was created
    assert!(Path::new(output_path).exists(), "Output file should exist");

    println!("\nOutput written to: {output_path}");
}

/// Helper structure to track channel information for comparison.
#[derive(Debug, Clone, PartialEq)]
struct ChannelSnapshot {
    topic: String,
    message_type: String,
    encoding: String,
    message_count: u64,
}

impl ChannelSnapshot {
    fn from_channel_info(channel: &robocodec::format::mcap::ChannelInfo) -> Self {
        Self {
            topic: channel.topic.clone(),
            message_type: channel.message_type.clone(),
            encoding: channel.encoding.clone(),
            message_count: channel.message_count,
        }
    }
}

/// Collect all channels from a reader into a map by topic.
fn collect_channels(reader: &RoboReader) -> BTreeMap<String, ChannelSnapshot> {
    reader
        .channels()
        .values()
        .map(|c| (c.topic.clone(), ChannelSnapshot::from_channel_info(c)))
        .collect()
}

#[test]
fn test_round_trip_topic_rename() {
    let input_path = "../strata-core/tests/fixtures/nissan_zala_50_zeg_4_0.mcap";
    let output_path = "/tmp/nissan_topic_rename.mcap";

    if !Path::new(input_path).exists() {
        eprintln!("Skipping test: fixture not found at {input_path}");
        return;
    }

    // Step 1: Read original file to capture topics
    let reader_original = RoboReader::open(input_path);
    assert!(
        reader_original.is_ok(),
        "Should open original file: {:?}",
        reader_original.err()
    );
    let reader_original = reader_original.unwrap();
    let original_channels = collect_channels(&reader_original);

    println!("Original channels:");
    for (topic, ch) in &original_channels {
        println!("  {} -> {} ({} messages)", topic, ch.message_type, ch.message_count);
    }

    // Step 2: Apply topic rename transform
    // Rename /nissan/gps/duro/imu to /sensors/imu
    // Rename /nissan/gps/duro/mag to /sensors/mag
    let options = RewriteOptions::default().with_transforms(
        TransformBuilder::new()
            .with_topic_rename("/nissan/gps/duro/imu", "/sensors/imu")
            .with_topic_rename("/nissan/gps/duro/mag", "/sensors/mag")
            .build(),
    );

    let mut rewriter = McapRewriter::with_options(options);
    let result = rewriter.rewrite(input_path, output_path);
    assert!(result.is_ok(), "Rewrite should succeed: {:?}", result.err());

    // Step 3: Read the output file to verify transformations
    let reader_output = RoboReader::open(output_path);
    assert!(
        reader_output.is_ok(),
        "Should open output file: {:?}",
        reader_output.err()
    );
    let reader_output = reader_output.unwrap();
    let output_channels = collect_channels(&reader_output);

    println!("\nTransformed channels:");
    for (topic, ch) in &output_channels {
        println!("  {} -> {} ({} messages)", topic, ch.message_type, ch.message_count);
    }

    // Step 4: Verify topic renames were applied
    // Check that /nissan/gps/duro/imu became /sensors/imu
    assert!(
        !output_channels.contains_key("/nissan/gps/duro/imu"),
        "Original topic '/nissan/gps/duro/imu' should not exist in output"
    );
    assert!(
        output_channels.contains_key("/sensors/imu"),
        "Renamed topic '/sensors/imu' should exist in output"
    );

    // Check that /nissan/gps/duro/mag became /sensors/mag
    assert!(
        !output_channels.contains_key("/nissan/gps/duro/mag"),
        "Original topic '/nissan/gps/duro/mag' should not exist in output"
    );
    assert!(
        output_channels.contains_key("/sensors/mag"),
        "Renamed topic '/sensors/mag' should exist in output"
    );

    // Verify message counts are preserved
    let original_count: u64 = original_channels.values().map(|c| c.message_count).sum();
    let output_count: u64 = output_channels.values().map(|c| c.message_count).sum();
    assert_eq!(
        original_count, output_count,
        "Total message count should be preserved"
    );

    println!("\nTopic rename test passed!");
}

#[test]
fn test_round_trip_type_rename_with_verification() {
    let input_path = "../strata-core/tests/fixtures/nissan_zala_50_zeg_4_0.mcap";
    let output_path = "/tmp/nissan_type_rename_verify.mcap";

    if !Path::new(input_path).exists() {
        eprintln!("Skipping test: fixture not found at {input_path}");
        return;
    }

    // Step 1: Read original file
    let reader_original = RoboReader::open(input_path).unwrap();
    let original_channels = collect_channels(&reader_original);

    println!("Original channels:");
    for (topic, ch) in &original_channels {
        println!("  {} -> {} ({} messages)", topic, ch.message_type, ch.message_count);
    }

    // Step 2: Apply type rename transforms
    let options = RewriteOptions::default().with_transforms(
        TransformBuilder::new()
            .with_type_rename_wildcard("sensor_msgs/*", "my_sensor_msgs/*")
            .with_type_rename_wildcard("geometry_msgs/*", "my_geometry_msgs/*")
            .build(),
    );

    let mut rewriter = McapRewriter::with_options(options);
    let result = rewriter.rewrite(input_path, output_path);
    assert!(result.is_ok(), "Rewrite should succeed: {:?}", result.err());

    let stats = result.unwrap();
    println!("\nRewrite stats:");
    println!("  Channels: {}", stats.channel_count);
    println!("  Messages: {}", stats.message_count);
    println!("  Types renamed: {}", stats.types_renamed);

    // Step 3: Read output and verify transformations
    let reader_output = RoboReader::open(output_path).unwrap();
    let output_channels = collect_channels(&reader_output);

    println!("\nTransformed channels:");
    for (topic, ch) in &output_channels {
        println!("  {} -> {} ({} messages)", topic, ch.message_type, ch.message_count);
    }

    // Step 4: Verify all sensor_msgs types were renamed
    for (topic, channel) in &output_channels {
        if channel.message_type.starts_with("sensor_msgs/") {
            panic!(
                "Found sensor_msgs type that wasn't renamed: {} -> {}",
                topic, channel.message_type
            );
        }
    }

    // Verify renamed types exist
    let has_my_sensor_msgs = output_channels
        .values()
        .any(|c| c.message_type.starts_with("my_sensor_msgs/"));
    assert!(
        has_my_sensor_msgs,
        "Should have my_sensor_msgs types in output"
    );

    let has_my_geometry_msgs = output_channels
        .values()
        .any(|c| c.message_type.starts_with("my_geometry_msgs/"));
    assert!(
        has_my_geometry_msgs,
        "Should have my_geometry_msgs types in output"
    );

    println!("\nType rename verification test passed!");
}

#[test]
fn test_round_trip_combined_topic_and_type_rename() {
    let input_path = "../strata-core/tests/fixtures/nissan_zala_50_zeg_4_0.mcap";
    let output_path = "/tmp/nissan_combined_rename.mcap";

    if !Path::new(input_path).exists() {
        eprintln!("Skipping test: fixture not found at {input_path}");
        return;
    }

    // Step 1: Read original file
    let reader_original = RoboReader::open(input_path).unwrap();
    let original_channels = collect_channels(&reader_original);
    let original_topics: BTreeSet<String> = original_channels.keys().cloned().collect();
    let original_types: BTreeSet<String> = original_channels
        .values()
        .map(|c| c.message_type.clone())
        .collect();

    println!("Original topics: {:?}", original_topics);
    println!("Original types: {:?}", original_types);

    // Step 2: Apply both topic and type renames
    let options = RewriteOptions::default().with_transforms(
        TransformBuilder::new()
            .with_topic_rename("/nissan/gps/duro/imu", "/sensors/imu")
            .with_type_rename_wildcard("sensor_msgs/*", "renamed_sensor/*")
            .build(),
    );

    let mut rewriter = McapRewriter::with_options(options);
    let result = rewriter.rewrite(input_path, output_path);
    assert!(result.is_ok(), "Rewrite should succeed: {:?}", result.err());

    // Step 3: Read output and verify
    let reader_output = RoboReader::open(output_path).unwrap();
    let output_channels = collect_channels(&reader_output);
    let output_topics: BTreeSet<String> = output_channels.keys().cloned().collect();
    let output_types: BTreeSet<String> = output_channels
        .values()
        .map(|c| c.message_type.clone())
        .collect();

    println!("\nOutput topics: {:?}", output_topics);
    println!("Output types: {:?}", output_types);

    // Verify topic rename
    assert!(
        !output_topics.contains("/nissan/gps/duro/imu"),
        "Original topic '/nissan/gps/duro/imu' should be renamed"
    );
    assert!(
        output_topics.contains("/sensors/imu"),
        "Topic should be renamed to '/sensors/imu'"
    );

    // Verify type renames
    for msg_type in &output_types {
        if msg_type.contains("sensor_msgs") {
            panic!(
                "Found sensor_msgs type that wasn't renamed: {}",
                msg_type
            );
        }
    }

    println!("\nCombined rename test passed!");
}
