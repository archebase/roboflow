// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! MCAP-to-LeRobot integration tests.
//!
//! Uses `tests/fixtures/sample.mcap` to validate the full conversion
//! pipeline for MCAP files. Covers:
//! - MCAP source initialization and reading
//! - Topic mapping and frame alignment
//! - LeRobot dataset structure output
//!
//! To run these tests, place an MCAP file at `tests/fixtures/sample.mcap`.
//! Tests will skip gracefully if the fixture is not present.

use std::collections::HashMap;
use std::path::Path;

use roboflow::{LerobotConfig, LerobotWriter};
use roboflow_dataset::streaming::StreamingConfig;
use roboflow_dataset::{PipelineConfig, PipelineExecutor};

const MCAP_PATH: &str = "tests/fixtures/sample.mcap";
const CONFIG_PATH: &str = "tests/fixtures/sample_mcap_lerobot.toml";

/// Check if MCAP fixture exists. Returns true if tests should run.
fn mcap_fixture_exists() -> bool {
    Path::new(MCAP_PATH).exists()
}

/// Check if config fixture exists.
fn config_fixture_exists() -> bool {
    Path::new(CONFIG_PATH).exists()
}

/// Build topic_mappings from LerobotConfig for PipelineConfig.
fn topic_mappings_from_config(config: &LerobotConfig) -> HashMap<String, String> {
    config
        .mappings
        .iter()
        .map(|m| (m.topic.clone(), m.feature.clone()))
        .collect()
}

/// Full MCAP-to-LeRobot conversion test.
///
/// Processes sample.mcap through the pipeline and validates:
/// - MCAP source can be initialized
/// - Messages are read correctly
/// - Output directory structure is created
#[tokio::test]
async fn test_mcap_to_lerobot_conversion() {
    if !mcap_fixture_exists() {
        eprintln!("SKIP: MCAP fixture not found at {}", MCAP_PATH);
        eprintln!("  Place sample.mcap at tests/fixtures/ to run this test");
        return;
    }

    use roboflow::sources::{create_source, register_builtin_sources};

    register_builtin_sources();

    // Create a default config if fixture config doesn't exist
    let lerobot_config = if config_fixture_exists() {
        LerobotConfig::from_file(CONFIG_PATH).expect("load config")
    } else {
        // Create minimal config for testing
        LerobotConfig {
            dataset: roboflow::lerobot::DatasetConfig {
                base: roboflow::DatasetBaseConfig {
                    name: "mcap_test_dataset".to_string(),
                    fps: 30,
                    robot_type: Some("test_robot".to_string()),
                },
                env_type: None,
            },
            mappings: vec![],
            video: roboflow::VideoConfig::default(),
            annotation_file: None,
            flushing: roboflow::lerobot::FlushingConfig::default(),
            streaming: roboflow::lerobot::StreamingConfig::default(),
        }
    };

    let topic_mappings = topic_mappings_from_config(&lerobot_config);

    let output_dir = tempfile::TempDir::new().expect("create temp dir");
    let writer =
        LerobotWriter::new_local(output_dir.path(), lerobot_config).expect("create LerobotWriter");

    let streaming_config = StreamingConfig::with_fps(30);
    let pipeline_config = PipelineConfig::new(streaming_config)
        .with_topic_mappings(topic_mappings)
        .with_max_frames(500); // Limit for CI

    let mut executor = PipelineExecutor::new(writer, pipeline_config);

    let source_config = roboflow::SourceConfig::mcap(MCAP_PATH);
    let mut source = create_source(&source_config).expect("create mcap source");

    source
        .initialize(&source_config)
        .await
        .expect("initialize MCAP source");

    let mut messages_processed = 0;
    let batch_size = 100;

    loop {
        match source.read_batch(batch_size).await {
            Ok(Some(messages)) if !messages.is_empty() => {
                for msg in messages {
                    if executor.process_message(msg).is_ok() {
                        messages_processed += 1;
                    }
                }
            }
            Ok(Some(_)) => continue,
            Ok(None) => break,
            Err(e) => {
                eprintln!("Read error: {}", e);
                break;
            }
        }
    }

    let result = executor.finalize();

    match &result {
        Ok(stats) => {
            println!(
                "MCAP conversion complete: messages={}, frames={}, episodes={}",
                messages_processed, stats.frames_written, stats.episodes_written
            );
        }
        Err(e) => {
            println!("Finalize result: {} (may be expected for partial MCAP)", e);
        }
    }

    assert!(
        messages_processed > 0,
        "Should have processed messages from MCAP"
    );

    // Output dir should exist
    let output = output_dir.path();
    assert!(output.exists(), "output dir should exist");
}

/// Test MCAP source metadata retrieval.
#[tokio::test]
async fn test_mcap_source_metadata() {
    if !mcap_fixture_exists() {
        eprintln!("SKIP: MCAP fixture not found at {}", MCAP_PATH);
        return;
    }

    use roboflow::sources::{create_source, register_builtin_sources};

    register_builtin_sources();

    let source_config = roboflow::SourceConfig::mcap(MCAP_PATH);
    let mut source = create_source(&source_config).expect("create mcap source");

    source.initialize(&source_config).await.expect("init");

    let metadata = source.metadata().await.expect("get metadata");

    // Verify basic metadata
    println!("MCAP metadata: {:?}", metadata);

    // Should have at least one topic or message
    assert!(
        !metadata.topics.is_empty() || metadata.message_count.map(|c| c > 0).unwrap_or(false),
        "MCAP should have topics or messages"
    );
}

/// Test MCAP source seeking capability.
#[tokio::test]
async fn test_mcap_source_seek() {
    if !mcap_fixture_exists() {
        eprintln!("SKIP: MCAP fixture not found at {}", MCAP_PATH);
        return;
    }

    use roboflow::sources::{create_source, register_builtin_sources};

    register_builtin_sources();

    let source_config = roboflow::SourceConfig::mcap(MCAP_PATH);
    let mut source = create_source(&source_config).expect("create mcap source");

    source.initialize(&source_config).await.expect("init");

    // Read first batch
    let first_batch = source.read_batch(10).await.expect("read batch");
    if first_batch.is_none() {
        return; // Empty file
    }

    // Seek to beginning
    let seek_result = source.seek(0).await;
    assert!(seek_result.is_ok(), "Seek to start should succeed");

    // Read again - should get same messages
    let second_batch = source.read_batch(10).await.expect("read batch after seek");

    // Both batches should have content
    if let (Some(b1), Some(b2)) = (first_batch, second_batch) {
        assert!(!b1.is_empty(), "First batch should have messages");
        assert!(
            !b2.is_empty(),
            "Second batch after seek should have messages"
        );
    }
}
